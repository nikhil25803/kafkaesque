package alerts

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	kafkaesque_auth "github.com/nikhil25803/kafkaesque/internals/auth"
	kafkaesque_consumers "github.com/nikhil25803/kafkaesque/internals/consumers"
	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
)

const (
	MetricAverage   = "average"
	MetricPartition = "partition"
	StateHealthy    = "healthy"
	StateFiring     = "firing"
	StateResolving  = "resolving"
)

var (
	ErrForbidden = errors.New("alert permission denied")
	ErrNotFound  = errors.New("alert subscription not found")
)

type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string { return e.Field + " " + e.Message }

type Subscription struct {
	ID                string
	ConsumerGroup     string
	Topic             string
	Metric            string
	LagThreshold      int64
	CooldownMinutes   int
	NotifyOnResolve   bool
	State             string
	LastValue         *float64
	LastEvaluatedAt   *time.Time
	LastAttemptAt     *time.Time
	LastNotifiedAt    *time.Time
	WebhookConfigured bool
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

type Input struct {
	ConsumerGroup   string
	Topic           string
	Metric          string
	LagThreshold    int64
	CooldownMinutes int
	NotifyOnResolve bool
	WebhookURL      string
}

type Manager interface {
	List(context.Context, kafkaesque_auth.User) ([]Subscription, error)
	Get(context.Context, kafkaesque_auth.User, string) (Subscription, error)
	Create(context.Context, kafkaesque_auth.User, Input) (Subscription, error)
	Update(context.Context, kafkaesque_auth.User, string, Input) (Subscription, error)
	Delete(context.Context, kafkaesque_auth.User, string) error
}

type Service struct {
	pool   *pgxpool.Pool
	conn   *kafkaesque.KafkaesqueConn
	cipher *webhookCipher
}

func NewService(pool *pgxpool.Pool, conn *kafkaesque.KafkaesqueConn, encryptionKey string) (*Service, error) {
	cipher, err := newWebhookCipher(encryptionKey)
	if err != nil {
		return nil, err
	}
	return &Service{pool: pool, conn: conn, cipher: cipher}, nil
}

func (s *Service) List(ctx context.Context, actor kafkaesque_auth.User) ([]Subscription, error) {
	if !kafkaesque_auth.HasPermission(actor, kafkaesque_auth.PermissionAlertView) {
		return nil, ErrForbidden
	}
	rows, err := s.pool.Query(ctx, subscriptionColumns+` ORDER BY consumer_group, topic, metric`)
	if err != nil {
		return nil, fmt.Errorf("list alert subscriptions: %w", err)
	}
	defer rows.Close()
	result := make([]Subscription, 0)
	for rows.Next() {
		subscription, _, _, err := scanSubscription(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, subscription)
	}
	return result, rows.Err()
}

func (s *Service) Get(ctx context.Context, actor kafkaesque_auth.User, id string) (Subscription, error) {
	if !kafkaesque_auth.HasPermission(actor, kafkaesque_auth.PermissionAlertView) {
		return Subscription{}, ErrForbidden
	}
	subscription, _, _, err := scanSubscription(s.pool.QueryRow(ctx, subscriptionColumns+` WHERE id::text=$1`, id))
	return subscription, classifyStoreError(err)
}

func (s *Service) Create(ctx context.Context, actor kafkaesque_auth.User, input Input) (Subscription, error) {
	if !hasMutationPermission(actor, kafkaesque_auth.PermissionAlertCreate) {
		return Subscription{}, ErrForbidden
	}
	input, err := validateInput(input, true)
	if err != nil {
		return Subscription{}, err
	}
	if err := s.validateTarget(ctx, input.ConsumerGroup, input.Topic); err != nil {
		return Subscription{}, err
	}
	ciphertext, nonce, err := s.cipher.encrypt(input.WebhookURL)
	if err != nil {
		return Subscription{}, fmt.Errorf("encrypt Slack webhook: %w", err)
	}
	const query = `
		INSERT INTO alert_subscriptions (
			consumer_group, topic, metric, lag_threshold, cooldown_minutes,
			notify_on_resolve, webhook_ciphertext, webhook_nonce, created_by
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		RETURNING id::text`
	var id string
	err = s.pool.QueryRow(ctx, query, input.ConsumerGroup, input.Topic, input.Metric, input.LagThreshold,
		input.CooldownMinutes, input.NotifyOnResolve, ciphertext, nonce, actor.ID).Scan(&id)
	if err != nil {
		return Subscription{}, classifyStoreError(err)
	}
	return s.Get(ctx, actor, id)
}

func (s *Service) Update(ctx context.Context, actor kafkaesque_auth.User, id string, input Input) (Subscription, error) {
	if !hasMutationPermission(actor, kafkaesque_auth.PermissionAlertEdit) {
		return Subscription{}, ErrForbidden
	}
	input, err := validateInput(input, false)
	if err != nil {
		return Subscription{}, err
	}
	current, ciphertext, nonce, err := scanSubscription(s.pool.QueryRow(ctx, subscriptionColumns+` WHERE id::text=$1`, id))
	if err != nil {
		return Subscription{}, classifyStoreError(err)
	}
	conditionChanged := alertConditionChanged(current, input)
	if conditionChanged {
		if err := s.validateTarget(ctx, input.ConsumerGroup, input.Topic); err != nil {
			return Subscription{}, err
		}
	}
	if input.WebhookURL != "" {
		ciphertext, nonce, err = s.cipher.encrypt(input.WebhookURL)
		if err != nil {
			return Subscription{}, fmt.Errorf("encrypt Slack webhook: %w", err)
		}
	}
	const query = `
		UPDATE alert_subscriptions SET
			consumer_group=$2, topic=$3, metric=$4, lag_threshold=$5, cooldown_minutes=$6,
			notify_on_resolve=$7, webhook_ciphertext=$8, webhook_nonce=$9,
			state=CASE WHEN $10 THEN 'healthy' ELSE state END,
			last_value=CASE WHEN $10 THEN NULL ELSE last_value END,
			last_evaluated_at=CASE WHEN $10 THEN NULL ELSE last_evaluated_at END,
			last_attempt_at=CASE WHEN $10 THEN NULL ELSE last_attempt_at END,
			last_notified_at=CASE WHEN $10 THEN NULL ELSE last_notified_at END,
			updated_at=now()
		WHERE id::text=$1`
	result, err := s.pool.Exec(ctx, query, id, input.ConsumerGroup, input.Topic, input.Metric, input.LagThreshold,
		input.CooldownMinutes, input.NotifyOnResolve, ciphertext, nonce, conditionChanged)
	if err != nil {
		return Subscription{}, classifyStoreError(err)
	}
	if result.RowsAffected() == 0 {
		return Subscription{}, ErrNotFound
	}
	return s.Get(ctx, actor, id)
}

func alertConditionChanged(current Subscription, input Input) bool {
	return current.ConsumerGroup != input.ConsumerGroup || current.Topic != input.Topic ||
		current.Metric != input.Metric || current.LagThreshold != input.LagThreshold
}

func (s *Service) Delete(ctx context.Context, actor kafkaesque_auth.User, id string) error {
	if !hasMutationPermission(actor, kafkaesque_auth.PermissionAlertDelete) {
		return ErrForbidden
	}
	result, err := s.pool.Exec(ctx, `DELETE FROM alert_subscriptions WHERE id::text=$1`, id)
	if err != nil {
		return fmt.Errorf("delete alert subscription: %w", err)
	}
	if result.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func hasMutationPermission(actor kafkaesque_auth.User, permission string) bool {
	return kafkaesque_auth.HasPermission(actor, kafkaesque_auth.PermissionAlertView) &&
		kafkaesque_auth.HasPermission(actor, permission)
}

func (s *Service) validateTarget(ctx context.Context, group, topic string) error {
	metadata, err := s.conn.GetMetadataSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("validate alert target: %w", err)
	}
	if _, err := kafkaesque_consumers.GetConsumerGroupTopicInformation(s.conn, ctx, metadata.Metadata, group, topic); err != nil {
		return &ValidationError{Field: "topic", Message: "must have committed offsets for the selected consumer group"}
	}
	return nil
}

func validateInput(input Input, requireWebhook bool) (Input, error) {
	input.ConsumerGroup = strings.TrimSpace(input.ConsumerGroup)
	input.Topic = strings.TrimSpace(input.Topic)
	input.Metric = strings.ToLower(strings.TrimSpace(input.Metric))
	input.WebhookURL = strings.TrimSpace(input.WebhookURL)
	if input.ConsumerGroup == "" {
		return Input{}, &ValidationError{Field: "consumer_group", Message: "is required"}
	}
	if input.Topic == "" {
		return Input{}, &ValidationError{Field: "topic", Message: "is required"}
	}
	if input.Metric != MetricAverage && input.Metric != MetricPartition {
		return Input{}, &ValidationError{Field: "metric", Message: "must be average or partition"}
	}
	if input.LagThreshold < 0 {
		return Input{}, &ValidationError{Field: "lag_threshold", Message: "cannot be negative"}
	}
	if input.CooldownMinutes < 1 || input.CooldownMinutes > 1440 {
		return Input{}, &ValidationError{Field: "cooldown_minutes", Message: "must be between 1 and 1440"}
	}
	if requireWebhook && input.WebhookURL == "" {
		return Input{}, &ValidationError{Field: "webhook_url", Message: "is required"}
	}
	if input.WebhookURL != "" {
		if err := validateSlackWebhookURL(input.WebhookURL); err != nil {
			return Input{}, &ValidationError{Field: "webhook_url", Message: err.Error()}
		}
	}
	return input, nil
}

func validateSlackWebhookURL(value string) error {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "https" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("must be an HTTPS Slack Incoming Webhook URL")
	}
	host := strings.ToLower(parsed.Hostname())
	if (host != "hooks.slack.com" && host != "hooks.slack-gov.com") ||
		(parsed.Port() != "" && parsed.Port() != "443") || !strings.HasPrefix(parsed.EscapedPath(), "/services/") {
		return fmt.Errorf("must use hooks.slack.com or hooks.slack-gov.com")
	}
	return nil
}

const subscriptionColumns = `SELECT
	id::text, consumer_group, topic, metric, lag_threshold, cooldown_minutes,
	notify_on_resolve, state, last_value, last_evaluated_at, last_attempt_at,
	last_notified_at, created_at, updated_at, webhook_ciphertext, webhook_nonce
	FROM alert_subscriptions`

type rowScanner interface{ Scan(...any) error }

func scanSubscription(row rowScanner) (Subscription, []byte, []byte, error) {
	var subscription Subscription
	var ciphertext, nonce []byte
	err := row.Scan(
		&subscription.ID, &subscription.ConsumerGroup, &subscription.Topic, &subscription.Metric,
		&subscription.LagThreshold, &subscription.CooldownMinutes, &subscription.NotifyOnResolve,
		&subscription.State, &subscription.LastValue, &subscription.LastEvaluatedAt,
		&subscription.LastAttemptAt, &subscription.LastNotifiedAt, &subscription.CreatedAt,
		&subscription.UpdatedAt, &ciphertext, &nonce,
	)
	subscription.WebhookConfigured = len(ciphertext) > 0
	return subscription, ciphertext, nonce, err
}

func classifyStoreError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	return err
}
