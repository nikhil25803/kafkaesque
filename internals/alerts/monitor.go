package alerts

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	kafkaesque_consumers "github.com/nikhil25803/kafkaesque/internals/consumers"
	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
)

const monitorInterval = 5 * time.Second

type deliverySubscription struct {
	Subscription
	WebhookURL   string
	WebhookError bool
}

type evaluationUpdate struct {
	ID          string
	State       string
	Value       float64
	EvaluatedAt time.Time
	Notified    bool
}

type alertStore interface {
	listForDelivery(context.Context) ([]deliverySubscription, error)
	updateEvaluation(context.Context, evaluationUpdate) error
}

type Monitor struct {
	store  alertStore
	conn   *kafkaesque.KafkaesqueConn
	sender SlackSender
	logger *slog.Logger
	now    func() time.Time
}

func NewMonitor(store alertStore, conn *kafkaesque.KafkaesqueConn, sender SlackSender, logger *slog.Logger) *Monitor {
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	return &Monitor{store: store, conn: conn, sender: sender, logger: logger, now: time.Now}
}

func (m *Monitor) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(monitorInterval)
		defer ticker.Stop()
		m.run(ctx, ticker.C)
	}()
}

func (m *Monitor) run(ctx context.Context, ticks <-chan time.Time) {
	m.evaluate(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticks:
			m.evaluate(ctx)
		}
	}
}

type lagResult struct {
	partitions []kafkaesque_consumers.ConsumerGroupPartitionLag
	err        error
}

func (m *Monitor) evaluate(ctx context.Context) {
	subscriptions, err := m.store.listForDelivery(ctx)
	if err != nil {
		m.logger.Error("alert_subscription_list_failed", "error_type", fmt.Sprintf("%T", err))
		return
	}
	if len(subscriptions) == 0 {
		return
	}
	metadata, err := m.conn.GetMetadataSnapshot(ctx)
	if err != nil {
		m.logger.Warn("alert_lag_collection_failed", "error_type", fmt.Sprintf("%T", err))
		return
	}

	results := make(map[string]lagResult)
	for _, subscription := range subscriptions {
		if subscription.WebhookError {
			m.logger.Error("alert_webhook_decryption_failed", "alert_id", subscription.ID)
			continue
		}
		key := subscription.ConsumerGroup + "\x00" + subscription.Topic
		result, ok := results[key]
		if !ok {
			details, fetchErr := kafkaesque_consumers.GetConsumerGroupTopicInformation(
				m.conn, ctx, metadata.Metadata, subscription.ConsumerGroup, subscription.Topic,
			)
			if fetchErr != nil {
				result.err = fetchErr
			} else {
				result.partitions = details.Partitions
			}
			results[key] = result
		}
		if result.err != nil || len(result.partitions) == 0 {
			m.logger.Warn("alert_lag_evaluation_failed", "alert_id", subscription.ID, "error_type", fmt.Sprintf("%T", result.err))
			continue
		}
		m.evaluateSubscription(ctx, subscription, result.partitions)
	}
}

func (m *Monitor) evaluateSubscription(
	ctx context.Context,
	subscription deliverySubscription,
	partitions []kafkaesque_consumers.ConsumerGroupPartitionLag,
) {
	now := m.now().UTC()
	value, worst, worstLag, breached := alertValue(subscription, partitions)
	isBreached := value > float64(subscription.LagThreshold)
	state, send, resolved := nextAction(subscription, isBreached)
	update := evaluationUpdate{ID: subscription.ID, State: state, Value: value, EvaluatedAt: now}
	if send {
		err := m.sender.Send(ctx, subscription.WebhookURL, BuildSlackPayload(Notification{
			Resolved: resolved, ConsumerGroup: subscription.ConsumerGroup,
			Topic: subscription.Topic, WorstPartition: worst,
			WorstPartitionLag: worstLag, PartitionCount: len(partitions),
			BreachedPartitions: breached,
		}))
		if err == nil {
			update.Notified = true
			if resolved {
				update.State = StateHealthy
			}
		} else {
			if subscription.State == StateHealthy {
				update.State = StateHealthy
			}
			m.logger.Warn("slack_alert_delivery_failed", "alert_id", subscription.ID, "error_type", fmt.Sprintf("%T", err))
		}
	}
	if err := m.store.updateEvaluation(ctx, update); err != nil {
		m.logger.Error("alert_state_update_failed", "alert_id", subscription.ID, "error_type", fmt.Sprintf("%T", err))
	}
}

func alertValue(subscription deliverySubscription, partitions []kafkaesque_consumers.ConsumerGroupPartitionLag) (float64, int, int64, int) {
	var total int64
	maxLag, worst, breached := int64(-1), 0, 0
	for _, partition := range partitions {
		total += partition.Lag
		if partition.Lag > maxLag {
			maxLag, worst = partition.Lag, partition.Partition
		}
		if partition.Lag > subscription.LagThreshold {
			breached++
		}
	}
	if subscription.Metric == MetricPartition {
		return float64(maxLag), worst, maxLag, breached
	}
	return float64(total) / float64(len(partitions)), worst, maxLag, breached
}

func nextAction(subscription deliverySubscription, breached bool) (string, bool, bool) {
	switch subscription.State {
	case StateHealthy:
		if breached {
			return StateFiring, true, false
		}
	case StateFiring:
		if !breached {
			if subscription.NotifyOnResolve {
				return StateResolving, true, true
			}
			return StateHealthy, false, false
		}
	case StateResolving:
		if breached {
			return StateFiring, false, false
		}
		return StateResolving, true, true
	}
	return subscription.State, false, false
}

func (s *Service) listForDelivery(ctx context.Context) ([]deliverySubscription, error) {
	rows, err := s.pool.Query(ctx, subscriptionColumns+` ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]deliverySubscription, 0)
	for rows.Next() {
		subscription, ciphertext, nonce, scanErr := scanSubscription(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		webhook, decryptErr := s.cipher.decrypt(ciphertext, nonce)
		result = append(result, deliverySubscription{
			Subscription: subscription, WebhookURL: webhook, WebhookError: decryptErr != nil,
		})
	}
	return result, rows.Err()
}

func (s *Service) updateEvaluation(ctx context.Context, update evaluationUpdate) error {
	const query = `
		UPDATE alert_subscriptions SET state=$2, last_value=$3, last_evaluated_at=$4,
			last_notified_at=CASE WHEN $5 THEN $4 ELSE last_notified_at END,
			updated_at=now()
		WHERE id::text=$1`
	_, err := s.pool.Exec(ctx, query, update.ID, update.State, update.Value, update.EvaluatedAt, update.Notified)
	return err
}
