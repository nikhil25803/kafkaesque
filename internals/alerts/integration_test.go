package alerts

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgreSQLResolutionStateCleanup(t *testing.T) {
	databaseURL := os.Getenv("KAFKAESQUE_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("KAFKAESQUE_TEST_DATABASE_URL is not set")
	}
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(config.ConnConfig.Database, "_test") {
		t.Fatal("alert integration database name must end with _test")
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	cipher, err := newWebhookCipher("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
	if err != nil {
		t.Fatal(err)
	}
	ciphertext, nonce, err := cipher.encrypt("https://hooks.slack.com/triggers/T/1/secret")
	if err != nil {
		t.Fatal(err)
	}
	var id string
	err = pool.QueryRow(context.Background(), `
		INSERT INTO alert_subscriptions (
			consumer_group, topic, metric, lag_threshold, notify_on_resolve,
			webhook_ciphertext, webhook_nonce
		) VALUES ('orders-service', 'orders', 'partition', 10, true, $1, $2)
		RETURNING id::text`, ciphertext, nonce).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM alert_subscriptions WHERE id::text=$1`, id)
	}()

	service := &Service{pool: pool, cipher: cipher}
	firedAt := time.Date(2026, time.October, 30, 12, 0, 0, 0, time.UTC)
	if err := service.updateEvaluation(context.Background(), evaluationUpdate{
		ID: id, State: StateFiring, Value: 20, EvaluatedAt: firedAt, Notified: true,
	}); err != nil {
		t.Fatal(err)
	}
	deliveries, err := service.listForDelivery(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	foundFiring := false
	for _, delivery := range deliveries {
		if delivery.ID == id {
			foundFiring = delivery.State == StateFiring && delivery.NotifyOnResolve && !delivery.WebhookError
		}
	}
	if !foundFiring {
		t.Fatalf("persisted firing subscription %s was not reloaded", id)
	}

	resolvedAt := firedAt.Add(5 * time.Second)
	if err := service.updateEvaluation(context.Background(), evaluationUpdate{
		ID: id, State: StateHealthy, Value: 0, EvaluatedAt: resolvedAt, Notified: true,
	}); err != nil {
		t.Fatal(err)
	}
	var state string
	var count int
	var notifiedAt time.Time
	if err := pool.QueryRow(context.Background(), `
		SELECT state, last_notified_at, count(*) OVER ()
		FROM alert_subscriptions WHERE id::text=$1`, id).Scan(&state, &notifiedAt, &count); err != nil {
		t.Fatal(err)
	}
	if state != StateHealthy || !notifiedAt.Equal(resolvedAt) || count != 1 {
		t.Fatalf("state=%s notifiedAt=%s count=%d", state, notifiedAt, count)
	}
}
