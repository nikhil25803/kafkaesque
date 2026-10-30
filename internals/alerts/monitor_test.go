package alerts

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	kafkaesque_consumers "github.com/nikhil25803/kafkaesque/internals/consumers"
)

type fakeAlertStore struct {
	mu      sync.Mutex
	sub     deliverySubscription
	updates []evaluationUpdate
}

func (s *fakeAlertStore) listForDelivery(context.Context) ([]deliverySubscription, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sub.ID == "" {
		return nil, nil
	}
	return []deliverySubscription{s.sub}, nil
}

func (s *fakeAlertStore) updateEvaluation(_ context.Context, update evaluationUpdate) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updates = append(s.updates, update)
	s.sub.State = update.State
	s.sub.LastValue = &update.Value
	s.sub.LastEvaluatedAt = &update.EvaluatedAt
	if update.Attempted {
		s.sub.LastAttemptAt = &update.EvaluatedAt
	}
	if update.Notified {
		s.sub.LastNotifiedAt = &update.EvaluatedAt
	}
	return nil
}

type fakeSlackSender struct {
	payloads []SlackPayload
	err      error
}

func (s *fakeSlackSender) Send(_ context.Context, _ string, payload SlackPayload) error {
	s.payloads = append(s.payloads, payload)
	return s.err
}

func TestMonitorFiringCooldownAndResolution(t *testing.T) {
	started := time.Date(2026, time.October, 30, 10, 0, 0, 0, time.UTC)
	store := &fakeAlertStore{sub: deliverySubscription{
		Subscription: Subscription{
			ID: "alert-1", ConsumerGroup: "orders-service", Topic: "orders", Metric: MetricAverage,
			LagThreshold: 100, CooldownMinutes: 5, NotifyOnResolve: true, State: StateHealthy,
		},
		WebhookURL: "https://hooks.slack.com/services/T/B/secret",
	}}
	sender := &fakeSlackSender{}
	monitor := NewMonitor(store, nil, sender, nil)
	monitor.now = func() time.Time { return started }
	breached := []kafkaesque_consumers.ConsumerGroupPartitionLag{{Partition: 0, Lag: 50}, {Partition: 1, Lag: 250}}
	monitor.evaluateSubscription(context.Background(), store.sub, breached)
	if len(sender.payloads) != 1 || !store.updates[0].Attempted || store.updates[0].State != StateFiring {
		t.Fatalf("first breach payloads=%d update=%+v", len(sender.payloads), store.updates[0])
	}

	monitor.now = func() time.Time { return started.Add(4 * time.Minute) }
	monitor.evaluateSubscription(context.Background(), store.sub, breached)
	if len(sender.payloads) != 1 {
		t.Fatal("reminder was sent before cooldown")
	}

	monitor.now = func() time.Time { return started.Add(5 * time.Minute) }
	monitor.evaluateSubscription(context.Background(), store.sub, breached)
	if len(sender.payloads) != 2 {
		t.Fatal("reminder was not sent after cooldown")
	}

	monitor.now = func() time.Time { return started.Add(6 * time.Minute) }
	resolved := []kafkaesque_consumers.ConsumerGroupPartitionLag{{Partition: 0, Lag: 10}, {Partition: 1, Lag: 20}}
	monitor.evaluateSubscription(context.Background(), store.sub, resolved)
	last := store.updates[len(store.updates)-1]
	if len(sender.payloads) != 3 || last.State != StateHealthy || !last.Notified {
		t.Fatalf("resolution payloads=%d update=%+v", len(sender.payloads), last)
	}
}

func TestMonitorRetriesFailedResolutionAfterCooldown(t *testing.T) {
	now := time.Now().UTC()
	lastNotified := now.Add(-time.Minute)
	store := &fakeAlertStore{sub: deliverySubscription{
		Subscription: Subscription{
			ID: "alert-1", Metric: MetricAverage, LagThreshold: 100, CooldownMinutes: 5,
			NotifyOnResolve: true, State: StateFiring, LastNotifiedAt: &lastNotified,
		},
		WebhookURL: "https://hooks.slack.com/services/T/B/secret",
	}}
	sender := &fakeSlackSender{err: errors.New("Slack unavailable")}
	monitor := NewMonitor(store, nil, sender, nil)
	monitor.now = func() time.Time { return now }
	partitions := []kafkaesque_consumers.ConsumerGroupPartitionLag{{Lag: 0}}
	monitor.evaluateSubscription(context.Background(), store.sub, partitions)
	if store.sub.State != StateResolving || len(sender.payloads) != 1 {
		t.Fatalf("state=%s payloads=%d", store.sub.State, len(sender.payloads))
	}
	monitor.now = func() time.Time { return now.Add(4 * time.Minute) }
	monitor.evaluateSubscription(context.Background(), store.sub, partitions)
	if len(sender.payloads) != 1 {
		t.Fatal("resolution retried before cooldown")
	}
	monitor.now = func() time.Time { return now.Add(5 * time.Minute) }
	monitor.evaluateSubscription(context.Background(), store.sub, partitions)
	if len(sender.payloads) != 2 {
		t.Fatal("resolution was not retried after cooldown")
	}
}

func TestMonitorDoesNotResolveUndeliveredFiringAlert(t *testing.T) {
	now := time.Now().UTC()
	subscription := deliverySubscription{Subscription: Subscription{
		State: StateFiring, CooldownMinutes: 5, NotifyOnResolve: true,
	}}
	state, send, resolved, _ := nextAction(subscription, false, now)
	if state != StateHealthy || send || resolved {
		t.Fatalf("state=%s send=%t resolved=%t", state, send, resolved)
	}
}

func TestPartitionAlertValue(t *testing.T) {
	subscription := deliverySubscription{Subscription: Subscription{Metric: MetricPartition, LagThreshold: 100}}
	value, worst, breached := alertValue(subscription, []kafkaesque_consumers.ConsumerGroupPartitionLag{
		{Partition: 0, Lag: 101}, {Partition: 1, Lag: 500}, {Partition: 2, Lag: 50},
	})
	if value != 500 || worst != 1 || breached != 2 {
		t.Fatalf("value=%v worst=%d breached=%d", value, worst, breached)
	}
}

func TestMonitorRunStopsWithContext(t *testing.T) {
	store := &fakeAlertStore{}
	monitor := NewMonitor(store, nil, &fakeSlackSender{}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		monitor.run(ctx, make(chan time.Time))
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("monitor did not stop")
	}
}
