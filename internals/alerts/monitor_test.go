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

func TestMonitorFiresOnceResolvesAndRearms(t *testing.T) {
	started := time.Date(2026, time.October, 30, 10, 0, 0, 0, time.UTC)
	store := &fakeAlertStore{sub: deliverySubscription{
		Subscription: Subscription{
			ID: "alert-1", ConsumerGroup: "orders-service", Topic: "orders", Metric: MetricAverage,
			LagThreshold: 100, NotifyOnResolve: true, State: StateHealthy,
		},
		WebhookURL: "https://hooks.slack.com/triggers/T/1/secret",
	}}
	sender := &fakeSlackSender{}
	monitor := NewMonitor(store, nil, sender, nil)
	monitor.now = func() time.Time { return started }
	breached := []kafkaesque_consumers.ConsumerGroupPartitionLag{{Partition: 0, Lag: 50}, {Partition: 1, Lag: 250}}
	monitor.evaluateSubscription(context.Background(), store.sub, breached, nil, 0)
	if len(sender.payloads) != 1 || !store.updates[0].Notified || store.updates[0].State != StateFiring {
		t.Fatalf("first breach payloads=%d update=%+v", len(sender.payloads), store.updates[0])
	}

	monitor.now = func() time.Time { return started.Add(time.Minute) }
	monitor.evaluateSubscription(context.Background(), store.sub, breached, nil, 0)
	if len(sender.payloads) != 1 {
		t.Fatal("a firing alert was repeated")
	}

	monitor.now = func() time.Time { return started.Add(2 * time.Minute) }
	resolved := []kafkaesque_consumers.ConsumerGroupPartitionLag{{Partition: 0, Lag: 10}, {Partition: 1, Lag: 20}}
	monitor.evaluateSubscription(context.Background(), store.sub, resolved, nil, 0)
	last := store.updates[len(store.updates)-1]
	if len(sender.payloads) != 2 || last.State != StateHealthy || !last.Notified {
		t.Fatalf("resolution payloads=%d update=%+v", len(sender.payloads), last)
	}

	monitor.now = func() time.Time { return started.Add(3 * time.Minute) }
	monitor.evaluateSubscription(context.Background(), store.sub, breached, nil, 0)
	if len(sender.payloads) != 3 || store.sub.State != StateFiring {
		t.Fatalf("rearmed payloads=%d state=%s", len(sender.payloads), store.sub.State)
	}
}

func TestMonitorRetriesFailedFiringNextEvaluation(t *testing.T) {
	now := time.Now().UTC()
	store := &fakeAlertStore{sub: deliverySubscription{
		Subscription: Subscription{
			ID: "alert-1", Metric: MetricAverage, LagThreshold: 100, State: StateHealthy,
		},
		WebhookURL: "https://hooks.slack.com/triggers/T/1/secret",
	}}
	sender := &fakeSlackSender{err: errors.New("Slack unavailable")}
	monitor := NewMonitor(store, nil, sender, nil)
	monitor.now = func() time.Time { return now }
	partitions := []kafkaesque_consumers.ConsumerGroupPartitionLag{{Lag: 200}}
	monitor.evaluateSubscription(context.Background(), store.sub, partitions, nil, 0)
	if store.sub.State != StateHealthy || len(sender.payloads) != 1 {
		t.Fatalf("state=%s payloads=%d", store.sub.State, len(sender.payloads))
	}
	monitor.now = func() time.Time { return now.Add(time.Minute) }
	monitor.evaluateSubscription(context.Background(), store.sub, partitions, nil, 0)
	if len(sender.payloads) != 2 {
		t.Fatal("failed firing alert was not retried on the next evaluation")
	}
}

func TestMonitorRetriesFailedResolutionNextEvaluation(t *testing.T) {
	now := time.Now().UTC()
	store := &fakeAlertStore{sub: deliverySubscription{
		Subscription: Subscription{ID: "alert-1", Metric: MetricAverage, LagThreshold: 100, NotifyOnResolve: true, State: StateFiring},
		WebhookURL:   "https://hooks.slack.com/triggers/T/1/secret",
	}}
	sender := &fakeSlackSender{err: errors.New("Slack unavailable")}
	monitor := NewMonitor(store, nil, sender, nil)
	monitor.now = func() time.Time { return now }
	partitions := []kafkaesque_consumers.ConsumerGroupPartitionLag{{Lag: 0}}
	monitor.evaluateSubscription(context.Background(), store.sub, partitions, nil, 0)
	monitor.now = func() time.Time { return now.Add(time.Minute) }
	monitor.evaluateSubscription(context.Background(), store.sub, partitions, nil, 0)
	if store.sub.State != StateResolving || len(sender.payloads) != 2 {
		t.Fatalf("state=%s payloads=%d", store.sub.State, len(sender.payloads))
	}
}

func TestNextActionOptionalResolutionAndPendingRecovery(t *testing.T) {
	withoutResolve := deliverySubscription{Subscription: Subscription{State: StateFiring}}
	state, send, resolved := nextAction(withoutResolve, false)
	if state != StateHealthy || send || resolved {
		t.Fatalf("optional resolution state=%s send=%t resolved=%t", state, send, resolved)
	}

	pendingRecovery := deliverySubscription{Subscription: Subscription{State: StateResolving, NotifyOnResolve: true}}
	state, send, resolved = nextAction(pendingRecovery, true)
	if state != StateFiring || send || resolved {
		t.Fatalf("new breach during recovery state=%s send=%t resolved=%t", state, send, resolved)
	}
}

func TestPartitionAlertValue(t *testing.T) {
	subscription := deliverySubscription{Subscription: Subscription{Metric: MetricPartition, LagThreshold: 100}}
	value, worst, worstLag, breached := alertValue(subscription, []kafkaesque_consumers.ConsumerGroupPartitionLag{
		{Partition: 0, Lag: 101}, {Partition: 1, Lag: 500}, {Partition: 2, Lag: 50},
	})
	if value != 500 || worst != 1 || worstLag != 500 || breached != 2 {
		t.Fatalf("value=%v worst=%d worstLag=%d breached=%d", value, worst, worstLag, breached)
	}
}

func TestMemberForPartition(t *testing.T) {
	members := []kafkaesque_consumers.ConsumerGroupMember{{
		MemberID: "member-1", ClientID: "worker", Host: "10.0.0.12",
		Assignments: []kafkaesque_consumers.ConsumerGroupMemberAssignment{{Topic: "orders", Partitions: []int{1, 2}}},
	}}
	if got := memberForPartition(members, "orders", 2); got.MemberID != "member-1" {
		t.Fatalf("member=%+v", got)
	}
	if got := memberForPartition(members, "orders", 3); got.MemberID != "" {
		t.Fatalf("unexpected member=%+v", got)
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
