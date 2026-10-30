package alerts

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	kafkaesque_consumers "github.com/nikhil25803/kafkaesque/internals/consumers"
	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol/describegroups"
	"github.com/segmentio/kafka-go/protocol/findcoordinator"
	"github.com/segmentio/kafka-go/protocol/listoffsets"
	"github.com/segmentio/kafka-go/protocol/metadata"
	"github.com/segmentio/kafka-go/protocol/offsetfetch"
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
	monitor.evaluateSubscription(context.Background(), store.sub, breached)
	if len(sender.payloads) != 1 || !store.updates[0].Notified || store.updates[0].State != StateFiring {
		t.Fatalf("first breach payloads=%d update=%+v", len(sender.payloads), store.updates[0])
	}

	monitor.now = func() time.Time { return started.Add(time.Minute) }
	monitor.evaluateSubscription(context.Background(), store.sub, breached)
	if len(sender.payloads) != 1 {
		t.Fatal("a firing alert was repeated")
	}

	monitor.now = func() time.Time { return started.Add(2 * time.Minute) }
	resolved := []kafkaesque_consumers.ConsumerGroupPartitionLag{{Partition: 0, Lag: 10}, {Partition: 1, Lag: 20}}
	monitor.evaluateSubscription(context.Background(), store.sub, resolved)
	last := store.updates[len(store.updates)-1]
	if len(sender.payloads) != 2 || last.State != StateHealthy || !last.Notified {
		t.Fatalf("resolution payloads=%d update=%+v", len(sender.payloads), last)
	}

	monitor.now = func() time.Time { return started.Add(3 * time.Minute) }
	monitor.evaluateSubscription(context.Background(), store.sub, breached)
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
	monitor.evaluateSubscription(context.Background(), store.sub, partitions)
	if store.sub.State != StateHealthy || len(sender.payloads) != 1 {
		t.Fatalf("state=%s payloads=%d", store.sub.State, len(sender.payloads))
	}
	monitor.now = func() time.Time { return now.Add(time.Minute) }
	monitor.evaluateSubscription(context.Background(), store.sub, partitions)
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
	monitor.evaluateSubscription(context.Background(), store.sub, partitions)
	monitor.now = func() time.Time { return now.Add(time.Minute) }
	monitor.evaluateSubscription(context.Background(), store.sub, partitions)
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

func TestAlertMetricsUseCompactPartitionLagLabel(t *testing.T) {
	partitions := []kafkaesque_consumers.ConsumerGroupPartitionLag{{Partition: 0, Lag: 200}, {Partition: 1, Lag: 300}}
	for _, metric := range []string{MetricAverage, MetricPartition} {
		store := &fakeAlertStore{sub: deliverySubscription{
			Subscription: Subscription{ID: metric, Metric: metric, LagThreshold: 100, State: StateHealthy},
			WebhookURL:   "https://hooks.slack.com/triggers/T/1/secret",
		}}
		sender := &fakeSlackSender{}
		NewMonitor(store, nil, sender, nil).evaluateSubscription(context.Background(), store.sub, partitions)
		if len(sender.payloads) != 1 || sender.payloads[0].Blocks[2].Fields[4].Text != "*Metric:* Partition Lag" {
			t.Fatalf("metric=%s payloads=%+v", metric, sender.payloads)
		}
	}
}

func TestMonitorInterval(t *testing.T) {
	if monitorInterval != 5*time.Second {
		t.Fatalf("monitor interval=%s", monitorInterval)
	}
}

type monitorTransport struct {
	mu       sync.Mutex
	requests map[string]int
}

func (t *monitorTransport) RoundTrip(_ context.Context, _ net.Addr, request kafka.Request) (kafka.Response, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	switch request := request.(type) {
	case *metadata.Request:
		t.requests["metadata"]++
		return &metadata.Response{
			Brokers: []metadata.ResponseBroker{{NodeID: 1, Host: "broker", Port: 9092}},
			Topics:  []metadata.ResponseTopic{{Name: "orders", Partitions: []metadata.ResponsePartition{{PartitionIndex: 0, LeaderID: 1}}}},
		}, nil
	case *findcoordinator.Request:
		t.requests["coordinator"]++
		return &findcoordinator.Response{NodeID: 2, Host: "coordinator", Port: 9093}, nil
	case *describegroups.Request:
		t.requests["describe"]++
		return &describegroups.Response{Groups: []describegroups.ResponseGroup{{GroupID: request.Groups[0], GroupState: "Stable"}}}, nil
	case *offsetfetch.Request:
		t.requests["offsets"]++
		return &offsetfetch.Response{Topics: []offsetfetch.ResponseTopic{{Name: "orders", Partitions: []offsetfetch.ResponsePartition{{PartitionIndex: 0, CommittedOffset: 10}}}}}, nil
	case *listoffsets.Request:
		t.requests["latest"]++
		return &listoffsets.Response{Topics: []listoffsets.ResponseTopic{{Topic: "orders", Partitions: []listoffsets.ResponsePartition{{Partition: 0, Timestamp: kafka.LastOffset, Offset: 20}}}}}, nil
	default:
		return nil, errors.New("unexpected Kafka request")
	}
}

type alertListStore struct {
	subscriptions []deliverySubscription
	updates       []evaluationUpdate
}

func (s *alertListStore) listForDelivery(context.Context) ([]deliverySubscription, error) {
	return s.subscriptions, nil
}

func (s *alertListStore) updateEvaluation(_ context.Context, update evaluationUpdate) error {
	s.updates = append(s.updates, update)
	return nil
}

func TestMonitorFetchesUniqueTargetOncePerCycle(t *testing.T) {
	transport := &monitorTransport{requests: make(map[string]int)}
	conn := &kafkaesque.KafkaesqueConn{Client: &kafka.Client{Addr: kafka.TCP("bootstrap:9092"), Transport: transport}}
	store := &alertListStore{subscriptions: []deliverySubscription{
		{Subscription: Subscription{ID: "average", ConsumerGroup: "orders-service", Topic: "orders", Metric: MetricAverage, LagThreshold: 5, State: StateHealthy}},
		{Subscription: Subscription{ID: "partition", ConsumerGroup: "orders-service", Topic: "orders", Metric: MetricPartition, LagThreshold: 5, State: StateHealthy}},
	}}
	NewMonitor(store, conn, &fakeSlackSender{}, nil).evaluate(context.Background())
	if len(store.updates) != 2 {
		t.Fatalf("updates=%d", len(store.updates))
	}
	for _, request := range []string{"metadata", "coordinator", "describe", "offsets", "latest"} {
		if transport.requests[request] != 1 {
			t.Fatalf("%s requests=%d", request, transport.requests[request])
		}
	}
}

func TestMonitorRunStopsWithContext(t *testing.T) {
	calls := make(chan struct{}, 2)
	store := &signalingAlertStore{calls: calls}
	monitor := NewMonitor(store, nil, &fakeSlackSender{}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	ticks := make(chan time.Time, 1)
	done := make(chan struct{})
	go func() {
		monitor.run(ctx, ticks)
		close(done)
	}()
	select {
	case <-calls:
	case <-time.After(time.Second):
		t.Fatal("monitor did not evaluate immediately")
	}
	ticks <- time.Now()
	select {
	case <-calls:
	case <-time.After(time.Second):
		t.Fatal("monitor did not evaluate on tick")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("monitor did not stop")
	}
}

type signalingAlertStore struct {
	calls chan struct{}
}

func (s *signalingAlertStore) listForDelivery(context.Context) ([]deliverySubscription, error) {
	s.calls <- struct{}{}
	return nil, nil
}

func (*signalingAlertStore) updateEvaluation(context.Context, evaluationUpdate) error { return nil }
