package topics

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol/listoffsets"
	metadataAPI "github.com/segmentio/kafka-go/protocol/metadata"
)

type messageRateTransport struct {
	mu                  sync.Mutex
	metadata            *metadataAPI.Response
	offsets             map[topicPartition]int64
	offsetError         error
	listOffsetsRequests int
}

func (t *messageRateTransport) RoundTrip(_ context.Context, _ net.Addr, request kafka.Request) (kafka.Response, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	switch request := request.(type) {
	case *metadataAPI.Request:
		return t.metadata, nil
	case *listoffsets.Request:
		if t.offsetError != nil {
			return nil, t.offsetError
		}
		t.listOffsetsRequests++
		response := &listoffsets.Response{}
		for _, topic := range request.Topics {
			result := listoffsets.ResponseTopic{Topic: topic.Topic}
			for _, partition := range topic.Partitions {
				result.Partitions = append(result.Partitions, listoffsets.ResponsePartition{
					Partition: partition.Partition,
					Offset:    t.offsets[topicPartition{topic: topic.Topic, partition: int(partition.Partition)}],
				})
			}
			response.Topics = append(response.Topics, result)
		}
		return response, nil
	default:
		return nil, errors.New("unexpected Kafka request")
	}
}

func messageRateMetadata(orderPartitions int, includeInternal bool) *metadataAPI.Response {
	partitions := make([]metadataAPI.ResponsePartition, orderPartitions)
	for i := range partitions {
		partitions[i] = metadataAPI.ResponsePartition{PartitionIndex: int32(i), LeaderID: 1}
	}
	topics := []metadataAPI.ResponseTopic{{Name: "orders", Partitions: partitions}}
	if includeInternal {
		topics = append(topics, metadataAPI.ResponseTopic{
			Name: "__internal", IsInternal: true,
			Partitions: []metadataAPI.ResponsePartition{{PartitionIndex: 0, LeaderID: 2}},
		})
	}
	return &metadataAPI.Response{
		Brokers: []metadataAPI.ResponseBroker{
			{NodeID: 1, Host: "broker-1", Port: 9092},
			{NodeID: 2, Host: "broker-2", Port: 9092},
		},
		Topics: topics,
	}
}

func newTestMessageRateCollector(transport *messageRateTransport) *MessageRateCollector {
	conn := &kafkaesque.KafkaesqueConn{Client: &kafka.Client{
		Addr: kafka.TCP("bootstrap:9092"), Transport: transport, Timeout: time.Second,
	}}
	return NewMessageRateCollector(conn, slog.New(slog.NewJSONHandler(io.Discard, nil)))
}

func TestMessageRateCollectorPublishesStagedCompletedRates(t *testing.T) {
	transport := &messageRateTransport{
		metadata: messageRateMetadata(2, true),
		offsets: map[topicPartition]int64{
			{topic: "orders", partition: 0}:     100,
			{topic: "orders", partition: 1}:     200,
			{topic: "__internal", partition: 0}: 50,
		},
	}
	collector := newTestMessageRateCollector(transport)
	started := time.Date(2026, time.October, 30, 10, 0, 0, 0, time.UTC)
	collector.collect(context.Background(), started)
	if len(collector.Snapshot()) != 0 {
		t.Fatal("first snapshot must only establish a baseline")
	}

	transport.offsets[topicPartition{topic: "orders", partition: 0}] = 102
	transport.offsets[topicPartition{topic: "orders", partition: 1}] = 203
	collector.collect(context.Background(), started.Add(5*time.Second))
	rates := collector.Snapshot()
	if got := rates["orders"]; rateValue(got.PerSecond) != 1 || got.PerMinute != nil || got.PerHour != nil {
		t.Fatalf("five-second rates = %+v", got)
	}
	if got := rates["__internal"]; rateValue(got.PerSecond) != 0 || got.PerMinute != nil || got.PerHour != nil {
		t.Fatalf("five-second zero-traffic rates = %+v", got)
	}

	transport.offsets[topicPartition{topic: "orders", partition: 0}] = 125
	transport.offsets[topicPartition{topic: "orders", partition: 1}] = 235
	collector.collect(context.Background(), started.Add(time.Minute))
	if got := collector.Snapshot()["orders"]; rateValue(got.PerSecond) != 1 || rateValue(got.PerMinute) != 60 || got.PerHour != nil {
		t.Fatalf("minute rates = %+v", got)
	}

	transport.offsets[topicPartition{topic: "orders", partition: 0}] = 1_900
	transport.offsets[topicPartition{topic: "orders", partition: 1}] = 2_000
	collector.collect(context.Background(), started.Add(time.Hour))
	rates = collector.Snapshot()
	if got := rates["orders"]; rateValue(got.PerSecond) != 1 || rateValue(got.PerMinute) != 60 || rateValue(got.PerHour) != 3_600 {
		t.Fatalf("orders rates = %+v", got)
	}
	if got := rates["__internal"]; rateValue(got.PerSecond) != 0 || rateValue(got.PerMinute) != 0 || rateValue(got.PerHour) != 0 {
		t.Fatalf("zero-traffic rates = %+v", got)
	}
	if transport.listOffsetsRequests != 8 {
		t.Fatalf("ListOffsets requests = %d, want 8 (one per broker per collection)", transport.listOffsetsRequests)
	}
}

func TestMessageRateCollectorInvalidatesChangedTopicsAndRemovesDeletedTopics(t *testing.T) {
	transport := &messageRateTransport{
		metadata: messageRateMetadata(1, true),
		offsets: map[topicPartition]int64{
			{topic: "orders", partition: 0}:     100,
			{topic: "__internal", partition: 0}: 50,
		},
	}
	collector := newTestMessageRateCollector(transport)
	started := time.Now()
	collector.collect(context.Background(), started)
	transport.offsets[topicPartition{topic: "orders", partition: 0}] = 200
	collector.collect(context.Background(), started.Add(time.Hour))

	transport.metadata = messageRateMetadata(2, false)
	transport.offsets[topicPartition{topic: "orders", partition: 1}] = 500
	collector.collect(context.Background(), started.Add(2*time.Hour))
	if _, ok := collector.Snapshot()["orders"]; ok {
		t.Fatal("topic with a new partition must collect a fresh complete interval")
	}
	if _, ok := collector.Snapshot()["__internal"]; ok {
		t.Fatal("deleted topic was retained")
	}
	if _, ok := collector.accumulators["__internal"]; ok {
		t.Fatal("deleted topic accumulator was retained")
	}

	transport.offsets[topicPartition{topic: "orders", partition: 0}] = 205
	transport.offsets[topicPartition{topic: "orders", partition: 1}] = 505
	collector.collect(context.Background(), started.Add(2*time.Hour+5*time.Second))
	if got := collector.Snapshot()["orders"]; rateValue(got.PerSecond) != 2 || got.PerMinute != nil || got.PerHour != nil {
		t.Fatalf("rate after partition rebaseline = %+v", got)
	}

	transport.offsets[topicPartition{topic: "orders", partition: 0}] = 10
	collector.collect(context.Background(), started.Add(3*time.Hour))
	if _, ok := collector.Snapshot()["orders"]; ok {
		t.Fatal("topic with a reset offset must collect a fresh complete interval")
	}
	transport.offsets[topicPartition{topic: "orders", partition: 0}] = 15
	transport.offsets[topicPartition{topic: "orders", partition: 1}] = 510
	collector.collect(context.Background(), started.Add(3*time.Hour+5*time.Second))
	if got := collector.Snapshot()["orders"]; rateValue(got.PerSecond) != 2 || got.PerMinute != nil || got.PerHour != nil {
		t.Fatalf("rate after offset rebaseline = %+v", got)
	}
}

func TestMessageRateCollectorRetainsCompletedBucketsWhileBuildingNext(t *testing.T) {
	transport := &messageRateTransport{
		metadata: messageRateMetadata(1, false),
		offsets:  map[topicPartition]int64{{topic: "orders", partition: 0}: 0},
	}
	collector := newTestMessageRateCollector(transport)
	started := time.Now()
	collector.collect(context.Background(), started)

	transport.offsets[topicPartition{topic: "orders", partition: 0}] = 60
	collector.collect(context.Background(), started.Add(time.Minute))
	if got := collector.Snapshot()["orders"]; rateValue(got.PerMinute) != 60 {
		t.Fatalf("first completed minute = %+v", got)
	}

	transport.offsets[topicPartition{topic: "orders", partition: 0}] = 70
	collector.collect(context.Background(), started.Add(time.Minute+5*time.Second))
	if got := collector.Snapshot()["orders"]; rateValue(got.PerSecond) != 2 || rateValue(got.PerMinute) != 60 {
		t.Fatalf("in-progress minute rates = %+v", got)
	}

	transport.offsets[topicPartition{topic: "orders", partition: 0}] = 180
	collector.collect(context.Background(), started.Add(2*time.Minute))
	if got := collector.Snapshot()["orders"]; rateValue(got.PerMinute) != 120 || got.PerHour != nil {
		t.Fatalf("second completed minute = %+v", got)
	}
	if len(collector.accumulators) != 1 || len(collector.previous.offsets) != 1 {
		t.Fatalf("collector retained unexpected state: accumulators=%d offsets=%d", len(collector.accumulators), len(collector.previous.offsets))
	}
}

func TestMessageRateCollectorRetainsLastSuccessAfterFailure(t *testing.T) {
	transport := &messageRateTransport{
		metadata: messageRateMetadata(1, false),
		offsets:  map[topicPartition]int64{{topic: "orders", partition: 0}: 100},
	}
	collector := newTestMessageRateCollector(transport)
	started := time.Now()
	collector.collect(context.Background(), started)
	transport.offsets[topicPartition{topic: "orders", partition: 0}] = 130
	collector.collect(context.Background(), started.Add(30*time.Second))
	want := collector.Snapshot()["orders"]

	transport.offsetError = errors.New("broker unavailable")
	collector.collect(context.Background(), started.Add(45*time.Second))
	if got := collector.Snapshot()["orders"]; !reflect.DeepEqual(got, want) {
		t.Fatalf("rate after failure = %+v, want %+v", got, want)
	}
	attempt, success := collector.collectionTimes()
	if attempt != started.Add(45*time.Second) || success != started.Add(30*time.Second) {
		t.Fatalf("attempt=%s success=%s", attempt, success)
	}

	transport.offsetError = nil
	transport.offsets[topicPartition{topic: "orders", partition: 0}] = 160
	collector.collect(context.Background(), started.Add(time.Minute))
	if got := collector.Snapshot()["orders"]; rateValue(got.PerSecond) != 1 || rateValue(got.PerMinute) != 60 {
		t.Fatalf("rates after recovery = %+v", got)
	}
}

func TestMessageRateCollectorRunStopsWithContext(t *testing.T) {
	transport := &messageRateTransport{
		metadata: messageRateMetadata(1, false),
		offsets:  map[topicPartition]int64{{topic: "orders", partition: 0}: 100},
	}
	collector := newTestMessageRateCollector(transport)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		collector.run(ctx, make(chan time.Time))
		close(done)
	}()

	for {
		_, success := collector.collectionTimes()
		if !success.IsZero() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("collector did not stop after cancellation")
	}
}

func TestMessageRateCollectorSupportsConcurrentSnapshots(t *testing.T) {
	collector := &MessageRateCollector{rates: map[string]MessageRates{"orders": {PerHour: ratePointer(1)}}}
	var wait sync.WaitGroup
	for range 20 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if rateValue(collector.Snapshot()["orders"].PerHour) != 1 {
				t.Error("unexpected concurrent snapshot")
			}
		}()
	}
	wait.Wait()
	snapshot := collector.Snapshot()
	*snapshot["orders"].PerHour = 99
	if rateValue(collector.Snapshot()["orders"].PerHour) != 1 {
		t.Fatal("snapshot mutation changed collector state")
	}
}

func ratePointer(value float64) *float64 { return &value }

func rateValue(value *float64) float64 {
	if value == nil {
		return -1
	}
	return *value
}
