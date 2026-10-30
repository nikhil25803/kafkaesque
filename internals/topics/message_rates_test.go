package topics

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
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

func TestMessageRateCollectorCalculatesCompleteHourlyRates(t *testing.T) {
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

	transport.offsets[topicPartition{topic: "orders", partition: 0}] = 1_900
	transport.offsets[topicPartition{topic: "orders", partition: 1}] = 3_800
	collector.collect(context.Background(), started.Add(90*time.Minute))
	rates := collector.Snapshot()
	if got := rates["orders"]; got.PerSecond != 1 || got.PerMinute != 60 || got.PerHour != 3_600 {
		t.Fatalf("orders rates = %+v", got)
	}
	if got := rates["__internal"]; got != (MessageRates{}) {
		t.Fatalf("zero-traffic rates = %+v", got)
	}
	if transport.listOffsetsRequests != 4 {
		t.Fatalf("ListOffsets requests = %d, want 4 (one per broker per collection)", transport.listOffsetsRequests)
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

	transport.offsets[topicPartition{topic: "orders", partition: 0}] = 10
	collector.collect(context.Background(), started.Add(3*time.Hour))
	if _, ok := collector.Snapshot()["orders"]; ok {
		t.Fatal("topic with a reset offset must collect a fresh complete interval")
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
	transport.offsets[topicPartition{topic: "orders", partition: 0}] = 200
	collector.collect(context.Background(), started.Add(time.Hour))
	want := collector.Snapshot()["orders"]

	transport.offsetError = errors.New("broker unavailable")
	collector.collect(context.Background(), started.Add(2*time.Hour))
	if got := collector.Snapshot()["orders"]; got != want {
		t.Fatalf("rate after failure = %+v, want %+v", got, want)
	}
	attempt, success := collector.collectionTimes()
	if attempt != started.Add(2*time.Hour) || success != started.Add(time.Hour) {
		t.Fatalf("attempt=%s success=%s", attempt, success)
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
	collector := &MessageRateCollector{rates: map[string]MessageRates{"orders": {PerHour: 1}}}
	var wait sync.WaitGroup
	for range 20 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if collector.Snapshot()["orders"].PerHour != 1 {
				t.Error("unexpected concurrent snapshot")
			}
		}()
	}
	wait.Wait()
}
