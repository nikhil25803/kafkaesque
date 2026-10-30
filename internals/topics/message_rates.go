package topics

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"

	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol/listoffsets"
)

const messageRateInterval = time.Hour

type topicPartition struct {
	topic     string
	partition int
}

type offsetSnapshot struct {
	offsets         map[topicPartition]int64
	partitionCounts map[string]int
}

// MessageRates contains one topic's normalized production rates.
type MessageRates struct {
	PerSecond float64
	PerMinute float64
	PerHour   float64
}

// MessageRateProvider exposes the latest complete topic rates.
type MessageRateProvider interface {
	Snapshot() map[string]MessageRates
}

// MessageRateCollector maintains hourly topic production rates in memory.
type MessageRateCollector struct {
	conn   *kafkaesque.KafkaesqueConn
	logger *slog.Logger

	mu          sync.RWMutex
	previous    offsetSnapshot
	previousAt  time.Time
	rates       map[string]MessageRates
	lastAttempt time.Time
	lastSuccess time.Time
}

// NewMessageRateCollector returns an empty collector backed by conn.
func NewMessageRateCollector(conn *kafkaesque.KafkaesqueConn, logger *slog.Logger) *MessageRateCollector {
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	return &MessageRateCollector{conn: conn, logger: logger, rates: make(map[string]MessageRates)}
}

// Start collects a baseline immediately and refreshes it hourly until ctx is cancelled.
func (c *MessageRateCollector) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(messageRateInterval)
		defer ticker.Stop()
		c.run(ctx, ticker.C)
	}()
}

func (c *MessageRateCollector) run(ctx context.Context, ticks <-chan time.Time) {
	c.collect(ctx, time.Now())
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticks:
			c.collect(ctx, now)
		}
	}
}

func (c *MessageRateCollector) collect(ctx context.Context, now time.Time) {
	c.mu.Lock()
	c.lastAttempt = now
	c.mu.Unlock()

	current, err := c.fetchOffsets(ctx)
	if err != nil {
		c.logger.Warn("topic_message_rate_collection_failed", "error_type", fmt.Sprintf("%T", err))
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.previousAt.IsZero() && now.After(c.previousAt) {
		c.rates = calculateMessageRates(c.previous, current, now.Sub(c.previousAt))
	}
	c.previous = current
	c.previousAt = now
	c.lastSuccess = now
}

func calculateMessageRates(previous, current offsetSnapshot, elapsed time.Duration) map[string]MessageRates {
	deltas := make(map[string]int64)
	comparable := make(map[string]int)
	for key, offset := range current.offsets {
		previousOffset, ok := previous.offsets[key]
		if !ok || offset < previousOffset {
			continue
		}
		deltas[key.topic] += offset - previousOffset
		comparable[key.topic]++
	}

	rates := make(map[string]MessageRates)
	for topic, count := range current.partitionCounts {
		previousCount, existed := previous.partitionCounts[topic]
		if !existed || previousCount != count || comparable[topic] != count {
			continue
		}
		perHour := float64(deltas[topic]) / elapsed.Hours()
		rates[topic] = MessageRates{
			PerSecond: perHour / 3600,
			PerMinute: perHour / 60,
			PerHour:   perHour,
		}
	}
	return rates
}

func (c *MessageRateCollector) collectionTimes() (time.Time, time.Time) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastAttempt, c.lastSuccess
}

// Snapshot returns a copy of the latest complete topic rates.
func (c *MessageRateCollector) Snapshot() map[string]MessageRates {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	rates := make(map[string]MessageRates, len(c.rates))
	for topic, rate := range c.rates {
		rates[topic] = rate
	}
	return rates
}

func (c *MessageRateCollector) fetchOffsets(ctx context.Context) (offsetSnapshot, error) {
	metadata, err := c.conn.GetMetadataSnapshot(ctx)
	if err != nil {
		return offsetSnapshot{}, fmt.Errorf("fetch metadata: %w", err)
	}

	current := offsetSnapshot{
		offsets:         make(map[topicPartition]int64),
		partitionCounts: make(map[string]int, len(metadata.Metadata.Topics)),
	}
	requestsByBroker := make(map[string]map[string][]listoffsets.RequestPartition)
	for _, topic := range metadata.Metadata.Topics {
		current.partitionCounts[topic.Name] = len(topic.Partitions)
		for _, partition := range topic.Partitions {
			if partition.Leader.Host == "" || partition.Leader.Port == 0 {
				return offsetSnapshot{}, fmt.Errorf("leader not found for %s partition %d", topic.Name, partition.ID)
			}
			broker := net.JoinHostPort(partition.Leader.Host, strconv.Itoa(partition.Leader.Port))
			if requestsByBroker[broker] == nil {
				requestsByBroker[broker] = make(map[string][]listoffsets.RequestPartition)
			}
			requestsByBroker[broker][topic.Name] = append(requestsByBroker[broker][topic.Name], listoffsets.RequestPartition{
				Partition:          int32(partition.ID),
				CurrentLeaderEpoch: -1,
				Timestamp:          kafka.LastOffset,
			})
		}
	}

	brokers := make([]string, 0, len(requestsByBroker))
	for broker := range requestsByBroker {
		brokers = append(brokers, broker)
	}
	sort.Strings(brokers)
	for _, broker := range brokers {
		response, err := c.fetchBrokerOffsets(ctx, broker, requestsByBroker[broker])
		if err != nil {
			return offsetSnapshot{}, fmt.Errorf("fetch latest offsets from %s: %w", broker, err)
		}
		for _, topic := range response.Topics {
			for _, partition := range topic.Partitions {
				if partition.ErrorCode != 0 {
					return offsetSnapshot{}, fmt.Errorf("fetch latest offset for %s partition %d: %w", topic.Topic, partition.Partition, kafka.Error(partition.ErrorCode))
				}
				current.offsets[topicPartition{topic: topic.Topic, partition: int(partition.Partition)}] = partition.Offset
			}
		}
	}

	expected := 0
	for _, count := range current.partitionCounts {
		expected += count
	}
	if len(current.offsets) != expected {
		return offsetSnapshot{}, fmt.Errorf("received %d of %d partition offsets", len(current.offsets), expected)
	}
	return current, nil
}

func (c *MessageRateCollector) fetchBrokerOffsets(
	ctx context.Context,
	broker string,
	topics map[string][]listoffsets.RequestPartition,
) (*listoffsets.Response, error) {
	if c.conn.Client.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.conn.Client.Timeout)
		defer cancel()
	}

	names := make([]string, 0, len(topics))
	for name := range topics {
		names = append(names, name)
	}
	sort.Strings(names)
	requestTopics := make([]listoffsets.RequestTopic, 0, len(names))
	for _, name := range names {
		partitions := topics[name]
		sort.Slice(partitions, func(i, j int) bool { return partitions[i].Partition < partitions[j].Partition })
		requestTopics = append(requestTopics, listoffsets.RequestTopic{Topic: name, Partitions: partitions})
	}

	transport := c.conn.Client.Transport
	if transport == nil {
		transport = kafka.DefaultTransport
	}
	raw, err := transport.RoundTrip(ctx, kafka.TCP(broker), &listoffsets.Request{ReplicaID: -1, Topics: requestTopics})
	if err != nil {
		return nil, err
	}
	response, ok := raw.(*listoffsets.Response)
	if !ok {
		return nil, fmt.Errorf("unexpected list offsets response type %T", raw)
	}
	return response, nil
}
