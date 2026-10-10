package consumers

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strconv"

	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
	kafka "github.com/segmentio/kafka-go"
)

const (
	LagStatusHealthy   = "HEALTHY"
	LagStatusWarning   = "WARNING"
	LagStatusUnhealthy = "UNHEALTHY"
)

// LagThresholds controls consumer lag health classification.
type LagThresholds struct {
	Warning   int64
	Unhealthy int64
}

// ConsumerGroupTopicLag contains aggregated committed-offset lag for one topic.
type ConsumerGroupTopicLag struct {
	Topic      string `json:"topic"`
	Partitions int    `json:"partitions"`
	Lag        int64  `json:"lag"`
	Status     string `json:"status"`
}

type topicPartition struct {
	topic     string
	partition int
}

// GetConsumerGroupLag returns per-topic and total lag for committed group offsets.
func GetConsumerGroupLag(
	c *kafkaesque.KafkaesqueConn,
	ctx context.Context,
	metadata *kafka.MetadataResponse,
	coordinatorAddress string,
	groupName string,
	thresholds LagThresholds,
) ([]ConsumerGroupTopicLag, int64, error) {
	offsets, err := c.Client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
		Addr:    kafka.TCP(coordinatorAddress),
		GroupID: groupName,
	})
	if err != nil {
		return nil, 0, fmt.Errorf("fetch committed offsets: %w", err)
	}
	if offsets.Error != nil {
		return nil, 0, fmt.Errorf("fetch committed offsets: %w", offsets.Error)
	}

	partitionsByTopic := make(map[string]map[int]kafka.Partition, len(metadata.Topics))
	for _, topic := range metadata.Topics {
		if topic.Error != nil {
			continue
		}
		partitionsByTopic[topic.Name] = make(map[int]kafka.Partition, len(topic.Partitions))
		for _, partition := range topic.Partitions {
			partitionsByTopic[topic.Name][partition.ID] = partition
		}
	}

	committed := make(map[topicPartition]int64)
	requestsByBroker := make(map[string]map[string][]kafka.OffsetRequest)
	partitionCounts := make(map[string]int)
	for topicName, topicOffsets := range offsets.Topics {
		for _, offset := range topicOffsets {
			if offset.Error != nil {
				return nil, 0, fmt.Errorf("fetch committed offset for %s partition %d: %w", topicName, offset.Partition, offset.Error)
			}
			if offset.CommittedOffset < 0 {
				continue
			}

			partition, ok := partitionsByTopic[topicName][offset.Partition]
			if !ok {
				return nil, 0, fmt.Errorf("metadata not found for %s partition %d", topicName, offset.Partition)
			}
			if partition.Error != nil {
				return nil, 0, fmt.Errorf("metadata error for %s partition %d: %w", topicName, offset.Partition, partition.Error)
			}
			if partition.Leader.Host == "" || partition.Leader.Port == 0 {
				return nil, 0, fmt.Errorf("leader not found for %s partition %d", topicName, offset.Partition)
			}

			brokerAddress := net.JoinHostPort(partition.Leader.Host, strconv.Itoa(partition.Leader.Port))
			if requestsByBroker[brokerAddress] == nil {
				requestsByBroker[brokerAddress] = make(map[string][]kafka.OffsetRequest)
			}
			requestsByBroker[brokerAddress][topicName] = append(
				requestsByBroker[brokerAddress][topicName],
				kafka.LastOffsetOf(offset.Partition),
			)
			committed[topicPartition{topic: topicName, partition: offset.Partition}] = offset.CommittedOffset
			partitionCounts[topicName]++
		}
	}

	topicLags := make(map[string]int64, len(partitionCounts))
	for brokerAddress, topics := range requestsByBroker {
		latest, err := c.Client.ListOffsets(ctx, &kafka.ListOffsetsRequest{
			Addr:   kafka.TCP(brokerAddress),
			Topics: topics,
		})
		if err != nil {
			return nil, 0, fmt.Errorf("fetch latest offsets from %s: %w", brokerAddress, err)
		}
		for topicName, partitions := range latest.Topics {
			for _, partition := range partitions {
				if partition.Error != nil {
					return nil, 0, fmt.Errorf("fetch latest offset for %s partition %d: %w", topicName, partition.Partition, partition.Error)
				}
				key := topicPartition{topic: topicName, partition: partition.Partition}
				committedOffset, ok := committed[key]
				if !ok {
					return nil, 0, fmt.Errorf("committed offset not found for %s partition %d", topicName, partition.Partition)
				}
				lag := partition.LastOffset - committedOffset
				if lag > 0 {
					topicLags[topicName] += lag
				}
			}
		}
	}

	topicNames := make([]string, 0, len(partitionCounts))
	for topicName := range partitionCounts {
		topicNames = append(topicNames, topicName)
	}
	sort.Strings(topicNames)

	result := make([]ConsumerGroupTopicLag, 0, len(topicNames))
	var totalLag int64
	for _, topicName := range topicNames {
		lag := topicLags[topicName]
		result = append(result, ConsumerGroupTopicLag{
			Topic:      topicName,
			Partitions: partitionCounts[topicName],
			Lag:        lag,
			Status:     ConsumerLagStatus(lag, thresholds),
		})
		totalLag += lag
	}

	return result, totalLag, nil
}

// ConsumerLagStatus classifies a lag value using configured thresholds.
func ConsumerLagStatus(lag int64, thresholds LagThresholds) string {
	if lag > thresholds.Unhealthy {
		return LagStatusUnhealthy
	}
	if lag > thresholds.Warning {
		return LagStatusWarning
	}
	return LagStatusHealthy
}
