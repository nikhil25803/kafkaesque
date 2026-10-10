package consumers

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strconv"

	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol/listoffsets"
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

// ConsumerGroupPartitionLag contains committed and log-end offsets for one partition.
type ConsumerGroupPartitionLag struct {
	Partition       int   `json:"partition"`
	CommittedOffset int64 `json:"committed_offset"`
	LogEndOffset    int64 `json:"log_end_offset"`
	Lag             int64 `json:"lag"`
}

// ConsumerGroupTopicInformation contains partition lag details for one group topic.
type ConsumerGroupTopicInformation struct {
	GroupName  string                      `json:"group_name"`
	Topic      string                      `json:"topic"`
	Partitions []ConsumerGroupPartitionLag `json:"partitions"`
	TotalLag   int64                       `json:"total_lag"`
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
	partitionLags, err := getConsumerGroupPartitionLags(c, ctx, metadata, coordinatorAddress, groupName, "")
	if err != nil {
		return nil, 0, err
	}

	topicNames := make([]string, 0, len(partitionLags))
	for topicName := range partitionLags {
		topicNames = append(topicNames, topicName)
	}
	sort.Strings(topicNames)

	result := make([]ConsumerGroupTopicLag, 0, len(topicNames))
	var totalLag int64
	for _, topicName := range topicNames {
		var topicLag int64
		for _, partition := range partitionLags[topicName] {
			topicLag += partition.Lag
		}
		result = append(result, ConsumerGroupTopicLag{
			Topic:      topicName,
			Partitions: len(partitionLags[topicName]),
			Lag:        topicLag,
			Status:     ConsumerLagStatus(topicLag, thresholds),
		})
		totalLag += topicLag
	}

	return result, totalLag, nil
}

// GetConsumerGroupTopicLag returns partition lag details for one group topic.
func GetConsumerGroupTopicLag(
	c *kafkaesque.KafkaesqueConn,
	ctx context.Context,
	metadata *kafka.MetadataResponse,
	coordinatorAddress string,
	groupName string,
	topicName string,
) (*ConsumerGroupTopicInformation, error) {
	partitionLags, err := getConsumerGroupPartitionLags(c, ctx, metadata, coordinatorAddress, groupName, topicName)
	if err != nil {
		return nil, err
	}
	partitions := partitionLags[topicName]
	if len(partitions) == 0 {
		return nil, fmt.Errorf("consumer group %s has no committed offsets for topic %s", groupName, topicName)
	}

	var totalLag int64
	for _, partition := range partitions {
		totalLag += partition.Lag
	}
	return &ConsumerGroupTopicInformation{
		GroupName:  groupName,
		Topic:      topicName,
		Partitions: partitions,
		TotalLag:   totalLag,
	}, nil
}

func getConsumerGroupPartitionLags(
	c *kafkaesque.KafkaesqueConn,
	ctx context.Context,
	metadata *kafka.MetadataResponse,
	coordinatorAddress string,
	groupName string,
	topicFilter string,
) (map[string][]ConsumerGroupPartitionLag, error) {
	partitionsByTopic := make(map[string]map[int]kafka.Partition, len(metadata.Topics))
	var requestedTopics map[string][]int
	topicFound := topicFilter == ""
	for _, topic := range metadata.Topics {
		if topic.Name == topicFilter {
			topicFound = true
			if topic.Error != nil {
				return nil, fmt.Errorf("metadata error for topic %s: %w", topicFilter, topic.Error)
			}
			requestedTopics = map[string][]int{topicFilter: {}}
		}
		if topic.Error != nil {
			continue
		}
		partitionsByTopic[topic.Name] = make(map[int]kafka.Partition, len(topic.Partitions))
		for _, partition := range topic.Partitions {
			partitionsByTopic[topic.Name][partition.ID] = partition
			if topic.Name == topicFilter {
				requestedTopics[topicFilter] = append(requestedTopics[topicFilter], partition.ID)
			}
		}
	}
	if !topicFound {
		return nil, fmt.Errorf("topic %s not found in cluster metadata", topicFilter)
	}

	offsets, err := c.Client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
		Addr:    kafka.TCP(coordinatorAddress),
		GroupID: groupName,
		Topics:  requestedTopics,
	})
	if err != nil {
		return nil, fmt.Errorf("fetch committed offsets: %w", err)
	}
	if offsets.Error != nil {
		return nil, fmt.Errorf("fetch committed offsets: %w", offsets.Error)
	}

	committed := make(map[topicPartition]int64)
	requestsByBroker := make(map[string]map[string][]kafka.OffsetRequest)
	for topicName, topicOffsets := range offsets.Topics {
		if topicFilter != "" && topicName != topicFilter {
			continue
		}
		for _, offset := range topicOffsets {
			if offset.Error != nil {
				return nil, fmt.Errorf("fetch committed offset for %s partition %d: %w", topicName, offset.Partition, offset.Error)
			}
			if offset.CommittedOffset < 0 {
				continue
			}

			partition, ok := partitionsByTopic[topicName][offset.Partition]
			if !ok {
				return nil, fmt.Errorf("metadata not found for %s partition %d", topicName, offset.Partition)
			}
			if partition.Error != nil {
				return nil, fmt.Errorf("metadata error for %s partition %d: %w", topicName, offset.Partition, partition.Error)
			}
			if partition.Leader.Host == "" || partition.Leader.Port == 0 {
				return nil, fmt.Errorf("leader not found for %s partition %d", topicName, offset.Partition)
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
		}
	}

	partitionLags := make(map[topicPartition]ConsumerGroupPartitionLag, len(committed))
	for brokerAddress, topics := range requestsByBroker {
		latest, err := getLatestOffsets(c, ctx, brokerAddress, topics)
		if err != nil {
			return nil, fmt.Errorf("fetch latest offsets from %s: %w", brokerAddress, err)
		}
		for _, topic := range latest.Topics {
			for _, partition := range topic.Partitions {
				if partition.ErrorCode != 0 {
					return nil, fmt.Errorf("fetch latest offset for %s partition %d: %w", topic.Topic, partition.Partition, kafka.Error(partition.ErrorCode))
				}
				key := topicPartition{topic: topic.Topic, partition: int(partition.Partition)}
				committedOffset, ok := committed[key]
				if !ok {
					return nil, fmt.Errorf("committed offset not found for %s partition %d", topic.Topic, partition.Partition)
				}
				lag := partition.Offset - committedOffset
				if lag < 0 {
					lag = 0
				}
				partitionLags[key] = ConsumerGroupPartitionLag{
					Partition:       int(partition.Partition),
					CommittedOffset: committedOffset,
					LogEndOffset:    partition.Offset,
					Lag:             lag,
				}
			}
		}
	}

	if len(partitionLags) != len(committed) {
		missing := make([]topicPartition, 0, len(committed)-len(partitionLags))
		for key := range committed {
			if _, ok := partitionLags[key]; !ok {
				missing = append(missing, key)
			}
		}
		sort.Slice(missing, func(i, j int) bool {
			if missing[i].topic != missing[j].topic {
				return missing[i].topic < missing[j].topic
			}
			return missing[i].partition < missing[j].partition
		})
		return nil, fmt.Errorf("log end offset not returned for %s partition %d", missing[0].topic, missing[0].partition)
	}

	result := make(map[string][]ConsumerGroupPartitionLag)
	for key, partition := range partitionLags {
		result[key.topic] = append(result[key.topic], partition)
	}
	for topicName := range result {
		sort.Slice(result[topicName], func(i, j int) bool {
			return result[topicName][i].Partition < result[topicName][j].Partition
		})
	}
	return result, nil
}

func getLatestOffsets(
	c *kafkaesque.KafkaesqueConn,
	ctx context.Context,
	brokerAddress string,
	topics map[string][]kafka.OffsetRequest,
) (*listoffsets.Response, error) {
	if c.Client.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.Client.Timeout)
		defer cancel()
	}

	topicNames := make([]string, 0, len(topics))
	for topicName := range topics {
		topicNames = append(topicNames, topicName)
	}
	sort.Strings(topicNames)

	requestTopics := make([]listoffsets.RequestTopic, 0, len(topicNames))
	for _, topicName := range topicNames {
		requests := topics[topicName]
		sort.Slice(requests, func(i, j int) bool {
			return requests[i].Partition < requests[j].Partition
		})
		partitions := make([]listoffsets.RequestPartition, 0, len(requests))
		for _, request := range requests {
			partitions = append(partitions, listoffsets.RequestPartition{
				Partition:          int32(request.Partition),
				CurrentLeaderEpoch: -1,
				Timestamp:          kafka.LastOffset,
			})
		}
		requestTopics = append(requestTopics, listoffsets.RequestTopic{
			Topic:      topicName,
			Partitions: partitions,
		})
	}

	transport := c.Client.Transport
	if transport == nil {
		transport = kafka.DefaultTransport
	}
	rawResponse, err := transport.RoundTrip(ctx, kafka.TCP(brokerAddress), &listoffsets.Request{
		ReplicaID: -1,
		Topics:    requestTopics,
	})
	if err != nil {
		return nil, err
	}
	response, ok := rawResponse.(*listoffsets.Response)
	if !ok {
		return nil, fmt.Errorf("unexpected list offsets response type %T", rawResponse)
	}
	return response, nil
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
