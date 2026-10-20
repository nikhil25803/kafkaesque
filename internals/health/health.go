package health

import (
	"context"
	"errors"
	"fmt"

	kafkaesque_consumers "github.com/nikhil25803/kafkaesque/internals/consumers"
	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
	kafka "github.com/segmentio/kafka-go"
)

const (
	StatusHealthy  = "HEALTHY"
	StatusDegraded = "DEGRADED"
)

// Information contains the cluster-wide health summary exposed by the CLI.
type Information struct {
	Status                string `json:"status"`
	BrokerCount           int    `json:"broker_count"`
	TopicCount            int    `json:"topic_count"`
	PartitionCount        int    `json:"partition_count"`
	ConsumerGroupCount    int    `json:"consumer_group_count"`
	OfflinePartitionCount int    `json:"offline_partition_count"`
	UnderReplicatedCount  int    `json:"under_replicated_count"`
	RebalancingGroupCount int    `json:"rebalancing_group_count"`
	DeadGroupCount        int    `json:"dead_group_count"`
	LaggingGroupCount     int    `json:"lagging_group_count"`
}

// GetInformation builds cluster health from metadata, group state, and group lag.
func GetInformation(
	c *kafkaesque.KafkaesqueConn,
	ctx context.Context,
	metadata *kafka.MetadataResponse,
	thresholds kafkaesque_consumers.LagThresholds,
) (*Information, error) {
	groups, err := kafkaesque_consumers.GetConsumerGroupsInformation(c, ctx, metadata, thresholds)
	if err != nil {
		return nil, err
	}
	return BuildInformation(metadata, groups, thresholds)
}

// BuildInformation derives health from an existing metadata and consumer-group snapshot.
func BuildInformation(metadata *kafka.MetadataResponse, groups []kafkaesque_consumers.ConsumerGroupInformation, thresholds kafkaesque_consumers.LagThresholds) (*Information, error) {
	info := &Information{Status: StatusHealthy, BrokerCount: len(metadata.Brokers), TopicCount: len(metadata.Topics)}
	for _, topic := range metadata.Topics {
		if topic.Error != nil {
			return nil, fmt.Errorf("metadata for topic %s: %w", topic.Name, topic.Error)
		}
		for _, partition := range topic.Partitions {
			if partition.Error != nil && !errors.Is(partition.Error, kafka.LeaderNotAvailable) && !errors.Is(partition.Error, kafka.ReplicaNotAvailable) {
				return nil, fmt.Errorf("metadata for topic %s partition %d: %w", topic.Name, partition.ID, partition.Error)
			}
			info.PartitionCount++
			if partition.Leader.ID < 0 {
				info.OfflinePartitionCount++
			}
			if len(partition.Isr) < len(partition.Replicas) {
				info.UnderReplicatedCount++
			}
		}
	}
	info.ConsumerGroupCount = len(groups)

	for _, group := range groups {
		switch kafkaesque_consumers.GroupStateCategory(group.State) {
		case kafkaesque_consumers.GroupStateDead:
			info.DeadGroupCount++
			continue
		case kafkaesque_consumers.GroupStateRebalancing, kafkaesque_consumers.GroupStateUnknown:
			info.RebalancingGroupCount++
		}
		if kafkaesque_consumers.ConsumerLagStatus(group.TotalLag, thresholds) != kafkaesque_consumers.LagStatusHealthy {
			info.LaggingGroupCount++
		}
	}

	if info.OfflinePartitionCount > 0 || info.UnderReplicatedCount > 0 ||
		info.RebalancingGroupCount > 0 || info.DeadGroupCount > 0 || info.LaggingGroupCount > 0 {
		info.Status = StatusDegraded
	}
	return info, nil
}
