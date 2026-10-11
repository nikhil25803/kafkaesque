package health

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	kafkaesque_brokers "github.com/nikhil25803/kafkaesque/internals/brokers"
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
	Status                string
	BrokerCount           int
	TopicCount            int
	PartitionCount        int
	ConsumerGroupCount    int
	OfflinePartitionCount int
	UnderReplicatedCount  int
	RebalancingGroupCount int
	DeadGroupCount        int
	LaggingGroupCount     int
}

// GetInformation builds cluster health from metadata, group state, and group lag.
func GetInformation(
	c *kafkaesque.KafkaesqueConn,
	ctx context.Context,
	metadata *kafka.MetadataResponse,
	thresholds kafkaesque_consumers.LagThresholds,
) (*Information, error) {
	info := &Information{
		Status:      StatusHealthy,
		BrokerCount: len(metadata.Brokers),
		TopicCount:  len(metadata.Topics),
	}

	for _, topic := range metadata.Topics {
		if topic.Error != nil {
			return nil, fmt.Errorf("metadata for topic %s: %w", topic.Name, topic.Error)
		}
		for _, partition := range topic.Partitions {
			if partition.Error != nil &&
				!errors.Is(partition.Error, kafka.LeaderNotAvailable) &&
				!errors.Is(partition.Error, kafka.ReplicaNotAvailable) {
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

	groups, err := kafkaesque_consumers.GetConsumerInformation(c, ctx)
	if err != nil {
		return nil, fmt.Errorf("list consumer groups: %w", err)
	}
	consumerGroups := groups[:0]
	for _, group := range groups {
		if group.Type == "consumer" {
			consumerGroups = append(consumerGroups, group)
		}
	}
	sort.Slice(consumerGroups, func(i, j int) bool {
		return consumerGroups[i].GroupName < consumerGroups[j].GroupName
	})
	info.ConsumerGroupCount = len(consumerGroups)

	brokerAddresses := make(map[int]string, len(metadata.Brokers))
	for _, broker := range kafkaesque_brokers.GetBrokerInformation(metadata, c.ResolveBrokerAddress) {
		brokerAddresses[broker.ID] = broker.ConnectAddress
	}

	for _, group := range consumerGroups {
		brokerAddress, ok := brokerAddresses[group.CoordinatorID]
		if !ok {
			return nil, fmt.Errorf("coordinator broker %d for consumer group %s not found", group.CoordinatorID, group.GroupName)
		}

		described, err := kafkaesque_consumers.GetConsumerGroupDescription(c, ctx, kafkaesque_consumers.ConsumerGroupDescriptionRequest{
			Group: group, BrokerAddress: brokerAddress,
		})
		if err != nil {
			return nil, fmt.Errorf("describe consumer group %s: %w", group.GroupName, err)
		}
		if len(described) != 1 {
			return nil, fmt.Errorf("describe consumer group %s returned %d groups, expected 1", group.GroupName, len(described))
		}

		state := strings.ToLower(described[0].State)
		if state == "dead" {
			info.DeadGroupCount++
			continue
		}
		if state != "stable" && state != "empty" {
			info.RebalancingGroupCount++
		}

		_, summary, err := kafkaesque_consumers.GetConsumerGroupLag(
			c, ctx, metadata, brokerAddress, group.GroupName, thresholds,
		)
		if err != nil {
			return nil, fmt.Errorf("get consumer group %s lag: %w", group.GroupName, err)
		}
		if kafkaesque_consumers.ConsumerLagStatus(summary.TotalLag, thresholds) != kafkaesque_consumers.LagStatusHealthy {
			info.LaggingGroupCount++
		}
	}

	if info.OfflinePartitionCount > 0 || info.UnderReplicatedCount > 0 ||
		info.RebalancingGroupCount > 0 || info.DeadGroupCount > 0 || info.LaggingGroupCount > 0 {
		info.Status = StatusDegraded
	}
	return info, nil
}
