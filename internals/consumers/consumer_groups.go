package consumers

import (
	"context"
	"fmt"
	"sort"
	"strings"

	kafkaesque_brokers "github.com/nikhil25803/kafkaesque/internals/brokers"
	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
	kafka "github.com/segmentio/kafka-go"
)

// GetConsumerGroupsInformation returns current state and lag for every consumer group.
func GetConsumerGroupsInformation(
	c *kafkaesque.KafkaesqueConn,
	ctx context.Context,
	metadata *kafka.MetadataResponse,
	thresholds LagThresholds,
) ([]ConsumerGroupInformation, error) {
	groups, err := GetConsumerInformation(c, ctx)
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

	brokerAddresses := make(map[int]string, len(metadata.Brokers))
	for _, broker := range kafkaesque_brokers.GetBrokerInformation(metadata, c.ResolveBrokerAddress) {
		brokerAddresses[broker.ID] = broker.ConnectAddress
	}

	result := make([]ConsumerGroupInformation, 0, len(consumerGroups))
	for _, group := range consumerGroups {
		brokerAddress, ok := brokerAddresses[group.CoordinatorID]
		if !ok {
			return nil, fmt.Errorf("coordinator broker %d for consumer group %s not found", group.CoordinatorID, group.GroupName)
		}
		described, err := GetConsumerGroupDescription(c, ctx, ConsumerGroupDescriptionRequest{
			Group:         group,
			BrokerAddress: brokerAddress,
		})
		if err != nil {
			return nil, fmt.Errorf("describe consumer group %s: %w", group.GroupName, err)
		}
		if len(described) != 1 {
			return nil, fmt.Errorf("describe consumer group %s returned %d groups, expected 1", group.GroupName, len(described))
		}

		information := ConsumerGroupInformation{
			GroupName:    group.GroupName,
			State:        described[0].State,
			MembersCount: described[0].MembersCount,
			TopicsCount:  described[0].TopicsCount,
			Topics:       []ConsumerGroupTopicLag{},
		}
		if !strings.EqualFold(information.State, "dead") {
			topics, summary, err := GetConsumerGroupLag(c, ctx, metadata, brokerAddress, group.GroupName, thresholds)
			if err != nil {
				return nil, fmt.Errorf("get consumer group %s lag: %w", group.GroupName, err)
			}
			information.Topics = topics
			information.TopicsCount = len(topics)
			information.TotalLag = summary.TotalLag
			information.LagSummary = summary
		}
		result = append(result, information)
	}

	return result, nil
}
