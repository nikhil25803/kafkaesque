package consumers

import (
	"context"
	"fmt"
	"net"
	"strconv"

	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
	kafka "github.com/segmentio/kafka-go"
)

// ConsumerGroupInformation contains detailed information about one consumer group.
type ConsumerGroupInformation struct {
	GroupName    string                  `json:"group_name"`
	State        string                  `json:"state"`
	MembersCount int                     `json:"members_count"`
	TopicsCount  int                     `json:"topics_count"`
	TotalLag     int64                   `json:"total_lag"`
	LagSummary   ConsumerGroupLagSummary `json:"lag_summary"`
	Members      []ConsumerGroupMember   `json:"members,omitempty"`
	Topics       []ConsumerGroupTopicLag `json:"topics"`
}

// ConsumerGroupTopicDetails combines topic lag with the group's active members.
type ConsumerGroupTopicDetails struct {
	State        string
	MembersCount int
	Members      []ConsumerGroupMember
	Topic        *ConsumerGroupTopicInformation
}

// GetConsumerGroupInformation returns group state, membership, and committed-offset lag.
func GetConsumerGroupInformation(
	c *kafkaesque.KafkaesqueConn,
	ctx context.Context,
	metadata *kafka.MetadataResponse,
	groupName string,
	thresholds LagThresholds,
	includeMembers bool,
) (*ConsumerGroupInformation, error) {
	described, brokerAddress, err := describeConsumerGroup(c, ctx, groupName, includeMembers)
	if err != nil {
		return nil, err
	}
	if described.State == "Dead" {
		return &ConsumerGroupInformation{
			GroupName: groupName, State: described.State, MembersCount: described.MembersCount,
			Members: described.Members, Topics: []ConsumerGroupTopicLag{},
		}, nil
	}

	topics, summary, err := GetConsumerGroupLag(c, ctx, metadata, brokerAddress, groupName, thresholds)
	if err != nil {
		return nil, err
	}

	return &ConsumerGroupInformation{
		GroupName:    groupName,
		State:        described.State,
		MembersCount: described.MembersCount,
		TopicsCount:  len(topics),
		TotalLag:     summary.TotalLag,
		LagSummary:   summary,
		Members:      described.Members,
		Topics:       topics,
	}, nil
}

// GetConsumerGroupTopicInformation returns partition lag for one group topic.
func GetConsumerGroupTopicInformation(
	c *kafkaesque.KafkaesqueConn,
	ctx context.Context,
	metadata *kafka.MetadataResponse,
	groupName string,
	topicName string,
) (*ConsumerGroupTopicInformation, error) {
	_, brokerAddress, err := describeConsumerGroup(c, ctx, groupName, false)
	if err != nil {
		return nil, err
	}
	return GetConsumerGroupTopicLag(c, ctx, metadata, brokerAddress, groupName, topicName)
}

// GetConsumerGroupTopicDetails returns topic lag and active member assignments.
func GetConsumerGroupTopicDetails(
	c *kafkaesque.KafkaesqueConn,
	ctx context.Context,
	metadata *kafka.MetadataResponse,
	groupName string,
	topicName string,
) (*ConsumerGroupTopicDetails, error) {
	described, brokerAddress, err := describeConsumerGroup(c, ctx, groupName, true)
	if err != nil {
		return nil, err
	}
	topic, err := GetConsumerGroupTopicLag(c, ctx, metadata, brokerAddress, groupName, topicName)
	if err != nil {
		return nil, err
	}
	return &ConsumerGroupTopicDetails{
		State: described.State, MembersCount: described.MembersCount,
		Members: described.Members, Topic: topic,
	}, nil
}

func describeConsumerGroup(
	c *kafkaesque.KafkaesqueConn,
	ctx context.Context,
	groupName string,
	includeMembers bool,
) (ConsumerGroups, string, error) {
	coordinator, err := c.Client.FindCoordinator(ctx, &kafka.FindCoordinatorRequest{
		Key:     groupName,
		KeyType: kafka.CoordinatorKeyTypeConsumer,
	})
	if err != nil {
		return ConsumerGroups{}, "", fmt.Errorf("find coordinator: %w", err)
	}
	if coordinator.Error != nil {
		return ConsumerGroups{}, "", fmt.Errorf("find coordinator: %w", coordinator.Error)
	}
	if coordinator.Coordinator == nil {
		return ConsumerGroups{}, "", fmt.Errorf("find coordinator returned no broker")
	}

	brokerAddress := net.JoinHostPort(
		coordinator.Coordinator.Host,
		strconv.Itoa(coordinator.Coordinator.Port),
	)
	brokerAddress = c.ResolveBrokerAddress(brokerAddress)
	described, err := GetConsumerGroupDescription(c, ctx, ConsumerGroupDescriptionRequest{
		Group: ConsumerGroups{
			GroupName:     groupName,
			Type:          "consumer",
			CoordinatorID: coordinator.Coordinator.NodeID,
		},
		BrokerAddress:  brokerAddress,
		IncludeMembers: includeMembers,
	})
	if err != nil {
		return ConsumerGroups{}, "", err
	}
	if len(described) != 1 {
		return ConsumerGroups{}, "", fmt.Errorf("describe group returned %d groups, expected 1", len(described))
	}
	return described[0], brokerAddress, nil
}
