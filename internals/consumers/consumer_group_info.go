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
	Topics       []ConsumerGroupTopicLag `json:"topics"`
}

// GetConsumerGroupInformation returns group state, membership, and committed-offset lag.
func GetConsumerGroupInformation(
	c *kafkaesque.KafkaesqueConn,
	ctx context.Context,
	metadata *kafka.MetadataResponse,
	groupName string,
	thresholds LagThresholds,
) (*ConsumerGroupInformation, error) {
	coordinator, err := c.Client.FindCoordinator(ctx, &kafka.FindCoordinatorRequest{
		Key:     groupName,
		KeyType: kafka.CoordinatorKeyTypeConsumer,
	})
	if err != nil {
		return nil, fmt.Errorf("find coordinator: %w", err)
	}
	if coordinator.Error != nil {
		return nil, fmt.Errorf("find coordinator: %w", coordinator.Error)
	}
	if coordinator.Coordinator == nil {
		return nil, fmt.Errorf("find coordinator returned no broker")
	}

	brokerAddress := net.JoinHostPort(
		coordinator.Coordinator.Host,
		strconv.Itoa(coordinator.Coordinator.Port),
	)
	described, err := GetConsumerGroupDescription(c, ctx, ConsumerGroupDescriptionRequest{
		Group: ConsumerGroups{
			GroupName:     groupName,
			Type:          "consumer",
			CoordinatorID: coordinator.Coordinator.NodeID,
		},
		BrokerAddress: brokerAddress,
	})
	if err != nil {
		return nil, err
	}
	if len(described) != 1 {
		return nil, fmt.Errorf("describe group returned %d groups, expected 1", len(described))
	}

	topics, totalLag, err := GetConsumerGroupLag(c, ctx, metadata, brokerAddress, groupName, thresholds)
	if err != nil {
		return nil, err
	}

	return &ConsumerGroupInformation{
		GroupName:    groupName,
		State:        described[0].State,
		MembersCount: described[0].MembersCount,
		TopicsCount:  len(topics),
		TotalLag:     totalLag,
		Topics:       topics,
	}, nil
}
