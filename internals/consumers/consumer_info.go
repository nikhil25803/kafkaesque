package consumers

import (
	"context"
	"fmt"

	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
	kafka "github.com/segmentio/kafka-go"
)

type ConsumerGroups struct {
	GroupName     string `json:"group_name"`
	Type          string `json:"type"`
	CoordinatorID int    `json:"coordinator_id"`
	MembersCount  int    `json:"members_count"`
	State         string `json:"state"`
	TopicsCount   int    `json:"topics_count"`
}

// GetConsumerInformation returns a simple list of basic consumer groups.
func GetConsumerInformation(c *kafkaesque.KafkaesqueConn, ctx context.Context) ([]ConsumerGroups, error) {
	groups, err := c.Client.ListGroups(ctx, &kafka.ListGroupsRequest{})
	if err != nil {
		return nil, err
	}

	if groups.Error != nil {
		return nil, fmt.Errorf("list groups: %w", groups.Error)
	}

	var consumerGroups []ConsumerGroups
	for _, group := range groups.Groups {
		consumerGroups = append(consumerGroups, ConsumerGroups{
			GroupName:     group.GroupID,
			Type:          group.ProtocolType,
			CoordinatorID: group.Coordinator,
		})
	}

	return consumerGroups, nil
}
