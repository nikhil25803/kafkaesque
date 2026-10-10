package consumers

import (
	"context"
	"fmt"

	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
	kafka "github.com/segmentio/kafka-go"
)

type ConsumerGroups struct {
	GroupName   string `json:"group_name"`
	State       string `json:"state"`
	Coordinator string `json:"coordinator"`
}

// GetConsumerInformation returns consumer groups.
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
		// if group.ProtocolType != "consumer" {
		// 	continue
		// }
		consumerGroups = append(consumerGroups, ConsumerGroups{
			GroupName:   group.GroupID,
			State:       group.ProtocolType,
			Coordinator: fmt.Sprintf("broker-%d", group.Coordinator),
		})
	}

	return consumerGroups, nil
}
