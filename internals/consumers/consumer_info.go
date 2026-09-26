package consumers

import (
	"context"
	"fmt"

	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
	kafka "github.com/segmentio/kafka-go"
)

// GetConsumerInformation returns consumer groups.
func GetConsumerInformation(c *kafkaesque.KafkaesqueConn, ctx context.Context) (*kafka.ListGroupsResponse, error) {
	groups, err := c.Client.ListGroups(ctx, &kafka.ListGroupsRequest{})
	if err != nil {
		return nil, err
	}
	if groups.Error != nil {
		return nil, fmt.Errorf("list groups: %w", groups.Error)
	}
	return groups, nil
}
