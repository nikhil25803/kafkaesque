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
}

type ConsumerGroupDescriptionRequest struct {
	Group         ConsumerGroups `json:"group"` // Pass the populated group struct from the previous step
	BrokerAddress string         `json:"broker_address"`
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

func GetConsumerGroupDescription(c *kafkaesque.KafkaesqueConn, ctx context.Context, req ConsumerGroupDescriptionRequest) ([]ConsumerGroups, error) {
	response, err := c.Client.DescribeGroups(ctx, &kafka.DescribeGroupsRequest{
		Addr:     kafka.TCP(req.BrokerAddress),
		GroupIDs: []string{req.Group.GroupName},
	})
	if err != nil {
		return nil, fmt.Errorf("describe groups network error: %w", err)
	}

	var consumerGroups []ConsumerGroups

	for _, group := range response.Groups {
		if group.Error != nil {
			return nil, fmt.Errorf("error describing group %s: %w", group.GroupID, group.Error)
		}

		completedGroup := req.Group
		completedGroup.State = group.GroupState
		completedGroup.MembersCount = len(group.Members)

		consumerGroups = append(consumerGroups, completedGroup)
	}

	return consumerGroups, nil
}
