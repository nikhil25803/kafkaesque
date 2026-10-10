package consumers

import (
	"context"
	"fmt"

	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol/describegroups"
)

type ConsumerGroupDescriptionRequest struct {
	Group         ConsumerGroups `json:"group"`
	BrokerAddress string         `json:"broker_address"`
}

// GetConsumerGroupDescription completes a consumer group with its state and member count.
func GetConsumerGroupDescription(c *kafkaesque.KafkaesqueConn, ctx context.Context, req ConsumerGroupDescriptionRequest) ([]ConsumerGroups, error) {
	if c.Client.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.Client.Timeout)
		defer cancel()
	}

	transport := c.Client.Transport
	if transport == nil {
		transport = kafka.DefaultTransport
	}

	rawResponse, err := transport.RoundTrip(ctx, kafka.TCP(req.BrokerAddress), &describegroups.Request{
		Groups: []string{req.Group.GroupName},
	})
	if err != nil {
		return nil, fmt.Errorf("describe groups network error: %w", err)
	}

	response, ok := rawResponse.(*describegroups.Response)
	if !ok {
		return nil, fmt.Errorf("unexpected describe groups response type %T", rawResponse)
	}

	var consumerGroups []ConsumerGroups

	for _, group := range response.Groups {
		if group.ErrorCode != 0 {
			return nil, fmt.Errorf("error describing group %s: %w", group.GroupID, kafka.Error(group.ErrorCode))
		}

		completedGroup := req.Group
		completedGroup.State = group.GroupState
		completedGroup.MembersCount = len(group.Members)

		consumerGroups = append(consumerGroups, completedGroup)
	}

	return consumerGroups, nil
}
