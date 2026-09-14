package consumers

import (
	"context"

	kafka_go "github.com/segmentio/kafka-go"

	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
)

type ConsumerInformation struct {
	GroupID     string `json:"group_id"`
	Coordinator int    `json:"coordinator"`
	Protocol    string `json:"protocol"`
}

func GetConsumerInformation(c *kafkaesque.KafkaesqueConn, ctx context.Context) ([]*ConsumerInformation, error) {

	consumers, err := c.Client.ListGroups(
		ctx,
		&kafka_go.ListGroupsRequest{})
	if err != nil {
		return nil, err
	}

	consumerDetails := make([]*ConsumerInformation, 0, len(consumers.Groups))

	for _, consumer := range consumers.Groups {

		consumerDetails = append(consumerDetails, &ConsumerInformation{
			GroupID:     consumer.GroupID,
			Coordinator: consumer.Coordinator,
			Protocol:    consumer.ProtocolType,
		})
	}

	return consumerDetails, nil
}
