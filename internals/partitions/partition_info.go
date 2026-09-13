package partitions

import (
	"context"

	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
	kafka_go "github.com/segmentio/kafka-go"
)

type PartitionInformation struct {
	ID       int                `json:"id"`
	Topic    string             `json:"topic"`
	Leader   *kafka_go.Broker   `json:"leader"`
	Replicas *[]kafka_go.Broker `json:"replicas"`
	ISR      *[]kafka_go.Broker `json:"isr"`
}

func GetPartitionInformation(
	c *kafkaesque.KafkaesqueConn,
	ctx context.Context,
	topic string,
) ([]PartitionInformation, error) {
	metadata, err := c.Client.Metadata(
		ctx,
		&kafka_go.MetadataRequest{
			Topics: []string{topic},
		},
	)
	if err != nil {
		return nil, err
	}

	for _, t := range metadata.Topics {
		if t.Name != topic {
			continue
		}

		partitions := make([]PartitionInformation, 0, len(t.Partitions))

		for _, p := range t.Partitions {
			partitions = append(partitions, PartitionInformation{
				ID:       p.ID,
				Topic:    t.Name,
				Leader:   &p.Leader,
				Replicas: &p.Replicas,
				ISR:      &p.Isr,
			})
		}

		return partitions, nil
	}

	return nil, nil
}
