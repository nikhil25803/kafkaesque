package brokers

import (
	"context"

	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
	kafka_go "github.com/segmentio/kafka-go"
)

type BrokerInformation struct {
	Host string `json:"host"`
	Port int    `json:"port"`
	ID   int    `json:"id"`
	Rack string `json:"rack"`
}

func GetBrokerInformation(c *kafkaesque.KafkaesqueConn, ctx context.Context) ([]*BrokerInformation, error) {
	metadata, err := c.Client.Metadata(
		ctx,
		&kafka_go.MetadataRequest{},
	)
	if err != nil {
		return nil, err
	}

	brokers := make([]*BrokerInformation, 0, len(metadata.Brokers))

	for _, broker := range metadata.Brokers {
		brokers = append(brokers, &BrokerInformation{
			Host: broker.Host,
			Port: broker.Port,
			ID:   broker.ID,
			Rack: broker.Rack,
		})
	}

	return brokers, nil
}
