package topics

import (
	"context"

	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
	kafka_go "github.com/segmentio/kafka-go"
)

type TopicInformation struct {
	Name           string `json:"name"`
	Internal       bool   `json:"internal"`
	PartitionCount int    `json:"partition_count"`
}

func GetTopics(c *kafkaesque.KafkaesqueConn, ctx context.Context) ([]*kafka_go.Topic, error) {

	metadata, err := c.Client.Metadata(
		ctx,
		&kafka_go.MetadataRequest{},
	)
	if err != nil {
		return nil, err
	}

	topics := make([]*kafka_go.Topic, 0, len(metadata.Topics))

	for _, topic := range metadata.Topics {
		topics = append(topics, &topic)
	}

	return topics, nil
}

func GetTopicInformation(c *kafkaesque.KafkaesqueConn, kafkaTopics []*kafka_go.Topic) (*[]TopicInformation, error) {
	topicDetails := make([]TopicInformation, 0)

	for _, topic := range kafkaTopics {
		topicDetails = append(topicDetails, TopicInformation{
			Name:           topic.Name,
			Internal:       topic.Internal,
			PartitionCount: len(topic.Partitions),
		})
	}

	return &topicDetails, nil
}
