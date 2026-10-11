package topics

import kafka "github.com/segmentio/kafka-go"

// TopicInformation contains the topic data exposed by the CLI.
type TopicInformation struct {
	Name              string `json:"name"`
	Level             string `json:"level"`
	PartitionCount    int    `json:"partition_count"`
	ReplicationFactor int    `json:"replication_factor"`
}

// GetTopicInformation returns topics from cluster metadata.
func GetTopicInformation(metadata *kafka.MetadataResponse) []TopicInformation {
	topics := make([]TopicInformation, len(metadata.Topics))

	for i, topic := range metadata.Topics {
		topicLevel := "External"
		if topic.Internal {
			topicLevel = "Internal"
		}

		replicationFactor := 0
		if len(topic.Partitions) > 0 {
			replicationFactor = len(topic.Partitions[0].Replicas)
		}

		topics[i] = TopicInformation{
			Name:              topic.Name,
			Level:             topicLevel,
			PartitionCount:    len(topic.Partitions),
			ReplicationFactor: replicationFactor,
		}
	}

	return topics
}
