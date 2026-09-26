package topics

import kafka "github.com/segmentio/kafka-go"

// GetTopicInformation returns topics from cluster metadata.
func GetTopicInformation(metadata *kafka.MetadataResponse) []kafka.Topic {
	return metadata.Topics
}
