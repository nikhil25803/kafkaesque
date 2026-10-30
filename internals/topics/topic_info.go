package topics

import kafka "github.com/segmentio/kafka-go"

// TopicInformation contains the topic data exposed by the CLI.
type TopicInformation struct {
	Name              string   `json:"name"`
	Level             string   `json:"level"`
	PartitionCount    int      `json:"partition_count"`
	ReplicationFactor int      `json:"replication_factor"`
	MessagesPerSecond *float64 `json:"-"`
	MessagesPerMinute *float64 `json:"-"`
	MessagesPerHour   *float64 `json:"-"`
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

// AddMessageRates attaches collected rates without changing the CLI JSON representation.
func AddMessageRates(topics []TopicInformation, rates map[string]MessageRates) []TopicInformation {
	for i := range topics {
		rate, ok := rates[topics[i].Name]
		if !ok {
			continue
		}
		perSecond, perMinute, perHour := rate.PerSecond, rate.PerMinute, rate.PerHour
		topics[i].MessagesPerSecond = &perSecond
		topics[i].MessagesPerMinute = &perMinute
		topics[i].MessagesPerHour = &perHour
	}
	return topics
}
