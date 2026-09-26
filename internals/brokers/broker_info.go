package brokers

import kafka "github.com/segmentio/kafka-go"

// GetBrokerInformation returns brokers from cluster metadata.
func GetBrokerInformation(metadata *kafka.MetadataResponse) []kafka.Broker {
	return metadata.Brokers
}
