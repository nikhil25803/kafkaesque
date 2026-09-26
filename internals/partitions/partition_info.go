package partitions

import (
	"fmt"

	kafka "github.com/segmentio/kafka-go"
)

// GetPartitionInformation returns partitions for a topic.
func GetPartitionInformation(metadata *kafka.MetadataResponse, topic string) ([]kafka.Partition, error) {
	for _, current := range metadata.Topics {
		if current.Name != topic {
			continue
		}
		if current.Error != nil {
			return nil, current.Error
		}
		return current.Partitions, nil
	}

	return nil, fmt.Errorf("topic %q not found", topic)
}
