package partitions

import (
	"fmt"

	kafka "github.com/segmentio/kafka-go"
)

// PartitionTopicInformation contains the partition data exposed by the CLI.
type PartitionTopicInformation struct {
	TopicName   string
	PartitionID int
	Leader      string
	Replicas    int
	Isr         int
}

// GetPartitionInformation returns partitions for a topic.
func GetPartitionInformation(metadata *kafka.MetadataResponse, topic string) ([]PartitionTopicInformation, error) {
	var partitions []PartitionTopicInformation

	for _, current := range metadata.Topics {
		if current.Name != topic {
			continue
		}
		if current.Error != nil {
			return nil, current.Error
		}
		for _, partition := range current.Partitions {
			partitions = append(partitions, PartitionTopicInformation{
				TopicName:   topic,
				PartitionID: partition.ID,
				Leader:      fmt.Sprintf("broker-%d", partition.Leader.ID),
				Replicas:    len(partition.Replicas),
				Isr:         len(partition.Isr),
			})
		}
		return partitions, nil
	}

	return nil, fmt.Errorf("topic %q not found", topic)
}
