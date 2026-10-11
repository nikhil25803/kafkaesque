package partitions

import (
	"fmt"
	"reflect"
	"sort"

	kafka "github.com/segmentio/kafka-go"
)

// PartitionTopicInformation contains the partition data exposed by the CLI.
type PartitionTopicInformation struct {
	TopicName   string
	PartitionID int
	Leader      string
	Replicas    []int
	ISR         []int
	Status      string
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
			replicas := brokerIDs(partition.Replicas)
			isr := brokerIDs(partition.Isr)
			leader := "-"
			if partition.Leader.ID >= 0 {
				leader = fmt.Sprintf("broker-%d", partition.Leader.ID)
			}
			status := "DEGRADED"
			if partition.Leader.ID >= 0 && len(replicas) > 0 && reflect.DeepEqual(replicas, isr) {
				status = "HEALTHY"
			}
			partitions = append(partitions, PartitionTopicInformation{
				TopicName:   topic,
				PartitionID: partition.ID,
				Leader:      leader,
				Replicas:    replicas,
				ISR:         isr,
				Status:      status,
			})
		}
		sort.Slice(partitions, func(i, j int) bool {
			return partitions[i].PartitionID < partitions[j].PartitionID
		})
		return partitions, nil
	}

	return nil, fmt.Errorf("topic %q not found", topic)
}

func brokerIDs(brokers []kafka.Broker) []int {
	ids := make([]int, 0, len(brokers))
	for _, broker := range brokers {
		ids = append(ids, broker.ID)
	}
	sort.Ints(ids)
	return ids
}
