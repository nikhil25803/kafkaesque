package topics

import (
	"reflect"
	"testing"

	kafka "github.com/segmentio/kafka-go"
)

func TestGetTopicInformation(t *testing.T) {
	metadata := &kafka.MetadataResponse{
		Topics: []kafka.Topic{
			{
				Name: "orders",
				Partitions: []kafka.Partition{
					{Replicas: []kafka.Broker{{ID: 1}, {ID: 2}, {ID: 3}}},
					{Replicas: []kafka.Broker{{ID: 2}, {ID: 3}, {ID: 1}}},
				},
			},
			{
				Name:     "__consumer_offsets",
				Internal: true,
				Partitions: []kafka.Partition{
					{Replicas: []kafka.Broker{{ID: 1}}},
				},
			},
			{Name: "empty-topic"},
		},
	}

	want := []TopicInformation{
		{Name: "orders", Level: "External", PartitionCount: 2, ReplicationFactor: 3},
		{Name: "__consumer_offsets", Level: "Internal", PartitionCount: 1, ReplicationFactor: 1},
		{Name: "empty-topic", Level: "External", PartitionCount: 0, ReplicationFactor: 0},
	}
	if got := GetTopicInformation(metadata); !reflect.DeepEqual(got, want) {
		t.Fatalf("topics = %+v, want %+v", got, want)
	}
}

func TestGetTopicInformationEmpty(t *testing.T) {
	got := GetTopicInformation(&kafka.MetadataResponse{})
	if len(got) != 0 {
		t.Fatalf("topics = %+v, want empty", got)
	}
}
