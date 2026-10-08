package partitions

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	kafka "github.com/segmentio/kafka-go"
)

func TestGetPartitionInformation(t *testing.T) {
	metadata := &kafka.MetadataResponse{
		Topics: []kafka.Topic{
			{Name: "payments"},
			{
				Name: "orders",
				Partitions: []kafka.Partition{
					{
						ID:       0,
						Leader:   kafka.Broker{ID: 2},
						Replicas: []kafka.Broker{{ID: 1}, {ID: 2}, {ID: 3}},
						Isr:      []kafka.Broker{{ID: 1}, {ID: 2}},
					},
				},
			},
		},
	}

	want := []PartitionTopicInformation{
		{TopicName: "orders", PartitionID: 0, Leader: "broker-2", Replicas: 3, Isr: 2},
	}
	got, err := GetPartitionInformation(metadata, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("partitions = %+v, want %+v", got, want)
	}
}

func TestGetPartitionInformationEmptyTopic(t *testing.T) {
	got, err := GetPartitionInformation(&kafka.MetadataResponse{
		Topics: []kafka.Topic{{Name: "orders"}},
	}, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("partitions = %+v, want empty", got)
	}
}

func TestGetPartitionInformationErrors(t *testing.T) {
	t.Run("missing topic", func(t *testing.T) {
		_, err := GetPartitionInformation(&kafka.MetadataResponse{}, "orders")
		if err == nil || !strings.Contains(err.Error(), "not found") {
			t.Fatalf("error = %v, want topic not found", err)
		}
	})

	t.Run("topic response", func(t *testing.T) {
		responseErr := errors.New("topic failed")
		_, err := GetPartitionInformation(&kafka.MetadataResponse{
			Topics: []kafka.Topic{{Name: "orders", Error: responseErr}},
		}, "orders")
		if !errors.Is(err, responseErr) {
			t.Fatalf("error = %v, want %v", err, responseErr)
		}
	})
}
