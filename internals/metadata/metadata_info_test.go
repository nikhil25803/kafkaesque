package metadata

import (
	"reflect"
	"testing"

	kafka "github.com/segmentio/kafka-go"
)

func TestGetMetadataInformation(t *testing.T) {
	metadata := &kafka.MetadataResponse{
		ClusterID:  "cluster-1",
		Controller: kafka.Broker{ID: 2},
		Brokers:    []kafka.Broker{{ID: 1}, {ID: 2}},
		Topics:     []kafka.Topic{{Name: "orders"}, {Name: "payments"}},
	}

	got := GetMetadataInformation(metadata)
	want := &MetadataInformation{
		ClusterID:    "cluster-1",
		ControllerID: 2,
		BrokerCount:  2,
		TopicCount:   2,
		Status:       statusConnected,
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("metadata information = %+v, want %+v", got, want)
	}
}
