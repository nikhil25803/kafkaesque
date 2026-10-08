package brokers

import (
	"reflect"
	"testing"

	kafka "github.com/segmentio/kafka-go"
)

func TestGetBrokerInformation(t *testing.T) {
	metadata := &kafka.MetadataResponse{Brokers: []kafka.Broker{
		{ID: 1, Host: "broker", Port: 9092, Rack: "rack-a"},
		{ID: 2, Host: "2001:db8::1", Port: 9093},
	}}

	got := GetBrokerInformation(metadata)
	want := []BrokerInformation{
		{ID: 1, Address: "broker:9092", Rack: "rack-a"},
		{ID: 2, Address: "[2001:db8::1]:9093"},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("broker information = %+v, want %+v", got, want)
	}
}

func TestGetBrokerInformationEmpty(t *testing.T) {
	got := GetBrokerInformation(&kafka.MetadataResponse{})
	if len(got) != 0 {
		t.Fatalf("broker information = %+v, want empty", got)
	}
}
