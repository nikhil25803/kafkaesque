package kafka

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	metadataAPI "github.com/segmentio/kafka-go/protocol/metadata"
)

type checkTransport struct {
	topicError int16
}

func (t checkTransport) RoundTrip(context.Context, net.Addr, kafkago.Request) (kafkago.Response, error) {
	return &metadataAPI.Response{
		ClusterID:    "cluster-1",
		ControllerID: 2,
		Brokers: []metadataAPI.ResponseBroker{
			{NodeID: 1, Host: "broker-1", Port: 9092},
			{NodeID: 2, Host: "broker-2", Port: 9092},
		},
		Topics: []metadataAPI.ResponseTopic{
			{
				Name:      "orders",
				ErrorCode: t.topicError,
				Partitions: []metadataAPI.ResponsePartition{
					{PartitionIndex: 0, LeaderID: 1},
					{PartitionIndex: 1, LeaderID: 2},
				},
			},
		},
	}, nil
}

func TestCheckReturnsCompleteMetadataSummary(t *testing.T) {
	conn := &KafkaesqueConn{Client: &kafkago.Client{
		Addr:      kafkago.TCP("localhost:9092"),
		Transport: checkTransport{},
	}}

	result, err := conn.Check(context.Background(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if result.BrokerCount != 2 || result.ControllerID != 2 || result.TopicCount != 1 || result.PartitionCount != 2 {
		t.Fatalf("result = %+v", result)
	}
	if result.Latency < 0 {
		t.Fatalf("latency = %s", result.Latency)
	}
}

func TestCheckReturnsMetadataAuthorizationErrors(t *testing.T) {
	conn := &KafkaesqueConn{Client: &kafkago.Client{
		Addr:      kafkago.TCP("localhost:9092"),
		Transport: checkTransport{topicError: int16(kafkago.TopicAuthorizationFailed)},
	}}

	_, err := conn.Check(context.Background(), time.Second)
	if !errors.Is(err, kafkago.TopicAuthorizationFailed) {
		t.Fatalf("error = %v", err)
	}
}
