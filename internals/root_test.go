package internals

import (
	"bytes"
	"context"
	"errors"
	"net"
	"reflect"
	"strings"
	"testing"

	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
	kafkaesque_metadata "github.com/nikhil25803/kafkaesque/internals/metadata"
	kafkaesque_partition "github.com/nikhil25803/kafkaesque/internals/partitions"
	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol/listgroups"
	"github.com/segmentio/kafka-go/protocol/metadata"
	"github.com/spf13/cobra"
)

type recordingTransport struct {
	metadataTopics [][]string
	groupRequests  int
	err            error
	groupErrorCode int16
}

func (t *recordingTransport) RoundTrip(_ context.Context, _ net.Addr, request kafka.Request) (kafka.Response, error) {
	if t.err != nil {
		return nil, t.err
	}

	switch request := request.(type) {
	case *metadata.Request:
		t.metadataTopics = append(t.metadataTopics, append([]string(nil), request.TopicNames...))
		return &metadata.Response{
			ClusterID:    "cluster-1",
			ControllerID: 1,
			Brokers: []metadata.ResponseBroker{
				{NodeID: 1, Host: "broker", Port: 9092, Rack: "rack-a"},
			},
			Topics: []metadata.ResponseTopic{
				{
					Name: "orders",
					Partitions: []metadata.ResponsePartition{
						{PartitionIndex: 0, LeaderID: 1, ReplicaNodes: []int32{1}, IsrNodes: []int32{1}},
					},
				},
			},
		}, nil
	case *listgroups.Request:
		t.groupRequests++
		return &listgroups.Response{
			ErrorCode: t.groupErrorCode,
			Groups: []listgroups.ResponseGroup{
				{GroupID: "workers", BrokerID: 1, ProtocolType: "consumer"},
			},
		}, nil
	default:
		return nil, errors.New("unexpected Kafka request")
	}
}

func connectionWithTransport(transport kafka.RoundTripper) *kafkaesque.KafkaesqueConn {
	return &kafkaesque.KafkaesqueConn{
		Client: &kafka.Client{
			Addr:      kafka.TCP("localhost:9092"),
			Transport: transport,
		},
	}
}

func TestGetKafkaInformationFetchesRequestedNativeResponses(t *testing.T) {
	transport := &recordingTransport{}
	request := InformationRequest{
		Metadata:   true,
		Topics:     true,
		Brokers:    true,
		Partitions: true,
		Consumers:  true,
		Topic:      "orders",
	}

	info, err := GetKafkaInformation(context.Background(), connectionWithTransport(transport), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(transport.metadataTopics) != 1 {
		t.Fatalf("metadata requests = %d, want 1", len(transport.metadataTopics))
	}
	if transport.metadataTopics[0] != nil {
		t.Fatalf("combined metadata topic filter = %v, want nil", transport.metadataTopics[0])
	}
	if transport.groupRequests != 1 {
		t.Fatalf("group requests = %d, want 1", transport.groupRequests)
	}
	if info.Metadata.ClusterID != "cluster-1" || info.Metadata.ControllerID != 1 || info.Metadata.BrokerCount != 1 || info.Metadata.TopicCount != 1 {
		t.Fatalf("unexpected metadata information: %+v", info.Metadata)
	}
	if info.Brokers[0].Host != "broker" {
		t.Fatalf("unexpected broker information: %+v", info.Brokers)
	}
	if info.Consumers.Groups[0].ProtocolType != "consumer" {
		t.Fatalf("unexpected native consumer response: %+v", info.Consumers)
	}
}

func TestGetKafkaInformationPartitionsOnlyFiltersTopic(t *testing.T) {
	transport := &recordingTransport{}
	request := InformationRequest{Partitions: true, Topic: "orders"}

	_, err := GetKafkaInformation(context.Background(), connectionWithTransport(transport), request)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(transport.metadataTopics, [][]string{{"orders"}}) {
		t.Fatalf("metadata topic filters = %v, want [[orders]]", transport.metadataTopics)
	}
	if transport.groupRequests != 0 {
		t.Fatalf("group requests = %d, want 0", transport.groupRequests)
	}
}

func TestGetKafkaInformationConsumersOnlySkipsMetadata(t *testing.T) {
	transport := &recordingTransport{}

	_, err := GetKafkaInformation(
		context.Background(),
		connectionWithTransport(transport),
		InformationRequest{Consumers: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(transport.metadataTopics) != 0 {
		t.Fatalf("metadata requests = %d, want 0", len(transport.metadataTopics))
	}
	if transport.groupRequests != 1 {
		t.Fatalf("group requests = %d, want 1", transport.groupRequests)
	}
}

func TestGetKafkaInformationReturnsKafkaErrors(t *testing.T) {
	t.Run("transport", func(t *testing.T) {
		transport := &recordingTransport{err: errors.New("transport failed")}
		_, err := GetKafkaInformation(
			context.Background(),
			connectionWithTransport(transport),
			InformationRequest{Metadata: true},
		)
		if err == nil || !strings.Contains(err.Error(), "transport failed") {
			t.Fatalf("error = %v, want transport failure", err)
		}
	})

	t.Run("group response", func(t *testing.T) {
		transport := &recordingTransport{groupErrorCode: 15}
		_, err := GetKafkaInformation(
			context.Background(),
			connectionWithTransport(transport),
			InformationRequest{Consumers: true},
		)
		if err == nil || !strings.Contains(err.Error(), "list groups") {
			t.Fatalf("error = %v, want group response error", err)
		}
	})
}

func TestPrintKafkaInformationPreservesOutput(t *testing.T) {
	request := InformationRequest{
		Metadata:   true,
		Topics:     true,
		Brokers:    true,
		Partitions: true,
		Consumers:  true,
		Topic:      "orders",
	}
	broker := kafka.Broker{Host: "broker", Port: 9092, ID: 1, Rack: "rack-a"}
	info := &KafkaInformation{
		Metadata: &kafkaesque_metadata.MetadataInformation{
			ClusterID:    "cluster-1",
			ControllerID: 1,
			BrokerCount:  1,
			TopicCount:   1,
		},
		Topics: []kafka.Topic{
			{
				Name: "orders",
				Partitions: []kafka.Partition{
					{ID: 0, Leader: broker, Replicas: []kafka.Broker{broker}, Isr: []kafka.Broker{broker}},
				},
			},
		},
		Brokers: []kafka.Broker{broker},
		Partitions: []kafka.Partition{
			{ID: 0, Leader: broker, Replicas: []kafka.Broker{broker}, Isr: []kafka.Broker{broker}},
		},
		Consumers: &kafka.ListGroupsResponse{
			Groups: []kafka.ListGroupsResponseGroup{
				{GroupID: "workers", Coordinator: 1, ProtocolType: "consumer"},
			},
		},
	}

	var output bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetOut(&output)
	if err := printKafkaInformation(cmd, request, info); err != nil {
		t.Fatal(err)
	}

	want := "\nKafka Cluster\n" +
		strings.Repeat("=", 80) + "\n" +
		"Cluster ID:                     cluster-1\n" +
		"Controller ID:                   broker-1\n" +
		"Brokers:                                1\n" +
		"Topics:                                 1\n" +
		"\n" +
		"\nTopic Information:\n" +
		"1. orders (Internal: false, Partitions: 1)\n" +
		"Broker Information:\n" +
		"1. Host: broker, Port: 9092, ID: 1, Rack: rack-a\n" +
		"Partition Information for topic 'orders':\n" +
		"1. Partition ID: 0 | Leader: broker:9092 | Total Replicas: 1 | Total ISR: 1\n" +
		"Consumer Information:\n" +
		"1. Group ID: workers | Coordinator: 1 | Protocol: consumer\n"
	if output.String() != want {
		t.Fatalf("output:\n%s\nwant:\n%s", output.String(), want)
	}
}

func TestPrintMetadataInformation(t *testing.T) {
	info := &kafkaesque_metadata.MetadataInformation{
		ClusterID:    "cluster-1",
		ControllerID: 1,
		BrokerCount:  2,
		TopicCount:   3,
	}

	var output bytes.Buffer
	printMetadataInformation(&output, info)

	want := "\nKafka Cluster\n" +
		strings.Repeat("=", 80) + "\n" +
		"Cluster ID:                     cluster-1\n" +
		"Controller ID:                   broker-1\n" +
		"Brokers:                                2\n" +
		"Topics:                                 3\n" +
		"\n"
	if output.String() != want {
		t.Fatalf("output:\n%q\nwant:\n%q", output.String(), want)
	}
}

func TestRootCommandWithoutFlagsShowsHelp(t *testing.T) {
	var output bytes.Buffer
	cmd := newRootCommand()
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs(nil)

	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "Usage:") || !strings.Contains(output.String(), "--metadata") {
		t.Fatalf("expected help output, got:\n%s", output.String())
	}
}

func TestRootCommandRequiresTopicForPartitions(t *testing.T) {
	cmd := newRootCommand()
	cmd.SetArgs([]string{"--partitions"})

	err := cmd.Execute()
	if err == nil || err.Error() != "please provide a topic name using the --topic flag" {
		t.Fatalf("error = %v", err)
	}
}

func TestGetPartitionInformationRejectsMissingAndErroredTopics(t *testing.T) {
	_, err := kafkaesque_partition.GetPartitionInformation(&kafka.MetadataResponse{}, "orders")
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("missing topic error = %v", err)
	}

	_, err = kafkaesque_partition.GetPartitionInformation(&kafka.MetadataResponse{
		Topics: []kafka.Topic{{Name: "orders", Error: errors.New("topic failed")}},
	}, "orders")
	if err == nil || !strings.Contains(err.Error(), "topic failed") {
		t.Fatalf("topic response error = %v", err)
	}
}
