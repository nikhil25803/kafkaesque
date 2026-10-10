package internals

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	kafkaesque_broker "github.com/nikhil25803/kafkaesque/internals/brokers"
	kafkaesque_config "github.com/nikhil25803/kafkaesque/internals/config"
	kafkaesque_consumers "github.com/nikhil25803/kafkaesque/internals/consumers"
	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
	kafkaesque_metadata "github.com/nikhil25803/kafkaesque/internals/metadata"
	kafkaesque_partition "github.com/nikhil25803/kafkaesque/internals/partitions"
	kafkaesque_topic "github.com/nikhil25803/kafkaesque/internals/topics"
	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol/describegroups"
	"github.com/segmentio/kafka-go/protocol/listgroups"
	"github.com/segmentio/kafka-go/protocol/metadata"
	"github.com/spf13/cobra"
)

type recordingTransport struct {
	metadataTopics    [][]string
	describeAddresses []string
	err               error
}

func (t *recordingTransport) RoundTrip(_ context.Context, address net.Addr, request kafka.Request) (kafka.Response, error) {
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
		return &listgroups.Response{
			Groups: []listgroups.ResponseGroup{
				{GroupID: "order-processor", ProtocolType: "consumer", BrokerID: 1},
			},
		}, nil
	case *describegroups.Request:
		t.describeAddresses = append(t.describeAddresses, address.String())
		return &describegroups.Response{
			Groups: []describegroups.ResponseGroup{
				{GroupID: request.Groups[0], GroupState: "Empty"},
			},
		}, nil
	default:
		return nil, errors.New("unexpected Kafka request")
	}
}

func TestGetKafkaInformationCompletesConsumerGroups(t *testing.T) {
	transport := &recordingTransport{}

	info, err := GetKafkaInformation(
		context.Background(),
		connectionWithTransport(transport),
		InformationRequest{Consumers: true},
	)
	if err != nil {
		t.Fatal(err)
	}

	want := []kafkaesque_consumers.ConsumerGroups{
		{GroupName: "order-processor", Type: "consumer", CoordinatorID: 1, State: "Empty"},
	}
	if !reflect.DeepEqual(info.Consumers, want) {
		t.Fatalf("consumers = %+v, want %+v", info.Consumers, want)
	}
	if !reflect.DeepEqual(transport.describeAddresses, []string{"broker:9092"}) {
		t.Fatalf("describe addresses = %v, want [broker:9092]", transport.describeAddresses)
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
	if info.Metadata.ClusterID != "cluster-1" || info.Metadata.ControllerID != 1 || info.Metadata.BrokerCount != 1 || info.Metadata.TopicCount != 1 || info.Metadata.Status != "CONNECTED" {
		t.Fatalf("unexpected metadata information: %+v", info.Metadata)
	}
	if info.Brokers[0].ID != 1 || info.Brokers[0].Address != "broker:9092" || info.Brokers[0].Rack != "rack-a" {
		t.Fatalf("unexpected broker information: %+v", info.Brokers)
	}
	if len(info.Topics) != 1 || info.Topics[0] != (kafkaesque_topic.TopicInformation{
		Name:              "orders",
		Level:             "External",
		PartitionCount:    1,
		ReplicationFactor: 1,
	}) {
		t.Fatalf("unexpected topic information: %+v", info.Topics)
	}
	if len(info.Partitions) != 1 || info.Partitions[0] != (kafkaesque_partition.PartitionTopicInformation{
		TopicName:   "orders",
		PartitionID: 0,
		Leader:      "broker-1",
		Replicas:    1,
		Isr:         1,
	}) {
		t.Fatalf("unexpected partition information: %+v", info.Partitions)
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

}

func TestPrintKafkaInformationPreservesOutput(t *testing.T) {
	request := InformationRequest{
		Metadata:   true,
		Topics:     true,
		Brokers:    true,
		Partitions: true,
		Topic:      "orders",
	}
	info := &KafkaInformation{
		Metadata: &kafkaesque_metadata.MetadataInformation{
			ClusterID:    "cluster-1",
			ControllerID: 1,
			BrokerCount:  1,
			TopicCount:   1,
			Status:       "CONNECTED",
		},
		Topics: []kafkaesque_topic.TopicInformation{
			{Name: "orders", Level: "External", PartitionCount: 1, ReplicationFactor: 1},
		},
		Brokers: []kafkaesque_broker.BrokerInformation{
			{ID: 1, Address: "broker:9092", Rack: "rack-a"},
		},
		Partitions: []kafkaesque_partition.PartitionTopicInformation{
			{TopicName: "orders", PartitionID: 0, Leader: "broker-1", Replicas: 1, Isr: 1},
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
		"STATUS: CONNECTED\n" +
		"\n" +
		"\nKafka Brokers\n" +
		strings.Repeat("=", 80) + "\n" +
		"ID   ADDRESS          RACK    \n" +
		"1    broker:9092      rack-a  \n" +
		"\n1 broker available\n" +
		"\nKafka Topics\n" +
		strings.Repeat("=", 80) + "\n" +
		"ID   NAME                             LEVEL        PARTITIONS       REPLICATION FACTOR  \n" +
		"1    orders                           External     1                1                   \n" +
		"\n1 topic available\n" +
		"\nTopic: orders\n" +
		strings.Repeat("=", 80) + "\n" +
		"PARTITION  LEADER                           REPLICAS     ISR             \n" +
		"0          broker-1                         1            1               \n"
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
		Status:       "CONNECTED",
	}

	var output bytes.Buffer
	printMetadataInformation(&output, info)

	want := "\nKafka Cluster\n" +
		strings.Repeat("=", 80) + "\n" +
		"Cluster ID:                     cluster-1\n" +
		"Controller ID:                   broker-1\n" +
		"Brokers:                                2\n" +
		"Topics:                                 3\n" +
		"\n" +
		"STATUS: CONNECTED\n" +
		"\n"
	if output.String() != want {
		t.Fatalf("output:\n%q\nwant:\n%q", output.String(), want)
	}
}

func TestPrintBrokersInformation(t *testing.T) {
	tests := []struct {
		name    string
		brokers []kafkaesque_broker.BrokerInformation
		want    string
	}{
		{
			name: "singular",
			brokers: []kafkaesque_broker.BrokerInformation{
				{ID: 1, Address: "localhost:9092"},
			},
			want: "\nKafka Brokers\n" +
				strings.Repeat("=", 80) + "\n" +
				"ID   ADDRESS          RACK    \n" +
				"1    localhost:9092           \n" +
				"\n1 broker available\n",
		},
		{
			name: "plural",
			brokers: []kafkaesque_broker.BrokerInformation{
				{ID: 1, Address: "broker-1:9092", Rack: "rack-a"},
				{ID: 2, Address: "broker-2:9092", Rack: "rack-b"},
			},
			want: "\nKafka Brokers\n" +
				strings.Repeat("=", 80) + "\n" +
				"ID   ADDRESS          RACK    \n" +
				"1    broker-1:9092    rack-a  \n" +
				"2    broker-2:9092    rack-b  \n" +
				"\n2 brokers available\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			printBrokersInformation(&output, test.brokers)
			if output.String() != test.want {
				t.Fatalf("output:\n%q\nwant:\n%q", output.String(), test.want)
			}
		})
	}
}

func TestPrintTopicsInformation(t *testing.T) {
	tests := []struct {
		name   string
		topics []kafkaesque_topic.TopicInformation
		want   string
	}{
		{
			name: "singular",
			topics: []kafkaesque_topic.TopicInformation{
				{Name: "orders", Level: "External", PartitionCount: 6, ReplicationFactor: 3},
			},
			want: "\nKafka Topics\n" +
				strings.Repeat("=", 80) + "\n" +
				"ID   NAME                             LEVEL        PARTITIONS       REPLICATION FACTOR  \n" +
				"1    orders                           External     6                3                   \n" +
				"\n1 topic available\n",
		},
		{
			name: "plural",
			topics: []kafkaesque_topic.TopicInformation{
				{Name: "orders", Level: "External", PartitionCount: 6, ReplicationFactor: 3},
				{Name: "__consumer_offsets", Level: "Internal", PartitionCount: 50, ReplicationFactor: 3},
			},
			want: "\nKafka Topics\n" +
				strings.Repeat("=", 80) + "\n" +
				"ID   NAME                             LEVEL        PARTITIONS       REPLICATION FACTOR  \n" +
				"1    orders                           External     6                3                   \n" +
				"2    __consumer_offsets               Internal     50               3                   \n" +
				"\n2 topics available\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			printTopicsInformation(&output, test.topics)
			if output.String() != test.want {
				t.Fatalf("output:\n%q\nwant:\n%q", output.String(), test.want)
			}
		})
	}
}

func TestPrintPartitionsInformation(t *testing.T) {
	partitions := []kafkaesque_partition.PartitionTopicInformation{
		{TopicName: "orders", PartitionID: 0, Leader: "broker-1", Replicas: 3, Isr: 3},
		{TopicName: "orders", PartitionID: 1, Leader: "broker-2", Replicas: 3, Isr: 2},
	}

	var output bytes.Buffer
	printPartitionsInformation(&output, "orders", partitions)

	want := "\nTopic: orders\n" +
		strings.Repeat("=", 80) + "\n" +
		"PARTITION  LEADER                           REPLICAS     ISR             \n" +
		"0          broker-1                         3            3               \n" +
		"1          broker-2                         3            2               \n"
	if output.String() != want {
		t.Fatalf("output:\n%q\nwant:\n%q", output.String(), want)
	}
}

func TestPrintConsumersInformation(t *testing.T) {
	consumerGroups := []kafkaesque_consumers.ConsumerGroups{
		{
			GroupName:     "order-processor",
			Type:          "consumer",
			CoordinatorID: 1,
			State:         "Empty",
			MembersCount:  0,
		},
	}

	var output bytes.Buffer
	printConsumersInformation(&output, consumerGroups)

	want := "\nKafka Consumers\n" +
		strings.Repeat("=", 100) + "\n" +
		"GROUP NAME                       TYPE         COORDINATOR      STATE                MEMBERS COUNT   \n" +
		"order-processor                  consumer     broker-1         Empty                0               \n" +
		"\n1 consumer group available\n"
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

func TestRootCommandExposesConsumers(t *testing.T) {
	var output bytes.Buffer
	cmd := newRootCommand()
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs([]string{"--help"})

	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "-c, --consumers") {
		t.Fatalf("consumer flag is missing from help:\n%s", output.String())
	}
}

func TestRootCommandChecksConfiguration(t *testing.T) {
	t.Setenv(kafkaesque_config.BootstrapServerEnv, "localhost:9092")
	var output bytes.Buffer
	cmd := newRootCommand()
	cmd.SetOut(&output)
	cmd.SetArgs([]string{"--check", "config", "--config", writeRootConfig(t, "localhost:9092")})

	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if output.String() != "Configuration is valid\n" {
		t.Fatalf("output = %q", output.String())
	}
}

func TestRootCommandChecksKafkaConnection(t *testing.T) {
	var output bytes.Buffer
	var checkedAddress string
	cmd := newRootCommandWithConnectionCheck(func(_ context.Context, address string) error {
		checkedAddress = address
		return nil
	})
	cmd.SetOut(&output)
	address := "broker.example.com:9092"
	t.Setenv(kafkaesque_config.BootstrapServerEnv, address)
	cmd.SetArgs([]string{"--check", "conn", "--config", writeRootConfig(t, address)})

	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if output.String() != "Kafka connection successful: "+address+"\n" {
		t.Fatalf("output = %q", output.String())
	}
	if checkedAddress != address {
		t.Fatalf("checked address = %q, want %q", checkedAddress, address)
	}
}

func TestRootCommandRejectsInvalidChecks(t *testing.T) {
	t.Run("unknown check", func(t *testing.T) {
		cmd := newRootCommand()
		cmd.SetArgs([]string{"--check", "unknown"})

		err := cmd.Execute()
		if err == nil || !strings.Contains(err.Error(), "must be config or conn") {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("combined with information flag", func(t *testing.T) {
		cmd := newRootCommand()
		cmd.SetArgs([]string{"--check", "config", "--metadata"})

		err := cmd.Execute()
		if err == nil || !strings.Contains(err.Error(), "cannot be combined") {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestRootCommandReturnsConnectionCheckErrors(t *testing.T) {
	address := "broker.example.com:9092"
	t.Setenv(kafkaesque_config.BootstrapServerEnv, address)
	cmd := newRootCommandWithConnectionCheck(func(context.Context, string) error {
		return errors.New("connection refused")
	})
	cmd.SetArgs([]string{"--check", "conn", "--config", writeRootConfig(t, address)})

	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "failed to connect to Kafka at "+address) {
		t.Fatalf("error = %v", err)
	}
}

func writeRootConfig(t *testing.T, bootstrapServer string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	content := "kafka:\n  bootstrap_server: " + bootstrapServer + "\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
