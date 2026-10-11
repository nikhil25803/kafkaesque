package internals

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	kafkaesque_broker "github.com/nikhil25803/kafkaesque/internals/brokers"
	kafkaesque_config "github.com/nikhil25803/kafkaesque/internals/config"
	kafkaesque_consumers "github.com/nikhil25803/kafkaesque/internals/consumers"
	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
	kafkaesque_metadata "github.com/nikhil25803/kafkaesque/internals/metadata"
	kafkaesque_partition "github.com/nikhil25803/kafkaesque/internals/partitions"
	kafkaesque_topic "github.com/nikhil25803/kafkaesque/internals/topics"
	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol/describegroups"
	"github.com/segmentio/kafka-go/protocol/findcoordinator"
	"github.com/segmentio/kafka-go/protocol/listgroups"
	"github.com/segmentio/kafka-go/protocol/listoffsets"
	"github.com/segmentio/kafka-go/protocol/metadata"
	"github.com/segmentio/kafka-go/protocol/offsetfetch"
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
	case *findcoordinator.Request:
		return &findcoordinator.Response{NodeID: 1, Host: "broker", Port: 9092}, nil
	case *offsetfetch.Request:
		return &offsetfetch.Response{Topics: []offsetfetch.ResponseTopic{
			{Name: "orders", Partitions: []offsetfetch.ResponsePartition{{PartitionIndex: 0, CommittedOffset: 0}}},
		}}, nil
	case *listoffsets.Request:
		return &listoffsets.Response{Topics: []listoffsets.ResponseTopic{
			{Topic: "orders", Partitions: []listoffsets.ResponsePartition{
				{Partition: 0, Timestamp: kafka.LastOffset, Offset: 0},
			}},
		}}, nil
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
	if info.Brokers[0].ID != 1 || info.Brokers[0].Address != "broker:9092" || info.Brokers[0].ConnectAddress != "broker:9092" || info.Brokers[0].Rack != "rack-a" {
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

func TestGetKafkaInformationConsumerTopicUsesOneFilteredMetadataRequest(t *testing.T) {
	transport := &recordingTransport{}
	request := InformationRequest{Consumer: true, Group: "orders-service", Topic: "orders"}

	info, err := GetKafkaInformation(context.Background(), connectionWithTransport(transport), request)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(transport.metadataTopics, [][]string{{"orders"}}) {
		t.Fatalf("metadata topic filters = %v, want [[orders]]", transport.metadataTopics)
	}
	want := &kafkaesque_consumers.ConsumerGroupTopicInformation{
		GroupName: "orders-service",
		Topic:     "orders",
		Partitions: []kafkaesque_consumers.ConsumerGroupPartitionLag{
			{Partition: 0, CommittedOffset: 0, LogEndOffset: 0, Lag: 0},
		},
		TotalLag: 0,
	}
	if !reflect.DeepEqual(info.ConsumerTopic, want) {
		t.Fatalf("consumer topic = %+v, want %+v", info.ConsumerTopic, want)
	}
}

func TestGetKafkaInformationCombinesMetadataAndConsumerTopicRequest(t *testing.T) {
	transport := &recordingTransport{}
	request := InformationRequest{
		Metadata: true,
		Consumer: true,
		Group:    "orders-service",
		Topic:    "orders",
	}

	info, err := GetKafkaInformation(context.Background(), connectionWithTransport(transport), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(transport.metadataTopics) != 1 || transport.metadataTopics[0] != nil {
		t.Fatalf("metadata requests = %v, want one unfiltered request", transport.metadataTopics)
	}
	if info.Metadata == nil || info.ConsumerTopic == nil {
		t.Fatalf("combined information is incomplete: %+v", info)
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
		{
			name: "address override",
			brokers: []kafkaesque_broker.BrokerInformation{
				{
					ID:             0,
					Address:        "broker-0.broker-headless.kafka-connect.svc.cluster.local:9092",
					ConnectAddress: "10.100.0.72:9094",
				},
			},
			want: "\nKafka Brokers\n" +
				strings.Repeat("=", 120) + "\n" +
				"ID   ADVERTISED ADDRESS                                                       CONNECT ADDRESS          RACK    \n" +
				"0    broker-0.broker-headless.kafka-connect.svc.cluster.local:9092            10.100.0.72:9094                 \n" +
				"\n1 broker available\n",
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
			TopicsCount:   0,
		},
	}

	var output bytes.Buffer
	printConsumersInformation(&output, consumerGroups)

	want := "\nKafka Consumers\n" +
		strings.Repeat("=", 117) + "\n" +
		"GROUP NAME                       TYPE         COORDINATOR      STATE                MEMBERS COUNT    TOPICS          \n" +
		"order-processor                  consumer     broker-1         Empty                0                0               \n" +
		"\n1 group · 0 topics · 0 members\n"
	if output.String() != want {
		t.Fatalf("output:\n%q\nwant:\n%q", output.String(), want)
	}
}

func TestPrintConsumersInformationTotals(t *testing.T) {
	consumerGroups := []kafkaesque_consumers.ConsumerGroups{
		{GroupName: "order-processor", MembersCount: 2, TopicsCount: 1},
		{GroupName: "notification-dispatcher", MembersCount: 3, TopicsCount: 2},
	}

	var output bytes.Buffer
	printConsumersInformation(&output, consumerGroups)

	if !strings.HasSuffix(output.String(), "\n2 groups · 3 topics · 5 members\n") {
		t.Fatalf("unexpected summary:\n%s", output.String())
	}
}

func TestPrintConsumerGroupInformation(t *testing.T) {
	consumer := &kafkaesque_consumers.ConsumerGroupInformation{
		GroupName:    "orders-service",
		State:        "Stable",
		MembersCount: 3,
		TopicsCount:  2,
		TotalLag:     136,
		Topics: []kafkaesque_consumers.ConsumerGroupTopicLag{
			{Topic: "orders", Partitions: 6, Lag: 124, Status: "WARNING"},
			{Topic: "payments", Partitions: 3, Lag: 12, Status: "HEALTHY"},
		},
	}

	var output bytes.Buffer
	printConsumerGroupInformation(&output, consumer)

	want := "\nConsumer Group: orders-service\n" +
		strings.Repeat("=", 80) + "\n" +
		"STATE: STABLE\n" +
		"MEMBERS: 3\n" +
		"TOPICS: 2\n" +
		"TOTAL LAG: 136\n" +
		"\nTOPICS\n" +
		strings.Repeat("=", 80) + "\n" +
		"TOPIC                            PARTITIONS   LAG        STATUS    \n" +
		"orders                           6            124        WARNING   \n" +
		"payments                         3            12         HEALTHY   \n"
	if output.String() != want {
		t.Fatalf("output:\n%q\nwant:\n%q", output.String(), want)
	}
}

func TestPrintConsumerGroupTopicInformation(t *testing.T) {
	topic := &kafkaesque_consumers.ConsumerGroupTopicInformation{
		GroupName: "orders-service",
		Topic:     "orders",
		Partitions: []kafkaesque_consumers.ConsumerGroupPartitionLag{
			{Partition: 0, CommittedOffset: 128932, LogEndOffset: 129056, Lag: 124},
			{Partition: 1, CommittedOffset: 98231, LogEndOffset: 98231, Lag: 0},
			{Partition: 2, CommittedOffset: 78291, LogEndOffset: 78452, Lag: 161},
			{Partition: 3, CommittedOffset: 91231, LogEndOffset: 91240, Lag: 9},
			{Partition: 4, CommittedOffset: 93111, LogEndOffset: 93111, Lag: 0},
			{Partition: 5, CommittedOffset: 88421, LogEndOffset: 88421, Lag: 0},
		},
		TotalLag: 294,
	}

	var output bytes.Buffer
	printConsumerGroupTopicInformation(&output, topic)

	want := "\nConsumer Group: orders-service\n" +
		"Topic: orders\n" +
		strings.Repeat("=", 80) + "\n" +
		"PARTITION    COMMITTED OFFSET    LOG END OFFSET    LAG         \n" +
		"0            128932              129056            124         \n" +
		"1            98231               98231             0           \n" +
		"2            78291               78452             161         \n" +
		"3            91231               91240             9           \n" +
		"4            93111               93111             0           \n" +
		"5            88421               88421             0           \n" +
		"TOTAL 294\n"
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

func TestRootCommandVersion(t *testing.T) {
	previousVersion := Version
	Version = "v0.1.1"
	t.Cleanup(func() { Version = previousVersion })

	var output bytes.Buffer
	cmd := newRootCommand()
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs([]string{"--version"})

	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if output.String() != "kafkaesque version v0.1.1\n" {
		t.Fatalf("version output = %q", output.String())
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
	if !strings.Contains(output.String(), "--consumer") || !strings.Contains(output.String(), "--group") {
		t.Fatalf("consumer detail flags are missing from help:\n%s", output.String())
	}
	consumerIndex := strings.Index(output.String(), "      --consumer ")
	groupIndex := strings.Index(output.String(), "      --group string")
	if consumerIndex == -1 || groupIndex == -1 || consumerIndex > groupIndex {
		t.Fatalf("--group should follow --consumer in help:\n%s", output.String())
	}
}

func TestRootCommandValidatesConsumerDetailFlags(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "missing group", args: []string{"--consumer"}, want: "please provide a consumer group name using the --group flag"},
		{name: "group without consumer", args: []string{"--group", "orders-service"}, want: "--group requires --consumer"},
		{name: "list and detail", args: []string{"--consumers", "--consumer", "--group", "orders-service"}, want: "--consumer cannot be combined with --consumers"},
		{name: "topic without inspection", args: []string{"--topic", "orders"}, want: "--topic requires --partitions or --consumer"},
		{name: "topic with consumer list", args: []string{"--consumers", "--topic", "orders"}, want: "--topic requires --partitions or --consumer"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd := newRootCommand()
			cmd.SetArgs(test.args)
			err := cmd.Execute()
			if err == nil || err.Error() != test.want {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
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
	var checkedAddresses []string
	cmd := newRootCommandWithConnectionCheck(func(_ context.Context, cfg kafkaesque_config.KafkaConfig) (*kafkaesque.CheckResult, error) {
		checkedAddresses = cfg.BootstrapServers
		return &kafkaesque.CheckResult{
			BrokerCount: 3, ControllerID: 2, TopicCount: 24, PartitionCount: 186, Latency: 42 * time.Millisecond,
		}, nil
	})
	cmd.SetOut(&output)
	address := "broker.example.com:9092"
	t.Setenv(kafkaesque_config.BootstrapServerEnv, address)
	cmd.SetArgs([]string{"--check", "--config", writeRootConfig(t, address)})

	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	want := "Kafka Connection\n" + strings.Repeat("=", 80) + "\n" +
		"Bootstrap Servers\n  " + address + "\n" +
		"Security\n  Protocol: PLAINTEXT\n" +
		"Status: CONNECTED\nCluster\n" +
		"  Brokers: 3\n  Controller: broker-2\n  Topics: 24\n  Partitions: 186\n" +
		"Connection latency: 42ms\n"
	if output.String() != want {
		t.Fatalf("output = %q", output.String())
	}
	if !reflect.DeepEqual(checkedAddresses, []string{address}) {
		t.Fatalf("checked addresses = %q, want %q", checkedAddresses, address)
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
	var output bytes.Buffer
	cmd := newRootCommandWithConnectionCheck(func(context.Context, kafkaesque_config.KafkaConfig) (*kafkaesque.CheckResult, error) {
		return nil, errors.New("connection refused")
	})
	cmd.SetOut(&output)
	cmd.SetArgs([]string{"--check", "conn", "--config", writeRootConfig(t, address)})

	err := cmd.Execute()
	if err == nil || ExitCode(err) != 1 || !ErrorWasReported(err) {
		t.Fatalf("error = %v", err)
	}
	if !strings.Contains(output.String(), "Status: FAILED") || !strings.Contains(output.String(), "Reason:\n  connection failed") || !strings.Contains(output.String(), "Exit code: 1") {
		t.Fatalf("output = %q", output.String())
	}
}

func TestRootCommandAcceptsConnectionCheckSyntaxes(t *testing.T) {
	for _, args := range [][]string{
		{"--check"},
		{"--check", "conn"},
		{"--check=conn"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			cmd := newRootCommandWithConnectionCheck(func(context.Context, kafkaesque_config.KafkaConfig) (*kafkaesque.CheckResult, error) {
				return &kafkaesque.CheckResult{}, nil
			})
			cmd.SetOut(&bytes.Buffer{})
			cmd.SetArgs(append(args, "--config", writeRootConfig(t, "localhost:9092")))
			if err := cmd.Execute(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestConnectionCheckErrorClassification(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantCode   int
		wantReason string
	}{
		{name: "timeout", err: context.DeadlineExceeded, wantCode: 3, wantReason: "connection timeout"},
		{name: "SASL", err: fmt.Errorf("authenticate: %w", kafka.SASLAuthenticationFailed), wantCode: 2, wantReason: "SASL authentication failed"},
		{name: "authorization", err: fmt.Errorf("metadata: %w", kafka.TopicAuthorizationFailed), wantCode: 2, wantReason: "Kafka authorization failed"},
		{name: "TLS", err: errors.New("tls: failed to verify certificate"), wantCode: 2, wantReason: "TLS authentication failed"},
		{name: "network", err: errors.New("connection refused"), wantCode: 1, wantReason: "connection failed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			code, reason := classifyConnectionCheckError(test.err)
			if code != test.wantCode || reason != test.wantReason {
				t.Fatalf("classification = %d/%q, want %d/%q", code, reason, test.wantCode, test.wantReason)
			}
		})
	}
}

func TestConnectionCheckFailureRedactsPassword(t *testing.T) {
	var output bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetOut(&output)
	cfg := kafkaesque_config.KafkaConfig{
		BootstrapServers:  []string{"localhost:9092"},
		ConnectionTimeout: 5 * time.Second,
		Security: kafkaesque_config.SecurityConfig{SASL: kafkaesque_config.SASLConfig{
			Mechanism: "PLAIN",
			Username:  "user",
			Password:  "top-secret",
		}},
	}
	printConnectionCheckFailure(cmd, cfg, "SASL authentication failed", errors.New("rejected top-secret"), 2)
	if strings.Contains(output.String(), "top-secret") || !strings.Contains(output.String(), "[REDACTED]") {
		t.Fatalf("output = %q", output.String())
	}
}

func TestSecurityProtocol(t *testing.T) {
	tests := []struct {
		name     string
		security kafkaesque_config.SecurityConfig
		want     string
	}{
		{name: "plaintext", want: "PLAINTEXT"},
		{name: "TLS", security: kafkaesque_config.SecurityConfig{TLS: kafkaesque_config.TLSConfig{Enabled: true}}, want: "SSL"},
		{name: "SASL", security: kafkaesque_config.SecurityConfig{SASL: kafkaesque_config.SASLConfig{Mechanism: "PLAIN"}}, want: "SASL_PLAINTEXT"},
		{name: "SASL TLS", security: kafkaesque_config.SecurityConfig{TLS: kafkaesque_config.TLSConfig{Enabled: true}, SASL: kafkaesque_config.SASLConfig{Mechanism: "PLAIN"}}, want: "SASL_SSL"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := securityProtocol(kafkaesque_config.KafkaConfig{Security: test.security}); got != test.want {
				t.Fatalf("protocol = %q, want %q", got, test.want)
			}
		})
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
