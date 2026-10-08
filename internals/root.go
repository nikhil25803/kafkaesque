package internals

import (
	"context"
	"fmt"
	"strings"

	kafkaesque_broker "github.com/nikhil25803/kafkaesque/internals/brokers"
	kafkaesque_consumer "github.com/nikhil25803/kafkaesque/internals/consumers"
	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
	kafkaesque_partition "github.com/nikhil25803/kafkaesque/internals/partitions"
	kafkaesque_topic "github.com/nikhil25803/kafkaesque/internals/topics"
	kafka "github.com/segmentio/kafka-go"
	"github.com/spf13/cobra"
)

// InformationRequest identifies the Kafka information the caller needs.
type InformationRequest struct {
	Metadata   bool
	Topics     bool
	Brokers    bool
	Partitions bool
	Consumers  bool
	Topic      string
}

func (r InformationRequest) any() bool {
	return r.Metadata || r.Topics || r.Brokers || r.Partitions || r.Consumers
}

func (r InformationRequest) needsMetadata() bool {
	return r.Metadata || r.Topics || r.Brokers || r.Partitions
}

// KafkaInformation contains the requested Kafka data.
type KafkaInformation struct {
	Metadata   *kafka.MetadataResponse
	Topics     []kafka.Topic
	Brokers    []kafka.Broker
	Partitions []kafka.Partition
	Consumers  *kafka.ListGroupsResponse
}

// GetKafkaInformation fetches the requested Kafka data.
func GetKafkaInformation(
	ctx context.Context,
	conn *kafkaesque.KafkaesqueConn,
	request InformationRequest,
) (*KafkaInformation, error) {
	if request.Partitions && request.Topic == "" {
		return nil, fmt.Errorf("please provide a topic name using the --topic flag")
	}

	info := &KafkaInformation{}
	if request.needsMetadata() {
		var topics []string
		if request.Partitions && !request.Metadata && !request.Topics && !request.Brokers {
			topics = []string{request.Topic}
		}

		metadata, err := conn.GetMetadata(ctx, topics)
		if err != nil {
			return nil, fmt.Errorf("failed to get cluster metadata: %w", err)
		}
		info.Metadata = metadata

		if request.Topics {
			info.Topics = kafkaesque_topic.GetTopicInformation(metadata)
		}
		if request.Brokers {
			info.Brokers = kafkaesque_broker.GetBrokerInformation(metadata)
		}
		if request.Partitions {
			partitions, err := kafkaesque_partition.GetPartitionInformation(metadata, request.Topic)
			if err != nil {
				return nil, fmt.Errorf("failed to get partition information for topic %s: %w", request.Topic, err)
			}
			info.Partitions = partitions
		}
	}

	if request.Consumers {
		consumers, err := kafkaesque_consumer.GetConsumerInformation(conn, ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get consumer information: %w", err)
		}
		info.Consumers = consumers
	}

	return info, nil
}

func printKafkaInformation(cmd *cobra.Command, request InformationRequest, info *KafkaInformation) error {
	out := cmd.OutOrStdout()

	if request.Metadata {
		fmt.Fprintf(out, "\nKafka Cluster\n")
		fmt.Fprintln(out, strings.Repeat("=", 80))
		fmt.Fprintf(out, "%-20s %20s\n", "Cluster ID:", info.Metadata.ClusterID)
		fmt.Fprintf(out, "%-20s %20s\n", "Controller ID:", "broker-"+fmt.Sprint(info.Metadata.Controller.ID))
		fmt.Fprintf(out, "%-20s %20d\n", "Brokers:", len(info.Metadata.Brokers))
		fmt.Fprintf(out, "%-20s %20d\n", "Topics:", len(info.Metadata.Topics))
		fmt.Println()
	}

	if request.Topics {
		fmt.Fprintln(out, "\nTopic Information:")
		for i, topic := range info.Topics {
			fmt.Fprintf(out, "%d. %s (Internal: %t, Partitions: %d)\n", i+1, topic.Name, topic.Internal, len(topic.Partitions))
		}
	}

	if request.Brokers {
		fmt.Fprintln(out, "Broker Information:")
		for i, broker := range info.Brokers {
			fmt.Fprintf(out, "%d. Host: %s, Port: %d, ID: %d, Rack: %s\n", i+1, broker.Host, broker.Port, broker.ID, broker.Rack)
		}
	}

	if request.Partitions {
		fmt.Fprintf(out, "Partition Information for topic '%s':\n", request.Topic)
		for i, partition := range info.Partitions {
			fmt.Fprintf(out, "%d. Partition ID: %d | Leader: %s:%d | Total Replicas: %v | Total ISR: %v\n",
				i+1,
				partition.ID,
				partition.Leader.Host,
				partition.Leader.Port,
				len(partition.Replicas),
				len(partition.Isr),
			)
		}
	}

	if request.Consumers {
		fmt.Fprintln(out, "Consumer Information:")
		for i, consumer := range info.Consumers.Groups {
			fmt.Fprintf(out, "%d. Group ID: %s | Coordinator: %d | Protocol: %s\n", i+1, consumer.GroupID, consumer.Coordinator, consumer.ProtocolType)
		}
	}

	return nil
}

func newRootCommand() *cobra.Command {
	var request InformationRequest

	cmd := &cobra.Command{
		Use:   "kafkaesque",
		Short: "Kafkaesque is a tool for interacting with Kafka clusters.",
		Long: `Kafkaesque is a lightweight, read-only Kafka observability tool focused primarily on
consumer groups, partition offsets, consumer lag, continuous monitoring, a server-rendered HTMX web
UI, and Slack-based alerting.`,
		Args:          cobra.NoArgs,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if !request.any() {
				return cmd.Help()
			}
			if request.Partitions && request.Topic == "" {
				return fmt.Errorf("please provide a topic name using the --topic flag")
			}

			ctx := cmd.Context()
			conn, err := kafkaesque.Connect(ctx, "localhost:9092")
			if err != nil {
				return fmt.Errorf("failed to connect to Kafka: %w", err)
			}
			defer conn.Close()

			info, err := GetKafkaInformation(ctx, conn, request)
			if err != nil {
				return fmt.Errorf("failed to get Kafka information: %w", err)
			}

			return printKafkaInformation(cmd, request, info)
		},
	}

	cmd.Flags().BoolVarP(&request.Metadata, "metadata", "m", false, "Retrieve cluster metadata")
	cmd.Flags().BoolVarP(&request.Topics, "topics", "t", false, "Retrieve topic information")
	cmd.Flags().BoolVarP(&request.Brokers, "brokers", "b", false, "Retrieve broker information")
	cmd.Flags().BoolVarP(&request.Partitions, "partitions", "p", false, "Retrieve partition information for a topic")
	cmd.Flags().StringVar(&request.Topic, "topic", "", "Topic name")
	cmd.Flags().BoolVarP(&request.Consumers, "consumers", "c", false, "Retrieve consumer information")

	return cmd
}

var rootCmd = newRootCommand()

// Execute runs the kafkaesque CLI and returns command errors to the caller.
func Execute() error {
	return rootCmd.Execute()
}
