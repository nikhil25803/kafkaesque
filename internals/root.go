package internals

import (
	"context"
	"fmt"
	"io"
	"strings"

	kafkaesque_broker "github.com/nikhil25803/kafkaesque/internals/brokers"
	kafkaesque_config "github.com/nikhil25803/kafkaesque/internals/config"
	kafkaesque_consumer "github.com/nikhil25803/kafkaesque/internals/consumers"
	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
	kafkaesque_metadata "github.com/nikhil25803/kafkaesque/internals/metadata"
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
	Metadata   *kafkaesque_metadata.MetadataInformation
	Topics     []kafka.Topic
	Brokers    []kafkaesque_broker.BrokerInformation
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
		if request.Metadata {
			info.Metadata = kafkaesque_metadata.GetMetadataInformation(metadata)
		}

		if request.Brokers {
			info.Brokers = kafkaesque_broker.GetBrokerInformation(metadata)
		}

		if request.Topics {
			info.Topics = kafkaesque_topic.GetTopicInformation(metadata)
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
		printMetadataInformation(out, info.Metadata)
	}

	if request.Brokers {
		printBrokersInformation(out, info.Brokers)
	}

	if request.Topics {
		fmt.Fprintln(out, "\nTopic Information:")
		for i, topic := range info.Topics {
			fmt.Fprintf(out, "%d. %s (Internal: %t, Partitions: %d)\n", i+1, topic.Name, topic.Internal, len(topic.Partitions))
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

func printMetadataInformation(out io.Writer, info *kafkaesque_metadata.MetadataInformation) {
	fmt.Fprintln(out, "\nKafka Cluster")
	fmt.Fprintln(out, strings.Repeat("=", 80))
	fmt.Fprintf(out, "%-20s %20s\n", "Cluster ID:", info.ClusterID)
	fmt.Fprintf(out, "%-20s %20s\n", "Controller ID:", "broker-"+fmt.Sprint(info.ControllerID))
	fmt.Fprintf(out, "%-20s %20d\n", "Brokers:", info.BrokerCount)
	fmt.Fprintf(out, "%-20s %20d\n", "Topics:", info.TopicCount)
	fmt.Fprintf(out, "STATUS: %s\n", info.Status)
	fmt.Fprintln(out)
}

func printBrokersInformation(out io.Writer, brokers []kafkaesque_broker.BrokerInformation) {
	fmt.Fprintln(out, "\nKafka Brokers")
	fmt.Fprintln(out, strings.Repeat("=", 80))

	fmt.Fprintf(out, "%-4s %-16s %-8s\n",
		"ID", "ADDRESS", "RACK",
	)

	for _, broker := range brokers {
		fmt.Fprintf(out, "%-4d %-16s %-8s\n",
			broker.ID,
			broker.Address,
			broker.Rack,
		)
	}

	noun := "brokers"
	if len(brokers) == 1 {
		noun = "broker"
	}
	fmt.Fprintf(out, "\n%d %s available\n", len(brokers), noun)
}

func newRootCommand() *cobra.Command {
	return newRootCommandWithConnectionCheck(checkKafkaConnection)
}

func newRootCommandWithConnectionCheck(checkConnection func(context.Context, string) error) *cobra.Command {
	var request InformationRequest
	var configPath string
	var check string

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
			if check == "" && !request.any() {
				return cmd.Help()
			}
			if check != "" && request.any() {
				return fmt.Errorf("--check cannot be combined with information flags")
			}
			if check != "" && check != "config" && check != "conn" {
				return fmt.Errorf("invalid --check value %q: must be config or conn", check)
			}
			if request.Partitions && request.Topic == "" {
				return fmt.Errorf("please provide a topic name using the --topic flag")
			}

			cfg, err := kafkaesque_config.Load(configPath)
			if err != nil {
				return fmt.Errorf("failed to load configuration: %w", err)
			}
			if check == "config" {
				fmt.Fprintln(cmd.OutOrStdout(), "Configuration is valid")
				return nil
			}

			ctx := cmd.Context()
			if check == "conn" {
				if err := checkConnection(ctx, cfg.Kafka.BootstrapServer); err != nil {
					return fmt.Errorf("failed to connect to Kafka at %s: %w", cfg.Kafka.BootstrapServer, err)
				}
				fmt.Fprintf(cmd.OutOrStdout(), "Kafka connection successful: %s\n", cfg.Kafka.BootstrapServer)
				return nil
			}

			conn, err := kafkaesque.Connect(ctx, cfg.Kafka.BootstrapServer)
			if err != nil {
				return fmt.Errorf("failed to connect to Kafka at %s: %w", cfg.Kafka.BootstrapServer, err)
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
	cmd.Flags().StringVar(&configPath, "config", "", "Path to the YAML configuration file")
	cmd.Flags().StringVar(&check, "check", "", "Check configuration or Kafka connection (config|conn)")

	return cmd
}

func checkKafkaConnection(ctx context.Context, bootstrapServer string) error {
	conn, err := kafkaesque.Connect(ctx, bootstrapServer)
	if err != nil {
		return err
	}
	return conn.Close()
}

var rootCmd = newRootCommand()

// Execute runs the kafkaesque CLI and returns command errors to the caller.
func Execute() error {
	return rootCmd.Execute()
}
