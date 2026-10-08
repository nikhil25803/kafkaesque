package internals

import (
	"fmt"
	"io"
	"strings"

	kafkaesque_broker "github.com/nikhil25803/kafkaesque/internals/brokers"
	kafkaesque_metadata "github.com/nikhil25803/kafkaesque/internals/metadata"
	kafkaesque_topic "github.com/nikhil25803/kafkaesque/internals/topics"
	kafka "github.com/segmentio/kafka-go"
	"github.com/spf13/cobra"
)

func printKafkaInformation(cmd *cobra.Command, request InformationRequest, info *KafkaInformation) error {
	out := cmd.OutOrStdout()

	if request.Metadata {
		printMetadataInformation(out, info.Metadata)
	}
	if request.Brokers {
		printBrokersInformation(out, info.Brokers)
	}
	if request.Topics {
		printTopicsInformation(out, info.Topics)
	}
	if request.Partitions {
		printPartitionsInformation(out, request.Topic, info.Partitions)
	}
	if request.Consumers {
		printConsumersInformation(out, info.Consumers)
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
	fmt.Fprintln(out)
	fmt.Fprintf(out, "STATUS: %s\n", info.Status)
	fmt.Fprintln(out)
}

func printBrokersInformation(out io.Writer, brokers []kafkaesque_broker.BrokerInformation) {
	fmt.Fprintln(out, "\nKafka Brokers")
	fmt.Fprintln(out, strings.Repeat("=", 80))
	fmt.Fprintf(out, "%-4s %-16s %-8s\n", "ID", "ADDRESS", "RACK")

	for _, broker := range brokers {
		fmt.Fprintf(out, "%-4d %-16s %-8s\n", broker.ID, broker.Address, broker.Rack)
	}

	noun := "brokers"
	if len(brokers) == 1 {
		noun = "broker"
	}
	fmt.Fprintf(out, "\n%d %s available\n", len(brokers), noun)
}

func printTopicsInformation(out io.Writer, topics []kafkaesque_topic.TopicInformation) {
	fmt.Fprintln(out, "\nKafka Topics")
	fmt.Fprintln(out, strings.Repeat("=", 80))
	fmt.Fprintf(out, "%-4s %-32s %-12s %-16s %-20s\n", "ID", "NAME", "LEVEL", "PARTITIONS", "REPLICATION FACTOR")

	for i, topic := range topics {
		fmt.Fprintf(out, "%-4d %-32s %-12s %-16d %-20d\n",
			i+1,
			topic.Name,
			topic.Level,
			topic.PartitionCount,
			topic.ReplicationFactor,
		)
	}

	noun := "topics"
	if len(topics) == 1 {
		noun = "topic"
	}
	fmt.Fprintf(out, "\n%d %s available\n", len(topics), noun)
}

func printPartitionsInformation(out io.Writer, topic string, partitions []kafka.Partition) {
	fmt.Fprintf(out, "Partition Information for topic '%s':\n", topic)
	for i, partition := range partitions {
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

func printConsumersInformation(out io.Writer, consumers *kafka.ListGroupsResponse) {
	fmt.Fprintln(out, "Consumer Information:")
	for i, consumer := range consumers.Groups {
		fmt.Fprintf(out, "%d. Group ID: %s | Coordinator: %d | Protocol: %s\n", i+1, consumer.GroupID, consumer.Coordinator, consumer.ProtocolType)
	}
}
