package internals

import (
	"fmt"
	"io"
	"strings"

	kafkaesque_broker "github.com/nikhil25803/kafkaesque/internals/brokers"
	kafkaesque_consumers "github.com/nikhil25803/kafkaesque/internals/consumers"
	kafkaesque_metadata "github.com/nikhil25803/kafkaesque/internals/metadata"
	kafkaesque_partition "github.com/nikhil25803/kafkaesque/internals/partitions"
	kafkaesque_topic "github.com/nikhil25803/kafkaesque/internals/topics"
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
	if request.Consumer {
		if request.Topic != "" {
			printConsumerGroupTopicInformation(out, info.ConsumerTopic)
		} else {
			printConsumerGroupInformation(out, info.Consumer)
		}
	}
	return nil
}

func printConsumerGroupTopicInformation(out io.Writer, topic *kafkaesque_consumers.ConsumerGroupTopicInformation) {
	fmt.Fprintf(out, "\nConsumer Group: %s\n", topic.GroupName)
	fmt.Fprintf(out, "Topic: %s\n", topic.Topic)
	fmt.Fprintln(out, strings.Repeat("=", 80))
	fmt.Fprintf(out, "%-12s %-19s %-17s %-12s\n", "PARTITION", "COMMITTED OFFSET", "LOG END OFFSET", "LAG")
	for _, partition := range topic.Partitions {
		fmt.Fprintf(out, "%-12d %-19d %-17d %-12d\n",
			partition.Partition,
			partition.CommittedOffset,
			partition.LogEndOffset,
			partition.Lag,
		)
	}
	fmt.Fprintf(out, "TOTAL %d\n", topic.TotalLag)
}

func printConsumerGroupInformation(out io.Writer, consumer *kafkaesque_consumers.ConsumerGroupInformation) {
	fmt.Fprintf(out, "\nConsumer Group: %s\n", consumer.GroupName)
	fmt.Fprintln(out, strings.Repeat("=", 80))
	fmt.Fprintf(out, "STATE: %s\n", strings.ToUpper(consumer.State))
	fmt.Fprintf(out, "MEMBERS: %d\n", consumer.MembersCount)
	fmt.Fprintf(out, "TOPICS: %d\n", consumer.TopicsCount)
	fmt.Fprintf(out, "TOTAL LAG: %d\n", consumer.TotalLag)
	fmt.Fprintln(out)
	fmt.Fprintln(out, "TOPICS")
	fmt.Fprintln(out, strings.Repeat("=", 80))

	fmt.Fprintf(out, "%-32s %-12s %-10s %-10s\n", "TOPIC", "PARTITIONS", "LAG", "STATUS")
	for _, topic := range consumer.Topics {
		fmt.Fprintf(out, "%-32s %-12d %-10d %-10s\n", topic.Topic, topic.Partitions, topic.Lag, topic.Status)
	}
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

func printPartitionsInformation(out io.Writer, topic string, partitions []kafkaesque_partition.PartitionTopicInformation) {
	fmt.Fprintln(out, "\nTopic: "+topic)
	fmt.Fprintln(out, strings.Repeat("=", 80))

	fmt.Fprintf(out, "%-10s %-32s %-12s %-16s\n", "PARTITION", "LEADER", "REPLICAS", "ISR")

	for _, partition := range partitions {
		fmt.Fprintf(out, "%-10d %-32s %-12d %-16d\n",
			partition.PartitionID,
			partition.Leader,
			partition.Replicas,
			partition.Isr,
		)
	}
}

func printConsumersInformation(out io.Writer, consumers []kafkaesque_consumers.ConsumerGroups) {
	fmt.Fprintln(out, "\nKafka Consumers")
	fmt.Fprintln(out, strings.Repeat("=", 117))
	fmt.Fprintf(out, "%-32s %-12s %-16s %-20s %-16s %-16s\n", "GROUP NAME", "TYPE", "COORDINATOR", "STATE", "MEMBERS COUNT", "TOPICS")

	totalMembers := 0
	totalTopics := 0
	for _, consumer := range consumers {
		fmt.Fprintf(out, "%-32s %-12s %-16s %-20s %-16d %-16d\n",
			consumer.GroupName,
			consumer.Type,
			fmt.Sprintf("broker-%d", consumer.CoordinatorID),
			consumer.State,
			consumer.MembersCount,
			consumer.TopicsCount,
		)
		totalMembers += consumer.MembersCount
		totalTopics += consumer.TopicsCount
	}

	groupNoun := "groups"
	if len(consumers) == 1 {
		groupNoun = "group"
	}
	topicNoun := "topics"
	if totalTopics == 1 {
		topicNoun = "topic"
	}
	memberNoun := "members"
	if totalMembers == 1 {
		memberNoun = "member"
	}
	fmt.Fprintf(out, "\n%d %s · %d %s · %d %s\n", len(consumers), groupNoun, totalTopics, topicNoun, totalMembers, memberNoun)
}
