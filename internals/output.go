package internals

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/tabwriter"

	kafkaesque_broker "github.com/nikhil25803/kafkaesque/internals/brokers"
	kafkaesque_consumers "github.com/nikhil25803/kafkaesque/internals/consumers"
	kafkaesque_health "github.com/nikhil25803/kafkaesque/internals/health"
	kafkaesque_metadata "github.com/nikhil25803/kafkaesque/internals/metadata"
	kafkaesque_partition "github.com/nikhil25803/kafkaesque/internals/partitions"
	kafkaesque_topic "github.com/nikhil25803/kafkaesque/internals/topics"
)

func printKafkaInformation(out io.Writer, request InformationRequest, info *KafkaInformation, output string) error {
	if output == "json" {
		return printKafkaInformationJSON(out, request, info)
	}

	if request.Health {
		printHealthInformation(out, info.Health)
	}
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
			printConsumerGroupInformation(out, info.Consumer, request.Member)
		}
	}
	return nil
}

type kafkaInformationJSON struct {
	Metadata   *kafkaesque_metadata.MetadataInformation          `json:"metadata,omitempty"`
	Health     *kafkaesque_health.Information                    `json:"health,omitempty"`
	Brokers    *[]kafkaesque_broker.BrokerInformation            `json:"brokers,omitempty"`
	Topics     *[]kafkaesque_topic.TopicInformation              `json:"topics,omitempty"`
	Partitions *[]kafkaesque_partition.PartitionTopicInformation `json:"partitions,omitempty"`
	Consumers  *[]kafkaesque_consumers.ConsumerGroups            `json:"consumers,omitempty"`
	Consumer   any                                               `json:"consumer,omitempty"`
}

type consumerGroupWithMembersJSON struct {
	*kafkaesque_consumers.ConsumerGroupInformation
	Members []kafkaesque_consumers.ConsumerGroupMember `json:"members"`
}

func printKafkaInformationJSON(out io.Writer, request InformationRequest, info *KafkaInformation) error {
	result := kafkaInformationJSON{}
	if request.Metadata {
		result.Metadata = info.Metadata
	}
	if request.Health {
		result.Health = info.Health
	}
	if request.Brokers {
		brokers := nonNilSlice(info.Brokers)
		result.Brokers = &brokers
	}
	if request.Topics {
		topics := nonNilSlice(info.Topics)
		result.Topics = &topics
	}
	if request.Partitions {
		partitions := nonNilSlice(info.Partitions)
		result.Partitions = &partitions
	}
	if request.Consumers {
		consumers := nonNilSlice(info.Consumers)
		result.Consumers = &consumers
	}
	if request.Consumer {
		if request.Topic != "" {
			result.Consumer = info.ConsumerTopic
		} else if request.Member {
			result.Consumer = consumerGroupWithMembersJSON{
				ConsumerGroupInformation: info.Consumer,
				Members:                  nonNilSlice(info.Consumer.Members),
			}
		} else {
			consumer := *info.Consumer
			consumer.Members = nil
			result.Consumer = &consumer
		}
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}

func nonNilSlice[T any](values []T) []T {
	if values == nil {
		return []T{}
	}
	return values
}

func printHealthInformation(out io.Writer, info *kafkaesque_health.Information) {
	fmt.Fprintln(out, "Kafka Cluster Health")
	fmt.Fprintln(out, strings.Repeat("=", 80))
	fmt.Fprintf(out, "Status: %s\n", info.Status)
	fmt.Fprintln(out, "CLUSTER")
	fmt.Fprintf(out, "  Brokers: %d\n", info.BrokerCount)
	fmt.Fprintf(out, "  Topics: %d\n", info.TopicCount)
	fmt.Fprintf(out, "  Partitions: %d\n", info.PartitionCount)
	fmt.Fprintf(out, "  Consumer Groups: %d\n", info.ConsumerGroupCount)
	fmt.Fprintln(out, "PARTITIONS")
	fmt.Fprintf(out, "  Offline: %d\n", info.OfflinePartitionCount)
	fmt.Fprintf(out, "  Under-replicated: %d\n", info.UnderReplicatedCount)
	fmt.Fprintln(out, "CONSUMERS")
	fmt.Fprintf(out, "  Rebalancing: %d\n", info.RebalancingGroupCount)
	fmt.Fprintf(out, "  Dead: %d\n", info.DeadGroupCount)
	fmt.Fprintf(out, "  Lagging: %d\n", info.LaggingGroupCount)
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

func printConsumerGroupInformation(
	out io.Writer,
	consumer *kafkaesque_consumers.ConsumerGroupInformation,
	showMembers bool,
) {
	fmt.Fprintf(out, "\nConsumer Group: %s\n", consumer.GroupName)
	fmt.Fprintf(out, "State: %s\n", strings.ToUpper(consumer.State))
	if showMembers {
		printConsumerGroupMembers(out, consumer.Members)
	}

	fmt.Fprintln(out, "\nLag Summary")
	fmt.Fprintln(out, strings.Repeat("=", 80))
	fmt.Fprintf(out, "Total Lag: %d\n", consumer.LagSummary.TotalLag)
	fmt.Fprintf(out, "Maximum Partition Lag: %d\n", consumer.LagSummary.MaximumPartitionLag)
	fmt.Fprintf(out, "Average Partition Lag: %.1f\n", consumer.LagSummary.AveragePartitionLag)
	fmt.Fprintf(out, "Healthy Partitions: %d\n", consumer.LagSummary.HealthyPartitions)
	fmt.Fprintf(out, "Warning Partitions: %d\n", consumer.LagSummary.WarningPartitions)
	fmt.Fprintf(out, "Unhealthy Partitions: %d\n", consumer.LagSummary.UnhealthyPartitions)
	fmt.Fprintln(out)
	fmt.Fprintln(out, "TOPICS")
	fmt.Fprintln(out, strings.Repeat("=", 80))

	fmt.Fprintf(out, "%-32s %-12s %-10s %-10s\n", "TOPIC", "PARTITIONS", "LAG", "STATUS")
	for _, topic := range consumer.Topics {
		fmt.Fprintf(out, "%-32s %-12d %-10d %-10s\n", topic.Topic, topic.Partitions, topic.Lag, topic.Status)
	}
}

func printConsumerGroupMembers(out io.Writer, members []kafkaesque_consumers.ConsumerGroupMember) {
	fmt.Fprintln(out, strings.Repeat("=", 80))
	table := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(table, "MEMBER\tCLIENT ID\tHOST\tPARTITIONS")
	assignedPartitions := 0
	qualifyTopics := membersUseMultipleTopics(members)
	for _, member := range members {
		assignments, count := formatMemberAssignments(member.Assignments, qualifyTopics)
		assignedPartitions += count
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\n", member.MemberID, member.ClientID, member.Host, assignments)
	}
	_ = table.Flush()
	fmt.Fprintf(out, "\n%d %s · %d assigned %s\n",
		len(members), plural(len(members), "member", "members"),
		assignedPartitions, plural(assignedPartitions, "partition", "partitions"),
	)
}

func formatMemberAssignments(
	assignments []kafkaesque_consumers.ConsumerGroupMemberAssignment,
	qualifyTopics bool,
) (string, int) {
	if len(assignments) == 0 {
		return "-", 0
	}

	formatted := make([]string, 0, len(assignments))
	total := 0
	for _, assignment := range assignments {
		partitions := formatInts(assignment.Partitions)
		if qualifyTopics {
			formatted = append(formatted, assignment.Topic+":"+partitions)
		} else {
			formatted = append(formatted, partitions)
		}
		total += len(assignment.Partitions)
	}
	return strings.Join(formatted, "; "), total
}

func membersUseMultipleTopics(members []kafkaesque_consumers.ConsumerGroupMember) bool {
	topics := make(map[string]struct{})
	for _, member := range members {
		for _, assignment := range member.Assignments {
			topics[assignment.Topic] = struct{}{}
			if len(topics) > 1 {
				return true
			}
		}
	}
	return false
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
	if brokersUseAddressOverrides(brokers) {
		fmt.Fprintln(out, strings.Repeat("=", 120))
		fmt.Fprintf(out, "%-4s %-72s %-24s %-8s\n", "ID", "ADVERTISED ADDRESS", "CONNECT ADDRESS", "RACK")
		for _, broker := range brokers {
			fmt.Fprintf(out, "%-4d %-72s %-24s %-8s\n", broker.ID, broker.Address, broker.ConnectAddress, broker.Rack)
		}
		printBrokerSummary(out, len(brokers))
		return
	}

	fmt.Fprintln(out, strings.Repeat("=", 80))
	fmt.Fprintf(out, "%-4s %-16s %-8s\n", "ID", "ADDRESS", "RACK")

	for _, broker := range brokers {
		fmt.Fprintf(out, "%-4d %-16s %-8s\n", broker.ID, broker.Address, broker.Rack)
	}

	printBrokerSummary(out, len(brokers))
}

func brokersUseAddressOverrides(brokers []kafkaesque_broker.BrokerInformation) bool {
	for _, broker := range brokers {
		if broker.ConnectAddress != "" && broker.ConnectAddress != broker.Address {
			return true
		}
	}
	return false
}

func printBrokerSummary(out io.Writer, count int) {
	noun := "brokers"
	if count == 1 {
		noun = "broker"
	}
	fmt.Fprintf(out, "\n%d %s available\n", count, noun)
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

func printPartitionsInformation(
	out io.Writer,
	topic string,
	partitions []kafkaesque_partition.PartitionTopicInformation,
) {
	fmt.Fprintln(out, "\nTopic: "+topic)
	fmt.Fprintln(out, strings.Repeat("=", 80))

	fmt.Fprintf(out, "%-10s %-20s %-16s %-16s %-10s\n", "PARTITION", "LEADER", "REPLICAS", "ISR", "STATUS")

	for _, partition := range partitions {
		fmt.Fprintf(out, "%-10d %-20s %-16s %-16s %-10s\n",
			partition.PartitionID,
			partition.Leader,
			formatInts(partition.Replicas),
			formatInts(partition.ISR),
			partition.Status,
		)
	}
}

func formatInts(values []int) string {
	if len(values) == 0 {
		return "-"
	}
	formatted := make([]string, len(values))
	for i, value := range values {
		formatted[i] = strconv.Itoa(value)
	}
	return strings.Join(formatted, ",")
}

func plural(count int, singular, plural string) string {
	if count == 1 {
		return singular
	}
	return plural
}

func printConsumersInformation(out io.Writer, consumers []kafkaesque_consumers.ConsumerGroups) {
	fmt.Fprintln(out, "\nKafka Consumers")
	fmt.Fprintln(out, strings.Repeat("=", 117))
	fmt.Fprintf(
		out,
		"%-32s %-12s %-16s %-20s %-16s %-16s\n",
		"GROUP NAME",
		"TYPE",
		"COORDINATOR",
		"STATE",
		"MEMBERS COUNT",
		"TOPICS",
	)

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
	fmt.Fprintf(
		out,
		"\n%d %s · %d %s · %d %s\n",
		len(consumers),
		groupNoun,
		totalTopics,
		topicNoun,
		totalMembers,
		memberNoun,
	)
}
