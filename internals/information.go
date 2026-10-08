package internals

import (
	"context"
	"fmt"

	kafkaesque_broker "github.com/nikhil25803/kafkaesque/internals/brokers"
	kafkaesque_consumer "github.com/nikhil25803/kafkaesque/internals/consumers"
	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
	kafkaesque_metadata "github.com/nikhil25803/kafkaesque/internals/metadata"
	kafkaesque_partition "github.com/nikhil25803/kafkaesque/internals/partitions"
	kafkaesque_topic "github.com/nikhil25803/kafkaesque/internals/topics"
	kafka "github.com/segmentio/kafka-go"
)

func (r InformationRequest) needsMetadata() bool {
	return r.Metadata || r.Topics || r.Brokers || r.Partitions
}

// KafkaInformation contains the requested Kafka data.
type KafkaInformation struct {
	Metadata   *kafkaesque_metadata.MetadataInformation
	Topics     []kafkaesque_topic.TopicInformation
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
