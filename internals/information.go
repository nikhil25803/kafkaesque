package internals

import (
	"context"
	"fmt"

	kafkaesque_broker "github.com/nikhil25803/kafkaesque/internals/brokers"
	kafkaesque_consumers "github.com/nikhil25803/kafkaesque/internals/consumers"
	kafkaesque_health "github.com/nikhil25803/kafkaesque/internals/health"
	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
	kafkaesque_metadata "github.com/nikhil25803/kafkaesque/internals/metadata"
	kafkaesque_partition "github.com/nikhil25803/kafkaesque/internals/partitions"
	kafkaesque_topic "github.com/nikhil25803/kafkaesque/internals/topics"
)

func (r InformationRequest) needsMetadata() bool {
	return r.Metadata || r.Health || r.Topics || r.Brokers || r.Partitions || r.Consumers || r.Consumer
}

// KafkaInformation contains the requested Kafka data.
type KafkaInformation struct {
	Metadata      *kafkaesque_metadata.MetadataInformation            `json:"metadata,omitempty"`
	Health        *kafkaesque_health.Information                      `json:"health,omitempty"`
	Topics        []kafkaesque_topic.TopicInformation                 `json:"topics,omitempty"`
	Brokers       []kafkaesque_broker.BrokerInformation               `json:"brokers,omitempty"`
	Partitions    []kafkaesque_partition.PartitionTopicInformation    `json:"partitions,omitempty"`
	Consumers     []kafkaesque_consumers.ConsumerGroups               `json:"consumers,omitempty"`
	Consumer      *kafkaesque_consumers.ConsumerGroupInformation      `json:"consumer,omitempty"`
	ConsumerTopic *kafkaesque_consumers.ConsumerGroupTopicInformation `json:"consumer_topic,omitempty"`
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
		singleTopic := request.Topic != "" &&
			(request.Partitions || request.Consumer) &&
			!request.Metadata &&
			!request.Topics &&
			!request.Brokers
		if singleTopic {
			topics = []string{request.Topic}
		}

		metadata, err := conn.GetMetadata(ctx, topics)
		if err != nil {
			return nil, fmt.Errorf("failed to get cluster metadata: %w", err)
		}

		if request.Metadata {
			info.Metadata = kafkaesque_metadata.GetMetadataInformation(metadata)
		}
		if request.Health {
			health, err := kafkaesque_health.GetInformation(conn, ctx, metadata, request.lagThresholds)
			if err != nil {
				return nil, fmt.Errorf("failed to get cluster health: %w", err)
			}
			info.Health = health
		}

		brokers := kafkaesque_broker.GetBrokerInformation(metadata, conn.ResolveBrokerAddress)
		if request.Brokers {
			info.Brokers = brokers
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

		if request.Consumers {
			consumerGroups, err := kafkaesque_consumers.GetConsumerInformation(conn, ctx)
			if err != nil {
				return nil, fmt.Errorf("failed to get consumer group information: %w", err)
			}

			brokerAddresses := make(map[int]string, len(brokers))
			for _, broker := range brokers {
				brokerAddresses[broker.ID] = broker.Address
			}

			for _, group := range consumerGroups {
				brokerAddress, ok := brokerAddresses[group.CoordinatorID]
				if !ok {
					return nil, fmt.Errorf(
						"coordinator broker %d for consumer group %s not found",
						group.CoordinatorID,
						group.GroupName,
					)
				}

				describedGroups, err := kafkaesque_consumers.GetConsumerGroupDescription(
					conn,
					ctx,
					kafkaesque_consumers.ConsumerGroupDescriptionRequest{
						Group:         group,
						BrokerAddress: brokerAddress,
					},
				)
				if err != nil {
					return nil, fmt.Errorf("failed to describe consumer group %s: %w", group.GroupName, err)
				}
				info.Consumers = append(info.Consumers, describedGroups...)
			}
		}

		if request.Consumer {
			if request.Topic != "" {
				consumerTopic, err := kafkaesque_consumers.GetConsumerGroupTopicInformation(
					conn,
					ctx,
					metadata,
					request.Group,
					request.Topic,
				)
				if err != nil {
					return nil, fmt.Errorf("failed to get consumer group %s topic %s: %w", request.Group, request.Topic, err)
				}
				info.ConsumerTopic = consumerTopic
			} else {
				consumer, err := kafkaesque_consumers.GetConsumerGroupInformation(
					conn,
					ctx,
					metadata,
					request.Group,
					request.lagThresholds,
					request.Member,
				)
				if err != nil {
					return nil, fmt.Errorf("failed to get consumer group %s: %w", request.Group, err)
				}
				info.Consumer = consumer
			}
		}
	}

	return info, nil
}
