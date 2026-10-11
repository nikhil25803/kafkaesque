package consumers

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"

	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol/describegroups"
)

type ConsumerGroupDescriptionRequest struct {
	Group          ConsumerGroups `json:"group"`
	BrokerAddress  string         `json:"broker_address"`
	IncludeMembers bool           `json:"include_members"`
}

// GetConsumerGroupDescription completes a consumer group with its state and member count.
func GetConsumerGroupDescription(c *kafkaesque.KafkaesqueConn, ctx context.Context, req ConsumerGroupDescriptionRequest) ([]ConsumerGroups, error) {
	if c.Client.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.Client.Timeout)
		defer cancel()
	}

	transport := c.Client.Transport
	if transport == nil {
		transport = kafka.DefaultTransport
	}

	rawResponse, err := transport.RoundTrip(ctx, kafka.TCP(req.BrokerAddress), &describegroups.Request{
		Groups: []string{req.Group.GroupName},
	})
	if err != nil {
		return nil, fmt.Errorf("describe groups network error: %w", err)
	}

	response, ok := rawResponse.(*describegroups.Response)
	if !ok {
		return nil, fmt.Errorf("unexpected describe groups response type %T", rawResponse)
	}

	var consumerGroups []ConsumerGroups

	for _, group := range response.Groups {
		if group.ErrorCode != 0 {
			return nil, fmt.Errorf("error describing group %s: %w", group.GroupID, kafka.Error(group.ErrorCode))
		}

		completedGroup := req.Group
		completedGroup.State = group.GroupState
		completedGroup.MembersCount = len(group.Members)

		if completedGroup.Type == "consumer" {
			topics := make(map[string]struct{})
			for _, member := range group.Members {
				memberTopics, err := decodeSubscribedTopics(member.MemberMetadata)
				if err != nil {
					return nil, fmt.Errorf("decode member %s metadata for group %s: %w", member.MemberID, group.GroupID, err)
				}
				for _, topic := range memberTopics {
					topics[topic] = struct{}{}
				}

				if req.IncludeMembers {
					assignments, err := decodeMemberAssignments(member.MemberAssignment)
					if err != nil {
						return nil, fmt.Errorf("decode member %s assignments for group %s: %w", member.MemberID, group.GroupID, err)
					}
					completedGroup.Members = append(completedGroup.Members, ConsumerGroupMember{
						MemberID:    member.MemberID,
						ClientID:    member.ClientID,
						Host:        strings.TrimPrefix(member.ClientHost, "/"),
						Assignments: assignments,
					})
				}
			}
			completedGroup.TopicsCount = len(topics)
			if req.IncludeMembers {
				sort.Slice(completedGroup.Members, func(i, j int) bool {
					return completedGroup.Members[i].MemberID < completedGroup.Members[j].MemberID
				})
			}
		}

		consumerGroups = append(consumerGroups, completedGroup)
	}

	return consumerGroups, nil
}

func decodeMemberAssignments(data []byte) ([]ConsumerGroupMemberAssignment, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) < 6 {
		return nil, fmt.Errorf("assignment metadata is too short")
	}

	version := int16(binary.BigEndian.Uint16(data[:2]))
	if version < 0 {
		return nil, fmt.Errorf("invalid assignment version %d", version)
	}
	topicCount := int32(binary.BigEndian.Uint32(data[2:6]))
	if topicCount < 0 {
		return nil, fmt.Errorf("invalid assignment topic count %d", topicCount)
	}

	offset := 6
	if int64(topicCount)*6 > int64(len(data)-offset) {
		return nil, fmt.Errorf("assignment topic count %d exceeds assignment metadata", topicCount)
	}
	assignments := make([]ConsumerGroupMemberAssignment, 0, topicCount)
	for i := range int(topicCount) {
		if len(data)-offset < 2 {
			return nil, fmt.Errorf("assignment topic %d is missing its name length", i)
		}
		nameLength := int16(binary.BigEndian.Uint16(data[offset : offset+2]))
		offset += 2
		if nameLength < 0 || int(nameLength) > len(data)-offset {
			return nil, fmt.Errorf("assignment topic %d has invalid name length %d", i, nameLength)
		}
		topic := string(data[offset : offset+int(nameLength)])
		offset += int(nameLength)

		if len(data)-offset < 4 {
			return nil, fmt.Errorf("assignment topic %s is missing its partition count", topic)
		}
		partitionCount := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4
		if partitionCount < 0 || int64(partitionCount)*4 > int64(len(data)-offset) {
			return nil, fmt.Errorf("assignment topic %s has invalid partition count %d", topic, partitionCount)
		}

		partitions := make([]int, 0, partitionCount)
		for range int(partitionCount) {
			partitions = append(partitions, int(int32(binary.BigEndian.Uint32(data[offset:offset+4]))))
			offset += 4
		}
		sort.Ints(partitions)
		assignments = append(assignments, ConsumerGroupMemberAssignment{Topic: topic, Partitions: partitions})
	}

	sort.Slice(assignments, func(i, j int) bool { return assignments[i].Topic < assignments[j].Topic })
	return assignments, nil
}

func decodeSubscribedTopics(metadata []byte) ([]string, error) {
	const (
		metadataVersionSize    = 2
		topicCountSize         = 4
		subscriptionPrefixSize = metadataVersionSize + topicCountSize
	)

	if len(metadata) == 0 {
		return nil, nil
	}
	if len(metadata) < subscriptionPrefixSize {
		return nil, fmt.Errorf("subscription metadata is too short")
	}
	metadataVersion := int16(binary.BigEndian.Uint16(metadata[:metadataVersionSize]))
	if metadataVersion < 0 {
		return nil, fmt.Errorf("invalid subscription metadata version %d", metadataVersion)
	}

	topicCount := int32(binary.BigEndian.Uint32(metadata[metadataVersionSize:subscriptionPrefixSize]))
	if topicCount < 0 {
		return nil, fmt.Errorf("invalid topic count %d", topicCount)
	}

	offset := subscriptionPrefixSize
	topics := make([]string, 0)
	for i := range int(topicCount) {
		if len(metadata)-offset < 2 {
			return nil, fmt.Errorf("topic %d is missing its name length", i)
		}

		topicLength := int16(binary.BigEndian.Uint16(metadata[offset : offset+2]))
		offset += 2
		if topicLength < 0 {
			return nil, fmt.Errorf("topic %d has invalid name length %d", i, topicLength)
		}
		if int(topicLength) > len(metadata)-offset {
			return nil, fmt.Errorf("topic %d name exceeds subscription metadata", i)
		}

		topics = append(topics, string(metadata[offset:offset+int(topicLength)]))
		offset += int(topicLength)
	}

	return topics, nil
}
