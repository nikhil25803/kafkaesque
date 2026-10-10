package consumers

import (
	"context"
	"errors"
	"net"
	"reflect"
	"testing"

	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol/describegroups"
	"github.com/segmentio/kafka-go/protocol/findcoordinator"
	"github.com/segmentio/kafka-go/protocol/listoffsets"
	"github.com/segmentio/kafka-go/protocol/offsetfetch"
)

type groupInformationTransport struct {
	requests []string
}

func (t *groupInformationTransport) RoundTrip(_ context.Context, address net.Addr, request kafka.Request) (kafka.Response, error) {
	switch request := request.(type) {
	case *findcoordinator.Request:
		t.requests = append(t.requests, "find:"+request.Key)
		return &findcoordinator.Response{NodeID: 2, Host: "coordinator", Port: 9093}, nil
	case *describegroups.Request:
		t.requests = append(t.requests, "describe:"+address.String()+":"+request.Groups[0])
		return &describegroups.Response{Groups: []describegroups.ResponseGroup{
			{
				GroupID:    request.Groups[0],
				GroupState: "Stable",
				Members: []describegroups.ResponseGroupMember{
					{MemberID: "member-1", MemberMetadata: memberMetadataV3("orders")},
					{MemberID: "member-2", MemberMetadata: memberMetadataV3("orders")},
				},
			},
		}}, nil
	case *offsetfetch.Request:
		t.requests = append(t.requests, "offsets:"+address.String()+":"+request.GroupID)
		return &offsetfetch.Response{Topics: []offsetfetch.ResponseTopic{
			{Name: "orders", Partitions: []offsetfetch.ResponsePartition{{PartitionIndex: 0, CommittedOffset: 10}}},
		}}, nil
	case *listoffsets.Request:
		t.requests = append(t.requests, "latest:"+address.String())
		return &listoffsets.Response{Topics: []listoffsets.ResponseTopic{
			{Topic: "orders", Partitions: []listoffsets.ResponsePartition{
				{Partition: 0, Timestamp: kafka.LastOffset, Offset: 134},
			}},
		}}, nil
	default:
		return nil, errors.New("unexpected request")
	}
}

func TestGetConsumerGroupInformation(t *testing.T) {
	transport := &groupInformationTransport{}
	metadata := &kafka.MetadataResponse{Topics: []kafka.Topic{
		{Name: "orders", Partitions: []kafka.Partition{
			{Topic: "orders", ID: 0, Leader: kafka.Broker{ID: 1, Host: "broker", Port: 9092}},
		}},
	}}

	connection := consumerConnection(transport)
	connection.Client.Addr = kafka.TCP("bootstrap:9092")
	got, err := GetConsumerGroupInformation(
		connection,
		context.Background(),
		metadata,
		"orders-service",
		LagThresholds{Warning: 100, Unhealthy: 500},
	)
	if err != nil {
		t.Fatal(err)
	}
	want := &ConsumerGroupInformation{
		GroupName:    "orders-service",
		State:        "Stable",
		MembersCount: 2,
		TopicsCount:  1,
		TotalLag:     124,
		Topics: []ConsumerGroupTopicLag{
			{Topic: "orders", Partitions: 1, Lag: 124, Status: LagStatusWarning},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("consumer group = %+v, want %+v", got, want)
	}
	wantRequests := []string{
		"find:orders-service",
		"describe:coordinator:9093:orders-service",
		"offsets:coordinator:9093:orders-service",
		"latest:broker:9092",
	}
	if !reflect.DeepEqual(transport.requests, wantRequests) {
		t.Fatalf("requests = %v, want %v", transport.requests, wantRequests)
	}
}
