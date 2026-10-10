package consumers

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol/describegroups"
)

type descriptionTransport struct {
	response kafka.Response
	err      error
	address  string
	groups   []string
	deadline bool
}

func (t *descriptionTransport) RoundTrip(ctx context.Context, address net.Addr, request kafka.Request) (kafka.Response, error) {
	if t.err != nil {
		return nil, t.err
	}

	_, t.deadline = ctx.Deadline()
	t.address = address.String()
	t.groups = append([]string(nil), request.(*describegroups.Request).Groups...)
	return t.response, nil
}

func consumerConnection(transport kafka.RoundTripper) *kafkaesque.KafkaesqueConn {
	return &kafkaesque.KafkaesqueConn{
		Client: &kafka.Client{Transport: transport, Timeout: time.Second},
	}
}

func memberMetadataV3(topics ...string) []byte {
	metadata := []byte{0x00, 0x03}
	metadata = binary.BigEndian.AppendUint32(metadata, uint32(len(topics)))
	for _, topic := range topics {
		metadata = binary.BigEndian.AppendUint16(metadata, uint16(len(topic)))
		metadata = append(metadata, topic...)
	}
	metadata = binary.BigEndian.AppendUint32(metadata, ^uint32(0)) // null user data
	metadata = binary.BigEndian.AppendUint32(metadata, 0)          // no owned partitions
	metadata = binary.BigEndian.AppendUint32(metadata, ^uint32(0)) // unknown generation
	metadata = binary.BigEndian.AppendUint16(metadata, ^uint16(0)) // null rack ID
	return metadata
}

var kafkaV3MemberMetadata = memberMetadataV3("orders")

func TestGetConsumerGroupDescription(t *testing.T) {
	transport := &descriptionTransport{
		response: &describegroups.Response{
			Groups: []describegroups.ResponseGroup{
				{
					GroupID:    "order-processor",
					GroupState: "Stable",
					Members: []describegroups.ResponseGroupMember{
						{MemberID: "member-1", MemberMetadata: kafkaV3MemberMetadata},
						{MemberID: "member-2", MemberMetadata: kafkaV3MemberMetadata},
					},
				},
			},
		},
	}
	request := ConsumerGroupDescriptionRequest{
		Group: ConsumerGroups{
			GroupName:     "order-processor",
			Type:          "consumer",
			CoordinatorID: 2,
		},
		BrokerAddress: "broker-2:9092",
	}

	got, err := GetConsumerGroupDescription(consumerConnection(transport), context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	want := []ConsumerGroups{
		{
			GroupName:     "order-processor",
			Type:          "consumer",
			CoordinatorID: 2,
			MembersCount:  2,
			State:         "Stable",
			TopicsCount:   1,
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("consumer groups = %+v, want %+v", got, want)
	}
	if transport.address != "broker-2:9092" {
		t.Fatalf("broker address = %q, want broker-2:9092", transport.address)
	}
	if !reflect.DeepEqual(transport.groups, []string{"order-processor"}) {
		t.Fatalf("requested groups = %v, want [order-processor]", transport.groups)
	}
	if !transport.deadline {
		t.Fatal("describe groups request has no deadline")
	}
}

func TestGetConsumerGroupDescriptionCountsUniqueSubscribedTopics(t *testing.T) {
	tests := []struct {
		name      string
		groupType string
		members   []describegroups.ResponseGroupMember
		want      int
	}{
		{
			name:      "one member with two topics",
			groupType: "consumer",
			members: []describegroups.ResponseGroupMember{
				{MemberID: "member-1", MemberMetadata: memberMetadataV3("notifications", "audit-events")},
			},
			want: 2,
		},
		{
			name:      "overlapping member subscriptions",
			groupType: "consumer",
			members: []describegroups.ResponseGroupMember{
				{MemberID: "member-1", MemberMetadata: memberMetadataV3("orders", "payments")},
				{MemberID: "member-2", MemberMetadata: memberMetadataV3("orders", "inventory")},
			},
			want: 3,
		},
		{
			name:      "empty group",
			groupType: "consumer",
			want:      0,
		},
		{
			name:      "non-consumer group",
			groupType: "connect",
			members: []describegroups.ResponseGroupMember{
				{MemberID: "member-1", MemberMetadata: []byte{0x00}},
			},
			want: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transport := &descriptionTransport{
				response: &describegroups.Response{
					Groups: []describegroups.ResponseGroup{
						{GroupID: "group-1", GroupState: "Stable", Members: test.members},
					},
				},
			}

			groups, err := GetConsumerGroupDescription(
				consumerConnection(transport),
				context.Background(),
				ConsumerGroupDescriptionRequest{
					Group:         ConsumerGroups{GroupName: "group-1", Type: test.groupType},
					BrokerAddress: "broker-1:9092",
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			if groups[0].TopicsCount != test.want {
				t.Fatalf("topics count = %d, want %d", groups[0].TopicsCount, test.want)
			}
		})
	}
}

func TestDecodeSubscribedTopicsRejectsMalformedMetadata(t *testing.T) {
	tests := []struct {
		name     string
		metadata []byte
	}{
		{name: "missing topic count", metadata: []byte{0x00, 0x03}},
		{name: "negative metadata version", metadata: []byte{0xff, 0xff, 0x00, 0x00, 0x00, 0x00}},
		{name: "negative topic count", metadata: []byte{0x00, 0x03, 0xff, 0xff, 0xff, 0xff}},
		{name: "missing topic length", metadata: []byte{0x00, 0x03, 0x00, 0x00, 0x00, 0x01}},
		{name: "negative topic length", metadata: []byte{0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0xff, 0xff}},
		{name: "truncated topic name", metadata: []byte{0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x03, 'a'}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := decodeSubscribedTopics(test.metadata); err == nil {
				t.Fatal("expected malformed metadata error")
			}
		})
	}
}

func TestGetConsumerGroupDescriptionReturnsErrors(t *testing.T) {
	t.Run("network", func(t *testing.T) {
		transport := &descriptionTransport{err: errors.New("request failed")}

		_, err := GetConsumerGroupDescription(
			consumerConnection(transport),
			context.Background(),
			ConsumerGroupDescriptionRequest{
				Group:         ConsumerGroups{GroupName: "order-processor"},
				BrokerAddress: "broker-2:9092",
			},
		)
		if err == nil || !strings.Contains(err.Error(), "describe groups network error: request failed") {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("group", func(t *testing.T) {
		transport := &descriptionTransport{
			response: &describegroups.Response{
				Groups: []describegroups.ResponseGroup{
					{
						ErrorCode: int16(kafka.GroupAuthorizationFailed),
						GroupID:   "order-processor",
					},
				},
			},
		}

		_, err := GetConsumerGroupDescription(
			consumerConnection(transport),
			context.Background(),
			ConsumerGroupDescriptionRequest{
				Group:         ConsumerGroups{GroupName: "order-processor"},
				BrokerAddress: "broker-2:9092",
			},
		)
		if err == nil || !strings.Contains(err.Error(), "error describing group order-processor") {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("unexpected response", func(t *testing.T) {
		transport := &descriptionTransport{}

		_, err := GetConsumerGroupDescription(
			consumerConnection(transport),
			context.Background(),
			ConsumerGroupDescriptionRequest{
				Group:         ConsumerGroups{GroupName: "order-processor"},
				BrokerAddress: "broker-2:9092",
			},
		)
		if err == nil || !strings.Contains(err.Error(), "unexpected describe groups response type") {
			t.Fatalf("error = %v", err)
		}
	})
}
