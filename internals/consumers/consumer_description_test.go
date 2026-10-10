package consumers

import (
	"context"
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

var kafkaV3MemberMetadata = []byte{
	0x00, 0x03, // version 3
	0x00, 0x00, 0x00, 0x01, // one topic
	0x00, 0x06, 'o', 'r', 'd', 'e', 'r', 's',
	0xff, 0xff, 0xff, 0xff, // null user data
	0x00, 0x00, 0x00, 0x00, // no owned partitions
	0xff, 0xff, 0xff, 0xff, // unknown generation
	0xff, 0xff, // null rack ID
}

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
