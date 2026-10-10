package consumers

import (
	"context"
	"errors"
	"net"
	"reflect"
	"sort"
	"strings"
	"testing"

	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol/listoffsets"
	"github.com/segmentio/kafka-go/protocol/offsetfetch"
)

type lagTransport struct {
	offsetResponse *offsetfetch.Response
	latestOffsets  map[topicPartition]int64
	latestError    int16
	requests       []string
	err            error
}

func (t *lagTransport) RoundTrip(_ context.Context, address net.Addr, request kafka.Request) (kafka.Response, error) {
	if t.err != nil {
		return nil, t.err
	}

	switch request := request.(type) {
	case *offsetfetch.Request:
		t.requests = append(t.requests, "offset-fetch:"+address.String()+":"+request.GroupID)
		return t.offsetResponse, nil
	case *listoffsets.Request:
		response := &listoffsets.Response{}
		for _, topic := range request.Topics {
			responseTopic := listoffsets.ResponseTopic{Topic: topic.Topic}
			for _, partition := range topic.Partitions {
				key := topicPartition{topic: topic.Topic, partition: int(partition.Partition)}
				t.requests = append(t.requests, "latest:"+address.String()+":"+topic.Topic)
				responseTopic.Partitions = append(responseTopic.Partitions, listoffsets.ResponsePartition{
					Partition: partition.Partition,
					ErrorCode: t.latestError,
					Timestamp: kafka.LastOffset,
					Offset:    t.latestOffsets[key],
				})
			}
			response.Topics = append(response.Topics, responseTopic)
		}
		return response, nil
	default:
		return nil, errors.New("unexpected request")
	}
}

func TestGetConsumerGroupLagAggregatesAndRoutesByLeader(t *testing.T) {
	transport := &lagTransport{
		offsetResponse: &offsetfetch.Response{Topics: []offsetfetch.ResponseTopic{
			{Name: "payments", Partitions: []offsetfetch.ResponsePartition{
				{PartitionIndex: 0, CommittedOffset: 5},
				{PartitionIndex: 1, CommittedOffset: -1},
			}},
			{Name: "orders", Partitions: []offsetfetch.ResponsePartition{
				{PartitionIndex: 0, CommittedOffset: 10},
				{PartitionIndex: 1, CommittedOffset: 20},
			}},
		}},
		latestOffsets: map[topicPartition]int64{
			{topic: "orders", partition: 0}:   80,
			{topic: "orders", partition: 1}:   74,
			{topic: "payments", partition: 0}: 17,
		},
	}
	metadata := consumerLagMetadata()

	got, total, err := GetConsumerGroupLag(
		consumerConnection(transport),
		context.Background(),
		metadata,
		"coordinator:9092",
		"orders-service",
		LagThresholds{Warning: 100, Unhealthy: 500},
	)
	if err != nil {
		t.Fatal(err)
	}
	want := []ConsumerGroupTopicLag{
		{Topic: "orders", Partitions: 2, Lag: 124, Status: LagStatusWarning},
		{Topic: "payments", Partitions: 1, Lag: 12, Status: LagStatusHealthy},
	}
	if !reflect.DeepEqual(got, want) || total != 136 {
		t.Fatalf("lag = %+v total %d, want %+v total 136", got, total, want)
	}

	sort.Strings(transport.requests)
	wantRequests := []string{
		"latest:broker-a:9092:orders",
		"latest:broker-a:9092:payments",
		"latest:broker-b:9093:orders",
		"offset-fetch:coordinator:9092:orders-service",
	}
	sort.Strings(wantRequests)
	if !reflect.DeepEqual(transport.requests, wantRequests) {
		t.Fatalf("requests = %v, want %v", transport.requests, wantRequests)
	}
}

func TestConsumerLagStatusBoundaries(t *testing.T) {
	thresholds := LagThresholds{Warning: 100, Unhealthy: 500}
	tests := []struct {
		lag  int64
		want string
	}{
		{lag: 100, want: LagStatusHealthy},
		{lag: 101, want: LagStatusWarning},
		{lag: 500, want: LagStatusWarning},
		{lag: 501, want: LagStatusUnhealthy},
	}
	for _, test := range tests {
		if got := ConsumerLagStatus(test.lag, thresholds); got != test.want {
			t.Fatalf("status for %d = %s, want %s", test.lag, got, test.want)
		}
	}
}

func TestGetConsumerGroupLagReturnsErrors(t *testing.T) {
	t.Run("network", func(t *testing.T) {
		transport := &lagTransport{err: errors.New("network failed")}
		_, _, err := GetConsumerGroupLag(consumerConnection(transport), context.Background(), consumerLagMetadata(), "coordinator:9092", "group", LagThresholds{})
		if err == nil || !strings.Contains(err.Error(), "fetch committed offsets: kafka.(*Client).OffsetFetch: network failed") {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("missing metadata", func(t *testing.T) {
		transport := &lagTransport{offsetResponse: &offsetfetch.Response{Topics: []offsetfetch.ResponseTopic{
			{Name: "unknown", Partitions: []offsetfetch.ResponsePartition{{PartitionIndex: 0, CommittedOffset: 1}}},
		}}}
		_, _, err := GetConsumerGroupLag(consumerConnection(transport), context.Background(), consumerLagMetadata(), "coordinator:9092", "group", LagThresholds{})
		if err == nil || !strings.Contains(err.Error(), "metadata not found for unknown partition 0") {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("group offset error", func(t *testing.T) {
		transport := &lagTransport{offsetResponse: &offsetfetch.Response{ErrorCode: int16(kafka.GroupAuthorizationFailed)}}
		_, _, err := GetConsumerGroupLag(consumerConnection(transport), context.Background(), consumerLagMetadata(), "coordinator:9092", "group", LagThresholds{})
		if err == nil || !strings.Contains(err.Error(), "fetch committed offsets") {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("partition offset error", func(t *testing.T) {
		transport := &lagTransport{offsetResponse: &offsetfetch.Response{Topics: []offsetfetch.ResponseTopic{
			{Name: "orders", Partitions: []offsetfetch.ResponsePartition{
				{PartitionIndex: 0, CommittedOffset: 1, ErrorCode: int16(kafka.TopicAuthorizationFailed)},
			}},
		}}}
		_, _, err := GetConsumerGroupLag(consumerConnection(transport), context.Background(), consumerLagMetadata(), "coordinator:9092", "group", LagThresholds{})
		if err == nil || !strings.Contains(err.Error(), "fetch committed offset for orders partition 0") {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("latest offset error", func(t *testing.T) {
		transport := &lagTransport{
			offsetResponse: &offsetfetch.Response{Topics: []offsetfetch.ResponseTopic{
				{Name: "orders", Partitions: []offsetfetch.ResponsePartition{{PartitionIndex: 0, CommittedOffset: 1}}},
			}},
			latestOffsets: map[topicPartition]int64{{topic: "orders", partition: 0}: 2},
			latestError:   int16(kafka.NotLeaderForPartition),
		}
		_, _, err := GetConsumerGroupLag(consumerConnection(transport), context.Background(), consumerLagMetadata(), "coordinator:9092", "group", LagThresholds{})
		if err == nil || !strings.Contains(err.Error(), "fetch latest offset for orders partition 0") {
			t.Fatalf("error = %v", err)
		}
	})
}

func consumerLagMetadata() *kafka.MetadataResponse {
	brokerA := kafka.Broker{ID: 1, Host: "broker-a", Port: 9092}
	brokerB := kafka.Broker{ID: 2, Host: "broker-b", Port: 9093}
	return &kafka.MetadataResponse{Topics: []kafka.Topic{
		{Name: "orders", Partitions: []kafka.Partition{
			{Topic: "orders", ID: 0, Leader: brokerA},
			{Topic: "orders", ID: 1, Leader: brokerB},
		}},
		{Name: "payments", Partitions: []kafka.Partition{
			{Topic: "payments", ID: 0, Leader: brokerA},
			{Topic: "payments", ID: 1, Leader: brokerA},
		}},
	}}
}

var _ kafka.RoundTripper = (*lagTransport)(nil)
