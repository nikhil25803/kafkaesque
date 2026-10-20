package health

import (
	"context"
	"errors"
	"net"
	"reflect"
	"strings"
	"testing"

	kafkaesque_consumers "github.com/nikhil25803/kafkaesque/internals/consumers"
	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol/describegroups"
	"github.com/segmentio/kafka-go/protocol/listgroups"
	"github.com/segmentio/kafka-go/protocol/listoffsets"
	"github.com/segmentio/kafka-go/protocol/offsetfetch"
)

type healthTransport struct {
	groups      []listgroups.ResponseGroup
	states      map[string]string
	committed   map[string]int64
	latest      int64
	requests    []string
	failRequest string
}

func (t *healthTransport) RoundTrip(_ context.Context, address net.Addr, request kafka.Request) (kafka.Response, error) {
	requestName := ""
	switch request := request.(type) {
	case *listgroups.Request:
		requestName = "list"
		t.requests = append(t.requests, requestName)
		if t.failRequest == requestName {
			return nil, errors.New("request failed")
		}
		return &listgroups.Response{Groups: t.groups}, nil
	case *describegroups.Request:
		group := request.Groups[0]
		requestName = "describe:" + group
		t.requests = append(t.requests, requestName+":"+address.String())
		if t.failRequest == requestName {
			return nil, errors.New("request failed")
		}
		return &describegroups.Response{Groups: []describegroups.ResponseGroup{{
			GroupID: group, GroupState: t.states[group],
		}}}, nil
	case *offsetfetch.Request:
		requestName = "offset:" + request.GroupID
		t.requests = append(t.requests, requestName+":"+address.String())
		if t.failRequest == requestName {
			return nil, errors.New("request failed")
		}
		return &offsetfetch.Response{Topics: []offsetfetch.ResponseTopic{{
			Name: "orders", Partitions: []offsetfetch.ResponsePartition{{PartitionIndex: 0, CommittedOffset: t.committed[request.GroupID]}},
		}}}, nil
	case *listoffsets.Request:
		requestName = "latest"
		t.requests = append(t.requests, requestName+":"+address.String())
		if t.failRequest == requestName {
			return nil, errors.New("request failed")
		}
		return &listoffsets.Response{Topics: []listoffsets.ResponseTopic{{
			Topic: "orders", Partitions: []listoffsets.ResponsePartition{{Partition: 0, Offset: t.latest}},
		}}}, nil
	default:
		return nil, errors.New("unexpected request")
	}
}

func TestGetInformationDegraded(t *testing.T) {
	transport := &healthTransport{
		groups: []listgroups.ResponseGroup{
			{GroupID: "lagging", ProtocolType: "consumer", BrokerID: 1},
			{GroupID: "boundary", ProtocolType: "consumer", BrokerID: 1},
			{GroupID: "classic-rebalance", ProtocolType: "consumer", BrokerID: 1},
			{GroupID: "modern-rebalance", ProtocolType: "consumer", BrokerID: 1},
			{GroupID: "dead", ProtocolType: "consumer", BrokerID: 1},
			{GroupID: "connector", ProtocolType: "connect", BrokerID: 1},
		},
		states: map[string]string{
			"lagging": "Stable", "boundary": "Empty", "classic-rebalance": "PreparingRebalance",
			"modern-rebalance": "Assigning", "dead": "Dead",
		},
		committed: map[string]int64{
			"lagging": 0, "boundary": 1, "classic-rebalance": 101, "modern-rebalance": 101,
		},
		latest: 101,
	}
	metadata := healthMetadata()
	metadata.Topics[0].Partitions = append(metadata.Topics[0].Partitions, kafka.Partition{
		Topic: "orders", ID: 1, Leader: kafka.Broker{ID: -1},
		Replicas: []kafka.Broker{{ID: 1}, {ID: 2}}, Isr: []kafka.Broker{{ID: 1}},
		Error: kafka.LeaderNotAvailable,
	})

	got, err := GetInformation(healthConnection(transport), context.Background(), metadata, kafkaesque_consumers.LagThresholds{Warning: 100, Unhealthy: 500})
	if err != nil {
		t.Fatal(err)
	}
	want := &Information{
		Status: StatusDegraded, BrokerCount: 2, TopicCount: 1, PartitionCount: 2,
		ConsumerGroupCount: 5, OfflinePartitionCount: 1, UnderReplicatedCount: 1,
		RebalancingGroupCount: 2, DeadGroupCount: 1, LaggingGroupCount: 1,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("health = %+v, want %+v", got, want)
	}
	for _, request := range transport.requests {
		if strings.HasPrefix(request, "offset:dead") {
			t.Fatalf("dead group lag was requested: %v", transport.requests)
		}
		if strings.Contains(request, "connector") {
			t.Fatalf("non-consumer group was inspected: %v", transport.requests)
		}
	}
	if !contains(transport.requests, "describe:lagging:broker:9092") || !contains(transport.requests, "latest:leader:9093") {
		t.Fatalf("requests were not routed through coordinator and leader: %v", transport.requests)
	}
}

func TestGetInformationHealthy(t *testing.T) {
	transport := &healthTransport{
		groups:    []listgroups.ResponseGroup{{GroupID: "orders", ProtocolType: "consumer", BrokerID: 1}},
		states:    map[string]string{"orders": "Stable"},
		committed: map[string]int64{"orders": 1},
		latest:    101,
	}
	got, err := GetInformation(healthConnection(transport), context.Background(), healthMetadata(), kafkaesque_consumers.LagThresholds{Warning: 100, Unhealthy: 500})
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusHealthy || got.LaggingGroupCount != 0 || got.ConsumerGroupCount != 1 {
		t.Fatalf("health = %+v, want healthy boundary result", got)
	}
}

func TestGetInformationDegradationTriggers(t *testing.T) {
	tests := []struct {
		name      string
		metadata  func() *kafka.MetadataResponse
		transport *healthTransport
		count     func(*Information) int
	}{
		{
			name: "offline partition",
			metadata: func() *kafka.MetadataResponse {
				metadata := healthMetadata()
				metadata.Topics[0].Partitions[0].Leader.ID = -1
				return metadata
			},
			transport: &healthTransport{},
			count:     func(info *Information) int { return info.OfflinePartitionCount },
		},
		{
			name: "under-replicated partition",
			metadata: func() *kafka.MetadataResponse {
				metadata := healthMetadata()
				metadata.Topics[0].Partitions[0].Isr = metadata.Topics[0].Partitions[0].Isr[:1]
				return metadata
			},
			transport: &healthTransport{},
			count:     func(info *Information) int { return info.UnderReplicatedCount },
		},
		{
			name:      "rebalancing group",
			metadata:  healthMetadata,
			transport: healthGroupTransport("Assigning", 0, 0),
			count:     func(info *Information) int { return info.RebalancingGroupCount },
		},
		{
			name:      "unknown group state",
			metadata:  healthMetadata,
			transport: healthGroupTransport("FutureState", 0, 0),
			count:     func(info *Information) int { return info.RebalancingGroupCount },
		},
		{
			name:      "dead group",
			metadata:  healthMetadata,
			transport: healthGroupTransport("Dead", 0, 0),
			count:     func(info *Information) int { return info.DeadGroupCount },
		},
		{
			name:      "lag above unhealthy threshold",
			metadata:  healthMetadata,
			transport: healthGroupTransport("Stable", 0, 501),
			count:     func(info *Information) int { return info.LaggingGroupCount },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := GetInformation(healthConnection(test.transport), context.Background(), test.metadata(), kafkaesque_consumers.LagThresholds{Warning: 100, Unhealthy: 500})
			if err != nil {
				t.Fatal(err)
			}
			if got.Status != StatusDegraded || test.count(got) != 1 {
				t.Fatalf("health = %+v, want one %s degradation", got, test.name)
			}
		})
	}
}

func TestGetInformationEmptyCluster(t *testing.T) {
	got, err := GetInformation(healthConnection(&healthTransport{}), context.Background(), &kafka.MetadataResponse{}, kafkaesque_consumers.LagThresholds{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusHealthy || got.BrokerCount != 0 || got.PartitionCount != 0 || got.ConsumerGroupCount != 0 {
		t.Fatalf("health = %+v, want empty healthy cluster", got)
	}
}

func TestGetInformationReturnsIncompleteDataErrors(t *testing.T) {
	tests := []struct {
		name      string
		metadata  *kafka.MetadataResponse
		transport *healthTransport
		want      string
	}{
		{name: "topic metadata", metadata: &kafka.MetadataResponse{Topics: []kafka.Topic{{Name: "orders", Error: kafka.TopicAuthorizationFailed}}}, transport: &healthTransport{}, want: "metadata for topic orders"},
		{name: "partition metadata", metadata: &kafka.MetadataResponse{Topics: []kafka.Topic{{Name: "orders", Partitions: []kafka.Partition{{ID: 0, Error: kafka.TopicAuthorizationFailed}}}}}, transport: &healthTransport{}, want: "metadata for topic orders partition 0"},
		{name: "list groups", metadata: healthMetadata(), transport: &healthTransport{failRequest: "list"}, want: "list consumer groups"},
		{name: "missing coordinator", metadata: healthMetadata(), transport: &healthTransport{groups: []listgroups.ResponseGroup{{GroupID: "orders", ProtocolType: "consumer", BrokerID: 9}}}, want: "coordinator broker 9"},
		{name: "describe", metadata: healthMetadata(), transport: &healthTransport{groups: []listgroups.ResponseGroup{{GroupID: "orders", ProtocolType: "consumer", BrokerID: 1}}, failRequest: "describe:orders"}, want: "describe consumer group orders"},
		{name: "lag", metadata: healthMetadata(), transport: &healthTransport{groups: []listgroups.ResponseGroup{{GroupID: "orders", ProtocolType: "consumer", BrokerID: 1}}, states: map[string]string{"orders": "Stable"}, failRequest: "offset:orders"}, want: "get consumer group orders lag"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := GetInformation(healthConnection(test.transport), context.Background(), test.metadata, kafkaesque_consumers.LagThresholds{Warning: 100, Unhealthy: 500})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func healthMetadata() *kafka.MetadataResponse {
	return &kafka.MetadataResponse{
		Brokers: []kafka.Broker{{ID: 1, Host: "broker", Port: 9092}, {ID: 2, Host: "leader", Port: 9093}},
		Topics: []kafka.Topic{{Name: "orders", Partitions: []kafka.Partition{{
			Topic: "orders", ID: 0, Leader: kafka.Broker{ID: 2, Host: "leader", Port: 9093},
			Replicas: []kafka.Broker{{ID: 1}, {ID: 2}}, Isr: []kafka.Broker{{ID: 1}, {ID: 2}},
		}}}},
	}
}

func healthConnection(transport kafka.RoundTripper) *kafkaesque.KafkaesqueConn {
	return &kafkaesque.KafkaesqueConn{Client: &kafka.Client{Addr: kafka.TCP("bootstrap:9092"), Transport: transport}}
}

func healthGroupTransport(state string, committed, latest int64) *healthTransport {
	return &healthTransport{
		groups:    []listgroups.ResponseGroup{{GroupID: "orders", ProtocolType: "consumer", BrokerID: 1}},
		states:    map[string]string{"orders": state},
		committed: map[string]int64{"orders": committed},
		latest:    latest,
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
