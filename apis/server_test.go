package apis

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	kafkaesque_config "github.com/nikhil25803/kafkaesque/internals/config"
	kafkaesque_consumers "github.com/nikhil25803/kafkaesque/internals/consumers"
	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
	kafkaesque_topics "github.com/nikhil25803/kafkaesque/internals/topics"
	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol/describegroups"
	"github.com/segmentio/kafka-go/protocol/findcoordinator"
	"github.com/segmentio/kafka-go/protocol/listgroups"
	"github.com/segmentio/kafka-go/protocol/listoffsets"
	metadataAPI "github.com/segmentio/kafka-go/protocol/metadata"
	"github.com/segmentio/kafka-go/protocol/offsetfetch"
)

type apiTransport struct {
	metadataError    error
	metadataResponse *metadataAPI.Response
	metadataRequests int
	emptyGroups      bool
	groupState       string
}

func (t *apiTransport) RoundTrip(_ context.Context, _ net.Addr, request kafka.Request) (kafka.Response, error) {
	switch request := request.(type) {
	case *metadataAPI.Request:
		t.metadataRequests++
		if t.metadataError != nil {
			return nil, t.metadataError
		}
		if t.metadataResponse != nil {
			return t.metadataResponse, nil
		}
		return testMetadata(), nil
	case *listgroups.Request:
		if t.emptyGroups {
			return &listgroups.Response{Groups: []listgroups.ResponseGroup{}}, nil
		}
		return &listgroups.Response{Groups: []listgroups.ResponseGroup{{GroupID: "orders-service", ProtocolType: "consumer", BrokerID: 1}}}, nil
	case *describegroups.Request:
		state := t.groupState
		if state == "" {
			state = kafkaesque_consumers.GroupStateStable
		}
		return &describegroups.Response{Groups: []describegroups.ResponseGroup{{
			GroupID: request.Groups[0], GroupState: state,
			Members: []describegroups.ResponseGroupMember{{MemberID: "member-1", ClientID: "worker", ClientHost: "/10.0.0.1"}},
		}}}, nil
	case *findcoordinator.Request:
		if request.Key == "missing" {
			return &findcoordinator.Response{ErrorCode: int16(kafka.GroupIdNotFound)}, nil
		}
		return &findcoordinator.Response{NodeID: 1, Host: "broker-1", Port: 9092}, nil
	case *offsetfetch.Request:
		return &offsetfetch.Response{Topics: []offsetfetch.ResponseTopic{{Name: "orders", Partitions: []offsetfetch.ResponsePartition{{PartitionIndex: 0, CommittedOffset: 90}}}}}, nil
	case *listoffsets.Request:
		return &listoffsets.Response{Topics: []listoffsets.ResponseTopic{{Topic: "orders", Partitions: []listoffsets.ResponsePartition{{Partition: 0, Offset: 100}}}}}, nil
	default:
		return nil, errors.New("unexpected Kafka request")
	}
}

func testMetadata() *metadataAPI.Response {
	return &metadataAPI.Response{
		ClusterID: "cluster-1", ControllerID: 1,
		Brokers: []metadataAPI.ResponseBroker{
			{NodeID: 2, Host: "broker-2", Port: 9092, Rack: "rack-b"},
			{NodeID: 1, Host: "broker-1", Port: 9092, Rack: "rack-a"},
		},
		Topics: []metadataAPI.ResponseTopic{
			{Name: "orders", Partitions: []metadataAPI.ResponsePartition{{PartitionIndex: 0, LeaderID: 1, ReplicaNodes: []int32{1, 2}, IsrNodes: []int32{1, 2}}}},
			{Name: "__internal", IsInternal: true, Partitions: []metadataAPI.ResponsePartition{{PartitionIndex: 0, LeaderID: 2, ReplicaNodes: []int32{2}, IsrNodes: []int32{2}}}},
		},
	}
}

func testHandler(transport kafka.RoundTripper) http.Handler {
	return testHandlerWithRates(transport, nil)
}

type staticMessageRates map[string]kafkaesque_topics.MessageRates

func (r staticMessageRates) Snapshot() map[string]kafkaesque_topics.MessageRates { return r }

func testHandlerWithRates(transport kafka.RoundTripper, rates kafkaesque_topics.MessageRateProvider) http.Handler {
	cfg := &kafkaesque_config.Config{
		Kafka: kafkaesque_config.KafkaConfig{BootstrapServers: []string{"bootstrap:9092"}, ConnectionTimeout: time.Second},
		Lag:   kafkaesque_config.LagConfig{WarningThreshold: 100, UnhealthyThreshold: 500},
	}
	conn := &kafkaesque.KafkaesqueConn{Client: &kafka.Client{Addr: kafka.TCP("bootstrap:9092"), Transport: transport}}
	return newHandlerWithTopicRates(conn, cfg, nil, nil, rates)
}

func TestRoutesReturnJSONEnvelopes(t *testing.T) {
	tests := []struct {
		path string
		want string
	}{
		{"/api/v1/health", `"status":"UP"`},
		{"/api/v1/metadata/connection", `"status":"CONNECTED"`},
		{"/api/v1/metadata/cluster", `"partition_count":2`},
		{"/api/v1/health/cluster", `"status":"HEALTHY"`},
		{"/api/v1/consumers/groups", `"group_name":"orders-service"`},
		{"/api/v1/consumers/groups/orders-service", `"member_id":"member-1"`},
		{"/api/v1/consumers/groups/orders-service/topics/orders/partitions", `"committed_offset":90`},
		{"/api/v1/topics", `"name":"orders"`},
		{"/api/v1/topics/orders", `"partition_count":1`},
		{"/api/v1/topics/orders/partitions", `"status":"HEALTHY"`},
		{"/api/v1/brokers", `"connect_address":"broker-1:9092"`},
	}
	for _, test := range tests {
		t.Run(test.path, func(t *testing.T) {
			transport := &apiTransport{}
			handler := testHandler(transport)
			response := request(t, handler, test.path)
			if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), test.want) ||
				!strings.Contains(response.Body.String(), `"success":true,"status":200,"data":`) || !strings.Contains(response.Body.String(), `"meta":`) {
				t.Fatalf("status/body = %d %s, want %s", response.Code, response.Body.String(), test.want)
			}
			wantMetadataRequests := 1
			if test.path == "/api/v1/health" {
				wantMetadataRequests = 0
			}
			if transport.metadataRequests != wantMetadataRequests {
				t.Fatalf("metadata requests = %d, want %d", transport.metadataRequests, wantMetadataRequests)
			}
			if response.Header().Get("Content-Type") != "application/json" || response.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("headers = %v", response.Header())
			}
		})
	}
}

func TestListFilteringPaginationAndSorting(t *testing.T) {
	handler := testHandler(&apiTransport{})
	response := request(t, handler, "/api/v1/topics?level=internal&q=INTER&page=1&page_size=1")
	body := response.Body.String()
	if response.Code != http.StatusOK || !strings.Contains(body, `"name":"__internal"`) || !strings.Contains(body, `"total_items":1`) {
		t.Fatalf("status/body = %d %s", response.Code, body)
	}

	response = request(t, handler, "/api/v1/brokers?page=2&page_size=1")
	body = response.Body.String()
	if !strings.Contains(body, `"id":2`) || !strings.Contains(body, `"total_pages":2`) {
		t.Fatalf("broker page = %s", body)
	}

	response = request(t, handler, "/api/v1/topics?page=99")
	if !strings.Contains(response.Body.String(), `"data":[]`) || !strings.Contains(response.Body.String(), `"page":99`) {
		t.Fatalf("out-of-range page = %s", response.Body.String())
	}
}

func TestTopicMessageRates(t *testing.T) {
	warmup := request(t, testHandler(&apiTransport{}), "/api/v1/topics/orders")
	for _, field := range []string{
		`"messages_per_second":null`,
		`"messages_per_minute":null`,
		`"messages_per_hour":null`,
	} {
		if !strings.Contains(warmup.Body.String(), field) {
			t.Fatalf("warm-up response missing %s: %s", field, warmup.Body.String())
		}
	}

	rates := staticMessageRates{"orders": {PerSecond: 1, PerMinute: 60, PerHour: 3600}}
	handler := testHandlerWithRates(&apiTransport{}, rates)
	for _, path := range []string{"/api/v1/topics", "/api/v1/topics/orders"} {
		response := request(t, handler, path)
		body := response.Body.String()
		if response.Code != http.StatusOK || !strings.Contains(body, `"messages_per_second":1`) ||
			!strings.Contains(body, `"messages_per_minute":60`) || !strings.Contains(body, `"messages_per_hour":3600`) {
			t.Fatalf("%s response = %d %s", path, response.Code, body)
		}
	}
}

func TestConsumerGroupStateFiltering(t *testing.T) {
	handler := testHandler(&apiTransport{groupState: "PreparingRebalance"})

	response := request(t, handler, "/api/v1/consumers/groups?state=rebalancing")
	body := response.Body.String()
	if response.Code != http.StatusOK || !strings.Contains(body, `"group_name":"orders-service"`) || !strings.Contains(body, `"state":"Rebalancing"`) {
		t.Fatalf("rebalancing filter = %d %s", response.Code, body)
	}

	response = request(t, handler, "/api/v1/consumers/groups?state=stable")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"data":[]`) {
		t.Fatalf("stable filter = %d %s", response.Code, response.Body.String())
	}

	response = request(t, handler, "/api/v1/consumers/groups/orders-service")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"state":"PreparingRebalance"`) {
		t.Fatalf("consumer detail state = %d %s", response.Code, response.Body.String())
	}
}

func TestInvalidQueriesAndMissingResources(t *testing.T) {
	handler := testHandler(&apiTransport{})
	for _, path := range []string{
		"/api/v1/topics?page=0", "/api/v1/topics?page_size=101", "/api/v1/topics?level=private",
		"/api/v1/topics/orders/partitions?status=warning", "/api/v1/consumers/groups?state=PreparingRebalance",
	} {
		response := request(t, handler, path)
		if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), `"success":false,"status":400,"data":null`) || !strings.Contains(response.Body.String(), `"code":"invalid_request"`) {
			t.Fatalf("%s = %d %s", path, response.Code, response.Body.String())
		}
	}
	for _, path := range []string{"/api/v1/topics/missing", "/api/v1/topics/missing/partitions", "/api/v1/consumers/groups/missing"} {
		response := request(t, handler, path)
		if response.Code != http.StatusNotFound || !strings.Contains(response.Body.String(), `"success":false,"status":404,"data":null`) || !strings.Contains(response.Body.String(), `"code":"not_found"`) {
			t.Fatalf("%s = %d %s", path, response.Code, response.Body.String())
		}
	}
}

func TestKafkaErrorsAreSanitizedAndMapped(t *testing.T) {
	secret := "super-secret"
	response := request(t, testHandler(&apiTransport{metadataError: errors.New("dial failed with " + secret)}), "/api/v1/metadata/cluster")
	if response.Code != http.StatusBadGateway || strings.Contains(response.Body.String(), secret) || !strings.Contains(response.Body.String(), `"success":false,"status":502,"data":null`) || !strings.Contains(response.Body.String(), `"code":"upstream_error"`) {
		t.Fatalf("upstream response = %d %s", response.Code, response.Body.String())
	}

	response = request(t, testHandler(&apiTransport{metadataError: context.DeadlineExceeded}), "/api/v1/topics")
	if response.Code != http.StatusGatewayTimeout || !strings.Contains(response.Body.String(), `"success":false,"status":504,"data":null`) || !strings.Contains(response.Body.String(), `"code":"timeout"`) {
		t.Fatalf("timeout response = %d %s", response.Code, response.Body.String())
	}

	response = request(t, testHandler(&apiTransport{metadataError: errors.New("SASL failed with " + secret)}), "/api/v1/metadata/connection")
	if response.Code != http.StatusBadGateway || strings.Contains(response.Body.String(), secret) || !strings.Contains(response.Body.String(), `"success":false,"status":502,"data":null`) {
		t.Fatalf("connection response = %d %s", response.Code, response.Body.String())
	}
}

func TestConnectionReportsSecurityWithoutSecrets(t *testing.T) {
	cfg := &kafkaesque_config.Config{
		Kafka: kafkaesque_config.KafkaConfig{
			BootstrapServers: []string{"bootstrap:9092"}, ConnectionTimeout: time.Second,
			Security: kafkaesque_config.SecurityConfig{
				TLS:  kafkaesque_config.TLSConfig{Enabled: true, ClientCertFile: "/private/client.pem"},
				SASL: kafkaesque_config.SASLConfig{Mechanism: "SCRAM-SHA-256", Username: "user", Password: "super-secret"},
			},
		},
	}
	conn := &kafkaesque.KafkaesqueConn{Client: &kafka.Client{Addr: kafka.TCP("bootstrap:9092"), Transport: &apiTransport{}}}
	response := request(t, newHandler(conn, cfg, nil, nil), "/api/v1/metadata/connection")
	body := response.Body.String()
	if !strings.Contains(body, `"protocol":"SASL_SSL"`) || !strings.Contains(body, `"sasl_mechanism":"SCRAM-SHA-256"`) || !strings.Contains(body, `"mtls":true`) {
		t.Fatalf("security response = %s", body)
	}
	if strings.Contains(body, "super-secret") || strings.Contains(body, "/private/client.pem") || strings.Contains(body, `"username"`) {
		t.Fatalf("security response exposed configuration: %s", body)
	}
}

func TestConnectionMiddlewareAllowsDegradedMetadata(t *testing.T) {
	metadata := testMetadata()
	metadata.Topics[0].Partitions[0].ErrorCode = int16(kafka.LeaderNotAvailable)
	metadata.Topics[0].Partitions[0].LeaderID = -1
	metadata.Topics[0].Partitions[0].IsrNodes = []int32{1}
	response := request(t, testHandler(&apiTransport{metadataResponse: metadata, emptyGroups: true}), "/api/v1/health/cluster")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"status":"DEGRADED"`) {
		t.Fatalf("degraded response = %d %s", response.Code, response.Body.String())
	}
}

func TestUnknownRouteDoesNotCheckKafka(t *testing.T) {
	transport := &apiTransport{metadataError: errors.New("must not be called")}
	response := request(t, testHandler(transport), "/api/v1/missing")
	if response.Code != http.StatusNotFound || transport.metadataRequests != 0 || !strings.Contains(response.Body.String(), `"success":false,"status":404,"data":null`) {
		t.Fatalf("unknown route = %d, metadata requests = %d", response.Code, transport.metadataRequests)
	}
}

func TestUptimeDoesNotRequireKafka(t *testing.T) {
	transport := &apiTransport{metadataError: errors.New("Kafka unavailable")}
	response := request(t, testHandler(transport), "/api/v1/health")
	if response.Code != http.StatusOK || transport.metadataRequests != 0 || !strings.Contains(response.Body.String(), `"success":true,"status":200,"data":{"status":"UP"}`) {
		t.Fatalf("uptime response = %d %s, metadata requests = %d", response.Code, response.Body.String(), transport.metadataRequests)
	}
}

func TestServeStopsAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Serve(ctx, "127.0.0.1:0", http.NewServeMux()); err != nil {
		t.Fatal(err)
	}
}

func request(t *testing.T, handler http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, path, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}
