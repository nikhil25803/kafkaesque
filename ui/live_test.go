package ui

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	kafkaesque_brokers "github.com/nikhil25803/kafkaesque/internals/brokers"
	kafkaesque_config "github.com/nikhil25803/kafkaesque/internals/config"
	kafkaesque_consumers "github.com/nikhil25803/kafkaesque/internals/consumers"
	kafkaesque_health "github.com/nikhil25803/kafkaesque/internals/health"
	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
	kafkaesque_topics "github.com/nikhil25803/kafkaesque/internals/topics"
	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol/listgroups"
	metadataAPI "github.com/segmentio/kafka-go/protocol/metadata"
)

type fakeDataSource struct {
	err            error
	dashboardCalls int
	groups         []kafkaesque_consumers.ConsumerGroupInformation
	health         *kafkaesque_health.Information
}

func (f *fakeDataSource) Dashboard(context.Context) (*dashboardData, error) {
	f.dashboardCalls++
	if f.err != nil {
		return nil, f.err
	}
	metadata := &kafka.MetadataResponse{
		Brokers: []kafka.Broker{{ID: 1}, {ID: 2}},
		Topics: []kafka.Topic{
			{Name: "orders", Partitions: []kafka.Partition{{ID: 0}, {ID: 1}}},
			{Name: "__consumer_offsets", Internal: true, Partitions: []kafka.Partition{{ID: 0}}},
		},
	}
	return &dashboardData{
		Snapshot: &kafkaesque.MetadataSnapshot{Metadata: metadata, Latency: 42 * time.Millisecond},
		Groups:   f.groupInformation(),
		Health:   f.healthInformation(),
	}, nil
}

func (f *fakeDataSource) ConsumerGroups(context.Context) ([]kafkaesque_consumers.ConsumerGroupInformation, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.groupInformation(), nil
}

func (f *fakeDataSource) ConsumerGroup(_ context.Context, group, topic string) (*groupDetailData, error) {
	if f.err != nil {
		return nil, f.err
	}
	if group != "orders-service" {
		return nil, kafka.GroupIdNotFound
	}
	if topic == "" {
		topic = "orders"
	}
	if topic != "orders" {
		return nil, errors.New("topic not found")
	}
	info := f.groupInformation()[1]
	info.Members = []kafkaesque_consumers.ConsumerGroupMember{{
		MemberID: "consumer-1", ClientID: "worker", Host: "10.0.0.12",
		Assignments: []kafkaesque_consumers.ConsumerGroupMemberAssignment{{Topic: "orders", Partitions: []int{0}}},
	}}
	return &groupDetailData{
		Group: &info, SelectedTopic: "orders",
		Topic: &kafkaesque_consumers.ConsumerGroupTopicInformation{
			GroupName: group, Topic: "orders", TotalLag: 136,
			Partitions: []kafkaesque_consumers.ConsumerGroupPartitionLag{
				{Partition: 0, CommittedOffset: 9_000_000_000, LogEndOffset: 9_000_000_124, Lag: 124},
				{Partition: 1, CommittedOffset: 80, LogEndOffset: 92, Lag: 12},
			},
		},
	}, nil
}

func (f *fakeDataSource) Topics(context.Context) ([]kafkaesque_topics.TopicInformation, error) {
	if f.err != nil {
		return nil, f.err
	}
	return []kafkaesque_topics.TopicInformation{
		{Name: "orders", Level: "External", PartitionCount: 2, ReplicationFactor: 2},
		{Name: "__consumer_offsets", Level: "Internal", PartitionCount: 1, ReplicationFactor: 2},
	}, nil
}

func (f *fakeDataSource) Brokers(context.Context) ([]kafkaesque_brokers.BrokerInformation, error) {
	if f.err != nil {
		return nil, f.err
	}
	return []kafkaesque_brokers.BrokerInformation{
		{ID: 1, Address: "broker-1:9092", ConnectAddress: "10.0.0.11:9092", Rack: "rack-a"},
		{ID: 2, Address: "broker-2:9092", ConnectAddress: "broker-2:9092"},
	}, nil
}

func (f *fakeDataSource) Health(context.Context) (*kafkaesque_health.Information, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.healthInformation(), nil
}

func (*fakeDataSource) Protocol() string       { return "SASL_SSL" }
func (*fakeDataSource) Authentication() string { return "SCRAM-SHA-256 + mTLS" }
func (*fakeDataSource) Thresholds() kafkaesque_consumers.LagThresholds {
	return kafkaesque_consumers.LagThresholds{Warning: 100, Unhealthy: 500}
}

func (f *fakeDataSource) groupInformation() []kafkaesque_consumers.ConsumerGroupInformation {
	if f.groups != nil {
		return f.groups
	}
	return fixtureGroups()
}

func (f *fakeDataSource) healthInformation() *kafkaesque_health.Information {
	if f.health != nil {
		return f.health
	}
	return fixtureHealth()
}

func fixtureGroups() []kafkaesque_consumers.ConsumerGroupInformation {
	return []kafkaesque_consumers.ConsumerGroupInformation{
		{GroupName: "analytics", State: "Stable", MembersCount: 2, TopicsCount: 1, TotalLag: 31},
		{
			GroupName: "orders-service", State: "Stable", MembersCount: 1, TopicsCount: 1, TotalLag: 136,
			LagSummary: kafkaesque_consumers.ConsumerGroupLagSummary{TotalLag: 136, MaximumPartitionLag: 124, AveragePartitionLag: 68, WarningPartitions: 1, HealthyPartitions: 1},
			Topics:     []kafkaesque_consumers.ConsumerGroupTopicLag{{Topic: "orders", Partitions: 2, Lag: 136, Status: kafkaesque_consumers.LagStatusWarning}},
		},
	}
}

func fixtureHealth() *kafkaesque_health.Information {
	return &kafkaesque_health.Information{
		Status: kafkaesque_health.StatusDegraded, BrokerCount: 2, TopicCount: 2, PartitionCount: 3,
		ConsumerGroupCount: 2, UnderReplicatedCount: 1, RebalancingGroupCount: 1, LaggingGroupCount: 1,
	}
}

func requestUI(t *testing.T, handler http.Handler, path string, htmx bool) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if htmx {
		req.Header.Set("HX-Request", "true")
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	return response
}

func requireContains(t *testing.T, body string, values ...string) {
	t.Helper()
	for _, value := range values {
		if !strings.Contains(body, value) {
			t.Errorf("response does not contain %q", value)
		}
	}
}

func TestLivePages(t *testing.T) {
	source := &fakeDataSource{}
	handler := newHandler(source)
	tests := []struct {
		path string
		want []string
	}{
		{"/ui", []string{"Dashboard", "CONNECTED", "42ms", "SASL_SSL", "SCRAM-SHA-256", "mTLS", "2 of 2", "orders-service", "DEGRADED"}},
		{"/ui/consumer-groups", []string{"Consumer Groups", "1–2 of 2 groups", "analytics", "orders-service", "Warning"}},
		{"/ui/topics", []string{"Topics", "1–2 of 2 topics", "orders", "__consumer_offsets", "Internal"}},
		{"/ui/brokers", []string{"Brokers", "1–2 of 2 brokers", "broker-1:9092", "10.0.0.11:9092", "rack-a", "—"}},
		{"/ui/cluster-health", []string{"Cluster Health", "Cluster requires attention", "Under-replicated partitions", "Lagging groups"}},
	}
	for _, test := range tests {
		t.Run(test.path, func(t *testing.T) {
			response := requestUI(t, handler, test.path, false)
			if response.Code != http.StatusOK {
				t.Fatalf("status = %d", response.Code)
			}
			requireContains(t, response.Body.String(), test.want...)
			if strings.Contains(response.Body.String(), "Demo data") {
				t.Error("live page contains demo-data label")
			}
		})
	}
	if source.dashboardCalls != 1 {
		t.Fatalf("dashboard collections = %d, want 1", source.dashboardCalls)
	}
}

func TestDashboardShowsHealthBeforeWorkloads(t *testing.T) {
	response := requestUI(t, newHandler(&fakeDataSource{}), "/ui", false)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}

	body := response.Body.String()
	healthPosition := strings.Index(body, "Cluster health")
	workloadsPosition := strings.Index(body, "Workloads")
	if healthPosition == -1 || workloadsPosition == -1 {
		t.Fatal("dashboard is missing cluster health or workloads")
	}
	if healthPosition > workloadsPosition {
		t.Fatal("cluster health is rendered after workloads")
	}
}

func TestHealthColorsFollowCurrentValues(t *testing.T) {
	healthy := &kafkaesque_health.Information{Status: kafkaesque_health.StatusHealthy}
	dashboard := requestUI(t, newHandler(&fakeDataSource{health: healthy}), "/ui", false)
	requireContains(t, dashboard.Body.String(),
		"<dt>Offline partitions</dt><dd>0</dd>",
		"<dt>Under-replicated</dt><dd>0</dd>",
		"<dt>Rebalancing groups</dt><dd>0</dd>",
		"<dt>Dead groups</dt><dd>0</dd>",
		"<dt>Lagging groups</dt><dd>0</dd>",
	)

	healthPage := requestUI(t, newHandler(&fakeDataSource{health: healthy}), "/ui/cluster-health", false)
	requireContains(t, healthPage.Body.String(), `class="health-status-panel healthy"`)

	degraded := &kafkaesque_health.Information{
		Status:                kafkaesque_health.StatusDegraded,
		OfflinePartitionCount: 1,
		UnderReplicatedCount:  2,
		RebalancingGroupCount: 3,
		DeadGroupCount:        4,
		LaggingGroupCount:     5,
	}
	dashboard = requestUI(t, newHandler(&fakeDataSource{health: degraded}), "/ui", false)
	requireContains(t, dashboard.Body.String(),
		`<dt>Offline partitions</dt><dd class="critical-text">1</dd>`,
		`<dt>Under-replicated</dt><dd class="warning-text">2</dd>`,
		`<dt>Rebalancing groups</dt><dd class="warning-text">3</dd>`,
		`<dt>Dead groups</dt><dd class="critical-text">4</dd>`,
		`<dt>Lagging groups</dt><dd class="warning-text">5</dd>`,
	)

	healthPage = requestUI(t, newHandler(&fakeDataSource{health: degraded}), "/ui/cluster-health", false)
	requireContains(t, healthPage.Body.String(), `class="health-status-panel warning"`)
}

func TestLagSummaryColorsOnlyPositiveCounts(t *testing.T) {
	response := requestUI(t, newHandler(&fakeDataSource{}), "/ui/consumer-groups/orders-service", false)
	requireContains(t, response.Body.String(),
		`<dt>Healthy Partitions</dt><dd class="healthy-text">1</dd>`,
		`<dt>Warning Partitions</dt><dd class="warning-text">1</dd>`,
		"<dt>Unhealthy Partitions</dt><dd>0</dd>",
	)

	groups := fixtureGroups()
	groups[1].LagSummary.HealthyPartitions = 0
	groups[1].LagSummary.WarningPartitions = 0
	groups[1].LagSummary.UnhealthyPartitions = 1
	response = requestUI(t, newHandler(&fakeDataSource{groups: groups}), "/ui/consumer-groups/orders-service", false)
	requireContains(t, response.Body.String(),
		"<dt>Healthy Partitions</dt><dd>0</dd>",
		"<dt>Warning Partitions</dt><dd>0</dd>",
		`<dt>Unhealthy Partitions</dt><dd class="critical-text">1</dd>`,
	)
}

func TestLiveFilteringAndFragments(t *testing.T) {
	handler := newHandler(&fakeDataSource{})
	tests := []struct {
		path, want string
	}{
		{"/ui/consumer-groups?q=ORDERS", "orders-service"},
		{"/ui/topics?level=internal", "__consumer_offsets"},
		{"/ui/brokers?q=rack-a", "broker-1:9092"},
	}
	for _, test := range tests {
		response := requestUI(t, handler, test.path, true)
		if response.Code != http.StatusOK {
			t.Fatalf("%s status = %d", test.path, response.Code)
		}
		requireContains(t, response.Body.String(), test.want)
		if strings.Contains(response.Body.String(), "<!doctype html>") {
			t.Fatalf("%s returned a full page", test.path)
		}
	}
}

func TestConsumerLagUsesLiveInt64OffsetsAndAssignments(t *testing.T) {
	handler := newHandler(&fakeDataSource{})
	response := requestUI(t, handler, "/ui/consumer-groups/orders-service?topic=orders&sort=lag&direction=desc&group_by=member", false)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
	requireContains(t, response.Body.String(), "9000000000", "9000000124", "consumer-1", "Unassigned", "Total lag 136")
}

func TestUIErrorsAreSanitizedAndHTMXSwappable(t *testing.T) {
	handler := newHandler(&fakeDataSource{err: context.DeadlineExceeded})
	response := requestUI(t, handler, "/ui/topics", true)
	if response.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504", response.Code)
	}
	requireContains(t, response.Body.String(), "Kafka request timed out")
	requireContains(t, response.Body.String(), `class="panel error-panel"`)
	if strings.Contains(response.Body.String(), "deadline exceeded") || strings.Contains(response.Body.String(), "<!doctype html>") {
		t.Fatalf("unsafe or full HTMX error response: %q", response.Body.String())
	}

	response = requestUI(t, newHandler(&fakeDataSource{}), "/ui/topics?page=0", false)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", response.Code)
	}
	requireContains(t, response.Body.String(), "page must be a positive integer")
	requireContains(t, response.Body.String(), `class="main-content error-page"`)
}

func TestUIAssetsAndRedirect(t *testing.T) {
	handler := newHandler(&fakeDataSource{})
	response := requestUI(t, handler, "/ui/", false)
	if response.Code != http.StatusPermanentRedirect || response.Header().Get("Location") != "/ui" {
		t.Fatalf("redirect = %d %q", response.Code, response.Header().Get("Location"))
	}
	response = requestUI(t, handler, "/ui/assets/app.js", false)
	if response.Code != http.StatusOK {
		t.Fatalf("asset status = %d", response.Code)
	}
	requireContains(t, response.Body.String(), "htmx:beforeSwap", "event.key === 'Escape'")
}

type dashboardTransport struct {
	metadataRequests int
	groupRequests    int
}

func (t *dashboardTransport) RoundTrip(_ context.Context, _ net.Addr, request kafka.Request) (kafka.Response, error) {
	switch request.(type) {
	case *metadataAPI.Request:
		t.metadataRequests++
		return &metadataAPI.Response{}, nil
	case *listgroups.Request:
		t.groupRequests++
		return &listgroups.Response{}, nil
	default:
		return nil, errors.New("unexpected Kafka request")
	}
}

func TestLiveDashboardReusesItsSnapshots(t *testing.T) {
	transport := &dashboardTransport{}
	conn := &kafkaesque.KafkaesqueConn{Client: &kafka.Client{Addr: kafka.TCP("broker:9092"), Transport: transport}}
	source := newLiveDataSource(conn, &kafkaesque_config.Config{})
	if _, err := source.Dashboard(context.Background()); err != nil {
		t.Fatal(err)
	}
	if transport.metadataRequests != 1 || transport.groupRequests != 1 {
		t.Fatalf("requests = metadata %d, groups %d; want one each", transport.metadataRequests, transport.groupRequests)
	}
}

func TestAuthenticationLabels(t *testing.T) {
	tests := []struct {
		name, mechanism, certificate, want string
	}{
		{name: "none", want: "None"},
		{name: "SASL", mechanism: "PLAIN", want: "PLAIN"},
		{name: "mTLS", certificate: "client.pem", want: "mTLS"},
		{name: "combined", mechanism: "SCRAM-SHA-512", certificate: "client.pem", want: "SCRAM-SHA-512 + mTLS"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := &kafkaesque_config.Config{}
			cfg.Kafka.Security.SASL.Mechanism = test.mechanism
			cfg.Kafka.Security.TLS.ClientCertFile = test.certificate
			if got := newLiveDataSource(nil, cfg).Authentication(); got != test.want {
				t.Fatalf("authentication = %q, want %q", got, test.want)
			}
		})
	}
}
