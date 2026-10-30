package apis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	kafkaesque_auth "github.com/nikhil25803/kafkaesque/internals/auth"
	kafkaesque_brokers "github.com/nikhil25803/kafkaesque/internals/brokers"
	kafkaesque_config "github.com/nikhil25803/kafkaesque/internals/config"
	kafkaesque_consumers "github.com/nikhil25803/kafkaesque/internals/consumers"
	kafkaesque_health "github.com/nikhil25803/kafkaesque/internals/health"
	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
	kafkaesque_metadata "github.com/nikhil25803/kafkaesque/internals/metadata"
	kafkaesque_partitions "github.com/nikhil25803/kafkaesque/internals/partitions"
	kafkaesque_topics "github.com/nikhil25803/kafkaesque/internals/topics"
	kafka "github.com/segmentio/kafka-go"
)

type server struct {
	conn       *kafkaesque.KafkaesqueConn
	config     *kafkaesque_config.Config
	topicRates kafkaesque_topics.MessageRateProvider
	thresholds kafkaesque_consumers.LagThresholds
	auth       kafkaesque_auth.Manager
	users      kafkaesque_auth.UserManager
	roles      kafkaesque_auth.RoleManager
	limiter    *kafkaesque_auth.Limiter
	logger     *slog.Logger
}

type envelope struct {
	Success bool      `json:"success"`
	Status  int       `json:"status"`
	Data    any       `json:"data"`
	Meta    any       `json:"meta,omitempty"`
	Error   *apiError `json:"error,omitempty"`
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type pageMeta struct {
	Page       int `json:"page"`
	PageSize   int `json:"page_size"`
	TotalItems int `json:"total_items"`
	TotalPages int `json:"total_pages"`
}

type pageRequest struct {
	query    string
	page     int
	pageSize int
}

type connectionSnapshotKey struct{}

type connectionInformation struct {
	Status        string `json:"status"`
	LatencyMS     int64  `json:"latency_ms"`
	Protocol      string `json:"protocol"`
	SASLMechanism string `json:"sasl_mechanism,omitempty"`
	MTLS          bool   `json:"mtls"`
}

type clusterInformation struct {
	ClusterID      string `json:"cluster_id"`
	ControllerID   int    `json:"controller_id"`
	BrokerCount    int    `json:"broker_count"`
	TopicCount     int    `json:"topic_count"`
	PartitionCount int    `json:"partition_count"`
}

type consumerGroupSummary struct {
	GroupName    string   `json:"group_name"`
	Topics       []string `json:"topics"`
	MembersCount int      `json:"members_count"`
	State        string   `json:"state"`
	TotalLag     int64    `json:"total_lag"`
}

type consumerGroupDetail struct {
	*kafkaesque_consumers.ConsumerGroupInformation
	Members []kafkaesque_consumers.ConsumerGroupMember `json:"members"`
}

type topicResponse struct {
	Name              string   `json:"name"`
	Level             string   `json:"level"`
	PartitionCount    int      `json:"partition_count"`
	ReplicationFactor int      `json:"replication_factor"`
	MessagesPerSecond *float64 `json:"messages_per_second"`
	MessagesPerMinute *float64 `json:"messages_per_minute"`
	MessagesPerHour   *float64 `json:"messages_per_hour"`
}

// NewHandler returns an authenticated API handler.
func NewHandler(
	conn *kafkaesque.KafkaesqueConn,
	cfg *kafkaesque_config.Config,
	authentication kafkaesque_auth.RuntimeManager,
	logger *slog.Logger,
) http.Handler {
	if authentication == nil {
		panic("authentication manager is required")
	}
	return NewHandlerWithTopicRates(conn, cfg, authentication, logger, nil)
}

// NewHandlerWithTopicRates returns an API handler backed by a shared topic-rate collector.
func NewHandlerWithTopicRates(
	conn *kafkaesque.KafkaesqueConn,
	cfg *kafkaesque_config.Config,
	authentication kafkaesque_auth.RuntimeManager,
	logger *slog.Logger,
	topicRates kafkaesque_topics.MessageRateProvider,
) http.Handler {
	if authentication == nil {
		panic("authentication manager is required")
	}
	return newHandlerWithTopicRates(conn, cfg, authentication, logger, topicRates)
}

func newHandler(
	conn *kafkaesque.KafkaesqueConn,
	cfg *kafkaesque_config.Config,
	authentication kafkaesque_auth.RuntimeManager,
	logger *slog.Logger,
) http.Handler {
	return newHandlerWithTopicRates(conn, cfg, authentication, logger, nil)
}

func newHandlerWithTopicRates(
	conn *kafkaesque.KafkaesqueConn,
	cfg *kafkaesque_config.Config,
	authentication kafkaesque_auth.RuntimeManager,
	logger *slog.Logger,
	topicRates kafkaesque_topics.MessageRateProvider,
) http.Handler {
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	s := &server{
		conn:       conn,
		config:     cfg,
		topicRates: topicRates,
		auth:       authentication,
		users:      authentication,
		roles:      authentication,
		limiter:    kafkaesque_auth.NewLimiter(),
		logger:     logger,
		thresholds: kafkaesque_consumers.LagThresholds{
			Warning:   cfg.Lag.WarningThreshold,
			Unhealthy: cfg.Lag.UnhealthyThreshold,
		},
	}
	mux := http.NewServeMux()

	// Public health check.
	mux.HandleFunc("GET /api/v1/health", s.uptime)

	// Authentication endpoints.
	if authentication != nil {
		mux.HandleFunc("POST /api/v1/auth/login", s.login)
		mux.HandleFunc("POST /api/v1/auth/refresh", s.refresh)
		mux.HandleFunc("POST /api/v1/auth/logout", s.logout)
		mux.HandleFunc("GET /api/v1/auth/me", s.authenticate(s.me))
	}

	// User accounts.
	if authentication != nil {
		mux.HandleFunc(
			"GET /api/v1/users",
			s.authenticate(s.authorize(kafkaesque_auth.PermissionUserView, s.listUsers)),
		)
		mux.HandleFunc(
			"POST /api/v1/users",
			s.authenticate(s.authorize(kafkaesque_auth.PermissionUserCreate, s.createUser)),
		)
		mux.HandleFunc(
			"GET /api/v1/users/{id}",
			s.authenticate(s.authorize(kafkaesque_auth.PermissionUserView, s.getUser)),
		)
		mux.HandleFunc(
			"PATCH /api/v1/users/{id}",
			s.authenticate(s.authorize(kafkaesque_auth.PermissionUserEdit, s.updateUser)),
		)
		mux.HandleFunc(
			"DELETE /api/v1/users/{id}",
			s.authenticate(s.authorize(kafkaesque_auth.PermissionUserDelete, s.deleteUser)),
		)

		// Roles and permissions.
		mux.HandleFunc("GET /api/v1/roles", s.authenticate(s.superAdmin(s.listRoles)))
		mux.HandleFunc("POST /api/v1/roles", s.authenticate(s.superAdmin(s.createRole)))
		mux.HandleFunc("GET /api/v1/roles/{id}", s.authenticate(s.superAdmin(s.getRole)))
		mux.HandleFunc("PATCH /api/v1/roles/{id}", s.authenticate(s.superAdmin(s.updateRole)))
		mux.HandleFunc("DELETE /api/v1/roles/{id}", s.authenticate(s.superAdmin(s.deleteRole)))
	}

	// Protect every Kafka-backed endpoint.
	protect := func(handler http.HandlerFunc) http.HandlerFunc {
		if authentication == nil {
			return handler
		}
		return s.authenticate(handler)
	}

	// Kafka metadata and cluster health.
	mux.HandleFunc("GET /api/v1/metadata/connection", protect(s.withConnection(s.connection)))
	mux.HandleFunc("GET /api/v1/metadata/cluster", protect(s.withConnection(s.cluster)))
	mux.HandleFunc("GET /api/v1/health/cluster", protect(s.withConnection(s.health)))

	// Consumer groups and lag.
	mux.HandleFunc("GET /api/v1/consumers/groups", protect(s.withConnection(s.consumerGroups)))
	mux.HandleFunc(
		"GET /api/v1/consumers/groups/{group}",
		protect(s.withConnection(s.consumerGroup)),
	)
	mux.HandleFunc(
		"GET /api/v1/consumers/groups/{group}/topics/{topic}/partitions",
		protect(s.withConnection(s.consumerTopicPartitions)),
	)

	// Topics and partitions.
	mux.HandleFunc("GET /api/v1/topics", protect(s.withConnection(s.topics)))
	mux.HandleFunc("GET /api/v1/topics/{topic}", protect(s.withConnection(s.topic)))
	mux.HandleFunc(
		"GET /api/v1/topics/{topic}/partitions",
		protect(s.withConnection(s.topicPartitions)),
	)

	// Brokers.
	mux.HandleFunc("GET /api/v1/brokers", protect(s.withConnection(s.brokers)))

	// Unknown routes.
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		writeNotFound(w, "route", "")
	})

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		mux.ServeHTTP(w, r)
	})
}

func (s *server) uptime(w http.ResponseWriter, _ *http.Request) {
	writeSuccess(w, http.StatusOK, map[string]string{"status": "UP"}, map[string]any{})
}

// Serve runs the API until ctx is cancelled, then allows ten seconds for shutdown.
func Serve(ctx context.Context, address string, handler http.Handler) error {
	if ctx.Err() != nil {
		return nil
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	httpServer := &http.Server{
		Addr: address, Handler: handler,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       time.Minute,
	}
	shutdownDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = httpServer.Shutdown(shutdownContext)
		case <-shutdownDone:
		}
	}()
	err = httpServer.Serve(listener)
	close(shutdownDone)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *server) connection(w http.ResponseWriter, r *http.Request) {
	snapshot := snapshotFrom(r)
	info := connectionInformation{
		Status: "CONNECTED", Protocol: kafkaesque.SecurityProtocol(s.config.Kafka),
		SASLMechanism: s.config.Kafka.Security.SASL.Mechanism,
		MTLS:          s.config.Kafka.Security.TLS.ClientCertFile != "",
		LatencyMS:     snapshot.Latency.Milliseconds(),
	}
	writeSuccess(w, http.StatusOK, info, map[string]any{})
}

func (s *server) cluster(w http.ResponseWriter, r *http.Request) {
	metadata := snapshotFrom(r).Metadata
	base := kafkaesque_metadata.GetMetadataInformation(metadata)
	writeSuccess(w, http.StatusOK, clusterInformation{
		ClusterID: base.ClusterID, ControllerID: base.ControllerID,
		BrokerCount: base.BrokerCount, TopicCount: base.TopicCount,
		PartitionCount: partitionCount(metadata),
	}, map[string]any{})
}

func (s *server) health(w http.ResponseWriter, r *http.Request) {
	metadata := snapshotFrom(r).Metadata
	info, err := kafkaesque_health.GetInformation(s.conn, r.Context(), metadata, s.thresholds)
	if err != nil {
		writeUpstreamError(w, err)
		return
	}
	writeSuccess(w, http.StatusOK, info, map[string]any{})
}

func (s *server) consumerGroups(w http.ResponseWriter, r *http.Request) {
	page, err := parsePage(r)
	if err != nil {
		writeInvalid(w, err.Error())
		return
	}
	state := strings.TrimSpace(r.URL.Query().Get("state"))
	if state != "" && !kafkaesque_consumers.IsGroupStateCategory(state) {
		writeInvalid(w, "state must be stable, rebalancing, empty, dead, or unknown")
		return
	}
	metadata := snapshotFrom(r).Metadata
	groups, err := kafkaesque_consumers.GetConsumerGroupsInformation(s.conn, r.Context(), metadata, s.thresholds)
	if err != nil {
		writeUpstreamError(w, err)
		return
	}
	items := make([]consumerGroupSummary, 0, len(groups))
	for _, group := range groups {
		groupState := kafkaesque_consumers.GroupStateCategory(group.State)
		if !containsFold(group.GroupName, page.query) || state != "" && !strings.EqualFold(groupState, state) {
			continue
		}
		topicNames := make([]string, 0, len(group.Topics))
		for _, topic := range group.Topics {
			topicNames = append(topicNames, topic.Topic)
		}
		items = append(items, consumerGroupSummary{
			GroupName: group.GroupName, Topics: topicNames, MembersCount: group.MembersCount,
			State: groupState, TotalLag: group.TotalLag,
		})
	}
	start, end, meta := pageBounds(len(items), page)
	writeSuccess(w, http.StatusOK, items[start:end], meta)
}

func (s *server) consumerGroup(w http.ResponseWriter, r *http.Request) {
	metadata := snapshotFrom(r).Metadata
	group := r.PathValue("group")
	info, err := kafkaesque_consumers.GetConsumerGroupInformation(s.conn, r.Context(), metadata, group, s.thresholds, true)
	if err != nil {
		writeNamedError(w, "consumer group", group, err)
		return
	}
	members := info.Members
	if members == nil {
		members = []kafkaesque_consumers.ConsumerGroupMember{}
	}
	writeSuccess(w, http.StatusOK, consumerGroupDetail{ConsumerGroupInformation: info, Members: members}, map[string]any{})
}

func (s *server) consumerTopicPartitions(w http.ResponseWriter, r *http.Request) {
	page, err := parsePage(r)
	if err != nil {
		writeInvalid(w, err.Error())
		return
	}
	metadata := snapshotFrom(r).Metadata
	group, topic := r.PathValue("group"), r.PathValue("topic")
	info, err := kafkaesque_consumers.GetConsumerGroupTopicInformation(s.conn, r.Context(), metadata, group, topic)
	if err != nil {
		writeNamedError(w, "consumer group or topic", group+"/"+topic, err)
		return
	}
	start, end, meta := pageBounds(len(info.Partitions), page)
	info.Partitions = info.Partitions[start:end]
	writeSuccess(w, http.StatusOK, info, meta)
}

func (s *server) topics(w http.ResponseWriter, r *http.Request) {
	page, err := parsePage(r)
	if err != nil {
		writeInvalid(w, err.Error())
		return
	}
	level := strings.TrimSpace(r.URL.Query().Get("level"))
	if level != "" && !strings.EqualFold(level, "internal") && !strings.EqualFold(level, "external") {
		writeInvalid(w, "level must be internal or external")
		return
	}
	metadata := snapshotFrom(r).Metadata
	items := kafkaesque_topics.AddMessageRates(
		kafkaesque_topics.GetTopicInformation(metadata),
		s.messageRates(),
	)
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	filtered := make([]kafkaesque_topics.TopicInformation, 0, len(items))
	for _, item := range items {
		if containsFold(item.Name, page.query) && (level == "" || strings.EqualFold(item.Level, level)) {
			filtered = append(filtered, item)
		}
	}
	start, end, meta := pageBounds(len(filtered), page)
	writeSuccess(w, http.StatusOK, topicResponses(filtered[start:end]), meta)
}

func (s *server) topic(w http.ResponseWriter, r *http.Request) {
	metadata := snapshotFrom(r).Metadata
	name := r.PathValue("topic")
	items := kafkaesque_topics.AddMessageRates(
		kafkaesque_topics.GetTopicInformation(metadata),
		s.messageRates(),
	)
	for _, topic := range items {
		if topic.Name == name {
			writeSuccess(w, http.StatusOK, newTopicResponse(topic), map[string]any{})
			return
		}
	}
	writeNotFound(w, "topic", name)
}

func (s *server) messageRates() map[string]kafkaesque_topics.MessageRates {
	if s.topicRates == nil {
		return nil
	}
	return s.topicRates.Snapshot()
}

func topicResponses(topics []kafkaesque_topics.TopicInformation) []topicResponse {
	responses := make([]topicResponse, len(topics))
	for i, topic := range topics {
		responses[i] = newTopicResponse(topic)
	}
	return responses
}

func newTopicResponse(topic kafkaesque_topics.TopicInformation) topicResponse {
	return topicResponse{
		Name:              topic.Name,
		Level:             topic.Level,
		PartitionCount:    topic.PartitionCount,
		ReplicationFactor: topic.ReplicationFactor,
		MessagesPerSecond: topic.MessagesPerSecond,
		MessagesPerMinute: topic.MessagesPerMinute,
		MessagesPerHour:   topic.MessagesPerHour,
	}
}

func (s *server) topicPartitions(w http.ResponseWriter, r *http.Request) {
	page, err := parsePage(r)
	if err != nil {
		writeInvalid(w, err.Error())
		return
	}
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	if status != "" && !strings.EqualFold(status, "healthy") && !strings.EqualFold(status, "degraded") {
		writeInvalid(w, "status must be healthy or degraded")
		return
	}
	metadata := snapshotFrom(r).Metadata
	name := r.PathValue("topic")
	items, err := kafkaesque_partitions.GetPartitionInformation(metadata, name)
	if err != nil {
		writeNamedError(w, "topic", name, err)
		return
	}
	filtered := make([]kafkaesque_partitions.PartitionTopicInformation, 0, len(items))
	for _, item := range items {
		if status == "" || strings.EqualFold(item.Status, status) {
			filtered = append(filtered, item)
		}
	}
	start, end, meta := pageBounds(len(filtered), page)
	writeSuccess(w, http.StatusOK, filtered[start:end], meta)
}

func (s *server) brokers(w http.ResponseWriter, r *http.Request) {
	page, err := parsePage(r)
	if err != nil {
		writeInvalid(w, err.Error())
		return
	}
	metadata := snapshotFrom(r).Metadata
	items := kafkaesque_brokers.GetBrokerInformation(metadata, s.conn.ResolveBrokerAddress)
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	filtered := make([]kafkaesque_brokers.BrokerInformation, 0, len(items))
	for _, item := range items {
		searchable := fmt.Sprintf("%d %s %s %s", item.ID, item.Address, item.ConnectAddress, item.Rack)
		if containsFold(searchable, page.query) {
			filtered = append(filtered, item)
		}
	}
	start, end, meta := pageBounds(len(filtered), page)
	writeSuccess(w, http.StatusOK, filtered[start:end], meta)
}

func (s *server) withConnection(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		snapshot, err := s.conn.GetMetadataSnapshot(r.Context())
		if err != nil {
			writeUpstreamError(w, err)
			return
		}
		next(w, r.WithContext(context.WithValue(r.Context(), connectionSnapshotKey{}, snapshot)))
	}
}

func snapshotFrom(r *http.Request) *kafkaesque.MetadataSnapshot {
	return r.Context().Value(connectionSnapshotKey{}).(*kafkaesque.MetadataSnapshot)
}

func parsePage(r *http.Request) (pageRequest, error) {
	result := pageRequest{query: strings.TrimSpace(r.URL.Query().Get("q")), page: 1, pageSize: 25}
	var err error
	if raw := r.URL.Query().Get("page"); raw != "" {
		result.page, err = strconv.Atoi(raw)
		if err != nil || result.page < 1 {
			return pageRequest{}, fmt.Errorf("page must be positive")
		}
	}
	if raw := r.URL.Query().Get("page_size"); raw != "" {
		result.pageSize, err = strconv.Atoi(raw)
		if err != nil || result.pageSize < 1 || result.pageSize > 100 {
			return pageRequest{}, fmt.Errorf("page_size must be between 1 and 100")
		}
	}
	return result, nil
}

func pageBounds(total int, page pageRequest) (int, int, pageMeta) {
	pages := 0
	if total > 0 {
		pages = (total + page.pageSize - 1) / page.pageSize
	}
	start := (page.page - 1) * page.pageSize
	if start > total {
		start = total
	}
	end := min(start+page.pageSize, total)
	return start, end, pageMeta{Page: page.page, PageSize: page.pageSize, TotalItems: total, TotalPages: pages}
}

func partitionCount(metadata *kafka.MetadataResponse) int {
	count := 0
	for _, topic := range metadata.Topics {
		count += len(topic.Partitions)
	}
	return count
}

func containsFold(value, query string) bool {
	return query == "" || strings.Contains(strings.ToLower(value), strings.ToLower(query))
}

func writeNamedError(w http.ResponseWriter, kind, name string, err error) {
	message := strings.ToLower(err.Error())
	notFound := errors.Is(err, kafka.GroupIdNotFound) ||
		strings.Contains(message, "not found") ||
		strings.Contains(message, "no committed offsets")
	if notFound {
		writeNotFound(w, kind, name)
		return
	}

	writeUpstreamError(w, err)
}

func writeInvalid(w http.ResponseWriter, message string) {
	writeError(w, http.StatusBadRequest, "invalid_request", message)
}

func writeNotFound(w http.ResponseWriter, kind, name string) {
	writeError(w, http.StatusNotFound, "not_found", fmt.Sprintf("%s %q not found", kind, name))
}

func writeUpstreamError(w http.ResponseWriter, err error) {
	status, code, message := http.StatusBadGateway, "upstream_error", "Kafka request failed"
	var networkError net.Error
	timedOut := errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, kafka.RequestTimedOut) ||
		errors.As(err, &networkError) && networkError.Timeout()
	if timedOut {
		status, code, message = http.StatusGatewayTimeout, "timeout", "Kafka request timed out"
	}

	writeError(w, status, code, message)
}

func writeSuccess(w http.ResponseWriter, status int, data, meta any) {
	writeJSON(w, status, envelope{Success: true, Status: status, Data: data, Meta: meta})
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, envelope{Status: status, Data: nil, Error: &apiError{Code: code, Message: message}})
}

func writeJSON(w http.ResponseWriter, status int, value envelope) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
