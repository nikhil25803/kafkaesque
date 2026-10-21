// Package ui serves Kafkaesque's embedded web interface.
package ui

import (
	"context"
	"embed"
	"errors"
	"html/template"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	imageassets "github.com/nikhil25803/kafkaesque/asset/image"
	kafkaesque_auth "github.com/nikhil25803/kafkaesque/internals/auth"
	kafkaesque_config "github.com/nikhil25803/kafkaesque/internals/config"
	kafkaesque_consumers "github.com/nikhil25803/kafkaesque/internals/consumers"
	kafkaesque_health "github.com/nikhil25803/kafkaesque/internals/health"
	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
)

//go:embed templates/*.html static/*
var assets embed.FS

var dashboardTemplate = template.Must(template.ParseFS(assets, "templates/*.html"))

type dashboardPage struct {
	Navigation     navigationView
	Connection     connectionView
	Metrics        []metricView
	ConsumerGroups []consumerGroupView
	Health         healthView
	GroupCountText string
}

type navigationView struct {
	Active    string
	Username  string
	Role      string
	CSRFToken string
	ShowUsers bool
}

type connectionView struct {
	Status    string
	Latency   string
	Protocol  string
	Mechanism string
}

type metricView struct {
	Label string
	Value int64
}

type consumerGroupView struct {
	Name        string
	URL         string
	Topics      int
	Members     int
	State       string
	StateClass  string
	TotalLag    int64
	Health      string
	HealthClass string
}

type consumerGroupsPage struct {
	Navigation navigationView
	Query      string
	States     []stateOption
	Results    consumerGroupsResults
}

type stateOption struct {
	Value    string
	Selected bool
}

type consumerGroupsResults struct {
	Groups      []consumerGroupView
	Range       string
	Pages       []pageLink
	PreviousURL string
	NextURL     string
	HasFilters  bool
}

type pageLink struct {
	Number int
	URL    string
	Active bool
}

type healthView struct {
	Status          string
	StatusClass     string
	Offline         int
	UnderReplicated int
	Rebalancing     int
	Dead            int
	Lagging         int
}

func dashboardView(r *http.Request, data *dashboardData, source dataSource) dashboardPage {
	metadata := data.Snapshot.Metadata
	partitionCount := 0
	for _, topic := range metadata.Topics {
		partitionCount += len(topic.Partitions)
	}
	groups := consumerGroupViews(data.Groups, source.Thresholds())
	total := len(groups)
	shown := min(4, total)
	return dashboardPage{
		Navigation:     navigationFor(r, "dashboard"),
		Connection:     connectionView{Status: "CONNECTED", Latency: strconv.FormatInt(data.Snapshot.Latency.Milliseconds(), 10) + "ms", Protocol: source.Protocol(), Mechanism: source.Authentication()},
		Metrics:        []metricView{{Label: "Brokers", Value: int64(len(metadata.Brokers))}, {Label: "Topics", Value: int64(len(metadata.Topics))}, {Label: "Partitions", Value: int64(partitionCount)}, {Label: "Consumer Groups", Value: int64(total)}},
		ConsumerGroups: groups[:shown], Health: healthViewFrom(data.Health),
		GroupCountText: strconv.Itoa(shown) + " of " + strconv.Itoa(total),
	}
}

func consumerGroupViews(groups []kafkaesque_consumers.ConsumerGroupInformation, thresholds kafkaesque_consumers.LagThresholds) []consumerGroupView {
	views := make([]consumerGroupView, 0, len(groups))
	for _, group := range groups {
		health, healthClass := lagStatusView(kafkaesque_consumers.ConsumerLagStatus(group.TotalLag, thresholds))
		state := kafkaesque_consumers.GroupStateCategory(group.State)
		views = append(views, consumerGroupView{Name: group.GroupName, URL: "/ui/consumer-groups/" + url.PathEscape(group.GroupName), Topics: group.TopicsCount, Members: group.MembersCount, State: state, StateClass: stateClass(state), TotalLag: group.TotalLag, Health: health, HealthClass: healthClass})
	}
	sort.Slice(views, func(i, j int) bool { return views[i].Name < views[j].Name })
	return views
}

func stateClass(state string) string {
	switch kafkaesque_consumers.GroupStateCategory(state) {
	case kafkaesque_consumers.GroupStateStable:
		return "healthy"
	case kafkaesque_consumers.GroupStateDead:
		return "critical"
	case kafkaesque_consumers.GroupStateEmpty:
		return "neutral"
	default:
		return "warning"
	}
}

func lagStatusView(status string) (string, string) {
	switch status {
	case kafkaesque_consumers.LagStatusUnhealthy:
		return "Critical", "critical"
	case kafkaesque_consumers.LagStatusWarning:
		return "Warning", "warning"
	default:
		return "Healthy", "healthy"
	}
}

func healthViewFrom(info *kafkaesque_health.Information) healthView {
	class := "healthy"
	if info.Status != kafkaesque_health.StatusHealthy {
		class = "warning"
	}
	return healthView{Status: info.Status, StatusClass: class, Offline: info.OfflinePartitionCount, UnderReplicated: info.UnderReplicatedCount, Rebalancing: info.RebalancingGroupCount, Dead: info.DeadGroupCount, Lagging: info.LaggingGroupCount}
}

// NewHandler returns an authenticated embedded UI handler.
func NewHandler(
	conn *kafkaesque.KafkaesqueConn,
	cfg *kafkaesque_config.Config,
	authentication kafkaesque_auth.RuntimeManager,
	logger *slog.Logger,
) http.Handler {
	if authentication == nil {
		panic("authentication manager is required")
	}
	return newHandlerWithAuth(
		newLiveDataSource(conn, cfg),
		authentication,
		cfg.Authentication.CookieSecure,
		logger,
	)
}

type dataSourceKey struct{}

func newHandler(source dataSource) http.Handler {
	return newHandlerWithAuth(source, nil, false, nil)
}

func newHandlerWithAuth(
	source dataSource,
	authentication kafkaesque_auth.RuntimeManager,
	cookieSecure bool,
	logger *slog.Logger,
) http.Handler {
	static, err := fs.Sub(assets, "static")
	if err != nil {
		panic(err)
	}

	mux := http.NewServeMux()
	limiter := kafkaesque_auth.NewLimiter()
	protect := func(handler http.HandlerFunc) http.HandlerFunc {
		if authentication == nil {
			return handler
		}
		return authenticateUI(authentication, cookieSecure, logger, handler)
	}
	if authentication != nil {
		mux.HandleFunc("GET /ui/login", renderLogin)
		mux.HandleFunc("POST /ui/login", loginUI(authentication, limiter, cookieSecure, logger))
		mux.HandleFunc("POST /ui/logout", protect(logoutUI(authentication, cookieSecure, logger)))
		mux.HandleFunc("GET /ui/users", protect(renderUsers(authentication, logger)))
		mux.HandleFunc("GET /ui/users/new", protect(renderNewUser))
		mux.HandleFunc("POST /ui/users", protect(createUserUI(authentication, logger)))
	}
	mux.HandleFunc("GET /ui", protect(renderDashboard))
	mux.HandleFunc("GET /ui/{$}", func(w http.ResponseWriter, r *http.Request) {
		protect(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/ui", http.StatusPermanentRedirect) })(w, r)
	})
	mux.HandleFunc("GET /ui/consumer-groups", protect(renderConsumerGroups))
	mux.HandleFunc("GET /ui/consumer-groups/{group}", protect(renderConsumerGroupDetail))
	mux.HandleFunc("GET /ui/topics", protect(renderTopics))
	mux.HandleFunc("GET /ui/brokers", protect(renderBrokers))
	mux.HandleFunc("GET /ui/cluster-health", protect(renderClusterHealth))
	mux.Handle("GET /ui/assets/KafkaesqueLogo.png", http.StripPrefix("/ui/assets/", http.FileServerFS(imageassets.Files)))
	mux.Handle("GET /ui/assets/", http.StripPrefix("/ui/assets/", http.FileServerFS(static)))
	return withHeaders(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), dataSourceKey{}, source)))
	}))
}

func sourceFrom(r *http.Request) dataSource { return r.Context().Value(dataSourceKey{}).(dataSource) }

func renderDashboard(w http.ResponseWriter, r *http.Request) {
	data, err := sourceFrom(r).Dashboard(r.Context())
	if err != nil {
		renderUpstreamError(w, r, "dashboard", err)
		return
	}
	view := dashboardView(r, data, sourceFrom(r))
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := dashboardTemplate.ExecuteTemplate(w, "dashboard", view); err != nil {
		http.Error(w, "Unable to render Dashboard", http.StatusInternalServerError)
	}
}

func renderConsumerGroups(w http.ResponseWriter, r *http.Request) {
	page, err := positivePage(r.URL.Query().Get("page"))
	if err != nil {
		renderUIError(w, r, http.StatusBadRequest, "consumer-groups", "Invalid consumer group request", err.Error())
		return
	}
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	state := strings.TrimSpace(r.URL.Query().Get("state"))
	if state != "" && !kafkaesque_consumers.IsGroupStateCategory(state) {
		renderUIError(w, r, http.StatusBadRequest, "consumer-groups", "Invalid consumer group request", "state must be stable, rebalancing, empty, dead, or unknown")
		return
	}
	information, err := sourceFrom(r).ConsumerGroups(r.Context())
	if err != nil {
		renderUpstreamError(w, r, "consumer-groups", err)
		return
	}
	groups := filterConsumerGroups(consumerGroupViews(information, sourceFrom(r).Thresholds()), query, state)
	results := paginateConsumerGroups(groups, query, state, page)

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if strings.EqualFold(r.Header.Get("HX-Request"), "true") {
		if err := dashboardTemplate.ExecuteTemplate(w, "consumer-groups-results", results); err != nil {
			http.Error(w, "Unable to render consumer groups", http.StatusInternalServerError)
		}
		return
	}
	view := consumerGroupsPage{
		Navigation: navigationFor(r, "consumer-groups"),
		Query:      query,
		States:     consumerStateOptions(state),
		Results:    results,
	}
	if err := dashboardTemplate.ExecuteTemplate(w, "consumer-groups", view); err != nil {
		http.Error(w, "Unable to render consumer groups", http.StatusInternalServerError)
	}
}

func positivePage(raw string) (int, error) {
	if raw == "" {
		return 1, nil
	}
	page, err := strconv.Atoi(raw)
	if err != nil || page < 1 {
		return 0, errors.New("page must be a positive integer")
	}
	return page, nil
}

func filterConsumerGroups(groups []consumerGroupView, query, state string) []consumerGroupView {
	filtered := make([]consumerGroupView, 0, len(groups))
	for _, group := range groups {
		if strings.Contains(strings.ToLower(group.Name), strings.ToLower(query)) &&
			(state == "" || strings.EqualFold(group.State, state)) {
			filtered = append(filtered, group)
		}
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].Name < filtered[j].Name })
	return filtered
}

func paginateConsumerGroups(groups []consumerGroupView, query, state string, page int) consumerGroupsResults {
	const pageSize = 10
	total := len(groups)
	totalPages := (total + pageSize - 1) / pageSize
	start := (page - 1) * pageSize
	if start > total {
		start = total
	}
	end := min(start+pageSize, total)
	pageLinks := make([]pageLink, 0, totalPages)
	for number := 1; number <= totalPages; number++ {
		pageLinks = append(pageLinks, pageLink{Number: number, URL: consumerGroupsURL(query, state, number), Active: number == page})
	}
	rangeText := "0 groups"
	if start < end {
		rangeText = strconv.Itoa(start+1) + "–" + strconv.Itoa(end) + " of " + strconv.Itoa(total) + " groups"
	} else if total > 0 {
		rangeText = "0 of " + strconv.Itoa(total) + " groups"
	}
	results := consumerGroupsResults{
		Groups: groups[start:end], Range: rangeText, Pages: pageLinks,
		HasFilters: query != "" || state != "",
	}
	if page > 1 && page <= totalPages {
		results.PreviousURL = consumerGroupsURL(query, state, page-1)
	}
	if page < totalPages {
		results.NextURL = consumerGroupsURL(query, state, page+1)
	}
	return results
}

func consumerGroupsURL(query, state string, page int) string {
	values := url.Values{}
	if query != "" {
		values.Set("q", query)
	}
	if state != "" {
		values.Set("state", state)
	}
	values.Set("page", strconv.Itoa(page))
	return "/ui/consumer-groups?" + values.Encode()
}

func consumerStateOptions(selected string) []stateOption {
	options := []stateOption{
		{Value: ""},
		{Value: kafkaesque_consumers.GroupStateStable},
		{Value: kafkaesque_consumers.GroupStateRebalancing},
		{Value: kafkaesque_consumers.GroupStateEmpty},
		{Value: kafkaesque_consumers.GroupStateDead},
		{Value: kafkaesque_consumers.GroupStateUnknown},
	}
	for i := range options {
		options[i].Selected = strings.EqualFold(options[i].Value, selected)
	}
	return options
}

func withHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; img-src 'self'; style-src 'self'; script-src 'self'; base-uri 'none'; frame-ancestors 'none'")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		next.ServeHTTP(w, r)
	})
}
