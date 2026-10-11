// Package ui serves Kafkaesque's embedded web interface.
package ui

import (
	"embed"
	"errors"
	"html/template"
	"io/fs"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
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
}

type navigationView struct {
	Active string
}

type connectionView struct {
	Status    string
	Latency   string
	Protocol  string
	Mechanism string
}

type metricView struct {
	Label string
	Value int
}

type consumerGroupView struct {
	Name        string
	Topics      int
	Members     int
	State       string
	StateClass  string
	TotalLag    int
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
	Offline         int
	UnderReplicated int
	Rebalancing     int
	Dead            int
	Lagging         int
}

// NewHandler returns the embedded UI handler.
func NewHandler() http.Handler {
	static, err := fs.Sub(assets, "static")
	if err != nil {
		panic(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /ui", renderDashboard)
	mux.HandleFunc("GET /ui/{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui", http.StatusPermanentRedirect)
	})
	mux.HandleFunc("GET /ui/consumer-groups", renderConsumerGroups)
	mux.HandleFunc("GET /ui/consumer-groups/{group}", renderConsumerGroupDetail)
	mux.Handle("GET /ui/assets/", http.StripPrefix("/ui/assets/", http.FileServerFS(static)))
	return withHeaders(mux)
}

func renderDashboard(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := dashboardTemplate.ExecuteTemplate(w, "dashboard", demoDashboard()); err != nil {
		http.Error(w, "Unable to render Dashboard", http.StatusInternalServerError)
	}
}

func renderConsumerGroups(w http.ResponseWriter, r *http.Request) {
	page, err := positivePage(r.URL.Query().Get("page"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	state := strings.TrimSpace(r.URL.Query().Get("state"))
	groups := filterConsumerGroups(demoConsumerGroups(), query, state)
	results := paginateConsumerGroups(groups, query, state, page)

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if strings.EqualFold(r.Header.Get("HX-Request"), "true") {
		if err := dashboardTemplate.ExecuteTemplate(w, "consumer-groups-results", results); err != nil {
			http.Error(w, "Unable to render consumer groups", http.StatusInternalServerError)
		}
		return
	}
	view := consumerGroupsPage{
		Navigation: navigationView{Active: "consumer-groups"},
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
	options := []stateOption{{Value: ""}, {Value: "Stable"}, {Value: "Rebalancing"}, {Value: "Empty"}, {Value: "Dead"}}
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

func demoDashboard() dashboardPage {
	return dashboardPage{
		Navigation: navigationView{Active: "dashboard"},
		Connection: connectionView{
			Status: "CONNECTED", Latency: "42ms", Protocol: "SASL_SSL", Mechanism: "SCRAM-SHA-256",
		},
		Metrics: []metricView{
			{Label: "Brokers", Value: 3},
			{Label: "Topics", Value: 24},
			{Label: "Partitions", Value: 186},
			{Label: "Consumer Groups", Value: 17},
		},
		ConsumerGroups: []consumerGroupView{
			{Name: "orders-service", Topics: 2, Members: 3, State: "Stable", TotalLag: 136, Health: "Warning", HealthClass: "warning"},
			{Name: "analytics", Topics: 5, Members: 2, State: "Stable", TotalLag: 31, Health: "Healthy", HealthClass: "healthy"},
			{Name: "notification", Topics: 1, Members: 1, State: "Rebalancing", TotalLag: 842, Health: "Critical", HealthClass: "critical"},
			{Name: "payments", Topics: 2, Members: 4, State: "Stable", TotalLag: 0, Health: "Healthy", HealthClass: "healthy"},
		},
		Health: healthView{
			Status: "DEGRADED", Offline: 0, UnderReplicated: 3, Rebalancing: 1, Dead: 0, Lagging: 2,
		},
	}
}

func demoConsumerGroups() []consumerGroupView {
	return []consumerGroupView{
		demoConsumerGroup("analytics", 5, 2, "Stable", 31),
		demoConsumerGroup("audit-log", 2, 1, "Stable", 12),
		demoConsumerGroup("billing-events", 4, 2, "Stable", 617),
		demoConsumerGroup("data-export", 1, 1, "Stable", 0),
		demoConsumerGroup("email-dispatch", 1, 0, "Empty", 0),
		demoConsumerGroup("fraud-detection", 6, 5, "Stable", 89),
		demoConsumerGroup("inventory-sync", 3, 2, "Stable", 54),
		demoConsumerGroup("mobile-events", 2, 3, "Stable", 520),
		demoConsumerGroup("notification", 1, 1, "Rebalancing", 842),
		demoConsumerGroup("orders-service", 2, 3, "Stable", 136),
		demoConsumerGroup("payments", 2, 4, "Stable", 0),
		demoConsumerGroup("reporting", 4, 2, "Stable", 114),
		demoConsumerGroup("recommendations", 5, 3, "Rebalancing", 376),
		demoConsumerGroup("search-indexer", 2, 0, "Dead", 0),
		demoConsumerGroup("shipping-service", 2, 3, "Stable", 208),
		demoConsumerGroup("user-profile", 3, 4, "Stable", 45),
		demoConsumerGroup("warehouse-sync", 3, 2, "Stable", 72),
	}
}

func demoConsumerGroup(name string, topics, members int, state string, lag int) consumerGroupView {
	stateClass := "healthy"
	switch state {
	case "Rebalancing":
		stateClass = "warning"
	case "Dead":
		stateClass = "critical"
	case "Empty":
		stateClass = "neutral"
	}
	health, healthClass := "Healthy", "healthy"
	if lag > 500 {
		health, healthClass = "Critical", "critical"
	} else if lag > 100 {
		health, healthClass = "Warning", "warning"
	}
	return consumerGroupView{
		Name: name, Topics: topics, Members: members, State: state, StateClass: stateClass,
		TotalLag: lag, Health: health, HealthClass: healthClass,
	}
}
