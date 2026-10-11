package ui

import (
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

type topicView struct {
	Name              string
	Level             string
	Partitions        int
	ReplicationFactor int
}

type topicsPage struct {
	Navigation navigationView
	Query      string
	Levels     []levelOption
	Results    topicsResults
}

type levelOption struct {
	Value    string
	Label    string
	Selected bool
}

type topicsResults struct {
	Topics      []topicView
	Range       string
	Pages       []pageLink
	PreviousURL string
	NextURL     string
	HasFilters  bool
}

func renderTopics(w http.ResponseWriter, r *http.Request) {
	page, err := positivePage(r.URL.Query().Get("page"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	level := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("level")))
	if level != "" && level != "internal" && level != "external" {
		http.Error(w, "level must be internal, external, or omitted", http.StatusBadRequest)
		return
	}
	results := paginateTopics(filterTopics(demoTopics(), query, level), query, level, page)

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if strings.EqualFold(r.Header.Get("HX-Request"), "true") {
		if err := dashboardTemplate.ExecuteTemplate(w, "topics-results", results); err != nil {
			http.Error(w, "Unable to render topics", http.StatusInternalServerError)
		}
		return
	}
	view := topicsPage{
		Navigation: navigationView{Active: "topics"},
		Query:      query,
		Levels:     topicLevelOptions(level),
		Results:    results,
	}
	if err := dashboardTemplate.ExecuteTemplate(w, "topics", view); err != nil {
		http.Error(w, "Unable to render topics", http.StatusInternalServerError)
	}
}

func filterTopics(topics []topicView, query, level string) []topicView {
	filtered := make([]topicView, 0, len(topics))
	for _, topic := range topics {
		if strings.Contains(strings.ToLower(topic.Name), strings.ToLower(query)) &&
			(level == "" || strings.EqualFold(topic.Level, level)) {
			filtered = append(filtered, topic)
		}
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].Name < filtered[j].Name })
	return filtered
}

func paginateTopics(topics []topicView, query, level string, page int) topicsResults {
	const pageSize = 10
	total := len(topics)
	totalPages := (total + pageSize - 1) / pageSize
	start := (page - 1) * pageSize
	if start > total {
		start = total
	}
	end := min(start+pageSize, total)
	pages := make([]pageLink, 0, totalPages)
	for number := 1; number <= totalPages; number++ {
		pages = append(pages, pageLink{Number: number, URL: topicsURL(query, level, number), Active: number == page})
	}
	rangeText := "0 topics"
	if start < end {
		rangeText = strconv.Itoa(start+1) + "–" + strconv.Itoa(end) + " of " + strconv.Itoa(total) + " topics"
	} else if total > 0 {
		rangeText = "0 of " + strconv.Itoa(total) + " topics"
	}
	results := topicsResults{
		Topics: topics[start:end], Range: rangeText, Pages: pages,
		HasFilters: query != "" || level != "",
	}
	if page > 1 && page <= totalPages {
		results.PreviousURL = topicsURL(query, level, page-1)
	}
	if page < totalPages {
		results.NextURL = topicsURL(query, level, page+1)
	}
	return results
}

func topicsURL(query, level string, page int) string {
	values := url.Values{}
	if query != "" {
		values.Set("q", query)
	}
	if level != "" {
		values.Set("level", level)
	}
	values.Set("page", strconv.Itoa(page))
	return "/ui/topics?" + values.Encode()
}

func topicLevelOptions(selected string) []levelOption {
	options := []levelOption{
		{Value: "", Label: "All levels"},
		{Value: "external", Label: "External"},
		{Value: "internal", Label: "Internal"},
	}
	for i := range options {
		options[i].Selected = options[i].Value == selected
	}
	return options
}

func demoTopics() []topicView {
	return []topicView{
		{Name: "analytics-events", Level: "External", Partitions: 6, ReplicationFactor: 3},
		{Name: "audit-events", Level: "External", Partitions: 3, ReplicationFactor: 3},
		{Name: "billing-events", Level: "External", Partitions: 6, ReplicationFactor: 3},
		{Name: "customer-events", Level: "External", Partitions: 3, ReplicationFactor: 3},
		{Name: "data-export", Level: "External", Partitions: 3, ReplicationFactor: 2},
		{Name: "email-events", Level: "External", Partitions: 3, ReplicationFactor: 3},
		{Name: "fraud-alerts", Level: "External", Partitions: 6, ReplicationFactor: 3},
		{Name: "inventory-events", Level: "External", Partitions: 3, ReplicationFactor: 3},
		{Name: "mobile-events", Level: "External", Partitions: 4, ReplicationFactor: 3},
		{Name: "notification-events", Level: "External", Partitions: 3, ReplicationFactor: 3},
		{Name: "orders", Level: "External", Partitions: 6, ReplicationFactor: 3},
		{Name: "orders-dlq", Level: "External", Partitions: 3, ReplicationFactor: 2},
		{Name: "payments", Level: "External", Partitions: 6, ReplicationFactor: 3},
		{Name: "payments-dlq", Level: "External", Partitions: 3, ReplicationFactor: 2},
		{Name: "recommendation-events", Level: "External", Partitions: 3, ReplicationFactor: 3},
		{Name: "reporting-events", Level: "External", Partitions: 3, ReplicationFactor: 2},
		{Name: "search-events", Level: "External", Partitions: 3, ReplicationFactor: 3},
		{Name: "shipping-events", Level: "External", Partitions: 6, ReplicationFactor: 3},
		{Name: "user-events", Level: "External", Partitions: 3, ReplicationFactor: 3},
		{Name: "warehouse-events", Level: "External", Partitions: 3, ReplicationFactor: 3},
		{Name: "web-events", Level: "External", Partitions: 6, ReplicationFactor: 3},
		{Name: "__cluster_metadata", Level: "Internal", Partitions: 1, ReplicationFactor: 3},
		{Name: "__consumer_offsets", Level: "Internal", Partitions: 50, ReplicationFactor: 3},
		{Name: "__transaction_state", Level: "Internal", Partitions: 50, ReplicationFactor: 3},
	}
}
