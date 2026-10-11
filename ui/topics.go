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
		renderUIError(w, r, http.StatusBadRequest, "topics", "Invalid topics request", err.Error())
		return
	}
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	level := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("level")))
	if level != "" && level != "internal" && level != "external" {
		renderUIError(w, r, http.StatusBadRequest, "topics", "Invalid topics request", "level must be internal, external, or omitted")
		return
	}
	information, err := sourceFrom(r).Topics(r.Context())
	if err != nil {
		renderUpstreamError(w, r, "topics", err)
		return
	}
	topics := make([]topicView, 0, len(information))
	for _, topic := range information {
		topics = append(topics, topicView{Name: topic.Name, Level: topic.Level, Partitions: topic.PartitionCount, ReplicationFactor: topic.ReplicationFactor})
	}
	results := paginateTopics(filterTopics(topics, query, level), query, level, page)

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
