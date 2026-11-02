package ui

import (
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	kafkaesque_topics "github.com/nikhil25803/kafkaesque/internals/topics"
)

const (
	defaultTopicLevel  = "external"
	defaultTopicMetric = "minute"
)

type topicView struct {
	Name              string
	Level             string
	Partitions        int
	ReplicationFactor int
	RateAvailable     bool
	MessageRate       string
	Rate              float64
}

type topicsPage struct {
	Navigation navigationView
	Query      string
	Levels     []filterOption
	Metrics    []filterOption
	Results    topicsResults
}

type filterOption struct {
	Value    string
	Label    string
	Selected bool
}

type topicMetricCard struct {
	Value  string
	Label  string
	Detail string
}

type topicsResults struct {
	Topics      []topicView
	Metrics     []topicMetricCard
	Coverage    string
	MetricLabel string
	Range       string
	Pages       []pageLink
	PreviousURL string
	NextURL     string
	HasFilters  bool
	ClearURL    string
	Pagination  pageSizeControl
}

func renderTopics(w http.ResponseWriter, r *http.Request) {
	page, err := positivePage(r.URL.Query().Get("page"))
	if err != nil {
		renderUIError(w, r, http.StatusBadRequest, "topics", "Invalid topics request", err.Error())
		return
	}
	pageSize, err := parsePageSize(r.URL.Query().Get("page_size"), 10)
	if err != nil {
		renderUIError(w, r, http.StatusBadRequest, "topics", "Invalid topics request", err.Error())
		return
	}
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	level, err := topicLevel(r.URL.Query().Get("level"))
	if err != nil {
		renderUIError(w, r, http.StatusBadRequest, "topics", "Invalid topics request", err.Error())
		return
	}
	metric, err := topicMetric(r.URL.Query().Get("metric"))
	if err != nil {
		renderUIError(w, r, http.StatusBadRequest, "topics", "Invalid topics request", err.Error())
		return
	}

	information, err := sourceFrom(r).Topics(r.Context())
	if err != nil {
		renderUpstreamError(w, r, "topics", err)
		return
	}
	topics := make([]topicView, 0, len(information))
	for _, topic := range information {
		rate := topicRate(topic, metric)
		view := topicView{
			Name: topic.Name, Level: topic.Level, Partitions: topic.PartitionCount,
			ReplicationFactor: topic.ReplicationFactor,
		}
		if rate != nil {
			view.RateAvailable = true
			view.Rate = *rate
			view.MessageRate = formatMessageRate(*rate)
		}
		topics = append(topics, view)
	}
	filtered := filterTopics(topics, query, level)
	results := paginateTopics(filtered, query, level, metric, page, pageSize)

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if strings.EqualFold(r.Header.Get("HX-Request"), "true") {
		if err := dashboardTemplate.ExecuteTemplate(w, "topics-results", results); err != nil {
			http.Error(w, "Unable to render topics", http.StatusInternalServerError)
		}
		return
	}
	view := topicsPage{
		Navigation: navigationFor(r, "topics"), Query: query,
		Levels: topicLevelOptions(level), Metrics: topicMetricOptions(metric), Results: results,
	}
	if err := dashboardTemplate.ExecuteTemplate(w, "topics", view); err != nil {
		http.Error(w, "Unable to render topics", http.StatusInternalServerError)
	}
}

func topicLevel(value string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return defaultTopicLevel, nil
	}
	if value != "external" && value != "internal" && value != "all" {
		return "", fmt.Errorf("level must be external, internal, or all")
	}
	return value, nil
}

func topicMetric(value string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return defaultTopicMetric, nil
	}
	if value != "second" && value != "minute" && value != "hour" {
		return "", fmt.Errorf("metric must be second, minute, or hour")
	}
	return value, nil
}

func topicRate(topic kafkaesque_topics.TopicInformation, metric string) *float64 {
	switch metric {
	case "second":
		return topic.MessagesPerSecond
	case "hour":
		return topic.MessagesPerHour
	default:
		return topic.MessagesPerMinute
	}
}

func formatMessageRate(rate float64) string {
	rounded := math.Round(rate*100) / 100
	return strconv.FormatFloat(rounded, 'f', -1, 64)
}

func filterTopics(topics []topicView, query, level string) []topicView {
	filtered := make([]topicView, 0, len(topics))
	for _, topic := range topics {
		if strings.Contains(strings.ToLower(topic.Name), strings.ToLower(query)) &&
			(level == "all" || strings.EqualFold(topic.Level, level)) {
			filtered = append(filtered, topic)
		}
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].Name < filtered[j].Name })
	return filtered
}

func paginateTopics(topics []topicView, query, level, metric string, page, pageSize int) topicsResults {
	total := len(topics)
	totalPages := (total + pageSize - 1) / pageSize
	start := (page - 1) * pageSize
	if start > total {
		start = total
	}
	end := min(start+pageSize, total)
	pages := make([]pageLink, 0, totalPages)
	for number := 1; number <= totalPages; number++ {
		pages = append(pages, pageLink{
			Number: number,
			URL:    topicsURL(query, level, metric, number, pageSize),
			Active: number == page,
		})
	}
	rangeText := "0 topics"
	if start < end {
		rangeText = strconv.Itoa(start+1) + "–" + strconv.Itoa(end) + " of " + strconv.Itoa(total) + " topics"
	} else if total > 0 {
		rangeText = "0 of " + strconv.Itoa(total) + " topics"
	}
	results := topicsResults{
		Topics: topics[start:end], Metrics: topicSummary(topics, metric),
		Coverage: topicCoverage(topics), MetricLabel: metricLabel(metric), Range: rangeText,
		Pages: pages, HasFilters: query != "" || level != defaultTopicLevel || metric != defaultTopicMetric,
		ClearURL: topicsURL("", defaultTopicLevel, defaultTopicMetric, 1, pageSize),
		Pagination: newPageSizeControl(
			"/ui/topics", "#topics-results", pageSize,
			queryField{Name: "q", Value: query},
			queryField{Name: "level", Value: level},
			queryField{Name: "metric", Value: metric},
		),
	}
	if page > 1 && page <= totalPages {
		results.PreviousURL = topicsURL(query, level, metric, page-1, pageSize)
	}
	if page < totalPages {
		results.NextURL = topicsURL(query, level, metric, page+1, pageSize)
	}
	return results
}

func topicSummary(topics []topicView, metric string) []topicMetricCard {
	measured, active := 0, 0
	total, activeTotal := 0.0, 0.0
	highestTopic, highestRate := "", 0.0
	for _, topic := range topics {
		if !topic.RateAvailable {
			continue
		}
		measured++
		total += topic.Rate
		if topic.Rate <= 0 {
			continue
		}
		active++
		activeTotal += topic.Rate
		if highestTopic == "" || topic.Rate > highestRate || (topic.Rate == highestRate && topic.Name < highestTopic) {
			highestTopic, highestRate = topic.Name, topic.Rate
		}
	}
	unit := metricLabel(metric)
	if measured == 0 {
		return []topicMetricCard{
			{Value: "Collecting…", Label: "Total throughput"},
			{Value: "Collecting…", Label: "Highest publisher"},
			{Value: "Collecting…", Label: "Average active throughput"},
			{Value: "Collecting…", Label: "Active topics"},
		}
	}
	highest := topicMetricCard{Value: "No traffic", Label: "Highest publisher"}
	average := topicMetricCard{Value: "—", Label: "Average active throughput"}
	if active > 0 {
		highest = topicMetricCard{Value: highestTopic, Label: "Highest publisher", Detail: formatMessageRate(highestRate) + " msg" + unit}
		average = topicMetricCard{Value: formatMessageRate(activeTotal / float64(active)), Label: "Average active throughput", Detail: "msg" + unit}
	}
	return []topicMetricCard{
		{Value: formatMessageRate(total), Label: "Total throughput", Detail: "msg" + unit},
		highest,
		average,
		{Value: strconv.Itoa(active), Label: "Active topics", Detail: "of " + strconv.Itoa(measured) + " measured"},
	}
}

func topicCoverage(topics []topicView) string {
	measured := 0
	for _, topic := range topics {
		if topic.RateAvailable {
			measured++
		}
	}
	switch {
	case len(topics) == 0:
		return "No topics match the current filters."
	case measured == 0:
		return fmt.Sprintf("Message rates are still collecting for %d filtered topics.", len(topics))
	case measured < len(topics):
		return fmt.Sprintf("Rates available for %d of %d filtered topics; new or reset topics are still collecting.", measured, len(topics))
	default:
		return fmt.Sprintf("Rates available for all %d filtered topics.", measured)
	}
}

func metricLabel(metric string) string {
	switch metric {
	case "second":
		return "/sec"
	case "hour":
		return "/hr"
	default:
		return "/min"
	}
}

func topicsURL(query, level, metric string, page, pageSize int) string {
	values := url.Values{}
	if query != "" {
		values.Set("q", query)
	}
	values.Set("level", level)
	values.Set("metric", metric)
	values.Set("page", strconv.Itoa(page))
	values.Set("page_size", strconv.Itoa(pageSize))
	return "/ui/topics?" + values.Encode()
}

func topicLevelOptions(selected string) []filterOption {
	return selectedOptions([]filterOption{
		{Value: "external", Label: "External"},
		{Value: "internal", Label: "Internal"},
		{Value: "all", Label: "All"},
	}, selected)
}

func topicMetricOptions(selected string) []filterOption {
	return selectedOptions([]filterOption{
		{Value: "minute", Label: "/min"},
		{Value: "second", Label: "/sec"},
		{Value: "hour", Label: "/hr"},
	}, selected)
}

func selectedOptions(options []filterOption, selected string) []filterOption {
	for i := range options {
		options[i].Selected = options[i].Value == selected
	}
	return options
}
