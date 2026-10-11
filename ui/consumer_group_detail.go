package ui

import (
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

type consumerGroupDetailPage struct {
	Navigation navigationView
	Group      demoGroupDetail
	Metrics    []metricView
	Summary    detailLagSummary
	Partition  partitionSection
}

type demoGroupDetail struct {
	Name       string
	State      string
	StateClass string
	Members    []detailMember
	Topics     []detailTopic
}

type detailMember struct {
	ID          string
	ClientID    string
	Host        string
	Assignments string
}

type detailTopic struct {
	Name        string
	Partitions  []detailPartition
	Lag         int
	Health      string
	HealthClass string
	URL         string
	Selected    bool
}

type detailPartition struct {
	Partition       int
	CommittedOffset int
	LogEndOffset    int
	Lag             int
	Member          string
	Health          string
	HealthClass     string
}

type detailLagSummary struct {
	Average     float64
	Healthy     int
	Warning     int
	Unhealthy   int
	TotalLag    int
	MaximumLag  int
	MemberCount int
	TopicCount  int
}

type partitionSection struct {
	Action       string
	Topic        string
	TopicOptions []topicOption
	GroupOptions []groupOption
	Groups       []partitionGroup
	TotalLag     int
	LagSortURL   string
	LagSortLabel string
	LagSortIcon  string
}

type topicOption struct {
	Name     string
	Selected bool
}

type groupOption struct {
	Value    string
	Label    string
	Selected bool
}

type partitionGroup struct {
	Name       string
	Partitions []detailPartition
}

type detailTopicNotFoundError string

func (e detailTopicNotFoundError) Error() string {
	return fmt.Sprintf("topic %q is not available for this consumer group", string(e))
}

func renderConsumerGroupDetail(w http.ResponseWriter, r *http.Request) {
	detail, found := demoConsumerGroupDetail(r.PathValue("group"))
	if !found {
		renderUIError(w, http.StatusNotFound, "Consumer group not found", "The requested consumer group is not available.")
		return
	}
	topic, groupBy, sortBy, direction, err := detailQuery(r, detail)
	if err != nil {
		if _, ok := err.(detailTopicNotFoundError); ok {
			renderUIError(w, http.StatusNotFound, "Consumer topic not found", err.Error())
			return
		}
		renderUIError(w, http.StatusBadRequest, "Invalid consumer lag request", err.Error())
		return
	}
	partition := buildPartitionSection(detail, topic, groupBy, sortBy, direction)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if strings.EqualFold(r.Header.Get("HX-Request"), "true") {
		if err := dashboardTemplate.ExecuteTemplate(w, "partition-lag", partition); err != nil {
			http.Error(w, "Unable to render partition lag", http.StatusInternalServerError)
		}
		return
	}
	summary := summarizeDetail(detail)
	for i := range detail.Topics {
		detail.Topics[i].Selected = detail.Topics[i].Name == topic
		detail.Topics[i].URL = detailURL(detail.Name, detail.Topics[i].Name, groupBy, "partition", "asc")
	}
	page := consumerGroupDetailPage{
		Navigation: navigationView{Active: "consumer-groups"},
		Group:      detail,
		Metrics: []metricView{
			{Label: "Topics", Value: summary.TopicCount},
			{Label: "Members", Value: summary.MemberCount},
			{Label: "Total Lag", Value: summary.TotalLag},
			{Label: "Maximum Partition Lag", Value: summary.MaximumLag},
		},
		Summary:   summary,
		Partition: partition,
	}
	if err := dashboardTemplate.ExecuteTemplate(w, "consumer-group-detail", page); err != nil {
		http.Error(w, "Unable to render consumer lag", http.StatusInternalServerError)
	}
}

func detailQuery(r *http.Request, detail demoGroupDetail) (string, string, string, string, error) {
	topic := strings.TrimSpace(r.URL.Query().Get("topic"))
	if topic == "" {
		topic = detail.Topics[0].Name
	}
	if _, found := detailTopicByName(detail, topic); !found {
		return "", "", "", "", detailTopicNotFoundError(topic)
	}
	groupBy := strings.TrimSpace(r.URL.Query().Get("group_by"))
	if groupBy != "" && groupBy != "member" {
		return "", "", "", "", fmt.Errorf("group_by must be member or omitted")
	}
	sortBy := strings.TrimSpace(r.URL.Query().Get("sort"))
	if sortBy == "" {
		sortBy = "partition"
	}
	if sortBy != "partition" && sortBy != "lag" {
		return "", "", "", "", fmt.Errorf("sort must be partition or lag")
	}
	direction := strings.TrimSpace(r.URL.Query().Get("direction"))
	if direction == "" {
		direction = "asc"
	}
	if direction != "asc" && direction != "desc" {
		return "", "", "", "", fmt.Errorf("direction must be asc or desc")
	}
	return topic, groupBy, sortBy, direction, nil
}

func buildPartitionSection(detail demoGroupDetail, topicName, groupBy, sortBy, direction string) partitionSection {
	topic, _ := detailTopicByName(detail, topicName)
	partitions := append([]detailPartition(nil), topic.Partitions...)
	sort.SliceStable(partitions, func(i, j int) bool {
		left, right := partitions[i].Partition, partitions[j].Partition
		if sortBy == "lag" {
			left, right = partitions[i].Lag, partitions[j].Lag
			if left == right {
				return partitions[i].Partition < partitions[j].Partition
			}
		}
		if direction == "desc" {
			return left > right
		}
		return left < right
	})

	groups := []partitionGroup{{Partitions: partitions}}
	if groupBy == "member" {
		groups = groupPartitionsByMember(partitions, detail.Members)
	}
	topicOptions := make([]topicOption, 0, len(detail.Topics))
	for _, item := range detail.Topics {
		topicOptions = append(topicOptions, topicOption{Name: item.Name, Selected: item.Name == topicName})
	}
	groupOptions := []groupOption{
		{Label: "None", Selected: groupBy == ""},
		{Value: "member", Label: "Consumer member", Selected: groupBy == "member"},
	}
	nextDirection, icon, label := "desc", "↕", "Sort lag highest first"
	if sortBy == "lag" && direction == "desc" {
		nextDirection, icon, label = "asc", "↓", "Sort lag lowest first"
	} else if sortBy == "lag" {
		icon, label = "↑", "Sort lag highest first"
	}
	return partitionSection{
		Action: "/ui/consumer-groups/" + url.PathEscape(detail.Name),
		Topic:  topicName, TopicOptions: topicOptions, GroupOptions: groupOptions,
		Groups: groups, TotalLag: topic.Lag,
		LagSortURL:   detailURL(detail.Name, topicName, groupBy, "lag", nextDirection),
		LagSortLabel: label, LagSortIcon: icon,
	}
}

func groupPartitionsByMember(partitions []detailPartition, members []detailMember) []partitionGroup {
	byMember := make(map[string][]detailPartition, len(members)+1)
	for _, partition := range partitions {
		byMember[partition.Member] = append(byMember[partition.Member], partition)
	}
	groups := make([]partitionGroup, 0, len(byMember))
	for _, member := range members {
		if rows := byMember[member.ID]; len(rows) > 0 {
			groups = append(groups, partitionGroup{Name: member.ID + " · " + member.ClientID, Partitions: rows})
		}
	}
	if rows := byMember["Unassigned"]; len(rows) > 0 {
		groups = append(groups, partitionGroup{Name: "Unassigned", Partitions: rows})
	}
	return groups
}

func summarizeDetail(detail demoGroupDetail) detailLagSummary {
	summary := detailLagSummary{MemberCount: len(detail.Members), TopicCount: len(detail.Topics)}
	partitions := 0
	for _, topic := range detail.Topics {
		summary.TotalLag += topic.Lag
		for _, partition := range topic.Partitions {
			partitions++
			if partition.Lag > summary.MaximumLag {
				summary.MaximumLag = partition.Lag
			}
			switch partition.Health {
			case "Healthy":
				summary.Healthy++
			case "Warning":
				summary.Warning++
			case "Unhealthy":
				summary.Unhealthy++
			}
		}
	}
	if partitions > 0 {
		summary.Average = float64(summary.TotalLag) / float64(partitions)
	}
	return summary
}

func detailURL(group, topic, groupBy, sortBy, direction string) string {
	values := url.Values{"topic": []string{topic}, "sort": []string{sortBy}, "direction": []string{direction}}
	if groupBy != "" {
		values.Set("group_by", groupBy)
	}
	return "/ui/consumer-groups/" + url.PathEscape(group) + "?" + values.Encode()
}

func detailTopicByName(detail demoGroupDetail, name string) (detailTopic, bool) {
	for _, topic := range detail.Topics {
		if topic.Name == name {
			return topic, true
		}
	}
	return detailTopic{}, false
}

func demoConsumerGroupDetail(name string) (demoGroupDetail, bool) {
	for _, group := range demoConsumerGroups() {
		if group.Name != name {
			continue
		}
		if name == "orders-service" {
			return demoOrdersServiceDetail(), true
		}
		return generatedConsumerGroupDetail(group), true
	}
	return demoGroupDetail{}, false
}

func demoOrdersServiceDetail() demoGroupDetail {
	members := []detailMember{
		{ID: "consumer-1", ClientID: "order-worker", Host: "10.0.0.12", Assignments: "orders:0; payments:0"},
		{ID: "consumer-2", ClientID: "order-worker", Host: "10.0.0.13", Assignments: "orders:1; payments:1"},
		{ID: "consumer-3", ClientID: "order-worker", Host: "10.0.0.14", Assignments: "orders:2; payments:2"},
	}
	return demoGroupDetail{
		Name: "orders-service", State: "Stable", StateClass: "healthy", Members: members,
		Topics: []detailTopic{
			newDetailTopic("orders", []detailPartition{
				newDetailPartition(0, 129932, 130056, "consumer-1"),
				newDetailPartition(1, 98231, 98231, "consumer-2"),
				newDetailPartition(2, 76891, 76903, "consumer-3"),
			}),
			newDetailTopic("payments", []detailPartition{
				newDetailPartition(0, 54000, 54000, "consumer-1"),
				newDetailPartition(1, 62000, 62000, "consumer-2"),
				newDetailPartition(2, 71000, 71000, "consumer-3"),
			}),
		},
	}
}

func generatedConsumerGroupDetail(group consumerGroupView) demoGroupDetail {
	detail := demoGroupDetail{Name: group.Name, State: group.State, StateClass: group.StateClass}
	for member := 0; member < group.Members; member++ {
		detail.Members = append(detail.Members, detailMember{
			ID: fmt.Sprintf("member-%d", member+1), ClientID: group.Name + "-worker",
			Host: fmt.Sprintf("10.0.1.%d", member+10),
		})
	}
	topicNames := generatedTopicNames(group.Name, group.Topics)
	partitionCount := len(topicNames) * 3
	lags := make([]int, partitionCount)
	if group.TotalLag > 0 {
		lags[0] = group.TotalLag * 60 / 100
		lags[1] = group.TotalLag * 30 / 100
		lags[2] = group.TotalLag - lags[0] - lags[1]
	}
	assignmentLists := make([]map[string][]int, len(detail.Members))
	for i := range assignmentLists {
		assignmentLists[i] = map[string][]int{}
	}
	index := 0
	for topicIndex, topicName := range topicNames {
		partitions := make([]detailPartition, 0, 3)
		for partition := 0; partition < 3; partition++ {
			memberName := "Unassigned"
			if len(detail.Members) > 0 {
				memberIndex := index % len(detail.Members)
				memberName = detail.Members[memberIndex].ID
				assignmentLists[memberIndex][topicName] = append(assignmentLists[memberIndex][topicName], partition)
			}
			logEnd := 100000 + topicIndex*10000 + partition*1000 + len(group.Name)*10
			lag := lags[index]
			partitions = append(partitions, newDetailPartition(partition, logEnd-lag, logEnd, memberName))
			index++
		}
		detail.Topics = append(detail.Topics, newDetailTopic(topicName, partitions))
	}
	for i := range detail.Members {
		assignments := make([]string, 0, len(assignmentLists[i]))
		for _, topicName := range topicNames {
			partitions := assignmentLists[i][topicName]
			if len(partitions) == 0 {
				continue
			}
			values := make([]string, len(partitions))
			for j, partition := range partitions {
				values[j] = strconv.Itoa(partition)
			}
			assignments = append(assignments, topicName+":"+strings.Join(values, ","))
		}
		detail.Members[i].Assignments = strings.Join(assignments, "; ")
	}
	return detail
}

func generatedTopicNames(group string, count int) []string {
	if count == 1 {
		return []string{group + "-events"}
	}
	names := []string{group + "-events", group + "-retry"}
	for index := 3; index <= count; index++ {
		names = append(names, fmt.Sprintf("%s-topic-%d", group, index))
	}
	sort.Strings(names)
	return names
}

func newDetailTopic(name string, partitions []detailPartition) detailTopic {
	topic := detailTopic{Name: name, Partitions: partitions}
	for _, partition := range partitions {
		topic.Lag += partition.Lag
	}
	topic.Health, topic.HealthClass = detailLagStatus(topic.Lag)
	return topic
}

func newDetailPartition(partition, committed, logEnd int, member string) detailPartition {
	lag := logEnd - committed
	health, healthClass := detailLagStatus(lag)
	return detailPartition{
		Partition: partition, CommittedOffset: committed, LogEndOffset: logEnd, Lag: lag,
		Member: member, Health: health, HealthClass: healthClass,
	}
}

func detailLagStatus(lag int) (string, string) {
	if lag > 500 {
		return "Unhealthy", "critical"
	}
	if lag > 100 {
		return "Warning", "warning"
	}
	return "Healthy", "healthy"
}

func renderUIError(w http.ResponseWriter, status int, title, message string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_ = dashboardTemplate.ExecuteTemplate(w, "ui-error", struct {
		Navigation navigationView
		Status     int
		Title      string
		Message    string
	}{Navigation: navigationView{Active: "consumer-groups"}, Status: status, Title: title, Message: message})
}
