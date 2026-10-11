package ui

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	kafkaesque_consumers "github.com/nikhil25803/kafkaesque/internals/consumers"
	kafka "github.com/segmentio/kafka-go"
)

type consumerGroupDetailPage struct {
	Navigation navigationView
	Group      groupDetailView
	Metrics    []metricView
	Summary    detailLagSummary
	Partition  partitionSection
}
type groupDetailView struct {
	Name, State, StateClass string
	Members                 []detailMember
	Topics                  []detailTopic
}
type detailMember struct{ ID, ClientID, Host, Assignments string }
type detailTopic struct {
	Name                     string
	Partitions               []detailPartition
	Lag                      int64
	Health, HealthClass, URL string
	Selected                 bool
}
type detailPartition struct {
	Partition                          int
	CommittedOffset, LogEndOffset, Lag int64
	Member, Health, HealthClass        string
}
type detailLagSummary struct {
	Average                     float64
	Healthy, Warning, Unhealthy int
	TotalLag, MaximumLag        int64
	MemberCount, TopicCount     int
}
type partitionSection struct {
	Action, Topic                         string
	TopicOptions                          []topicOption
	GroupOptions                          []groupOption
	Groups                                []partitionGroup
	TotalLag                              int64
	LagSortURL, LagSortLabel, LagSortIcon string
	Empty                                 bool
}
type topicOption struct {
	Name     string
	Selected bool
}
type groupOption struct {
	Value, Label string
	Selected     bool
}
type partitionGroup struct {
	Name       string
	Partitions []detailPartition
}

func renderConsumerGroupDetail(w http.ResponseWriter, r *http.Request) {
	groupBy, sortBy, direction, err := detailControls(r)
	if err != nil {
		renderUIError(w, r, http.StatusBadRequest, "consumer-groups", "Invalid consumer lag request", err.Error())
		return
	}
	data, err := sourceFrom(r).ConsumerGroup(r.Context(), r.PathValue("group"), strings.TrimSpace(r.URL.Query().Get("topic")))
	if err != nil {
		if isNotFound(err) {
			renderUIError(w, r, http.StatusNotFound, "consumer-groups", "Consumer group or topic not found", "The requested consumer group or topic is not available.")
			return
		}
		renderUpstreamError(w, r, "consumer-groups", err)
		return
	}
	detail := groupDetailViewFrom(data, sourceFrom(r).Thresholds())
	partition := buildPartitionSection(detail, data.SelectedTopic, groupBy, sortBy, direction)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if strings.EqualFold(r.Header.Get("HX-Request"), "true") {
		if err := dashboardTemplate.ExecuteTemplate(w, "partition-lag", partition); err != nil {
			http.Error(w, "Unable to render partition lag", http.StatusInternalServerError)
		}
		return
	}
	s := data.Group.LagSummary
	page := consumerGroupDetailPage{Navigation: navigationView{Active: "consumer-groups"}, Group: detail,
		Metrics: []metricView{{Label: "Topics", Value: int64(data.Group.TopicsCount)}, {Label: "Members", Value: int64(data.Group.MembersCount)}, {Label: "Total Lag", Value: data.Group.TotalLag}, {Label: "Maximum Partition Lag", Value: s.MaximumPartitionLag}},
		Summary: detailLagSummary{Average: s.AveragePartitionLag, Healthy: s.HealthyPartitions, Warning: s.WarningPartitions, Unhealthy: s.UnhealthyPartitions, TotalLag: s.TotalLag, MaximumLag: s.MaximumPartitionLag, MemberCount: data.Group.MembersCount, TopicCount: data.Group.TopicsCount}, Partition: partition}
	if err := dashboardTemplate.ExecuteTemplate(w, "consumer-group-detail", page); err != nil {
		http.Error(w, "Unable to render consumer lag", http.StatusInternalServerError)
	}
}

func detailControls(r *http.Request) (string, string, string, error) {
	groupBy := strings.TrimSpace(r.URL.Query().Get("group_by"))
	if groupBy != "" && groupBy != "member" {
		return "", "", "", fmt.Errorf("group_by must be member or omitted")
	}
	sortBy := strings.TrimSpace(r.URL.Query().Get("sort"))
	if sortBy == "" {
		sortBy = "partition"
	}
	if sortBy != "partition" && sortBy != "lag" {
		return "", "", "", fmt.Errorf("sort must be partition or lag")
	}
	direction := strings.TrimSpace(r.URL.Query().Get("direction"))
	if direction == "" {
		direction = "asc"
	}
	if direction != "asc" && direction != "desc" {
		return "", "", "", fmt.Errorf("direction must be asc or desc")
	}
	return groupBy, sortBy, direction, nil
}

func groupDetailViewFrom(data *groupDetailData, thresholds kafkaesque_consumers.LagThresholds) groupDetailView {
	view := groupDetailView{Name: data.Group.GroupName, State: data.Group.State, StateClass: stateClass(data.Group.State)}
	owners := map[int]string{}
	for _, member := range data.Group.Members {
		assignments := []string{}
		for _, a := range member.Assignments {
			parts := make([]string, len(a.Partitions))
			for i, p := range a.Partitions {
				parts[i] = strconv.Itoa(p)
				if a.Topic == data.SelectedTopic {
					owners[p] = member.MemberID
				}
			}
			assignments = append(assignments, a.Topic+":"+strings.Join(parts, ","))
		}
		view.Members = append(view.Members, detailMember{ID: member.MemberID, ClientID: member.ClientID, Host: member.Host, Assignments: strings.Join(assignments, "; ")})
	}
	for _, topic := range data.Group.Topics {
		health, class := lagStatusView(topic.Status)
		view.Topics = append(view.Topics, detailTopic{Name: topic.Topic, Partitions: make([]detailPartition, topic.Partitions), Lag: topic.Lag, Health: health, HealthClass: class, Selected: topic.Topic == data.SelectedTopic, URL: detailURL(data.Group.GroupName, topic.Topic, "", "partition", "asc")})
	}
	if data.Topic != nil {
		for i := range view.Topics {
			if view.Topics[i].Name == data.Topic.Topic {
				view.Topics[i].Partitions = nil
				for _, p := range data.Topic.Partitions {
					member := owners[p.Partition]
					if member == "" {
						member = "Unassigned"
					}
					health, class := lagStatusView(kafkaesque_consumers.ConsumerLagStatus(p.Lag, thresholds))
					view.Topics[i].Partitions = append(view.Topics[i].Partitions, detailPartition{Partition: p.Partition, CommittedOffset: p.CommittedOffset, LogEndOffset: p.LogEndOffset, Lag: p.Lag, Member: member, Health: health, HealthClass: class})
				}
			}
		}
	}
	return view
}

func buildPartitionSection(detail groupDetailView, topicName, groupBy, sortBy, direction string) partitionSection {
	section := partitionSection{Action: "/ui/consumer-groups/" + url.PathEscape(detail.Name), Topic: topicName, Empty: topicName == "", GroupOptions: []groupOption{{Label: "None", Selected: groupBy == ""}, {Value: "member", Label: "Consumer member", Selected: groupBy == "member"}}}
	for _, item := range detail.Topics {
		section.TopicOptions = append(section.TopicOptions, topicOption{Name: item.Name, Selected: item.Name == topicName})
		if item.Name == topicName {
			section.TotalLag = item.Lag
			partitions := append([]detailPartition(nil), item.Partitions...)
			sort.SliceStable(partitions, func(i, j int) bool {
				left, right := int64(partitions[i].Partition), int64(partitions[j].Partition)
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
			section.Groups = []partitionGroup{{Partitions: partitions}}
			if groupBy == "member" {
				section.Groups = groupPartitionsByMember(partitions, detail.Members)
			}
		}
	}
	next, icon, label := "desc", "↕", "Sort lag highest first"
	if sortBy == "lag" && direction == "desc" {
		next, icon, label = "asc", "↓", "Sort lag lowest first"
	} else if sortBy == "lag" {
		icon, label = "↑", "Sort lag highest first"
	}
	if topicName != "" {
		section.LagSortURL = detailURL(detail.Name, topicName, groupBy, "lag", next)
	}
	section.LagSortIcon, section.LagSortLabel = icon, label
	return section
}

func groupPartitionsByMember(partitions []detailPartition, members []detailMember) []partitionGroup {
	by := map[string][]detailPartition{}
	for _, p := range partitions {
		by[p.Member] = append(by[p.Member], p)
	}
	groups := []partitionGroup{}
	for _, m := range members {
		if rows := by[m.ID]; len(rows) > 0 {
			groups = append(groups, partitionGroup{Name: m.ID + " · " + m.ClientID, Partitions: rows})
		}
	}
	if rows := by["Unassigned"]; len(rows) > 0 {
		groups = append(groups, partitionGroup{Name: "Unassigned", Partitions: rows})
	}
	return groups
}
func detailURL(group, topic, groupBy, sortBy, direction string) string {
	values := url.Values{"topic": {topic}, "sort": {sortBy}, "direction": {direction}}
	if groupBy != "" {
		values.Set("group_by", groupBy)
	}
	return "/ui/consumer-groups/" + url.PathEscape(group) + "?" + values.Encode()
}
func isNotFound(err error) bool {
	return errors.Is(err, kafka.GroupIdNotFound) || strings.Contains(strings.ToLower(err.Error()), "not found") || strings.Contains(strings.ToLower(err.Error()), "no committed offsets")
}
