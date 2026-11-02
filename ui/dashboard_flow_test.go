package ui

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	kafkaesque_consumers "github.com/nikhil25803/kafkaesque/internals/consumers"
	kafkaesque_topics "github.com/nikhil25803/kafkaesque/internals/topics"
)

func TestDashboardRendersTopicOverviewFlowAndDeadResources(t *testing.T) {
	activeRate, zeroRate := 60.0, 0.0
	source := &fakeDataSource{
		topics: []kafkaesque_topics.TopicInformation{
			{Name: "orders", Level: "External", PartitionCount: 3, ReplicationFactor: 2, MessagesPerMinute: &activeRate},
			{Name: "audit", Level: "External", PartitionCount: 1, ReplicationFactor: 1, MessagesPerMinute: &zeroRate},
			{Name: "pending", Level: "External", PartitionCount: 1, ReplicationFactor: 1},
			{Name: "__internal", Level: "Internal", PartitionCount: 1, ReplicationFactor: 1, MessagesPerMinute: &zeroRate},
		},
		groups: []kafkaesque_consumers.ConsumerGroupInformation{
			{GroupName: "alpha", State: "Stable", MembersCount: 1, TopicsCount: 2, TotalLag: 7, Topics: []kafkaesque_consumers.ConsumerGroupTopicLag{
				{Topic: "audit", Lag: 0}, {Topic: "orders", Lag: 7},
			}},
			{GroupName: "beta", State: "Empty", TopicsCount: 1, TotalLag: 2, Topics: []kafkaesque_consumers.ConsumerGroupTopicLag{{Topic: "orders", Lag: 2}}},
			{GroupName: "dead-group", State: "Dead", TopicsCount: 1},
			{GroupName: "empty-group", State: "Empty"},
		},
	}
	response := requestUI(t, newHandler(source), "/ui", false)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	body := response.Body.String()
	requireContains(t, body,
		"Topics overview", "Total throughput", "60 msg/min", "Highest publisher", "orders",
		"Average active throughput", "Active topics", "Rates available for 2 of 3 filtered topics",
		"Topic → Consumer Flow", "3 relationships", "orders to alpha", "orders to beta", "audit to alpha",
		"Topic rate: 60 msg/min", "Consumer lag: 7", `class="flow-link"`,
		"Dead topics", "audit", "No messages/min", "1 external topic is still collecting and excluded",
		"Dead consumers", "dead-group", `id="dead-topics-panel"`, `id="dead-consumers-panel"`,
		`class="dead-resource-content"`, "horizontally and vertically scrollable",
	)
	for _, forbidden := range []string{"__internal", "#ZgotmplZ"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("dashboard contains %q: %s", forbidden, body)
		}
	}
	if got := strings.Count(body, `class="dead-resource-content"`); got != 2 {
		t.Fatalf("dead resource content regions=%d, want 2", got)
	}

	cluster := strings.Index(body, "Cluster overview")
	topics := strings.Index(body, "Topics overview")
	flow := strings.Index(body, "Topic → Consumer Flow")
	dead := strings.Index(body, "Dead topics")
	health := strings.Index(body, "Cluster health")
	if cluster < 0 || !(cluster < topics && topics < flow && flow < dead && dead < health) {
		t.Fatalf("dashboard section order cluster=%d topics=%d flow=%d dead=%d health=%d", cluster, topics, flow, dead, health)
	}
	deadConsumers := buildDeadConsumers(source.groups, 1, 10)
	if len(deadConsumers.Consumers) != 1 || deadConsumers.Consumers[0].Name != "dead-group" {
		t.Fatalf("dead consumers=%+v", deadConsumers.Consumers)
	}
}

func TestTopicFlowIsDeterministicUnboundedAndHandlesEmpty(t *testing.T) {
	rate := 1.0
	topics := make([]kafkaesque_topics.TopicInformation, 0, 25)
	groupTopics := make([]kafkaesque_consumers.ConsumerGroupTopicLag, 0, 25)
	for index := 0; index < 25; index++ {
		name := fmt.Sprintf("topic-%02d", index)
		topics = append(topics, kafkaesque_topics.TopicInformation{Name: name, Level: "External", MessagesPerMinute: &rate})
		groupTopics = append(groupTopics, kafkaesque_consumers.ConsumerGroupTopicLag{Topic: name, Lag: int64(index)})
	}
	groups := []kafkaesque_consumers.ConsumerGroupInformation{{GroupName: "shared", State: "Stable", Topics: groupTopics}}
	first := buildTopicFlow(topics, groups)
	second := buildTopicFlow(topics, groups)
	if len(first.Topics) != 25 || len(first.Links) != 25 || first.Height <= 720 {
		t.Fatalf("flow topics=%d links=%d height=%d", len(first.Topics), len(first.Links), first.Height)
	}
	if fmt.Sprintf("%+v", first) != fmt.Sprintf("%+v", second) {
		t.Fatal("topic flow layout is not deterministic")
	}
	if got := buildTopicFlow(topics, nil); len(got.Links) != 0 || len(got.Topics) != 0 {
		t.Fatalf("empty relationships flow=%+v", got)
	}
}

func TestDashboardDeadResourcePaginationAndFallbacks(t *testing.T) {
	zeroRate := 0.0
	topics := make([]kafkaesque_topics.TopicInformation, 0, 27)
	for index := 0; index < 26; index++ {
		topics = append(topics, kafkaesque_topics.TopicInformation{
			Name: fmt.Sprintf("dead-topic-%02d", index), Level: "External", MessagesPerMinute: &zeroRate,
		})
	}
	topics = append(topics, kafkaesque_topics.TopicInformation{Name: "pending", Level: "External"})
	groups := []kafkaesque_consumers.ConsumerGroupInformation{{GroupName: "empty", State: "Empty"}}
	for index := 0; index < 12; index++ {
		groups = append(groups, kafkaesque_consumers.ConsumerGroupInformation{GroupName: fmt.Sprintf("dead-%02d", index), State: "Dead"})
	}
	handler := newHandler(&fakeDataSource{topics: topics, groups: groups})

	response := requestUI(t, handler, "/ui?dead_topic_page=2&dead_topic_page_size=10&dead_consumer_page=2&dead_consumer_page_size=10", false)
	body := response.Body.String()
	requireContains(t, body, "11–20 of 26", "dead-topic-10", "dead-topic-19", "11–12 of 12", "dead-10", "dead-11")
	if strings.Contains(body, "dead-topic-00") || strings.Contains(body, ">empty</a>") {
		t.Fatalf("dashboard pagination leaked rows from another page: %s", body)
	}

	response = requestUI(t, handler, deadTopicsPath+"?page=2&page_size=10", true)
	body = response.Body.String()
	requireContains(t, body, `id="dead-topics-panel"`, "11–20 of 26", "dead-topic-10")
	if strings.Contains(body, "<!doctype html>") || strings.Contains(body, "Dead consumers") {
		t.Fatalf("dead-topic fragment contains unrelated page content: %s", body)
	}

	response = requestUI(t, handler, deadConsumersPath+"?page=2&page_size=10", true)
	body = response.Body.String()
	requireContains(t, body, `id="dead-consumers-panel"`, "11–12 of 12", "dead-10")
	if strings.Contains(body, "<!doctype html>") || strings.Contains(body, "Dead topics") {
		t.Fatalf("dead-consumer fragment contains unrelated page content: %s", body)
	}

	response = requestUI(t, handler, deadTopicsPath+"?page=2&page_size=25", false)
	if response.Code != http.StatusSeeOther || response.Header().Get("Location") != "/ui?dead_topic_page=2&dead_topic_page_size=25#dead-topics-panel" {
		t.Fatalf("fallback status=%d location=%q", response.Code, response.Header().Get("Location"))
	}
	for _, path := range []string{
		"/ui?dead_topic_page=0", deadTopicsPath + "?page=bad", deadConsumersPath + "?page_size=20",
	} {
		response = requestUI(t, handler, path, strings.Contains(path, "/dashboard/"))
		if response.Code != http.StatusBadRequest {
			t.Fatalf("%s status=%d", path, response.Code)
		}
	}
}

func TestCompactDashboardPagesBoundsLargePagination(t *testing.T) {
	pages := compactDashboardPages(50, 100, deadTopicsPath, 10)
	if len(pages) != 9 || !pages[1].Ellipsis || !pages[len(pages)-2].Ellipsis {
		t.Fatalf("pages=%+v", pages)
	}
}
