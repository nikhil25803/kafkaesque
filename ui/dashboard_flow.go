package ui

import (
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"

	kafkaesque_consumers "github.com/nikhil25803/kafkaesque/internals/consumers"
	kafkaesque_topics "github.com/nikhil25803/kafkaesque/internals/topics"
)

const (
	deadTopicsPath    = "/ui/dashboard/dead-topics"
	deadConsumersPath = "/ui/dashboard/dead-consumers"
)

type dashboardPagination struct {
	Range       string
	Pages       []dashboardPageLink
	PreviousURL string
	NextURL     string
	PageSize    pageSizeControl
}

type dashboardPageSelection struct {
	DeadTopicPage        int
	DeadTopicPageSize    int
	DeadConsumerPage     int
	DeadConsumerPageSize int
}

type dashboardPageLink struct {
	Number   int
	URL      string
	Active   bool
	Ellipsis bool
}

type deadTopicView struct {
	Name              string
	URL               string
	Partitions        int
	ReplicationFactor int
	ConsumerGroups    int
}

type deadTopicsResults struct {
	Topics     []deadTopicView
	Collecting int
	Pagination dashboardPagination
}

type deadConsumerView struct {
	Name    string
	URL     string
	Topics  int
	Members int
	State   string
}

type deadConsumersResults struct {
	Consumers  []deadConsumerView
	Pagination dashboardPagination
}

type topicFlowView struct {
	ViewBox       string
	Width         int
	Height        int
	Topics        []topicFlowNode
	Consumers     []topicFlowNode
	Links         []topicFlowLink
	Relationships int
}

type topicFlowNode struct {
	Name        string
	DisplayName string
	Detail      string
	URL         string
	X           int
	Y           int
	Width       int
	Height      int
	LabelY      int
	DetailY     int
}

type topicFlowLink struct {
	Path  string
	Label string
}

type topicConsumerRelationship struct {
	Topic         string
	ConsumerGroup string
	Lag           int64
}

func buildTopicFlow(topics []kafkaesque_topics.TopicInformation, groups []kafkaesque_consumers.ConsumerGroupInformation) topicFlowView {
	external := make(map[string]kafkaesque_topics.TopicInformation)
	for _, topic := range topics {
		if strings.EqualFold(topic.Level, "external") {
			external[topic.Name] = topic
		}
	}

	relationships := make([]topicConsumerRelationship, 0)
	for _, group := range groups {
		for _, topic := range group.Topics {
			if _, ok := external[topic.Topic]; ok {
				relationships = append(relationships, topicConsumerRelationship{
					Topic: topic.Topic, ConsumerGroup: group.GroupName, Lag: topic.Lag,
				})
			}
		}
	}
	if len(relationships) == 0 {
		return topicFlowView{}
	}
	sort.Slice(relationships, func(i, j int) bool {
		if relationships[i].Topic != relationships[j].Topic {
			return relationships[i].Topic < relationships[j].Topic
		}
		return relationships[i].ConsumerGroup < relationships[j].ConsumerGroup
	})

	topicDegrees, consumerDegrees := make(map[string]int), make(map[string]int)
	consumerInfo := make(map[string]kafkaesque_consumers.ConsumerGroupInformation)
	for _, group := range groups {
		consumerInfo[group.GroupName] = group
	}
	for _, relationship := range relationships {
		topicDegrees[relationship.Topic]++
		consumerDegrees[relationship.ConsumerGroup]++
	}
	topicNames := sortedKeys(topicDegrees)
	consumerNames := sortedKeys(consumerDegrees)

	const canvasWidth, nodeWidth, leftX, rightX = 1200, 230, 24, 946
	topicNodes, topicPositions, topicHeight := layoutFlowNodes(topicNames, topicDegrees, leftX, nodeWidth, func(name string) (string, string) {
		topic := external[name]
		detail := "Collecting messages/min"
		if topic.MessagesPerMinute != nil {
			detail = formatMessageRate(*topic.MessagesPerMinute) + " msg/min"
		}
		return detail, topicsURL(name, "external", "minute", 1, 10)
	})
	consumerNodes, consumerPositions, consumerHeight := layoutFlowNodes(consumerNames, consumerDegrees, rightX, nodeWidth, func(name string) (string, string) {
		group := consumerInfo[name]
		detail := kafkaesque_consumers.GroupStateCategory(group.State) + " · lag " + strconv.FormatInt(group.TotalLag, 10)
		return detail, "/ui/consumer-groups/" + url.PathEscape(name)
	})

	topicSlots, consumerSlots := make(map[string]int), make(map[string]int)
	links := make([]topicFlowLink, 0, len(relationships))
	for _, relationship := range relationships {
		topicNode := topicPositions[relationship.Topic]
		consumerNode := consumerPositions[relationship.ConsumerGroup]
		topicSlots[relationship.Topic]++
		consumerSlots[relationship.ConsumerGroup]++
		startY := flowSlotY(topicNode, topicSlots[relationship.Topic], topicDegrees[relationship.Topic])
		endY := flowSlotY(consumerNode, consumerSlots[relationship.ConsumerGroup], consumerDegrees[relationship.ConsumerGroup])
		startX, endX := topicNode.X+topicNode.Width, consumerNode.X
		controlX := (startX + endX) / 2
		rate := "Collecting"
		if topic := external[relationship.Topic]; topic.MessagesPerMinute != nil {
			rate = formatMessageRate(*topic.MessagesPerMinute) + " msg/min"
		}
		links = append(links, topicFlowLink{
			Path:  fmt.Sprintf("M %d %d C %d %d, %d %d, %d %d", startX, startY, controlX, startY, controlX, endY, endX, endY),
			Label: fmt.Sprintf("%s to %s. Topic rate: %s. Consumer lag: %d.", relationship.Topic, relationship.ConsumerGroup, rate, relationship.Lag),
		})
	}
	height := max(360, max(topicHeight, consumerHeight))
	return topicFlowView{
		ViewBox: fmt.Sprintf("0 0 %d %d", canvasWidth, height), Width: canvasWidth, Height: height,
		Topics: topicNodes, Consumers: consumerNodes, Links: links, Relationships: len(links),
	}
}

func layoutFlowNodes(
	names []string,
	degrees map[string]int,
	x, width int,
	detail func(string) (string, string),
) ([]topicFlowNode, map[string]topicFlowNode, int) {
	const top, gap = 20, 16
	y := top
	nodes := make([]topicFlowNode, 0, len(names))
	positions := make(map[string]topicFlowNode, len(names))
	for _, name := range names {
		height := max(48, 24+degrees[name]*6)
		nodeDetail, nodeURL := detail(name)
		node := topicFlowNode{
			Name: name, DisplayName: truncateRunes(name, 30), Detail: nodeDetail, URL: nodeURL,
			X: x, Y: y, Width: width, Height: height, LabelY: y + 20, DetailY: y + 37,
		}
		nodes = append(nodes, node)
		positions[name] = node
		y += height + gap
	}
	return nodes, positions, y + top - gap
}

func flowSlotY(node topicFlowNode, index, total int) int {
	return node.Y + index*node.Height/(total+1)
}

func sortedKeys(values map[string]int) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func truncateRunes(value string, limit int) string {
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[:limit-1]) + "…"
}

func buildDeadTopics(
	topics []kafkaesque_topics.TopicInformation,
	groups []kafkaesque_consumers.ConsumerGroupInformation,
	page, pageSize int,
) deadTopicsResults {
	consumerGroups := make(map[string]map[string]struct{})
	for _, group := range groups {
		for _, topic := range group.Topics {
			if consumerGroups[topic.Topic] == nil {
				consumerGroups[topic.Topic] = make(map[string]struct{})
			}
			consumerGroups[topic.Topic][group.GroupName] = struct{}{}
		}
	}
	rows, collecting := make([]deadTopicView, 0), 0
	for _, topic := range topics {
		if !strings.EqualFold(topic.Level, "external") {
			continue
		}
		if topic.MessagesPerMinute == nil {
			collecting++
			continue
		}
		if *topic.MessagesPerMinute != 0 {
			continue
		}
		rows = append(rows, deadTopicView{
			Name: topic.Name, URL: topicsURL(topic.Name, "external", "minute", 1, 10),
			Partitions: topic.PartitionCount, ReplicationFactor: topic.ReplicationFactor,
			ConsumerGroups: len(consumerGroups[topic.Name]),
		})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })
	start, end, pagination := dashboardPaginationFor(len(rows), page, pageSize, deadTopicsPath, "#dead-topics-panel")
	return deadTopicsResults{Topics: rows[start:end], Collecting: collecting, Pagination: pagination}
}

func buildDeadConsumers(groups []kafkaesque_consumers.ConsumerGroupInformation, page, pageSize int) deadConsumersResults {
	rows := make([]deadConsumerView, 0)
	for _, group := range groups {
		if kafkaesque_consumers.GroupStateCategory(group.State) != kafkaesque_consumers.GroupStateDead {
			continue
		}
		rows = append(rows, deadConsumerView{
			Name: group.GroupName, URL: "/ui/consumer-groups/" + url.PathEscape(group.GroupName),
			Topics: group.TopicsCount, Members: group.MembersCount, State: kafkaesque_consumers.GroupStateDead,
		})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })
	start, end, pagination := dashboardPaginationFor(len(rows), page, pageSize, deadConsumersPath, "#dead-consumers-panel")
	return deadConsumersResults{Consumers: rows[start:end], Pagination: pagination}
}

func dashboardPaginationFor(total, page, pageSize int, path, target string) (int, int, dashboardPagination) {
	totalPages := (total + pageSize - 1) / pageSize
	start := min((page-1)*pageSize, total)
	end := min(start+pageSize, total)
	rangeText := "0 results"
	if start < end {
		rangeText = strconv.Itoa(start+1) + "–" + strconv.Itoa(end) + " of " + strconv.Itoa(total)
	} else if total > 0 {
		rangeText = "0 of " + strconv.Itoa(total)
	}
	pagination := dashboardPagination{
		Range:    rangeText,
		Pages:    compactDashboardPages(page, totalPages, path, pageSize),
		PageSize: newPageSizeControl(path, target, pageSize),
	}
	if page > 1 && page <= totalPages {
		pagination.PreviousURL = dashboardResourceURL(path, page-1, pageSize)
	}
	if page < totalPages {
		pagination.NextURL = dashboardResourceURL(path, page+1, pageSize)
	}
	return start, end, pagination
}

func compactDashboardPages(page, totalPages int, path string, pageSize int) []dashboardPageLink {
	if totalPages == 0 {
		return nil
	}
	wanted := map[int]bool{1: true, totalPages: true}
	for number := max(1, page-2); number <= min(totalPages, page+2); number++ {
		wanted[number] = true
	}
	numbers := make([]int, 0, len(wanted))
	for number := range wanted {
		numbers = append(numbers, number)
	}
	sort.Ints(numbers)
	links := make([]dashboardPageLink, 0, len(numbers)+2)
	previous := 0
	for _, number := range numbers {
		if previous != 0 && number > previous+1 {
			links = append(links, dashboardPageLink{Ellipsis: true})
		}
		links = append(links, dashboardPageLink{
			Number: number, URL: dashboardResourceURL(path, number, pageSize), Active: number == page,
		})
		previous = number
	}
	return links
}

func dashboardResourceURL(path string, page, pageSize int) string {
	return path + "?page=" + strconv.Itoa(page) + "&page_size=" + strconv.Itoa(pageSize)
}

func parseDashboardPageSelection(values url.Values) (dashboardPageSelection, error) {
	deadTopicPage, err := positivePage(values.Get("dead_topic_page"))
	if err != nil {
		return dashboardPageSelection{}, fmt.Errorf("dead_topic_page %w", err)
	}
	deadTopicPageSize, err := parsePageSize(values.Get("dead_topic_page_size"), 10)
	if err != nil {
		return dashboardPageSelection{}, fmt.Errorf("dead_topic_page_size %w", err)
	}
	deadConsumerPage, err := positivePage(values.Get("dead_consumer_page"))
	if err != nil {
		return dashboardPageSelection{}, fmt.Errorf("dead_consumer_page %w", err)
	}
	deadConsumerPageSize, err := parsePageSize(values.Get("dead_consumer_page_size"), 10)
	if err != nil {
		return dashboardPageSelection{}, fmt.Errorf("dead_consumer_page_size %w", err)
	}
	return dashboardPageSelection{
		DeadTopicPage: deadTopicPage, DeadTopicPageSize: deadTopicPageSize,
		DeadConsumerPage: deadConsumerPage, DeadConsumerPageSize: deadConsumerPageSize,
	}, nil
}

func parseDashboardFragmentPage(values url.Values) (int, int, error) {
	page, err := positivePage(values.Get("page"))
	if err != nil {
		return 0, 0, err
	}
	pageSize, err := parsePageSize(values.Get("page_size"), 10)
	if err != nil {
		return 0, 0, err
	}
	return page, pageSize, nil
}

func dashboardFallbackURL(kind string, page, pageSize int) string {
	values := url.Values{}
	values.Set("dead_"+kind+"_page", strconv.Itoa(page))
	values.Set("dead_"+kind+"_page_size", strconv.Itoa(pageSize))
	return "/ui?" + values.Encode() + "#dead-" + kind + "s-panel"
}
