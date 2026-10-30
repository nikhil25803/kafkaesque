package alerts

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type SlackSender interface {
	Send(context.Context, string, SlackPayload) error
}

type HTTPSlackSender struct {
	client *http.Client
}

func NewHTTPSlackSender() *HTTPSlackSender {
	return &HTTPSlackSender{client: &http.Client{
		Timeout: 10 * time.Second,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return fmt.Errorf("Slack webhook redirects are not allowed")
		},
	}}
}

type SlackPayload struct {
	Message string
	Text    string
	Blocks  []SlackBlock
}

type SlackBlock struct {
	Type     string      `json:"type"`
	Text     *SlackText  `json:"text,omitempty"`
	Fields   []SlackText `json:"fields,omitempty"`
	Elements []SlackText `json:"elements,omitempty"`
}

type SlackText struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type Notification struct {
	Resolved           bool
	ConsumerGroup      string
	Topic              string
	Metric             string
	Value              float64
	Threshold          int64
	PartitionCount     int
	WorstPartition     int
	WorstPartitionLag  int64
	BreachedPartitions int
	ActiveMembers      int
	MemberID           string
	ClientID           string
	Host               string
	OccurredAt         time.Time
}

func BuildSlackPayload(notification Notification) SlackPayload {
	status := "firing"
	marker := "🔴"
	if notification.Resolved {
		status, marker = "resolved", "🟢"
	}
	return SlackPayload{
		Message: buildWorkflowMessage(notification),
		Text: fmt.Sprintf("%s Kafkaesque lag alert %s: %s/%s lag %.2f (threshold %d)",
			marker, status, messageValue(notification.ConsumerGroup), messageValue(notification.Topic),
			notification.Value, notification.Threshold),
		Blocks: buildSlackBlocks(notification),
	}
}

func buildWorkflowMessage(notification Notification) string {
	status := "🔴 *FIRING*"
	title, metric, value := notificationPresentation(notification)
	if notification.Resolved {
		status = "🟢 *RESOLVED*"
		title = "Partition lag recovered"
	}

	var builder strings.Builder
	builder.WriteString("*KAFKAESQUE*\n")
	builder.WriteString(status + "\n\n")
	builder.WriteString("*" + title + "*\n\n")
	builder.WriteString("*AFFECTED RESOURCE*\n")
	fmt.Fprintf(&builder, "• Topic: %s\n", slackCode(notification.Topic))
	fmt.Fprintf(&builder, "• Consumer group: %s\n", slackCode(notification.ConsumerGroup))
	fmt.Fprintf(&builder, "• Partitions: `%d`\n", notification.PartitionCount)
	fmt.Fprintf(&builder, "• Metric: %s\n", metric)
	builder.WriteString("\n────────────────────\n\n")
	builder.WriteString("*METRIC OVERVIEW*\n")
	fmt.Fprintf(&builder, "• Current lag: *%s*\n", value)
	fmt.Fprintf(&builder, "• Threshold: `%.2f`\n", float64(notification.Threshold))

	if notification.Resolved {
		builder.WriteString("\n🟢 _Lag is back within the configured threshold._\n")
	} else {
		fmt.Fprintf(&builder, "• Breached partitions: *%d / %d*\n", notification.BreachedPartitions, notification.PartitionCount)
		builder.WriteString("\n────────────────────\n\n")
		builder.WriteString("*PARTITION DETAILS*\n")
		fmt.Fprintf(&builder, "• Worst partition: `%d` (lag: `%d`)\n", notification.WorstPartition, notification.WorstPartitionLag)
		fmt.Fprintf(&builder, "• Active members: *%d*\n", notification.ActiveMembers)
		if notification.ActiveMembers == 0 {
			builder.WriteString("• Consumer status: ⚠️ *No active members*\n")
		} else {
			writeWorkflowMemberDetails(&builder, notification)
		}
	}

	if !notification.OccurredAt.IsZero() {
		builder.WriteString("\n────────────────────\n\n")
		fmt.Fprintf(&builder, "_%s_\n", notification.OccurredAt.UTC().Format("02 Jan 2006 · 15:04:05 UTC"))
	}
	return builder.String()
}

func writeWorkflowMemberDetails(builder *strings.Builder, notification Notification) {
	if notification.MemberID != "" {
		fmt.Fprintf(builder, "• Member: %s\n", slackCode(notification.MemberID))
	}
	if notification.ClientID != "" {
		fmt.Fprintf(builder, "• Client: %s\n", slackCode(notification.ClientID))
	}
	if notification.Host != "" {
		fmt.Fprintf(builder, "• Host: %s\n", slackCode(notification.Host))
	}
}

func buildSlackBlocks(notification Notification) []SlackBlock {
	status := "🔴 FIRING"
	title, metric, value := notificationPresentation(notification)
	if notification.Resolved {
		status = "🟢 RESOLVED"
		title = "Partition lag recovered"
	}
	blocks := []SlackBlock{
		headerBlock("Kafkaesque"),
		sectionBlock(fmt.Sprintf("*%s*\n*%s*", status, title)),
		dividerBlock(),
		sectionFieldsBlock(
			field("Topic", slackCode(notification.Topic)),
			field("Consumer group", slackCode(notification.ConsumerGroup)),
			field("Partitions", strconv.Itoa(notification.PartitionCount)),
			field("Metric", metric),
		),
		dividerBlock(),
		sectionBlock("*METRIC OVERVIEW*"),
		sectionFieldsBlock(
			field(metric, "*"+value+"*"),
			field("Threshold", strconv.FormatInt(notification.Threshold, 10)),
			field("Breached partitions", fmt.Sprintf("%d / %d", notification.BreachedPartitions, notification.PartitionCount)),
		),
	}
	if notification.Resolved {
		blocks = append(blocks, dividerBlock(), sectionBlock("🟢 _Lag is back within the configured threshold._"))
	} else {
		blocks = append(blocks,
			dividerBlock(),
			sectionBlock("*PARTITION DETAILS*"),
			sectionFieldsBlock(
				field("Worst partition", fmt.Sprintf("#%d", notification.WorstPartition)),
				field("Worst partition lag", strconv.FormatInt(notification.WorstPartitionLag, 10)),
				field("Active members", strconv.Itoa(notification.ActiveMembers)),
			),
		)
		if details := memberContext(notification); details != "" {
			blocks = append(blocks, contextBlock(details))
		}
	}
	if !notification.OccurredAt.IsZero() {
		blocks = append(blocks, dividerBlock(), contextBlock(notification.OccurredAt.UTC().Format("02 Jan 2006 · 15:04:05 UTC")))
	}
	return blocks
}

func notificationPresentation(notification Notification) (string, string, string) {
	title, metric := "Average partition lag exceeded", "Average lag"
	value := strconv.FormatFloat(notification.Value, 'f', 2, 64)
	if notification.Metric == MetricPartition {
		title, metric = "Maximum partition lag exceeded", "Maximum lag"
		value = strconv.FormatFloat(notification.Value, 'f', 0, 64)
	}
	return title, metric, value
}

func memberContext(notification Notification) string {
	if notification.ActiveMembers == 0 {
		return "⚠️ *No active members*"
	}
	details := make([]string, 0, 3)
	if notification.MemberID != "" {
		details = append(details, "Member: "+slackCode(notification.MemberID))
	}
	if notification.ClientID != "" {
		details = append(details, "Client: "+slackCode(notification.ClientID))
	}
	if notification.Host != "" {
		details = append(details, "Host: "+slackCode(notification.Host))
	}
	return strings.Join(details, " · ")
}

func (s *HTTPSlackSender) Send(ctx context.Context, webhook string, payload SlackPayload) error {
	webhookType, err := classifySlackWebhookURL(webhook)
	if err != nil {
		return err
	}
	var body []byte
	switch webhookType {
	case slackIncomingWebhook:
		body, err = json.Marshal(struct {
			Text   string       `json:"text"`
			Blocks []SlackBlock `json:"blocks"`
		}{Text: payload.Text, Blocks: payload.Blocks})
	case slackWorkflowWebhook:
		body, err = json.Marshal(struct {
			Message string `json:"message"`
		}{Message: payload.Message})
	}
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, webhook, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json; charset=utf-8")
	response, err := s.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	if readErr != nil {
		return fmt.Errorf("read Slack webhook response: %w", readErr)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("Slack webhook returned HTTP %d", response.StatusCode)
	}
	if webhookType == slackIncomingWebhook {
		if strings.TrimSpace(string(responseBody)) != "ok" {
			return fmt.Errorf("Slack Incoming Webhook did not acknowledge the alert")
		}
		return nil
	}
	var result struct {
		OK bool `json:"ok"`
	}
	if err := json.Unmarshal(responseBody, &result); err != nil || !result.OK {
		return fmt.Errorf("Slack workflow did not acknowledge the alert")
	}
	return nil
}

func headerBlock(text string) SlackBlock {
	return SlackBlock{Type: "header", Text: &SlackText{Type: "plain_text", Text: text}}
}

func sectionBlock(text string) SlackBlock {
	return SlackBlock{Type: "section", Text: &SlackText{Type: "mrkdwn", Text: text}}
}

func sectionFieldsBlock(fields ...string) SlackBlock {
	block := SlackBlock{Type: "section"}
	for _, value := range fields {
		block.Fields = append(block.Fields, SlackText{Type: "mrkdwn", Text: value})
	}
	return block
}

func field(label, value string) string { return fmt.Sprintf("*%s:* %s", label, value) }

func dividerBlock() SlackBlock { return SlackBlock{Type: "divider"} }

func contextBlock(text string) SlackBlock {
	return SlackBlock{Type: "context", Elements: []SlackText{{Type: "mrkdwn", Text: text}}}
}

func slackCode(value string) string {
	value = strings.ReplaceAll(messageValue(value), "`", "ˋ")
	return "`" + value + "`"
}

func messageValue(value string) string {
	value = strings.ReplaceAll(value, "\r", " ")
	value = strings.ReplaceAll(value, "\n", " ")
	return escapeSlack(value)
}

func escapeSlack(value string) string {
	value = strings.ReplaceAll(value, "&", "&amp;")
	value = strings.ReplaceAll(value, "<", "&lt;")
	return strings.ReplaceAll(value, ">", "&gt;")
}
