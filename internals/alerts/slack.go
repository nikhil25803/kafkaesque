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
	Type   string      `json:"type"`
	Text   *SlackText  `json:"text,omitempty"`
	Fields []SlackText `json:"fields,omitempty"`
}

type SlackText struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type Notification struct {
	Resolved           bool
	ConsumerGroup      string
	Topic              string
	PartitionCount     int
	WorstPartition     int
	WorstPartitionLag  int64
	BreachedPartitions int
}

func BuildSlackPayload(notification Notification) SlackPayload {
	status := "🔴 Kafkaesque Update: Lag Alert Fired"
	if notification.Resolved {
		status = "🟢 Kafkaesque Update: Lag Alert Resolved"
	}
	fields := notificationFields(notification)
	return SlackPayload{
		Message: workflowMessage(status, notification),
		Text:    status,
		Blocks: []SlackBlock{
			sectionBlock("*" + status + "*"),
			dividerBlock(),
			{Type: "section", Fields: fields},
		},
	}
}

func workflowMessage(status string, notification Notification) string {
	return strings.Join([]string{
		status,
		"Topic: " + plainValue(notification.Topic),
		"Consumer group: " + plainValue(notification.ConsumerGroup),
		"Total partitions: " + strconv.Itoa(notification.PartitionCount),
		"Lag partitions: " + strconv.Itoa(notification.BreachedPartitions),
		"Metric: Partition Lag",
		fmt.Sprintf("Worst partition lag: %d (#%d)", notification.WorstPartitionLag, notification.WorstPartition),
	}, "\n")
}

func notificationFields(notification Notification) []SlackText {
	values := []string{
		field("Topic", slackCode(notification.Topic)),
		field("Consumer group", slackCode(notification.ConsumerGroup)),
		field("Total partitions", strconv.Itoa(notification.PartitionCount)),
		field("Lag partitions", strconv.Itoa(notification.BreachedPartitions)),
		field("Metric", "Partition Lag"),
		field("Worst partition lag", fmt.Sprintf("%d (#%d)", notification.WorstPartitionLag, notification.WorstPartition)),
	}
	fields := make([]SlackText, 0, len(values))
	for _, value := range values {
		fields = append(fields, SlackText{Type: "mrkdwn", Text: value})
	}
	return fields
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

func sectionBlock(text string) SlackBlock {
	return SlackBlock{Type: "section", Text: &SlackText{Type: "mrkdwn", Text: text}}
}

func field(label, value string) string { return fmt.Sprintf("*%s:* %s", label, value) }

func dividerBlock() SlackBlock { return SlackBlock{Type: "divider"} }

func slackCode(value string) string {
	value = strings.ReplaceAll(messageValue(value), "`", "ˋ")
	return "`" + value + "`"
}

func messageValue(value string) string {
	value = strings.ReplaceAll(value, "\r", " ")
	value = strings.ReplaceAll(value, "\n", " ")
	return escapeSlack(value)
}

func plainValue(value string) string {
	return strings.NewReplacer("\r", " ", "\n", " ", "*", "", "`", "'").Replace(value)
}

func escapeSlack(value string) string {
	value = strings.ReplaceAll(value, "&", "&amp;")
	value = strings.ReplaceAll(value, "<", "&lt;")
	return strings.ReplaceAll(value, ">", "&gt;")
}
