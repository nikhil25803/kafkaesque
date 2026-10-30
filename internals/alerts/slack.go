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
		CheckRedirect: func(request *http.Request, _ []*http.Request) error {
			return validateSlackWebhookURL(request.URL.String())
		},
	}}
}

type SlackPayload struct {
	Text   string       `json:"text"`
	Blocks []SlackBlock `json:"blocks"`
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
	Reminder           bool
	ConsumerGroup      string
	Topic              string
	Metric             string
	Value              float64
	Threshold          int64
	WorstPartition     int
	BreachedPartitions int
	OccurredAt         time.Time
}

func BuildSlackPayload(notification Notification) SlackPayload {
	status, marker := "FIRING", "🔴"
	if notification.Resolved {
		status, marker = "RESOLVED", "🟢"
	} else if notification.Reminder {
		status = "STILL FIRING"
	}
	metric := "Average partition lag"
	value := strconv.FormatFloat(notification.Value, 'f', 2, 64)
	if notification.Metric == MetricPartition {
		metric = "Maximum partition lag"
		value = strconv.FormatFloat(notification.Value, 'f', 0, 64)
	}
	fields := []SlackText{
		mrkdwn("*Status*\n" + marker + " " + status),
		mrkdwn("*Consumer group*\n" + escapeSlack(notification.ConsumerGroup)),
		mrkdwn("*Topic*\n" + escapeSlack(notification.Topic)),
		mrkdwn("*Metric*\n" + metric),
		mrkdwn("*Current lag*\n" + value),
		mrkdwn("*Threshold*\n" + strconv.FormatInt(notification.Threshold, 10)),
	}
	if notification.Metric == MetricPartition {
		fields = append(fields,
			mrkdwn("*Worst partition*\n"+strconv.Itoa(notification.WorstPartition)),
			mrkdwn("*Breached partitions*\n"+strconv.Itoa(notification.BreachedPartitions)),
		)
	}
	fields = append(fields, mrkdwn("*Observed at*\n"+notification.OccurredAt.UTC().Format(time.RFC3339)))
	text := fmt.Sprintf("%s Kafkaesque lag alert %s: %s/%s lag %.2f (threshold %d)",
		marker, strings.ToLower(status), escapeSlack(notification.ConsumerGroup), escapeSlack(notification.Topic),
		notification.Value, notification.Threshold)
	return SlackPayload{
		Text: text,
		Blocks: []SlackBlock{
			{Type: "header", Text: &SlackText{Type: "plain_text", Text: marker + " Kafkaesque lag alert"}},
			{Type: "section", Fields: fields},
		},
	}
}

func (s *HTTPSlackSender) Send(ctx context.Context, webhook string, payload SlackPayload) error {
	if err := validateSlackWebhookURL(webhook); err != nil {
		return err
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, webhook, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := s.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("Slack webhook returned HTTP %d", response.StatusCode)
	}
	return nil
}

func mrkdwn(value string) SlackText { return SlackText{Type: "mrkdwn", Text: value} }

func escapeSlack(value string) string {
	value = strings.ReplaceAll(value, "&", "&amp;")
	value = strings.ReplaceAll(value, "<", "&lt;")
	return strings.ReplaceAll(value, ">", "&gt;")
}
