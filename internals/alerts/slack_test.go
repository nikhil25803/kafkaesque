package alerts

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestBuildSlackPayload(t *testing.T) {
	now := time.Date(2026, time.October, 30, 12, 0, 0, 0, time.UTC)
	firing := BuildSlackPayload(Notification{
		ConsumerGroup: "orders<&>", Topic: "orders", Metric: MetricPartition,
		Value: 500, Threshold: 100, WorstPartition: 3, BreachedPartitions: 2, OccurredAt: now,
	})
	body, err := json.Marshal(firing)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, want := range []string{"🔴", "FIRING", "Maximum partition lag", "Worst partition", "Breached partitions", now.Format(time.RFC3339)} {
		if !strings.Contains(text, want) {
			t.Fatalf("payload missing %q: %s", want, text)
		}
	}
	if firing.Blocks[1].Fields[1].Text != "*Consumer group*\norders&lt;&amp;&gt;" {
		t.Fatalf("consumer group was not Slack-escaped: %q", firing.Blocks[1].Fields[1].Text)
	}
	if firing.Text == "" || len(firing.Blocks) != 2 {
		t.Fatalf("payload=%+v", firing)
	}
	expected := SlackPayload{
		Text: "🔴 Kafkaesque lag alert firing: orders&lt;&amp;&gt;/orders lag 500.00 (threshold 100)",
		Blocks: []SlackBlock{
			{Type: "header", Text: &SlackText{Type: "plain_text", Text: "🔴 Kafkaesque lag alert"}},
			{Type: "section", Fields: []SlackText{
				mrkdwn("*Status*\n🔴 FIRING"),
				mrkdwn("*Consumer group*\norders&lt;&amp;&gt;"),
				mrkdwn("*Topic*\norders"),
				mrkdwn("*Metric*\nMaximum partition lag"),
				mrkdwn("*Current lag*\n500"),
				mrkdwn("*Threshold*\n100"),
				mrkdwn("*Worst partition*\n3"),
				mrkdwn("*Breached partitions*\n2"),
				mrkdwn("*Observed at*\n2026-10-30T12:00:00Z"),
			}},
		},
	}
	if !reflect.DeepEqual(firing, expected) {
		t.Fatalf("payload mismatch\n got: %#v\nwant: %#v", firing, expected)
	}

	resolved := BuildSlackPayload(Notification{Resolved: true, ConsumerGroup: "orders", Topic: "orders", Metric: MetricAverage, OccurredAt: now})
	resolvedBody, _ := json.Marshal(resolved)
	if !strings.Contains(string(resolvedBody), "🟢") || !strings.Contains(string(resolvedBody), "RESOLVED") {
		t.Fatalf("resolved payload=%s", resolvedBody)
	}
}
