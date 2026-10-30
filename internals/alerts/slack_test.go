package alerts

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestBuildSlackPayloadFiring(t *testing.T) {
	now := time.Date(2026, time.October, 30, 12, 0, 0, 0, time.UTC)
	payload := BuildSlackPayload(Notification{
		ConsumerGroup: "orders<&>", Topic: "orders", Metric: MetricPartition,
		Value: 500, Threshold: 100, PartitionCount: 6, WorstPartition: 3,
		WorstPartitionLag: 500, BreachedPartitions: 2, ActiveMembers: 1,
		MemberID: "member<&>", ClientID: "worker", Host: "10.0.0.12", OccurredAt: now,
	})
	wantMessage := strings.Join([]string{
		"*KAFKAESQUE*",
		"🔴 *FIRING*",
		"",
		"*Maximum partition lag exceeded*",
		"",
		"*AFFECTED RESOURCE*",
		"• Topic: `orders`",
		"• Consumer group: `orders&lt;&amp;&gt;`",
		"• Partitions: `6`",
		"• Metric: Maximum lag",
		"",
		"────────────────────",
		"",
		"*METRIC OVERVIEW*",
		"• Current lag: *500*",
		"• Threshold: `100.00`",
		"• Breached partitions: *2 / 6*",
		"",
		"────────────────────",
		"",
		"*PARTITION DETAILS*",
		"• Worst partition: `3` (lag: `500`)",
		"• Active members: *1*",
		"• Member: `member&lt;&amp;&gt;`",
		"• Client: `worker`",
		"• Host: `10.0.0.12`",
		"",
		"────────────────────",
		"",
		"_30 Oct 2026 · 12:00:00 UTC_",
		"",
	}, "\n")
	if payload.Message != wantMessage {
		t.Fatalf("message mismatch\n got: %q\nwant: %q", payload.Message, wantMessage)
	}
	if payload.Text != "🔴 Kafkaesque lag alert firing: orders&lt;&amp;&gt;/orders lag 500.00 (threshold 100)" {
		t.Fatalf("fallback text=%q", payload.Text)
	}
	if len(payload.Blocks) != 13 {
		t.Fatalf("blocks=%d: %+v", len(payload.Blocks), payload.Blocks)
	}
	if payload.Blocks[0].Type != "header" || payload.Blocks[0].Text.Text != "Kafkaesque" {
		t.Fatalf("header=%+v", payload.Blocks[0])
	}
	if payload.Blocks[1].Text.Text != "*🔴 FIRING*\n*Maximum partition lag exceeded*" {
		t.Fatalf("status=%+v", payload.Blocks[1])
	}
	if got := payload.Blocks[3].Fields[1].Text; got != "*Consumer group:* `orders&lt;&amp;&gt;`" {
		t.Fatalf("consumer field=%q", got)
	}
	if got := payload.Blocks[9].Fields[0].Text; got != "*Worst partition:* #3" {
		t.Fatalf("partition field=%q", got)
	}
	if got := payload.Blocks[10].Elements[0].Text; !strings.Contains(got, "Member: `member&lt;&amp;&gt;`") {
		t.Fatalf("member context=%q", got)
	}
	encoded, err := json.Marshal(struct {
		Message string `json:"message"`
	}{Message: payload.Message})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `\n🔴`) || strings.Contains(string(encoded), `/n`) {
		t.Fatalf("encoded newlines=%s", encoded)
	}
}

func TestBuildSlackPayloadResolved(t *testing.T) {
	payload := BuildSlackPayload(Notification{
		Resolved: true, ConsumerGroup: "orders\ninjected", Topic: "orders`raw", Metric: MetricAverage,
		Value: 1.5, Threshold: 10, PartitionCount: 2, WorstPartition: 1,
		WorstPartitionLag: 3, OccurredAt: time.Date(2026, time.October, 30, 12, 0, 0, 0, time.UTC),
	})
	for _, want := range []string{
		"🟢 *RESOLVED*",
		"*Partition lag recovered*",
		"• Topic: `ordersˋraw`",
		"• Consumer group: `orders injected`",
		"• Current lag: *1.50*",
		"🟢 _Lag is back within the configured threshold._",
	} {
		if !strings.Contains(payload.Message, want) {
			t.Fatalf("message missing %q: %q", want, payload.Message)
		}
	}
	if strings.Contains(payload.Message, "PARTITION DETAILS") {
		t.Fatalf("resolved message contains partition details: %q", payload.Message)
	}
	if payload.Blocks[1].Text.Text != "*🟢 RESOLVED*\n*Partition lag recovered*" {
		t.Fatalf("resolved status=%+v", payload.Blocks[1])
	}
}

func TestBuildSlackPayloadNoActiveMembers(t *testing.T) {
	payload := BuildSlackPayload(Notification{Topic: "orders", ConsumerGroup: "orders", PartitionCount: 1})
	if !strings.Contains(payload.Message, "Consumer status: ⚠️ *No active members*") {
		t.Fatalf("message=%q", payload.Message)
	}
	if got := payload.Blocks[len(payload.Blocks)-1].Elements[0].Text; got != "⚠️ *No active members*" {
		t.Fatalf("context=%q", got)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func TestSlackSenderRoutesPayloadByWebhookType(t *testing.T) {
	tests := []struct {
		name        string
		webhook     string
		response    string
		wantMessage bool
	}{
		{name: "workflow", webhook: "https://hooks.slack.com/triggers/T/1/secret", response: `{"ok":true}`, wantMessage: true},
		{name: "incoming", webhook: "https://hooks.slack.com/services/T/B/secret", response: "ok"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sender := NewHTTPSlackSender()
			var sent map[string]json.RawMessage
			sender.client.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
				if got := request.Header.Get("Content-Type"); got != "application/json; charset=utf-8" {
					t.Fatalf("Content-Type=%q", got)
				}
				body, err := io.ReadAll(request.Body)
				if err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(body, &sent); err != nil {
					t.Fatal(err)
				}
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(test.response)), Header: make(http.Header)}, nil
			})
			payload := SlackPayload{Message: "workflow", Text: "fallback", Blocks: []SlackBlock{headerBlock("Kafkaesque")}}
			if err := sender.Send(context.Background(), test.webhook, payload); err != nil {
				t.Fatal(err)
			}
			_, hasMessage := sent["message"]
			_, hasText := sent["text"]
			_, hasBlocks := sent["blocks"]
			if test.wantMessage != hasMessage || test.wantMessage == hasText || test.wantMessage == hasBlocks {
				t.Fatalf("payload keys message=%t text=%t blocks=%t", hasMessage, hasText, hasBlocks)
			}
		})
	}
}

func TestSlackSenderResponses(t *testing.T) {
	tests := []struct {
		name         string
		webhook      string
		status       int
		body         string
		transportErr error
		wantError    bool
	}{
		{name: "workflow rejected", webhook: "https://hooks.slack.com/triggers/T/1/secret", status: http.StatusOK, body: `{"ok":false}`, wantError: true},
		{name: "workflow malformed", webhook: "https://hooks.slack.com/triggers/T/1/secret", status: http.StatusOK, body: "not-json", wantError: true},
		{name: "incoming rejected", webhook: "https://hooks.slack.com/services/T/B/secret", status: http.StatusOK, body: "not-ok", wantError: true},
		{name: "non-2xx", webhook: "https://hooks.slack.com/services/T/B/secret", status: http.StatusBadRequest, body: "invalid", wantError: true},
		{name: "transport", webhook: "https://hooks.slack.com/triggers/T/1/secret", transportErr: errors.New("timeout"), wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sender := NewHTTPSlackSender()
			sender.client.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
				if test.transportErr != nil {
					return nil, test.transportErr
				}
				return &http.Response{StatusCode: test.status, Body: io.NopCloser(strings.NewReader(test.body)), Header: make(http.Header)}, nil
			})
			err := sender.Send(context.Background(), test.webhook, SlackPayload{Message: "message", Text: "text"})
			if (err != nil) != test.wantError {
				t.Fatalf("error=%v wantError=%t", err, test.wantError)
			}
		})
	}
}

func TestSlackSenderRejectsRedirects(t *testing.T) {
	sender := NewHTTPSlackSender()
	request, err := http.NewRequest(http.MethodPost, "https://hooks.slack.com/triggers/T/1/other", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := sender.client.CheckRedirect(request, nil); err == nil {
		t.Fatal("redirect was accepted")
	}
}

func TestSlackSenderTimeout(t *testing.T) {
	sender := NewHTTPSlackSender()
	sender.client.Timeout = 10 * time.Millisecond
	sender.client.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		<-request.Context().Done()
		return nil, request.Context().Err()
	})
	if err := sender.Send(context.Background(), "https://hooks.slack.com/triggers/T/1/secret", SlackPayload{Message: "hello"}); err == nil {
		t.Fatal("timeout was accepted")
	}
}
