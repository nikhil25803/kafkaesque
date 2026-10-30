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
	payload := BuildSlackPayload(Notification{
		ConsumerGroup: "orders<&>", Topic: "orders", PartitionCount: 6,
		WorstPartition: 3, WorstPartitionLag: 500, BreachedPartitions: 2,
	})
	wantMessage := strings.Join([]string{
		"🔴 Kafkaesque Update: Lag Alert Fired",
		"Topic: orders",
		"Consumer group: orders<&>",
		"Total partitions: 6",
		"Lag partitions: 2",
		"Metric: Partition Lag",
		"Worst partition lag: 500 (#3)",
	}, "\n")
	if payload.Message != wantMessage {
		t.Fatalf("message mismatch\n got: %q\nwant: %q", payload.Message, wantMessage)
	}
	if payload.Text != "🔴 Kafkaesque Update: Lag Alert Fired" {
		t.Fatalf("fallback text=%q", payload.Text)
	}
	if len(payload.Blocks) != 3 {
		t.Fatalf("blocks=%d: %+v", len(payload.Blocks), payload.Blocks)
	}
	if payload.Blocks[0].Text.Text != "*🔴 Kafkaesque Update: Lag Alert Fired*" {
		t.Fatalf("status=%+v", payload.Blocks[0])
	}
	if len(payload.Blocks[2].Fields) != 6 {
		t.Fatalf("fields=%+v", payload.Blocks[2].Fields)
	}
	if got := payload.Blocks[2].Fields[1].Text; got != "*Consumer group:* `orders&lt;&amp;&gt;`" {
		t.Fatalf("consumer field=%q", got)
	}
	if got := payload.Blocks[2].Fields[5].Text; got != "*Worst partition lag:* 500 (#3)" {
		t.Fatalf("worst partition field=%q", got)
	}
	encoded, err := json.Marshal(struct {
		Message string `json:"message"`
	}{Message: payload.Message})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `\nTopic:`) || strings.Contains(string(encoded), `/n`) {
		t.Fatalf("encoded newlines=%s", encoded)
	}
	if strings.ContainsAny(payload.Message, "*`") {
		t.Fatalf("workflow message contains mrkdwn markers: %q", payload.Message)
	}
}

func TestBuildSlackPayloadResolved(t *testing.T) {
	payload := BuildSlackPayload(Notification{
		Resolved: true, ConsumerGroup: "orders\ninjected", Topic: "orders`raw",
		PartitionCount: 2, BreachedPartitions: 0, WorstPartition: 1, WorstPartitionLag: 3,
	})
	want := "🟢 Kafkaesque Update: Lag Alert Resolved\n" +
		"Topic: orders'raw\nConsumer group: orders injected\n" +
		"Total partitions: 2\nLag partitions: 0\nMetric: Partition Lag\n" +
		"Worst partition lag: 3 (#1)"
	if payload.Message != want {
		t.Fatalf("message mismatch\n got: %q\nwant: %q", payload.Message, want)
	}
	if payload.Text != "🟢 Kafkaesque Update: Lag Alert Resolved" {
		t.Fatalf("fallback text=%q", payload.Text)
	}
	if strings.ContainsAny(payload.Message, "*`") {
		t.Fatalf("workflow message contains mrkdwn markers: %q", payload.Message)
	}
	for _, removed := range []string{"Maximum", "Average", "Threshold", "Active members", "Observed at", "recovered"} {
		if strings.Contains(payload.Message, removed) {
			t.Fatalf("message contains removed detail %q: %q", removed, payload.Message)
		}
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
			payload := SlackPayload{Message: "workflow", Text: "fallback", Blocks: []SlackBlock{sectionBlock("Kafkaesque")}}
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
