package alerts

import (
	"errors"
	"testing"

	kafkaesque_auth "github.com/nikhil25803/kafkaesque/internals/auth"
)

func TestValidateInput(t *testing.T) {
	valid := Input{
		ConsumerGroup: "orders-service", Topic: "orders", Metric: MetricAverage,
		LagThreshold: 0, NotifyOnResolve: true,
		WebhookURL: "https://hooks.slack.com/triggers/T/1/secret",
	}
	if _, err := validateInput(valid, true); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*Input)
	}{
		{name: "group", mutate: func(input *Input) { input.ConsumerGroup = "" }},
		{name: "topic", mutate: func(input *Input) { input.Topic = "" }},
		{name: "metric", mutate: func(input *Input) { input.Metric = "total" }},
		{name: "threshold", mutate: func(input *Input) { input.LagThreshold = -1 }},
		{name: "webhook host", mutate: func(input *Input) { input.WebhookURL = "https://example.com/triggers/T/1/secret" }},
		{name: "webhook scheme", mutate: func(input *Input) { input.WebhookURL = "http://hooks.slack.com/triggers/T/1/secret" }},
		{name: "query string", mutate: func(input *Input) { input.WebhookURL += "?token=secret" }},
		{name: "incomplete trigger", mutate: func(input *Input) { input.WebhookURL = "https://hooks.slack.com/triggers/T/1" }},
		{name: "unsupported path", mutate: func(input *Input) { input.WebhookURL = "https://hooks.slack.com/actions/T/1/secret" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := valid
			test.mutate(&input)
			var validation *ValidationError
			if _, err := validateInput(input, true); !errors.As(err, &validation) {
				t.Fatalf("error=%v", err)
			}
		})
	}
	valid.WebhookURL = "https://hooks.slack-gov.com/triggers/T/1/secret"
	if _, err := validateInput(valid, true); err != nil {
		t.Fatalf("GovSlack workflow webhook: %v", err)
	}
	valid.WebhookURL = "https://hooks.slack.com/services/T/B/secret"
	if _, err := validateInput(valid, true); err != nil {
		t.Fatalf("Slack Incoming Webhook: %v", err)
	}

	valid.WebhookURL = ""
	if _, err := validateInput(valid, false); err != nil {
		t.Fatalf("blank webhook on edit: %v", err)
	}
}

func TestAlertMutationPermissionRequiresView(t *testing.T) {
	actor := kafkaesque_auth.User{Permissions: []string{kafkaesque_auth.PermissionAlertCreate}}
	if hasMutationPermission(actor, kafkaesque_auth.PermissionAlertCreate) {
		t.Fatal("create permission without view was accepted")
	}
	actor.Permissions = append(actor.Permissions, kafkaesque_auth.PermissionAlertView)
	if !hasMutationPermission(actor, kafkaesque_auth.PermissionAlertCreate) {
		t.Fatal("create and view permissions were rejected")
	}
}

func TestClassifySlackWebhookURL(t *testing.T) {
	for _, test := range []struct {
		url  string
		want slackWebhookType
	}{
		{url: "https://hooks.slack.com/services/T/B/secret", want: slackIncomingWebhook},
		{url: "https://hooks.slack.com/triggers/T/1/secret", want: slackWorkflowWebhook},
		{url: "https://hooks.slack-gov.com/services/T/B/secret", want: slackIncomingWebhook},
		{url: "https://hooks.slack-gov.com/triggers/T/1/secret", want: slackWorkflowWebhook},
	} {
		got, err := classifySlackWebhookURL(test.url)
		if err != nil || got != test.want {
			t.Fatalf("url=%q type=%v error=%v", test.url, got, err)
		}
	}
}

func TestAlertConditionResetRules(t *testing.T) {
	current := Subscription{ConsumerGroup: "orders-service", Topic: "orders", Metric: MetricAverage, LagThreshold: 100}
	base := Input{
		ConsumerGroup: current.ConsumerGroup, Topic: current.Topic, Metric: current.Metric,
		LagThreshold: current.LagThreshold, NotifyOnResolve: true,
	}
	if alertConditionChanged(current, base) {
		t.Fatal("delivery-only edit reset the condition")
	}
	for _, mutate := range []func(*Input){
		func(input *Input) { input.ConsumerGroup = "payments-service" },
		func(input *Input) { input.Topic = "payments" },
		func(input *Input) { input.Metric = MetricPartition },
		func(input *Input) { input.LagThreshold = 101 },
	} {
		input := base
		mutate(&input)
		if !alertConditionChanged(current, input) {
			t.Fatal("condition edit did not request a state reset")
		}
	}
}
