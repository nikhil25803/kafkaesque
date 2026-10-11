package kafka

import (
	"context"
	"testing"

	kafkaesque_config "github.com/nikhil25803/kafkaesque/internals/config"
	kafkago "github.com/segmentio/kafka-go"
)

func TestSecurityProtocol(t *testing.T) {
	tests := []struct {
		name string
		cfg  kafkaesque_config.KafkaConfig
		want string
	}{
		{name: "plaintext", want: "PLAINTEXT"},
		{name: "TLS", cfg: kafkaesque_config.KafkaConfig{Security: kafkaesque_config.SecurityConfig{TLS: kafkaesque_config.TLSConfig{Enabled: true}}}, want: "SSL"},
		{name: "SASL", cfg: kafkaesque_config.KafkaConfig{Security: kafkaesque_config.SecurityConfig{SASL: kafkaesque_config.SASLConfig{Mechanism: "PLAIN"}}}, want: "SASL_PLAINTEXT"},
		{name: "SASL TLS", cfg: kafkaesque_config.KafkaConfig{Security: kafkaesque_config.SecurityConfig{TLS: kafkaesque_config.TLSConfig{Enabled: true}, SASL: kafkaesque_config.SASLConfig{Mechanism: "SCRAM-SHA-256"}}}, want: "SASL_SSL"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := SecurityProtocol(test.cfg); got != test.want {
				t.Fatalf("protocol = %q, want %q", got, test.want)
			}
		})
	}
}

func TestClassifyConnectionError(t *testing.T) {
	if code, reason := ClassifyConnectionError(context.DeadlineExceeded); code != 3 || reason != "connection timeout" {
		t.Fatalf("timeout = %d %q", code, reason)
	}
	if code, reason := ClassifyConnectionError(kafkago.RequestTimedOut); code != 3 || reason != "connection timeout" {
		t.Fatalf("Kafka timeout = %d %q", code, reason)
	}
	if code, reason := ClassifyConnectionError(kafkago.SASLAuthenticationFailed); code != 2 || reason != "SASL authentication failed" {
		t.Fatalf("authentication = %d %q", code, reason)
	}
}
