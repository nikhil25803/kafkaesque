package kafka

import (
	"context"
	"errors"
	"net"
	"strings"

	kafkaesque_config "github.com/nikhil25803/kafkaesque/internals/config"
	kafkago "github.com/segmentio/kafka-go"
)

// ClassifyConnectionError returns the CLI exit code and stable public reason for a connection failure.
func ClassifyConnectionError(err error) (int, string) {
	if errors.Is(err, context.DeadlineExceeded) {
		return 3, "connection timeout"
	}
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		return 3, "connection timeout"
	}

	securityErrors := []error{
		kafkago.SASLAuthenticationFailed,
		kafkago.UnsupportedSASLMechanism,
		kafkago.IllegalSASLState,
		kafkago.TopicAuthorizationFailed,
		kafkago.GroupAuthorizationFailed,
		kafkago.ClusterAuthorizationFailed,
		kafkago.BrokerAuthorizationFailed,
	}
	for _, securityError := range securityErrors {
		if errors.Is(err, securityError) {
			if errors.Is(err, kafkago.SASLAuthenticationFailed) || errors.Is(err, kafkago.UnsupportedSASLMechanism) || errors.Is(err, kafkago.IllegalSASLState) {
				return 2, "SASL authentication failed"
			}
			return 2, "Kafka authorization failed"
		}
	}
	lower := strings.ToLower(err.Error())
	if strings.Contains(lower, "tls") || strings.Contains(lower, "x509") || strings.Contains(lower, "certificate") {
		return 2, "TLS authentication failed"
	}
	return 1, "connection failed"
}

// SecurityProtocol derives Kafka's effective transport protocol without exposing credentials.
func SecurityProtocol(cfg kafkaesque_config.KafkaConfig) string {
	if cfg.Security.SASL.Mechanism != "" {
		if cfg.Security.TLS.Enabled {
			return "SASL_SSL"
		}
		return "SASL_PLAINTEXT"
	}
	if cfg.Security.TLS.Enabled {
		return "SSL"
	}
	return "PLAINTEXT"
}
