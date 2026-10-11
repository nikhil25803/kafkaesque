package kafka

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	kafkaesque_config "github.com/nikhil25803/kafkaesque/internals/config"
)

func TestResolveBrokerAddress(t *testing.T) {
	conn := &KafkaesqueConn{brokerAddressOverrides: map[string]string{
		"broker.internal:9092": "10.100.0.72:9094",
		"[2001:db8::1]:9092":   "[2001:db8::2]:9094",
	}}

	tests := []struct {
		advertised string
		want       string
	}{
		{advertised: "broker.internal:9092", want: "10.100.0.72:9094"},
		{advertised: "[2001:db8::1]:9092", want: "[2001:db8::2]:9094"},
		{advertised: "other.internal:9092", want: "other.internal:9092"},
	}
	for _, test := range tests {
		if got := conn.ResolveBrokerAddress(test.advertised); got != test.want {
			t.Fatalf("ResolveBrokerAddress(%q) = %q, want %q", test.advertised, got, test.want)
		}
	}
}

func TestConnectBuildsSharedSecurityTransport(t *testing.T) {
	tests := []struct {
		name     string
		security kafkaesque_config.SecurityConfig
		wantTLS  bool
		wantSASL string
	}{
		{name: "plaintext"},
		{name: "TLS", security: kafkaesque_config.SecurityConfig{TLS: kafkaesque_config.TLSConfig{Enabled: true}}, wantTLS: true},
		{name: "PLAIN", security: kafkaesque_config.SecurityConfig{SASL: kafkaesque_config.SASLConfig{Mechanism: "PLAIN", Username: "user", Password: "pass"}}, wantSASL: "PLAIN"},
		{name: "SCRAM SHA-256", security: kafkaesque_config.SecurityConfig{SASL: kafkaesque_config.SASLConfig{Mechanism: "SCRAM-SHA-256", Username: "user", Password: "pass"}}, wantSASL: "SCRAM-SHA-256"},
		{name: "SCRAM SHA-512", security: kafkaesque_config.SecurityConfig{SASL: kafkaesque_config.SASLConfig{Mechanism: "SCRAM-SHA-512", Username: "user", Password: "pass"}}, wantSASL: "SCRAM-SHA-512"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := kafkaesque_config.KafkaConfig{
				BootstrapServers:  []string{"kafka-1:9092", "kafka-2:9092"},
				ConnectionTimeout: 7 * time.Second,
				Security:          test.security,
			}
			conn, err := Connect(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if (conn.transport.TLS != nil) != test.wantTLS {
				t.Fatalf("TLS configured = %t, want %t", conn.transport.TLS != nil, test.wantTLS)
			}
			if conn.transport.DialTimeout != 7*time.Second {
				t.Fatalf("dial timeout = %s", conn.transport.DialTimeout)
			}
			if conn.Client.Timeout != 7*time.Second {
				t.Fatalf("client timeout = %s", conn.Client.Timeout)
			}
			if test.wantSASL == "" && conn.transport.SASL != nil {
				t.Fatalf("unexpected SASL mechanism %s", conn.transport.SASL.Name())
			}
			if test.wantSASL != "" && (conn.transport.SASL == nil || conn.transport.SASL.Name() != test.wantSASL) {
				t.Fatalf("SASL mechanism = %v, want %s", conn.transport.SASL, test.wantSASL)
			}
			if got := conn.Client.Addr.String(); !strings.Contains(got, "kafka-1:9092") || !strings.Contains(got, "kafka-2:9092") {
				t.Fatalf("client address = %q", got)
			}
		})
	}
}

func TestConnectReportsTLSFileErrors(t *testing.T) {
	_, err := Connect(context.Background(), kafkaesque_config.KafkaConfig{
		BootstrapServers:  []string{"localhost:9092"},
		ConnectionTimeout: time.Second,
		Security: kafkaesque_config.SecurityConfig{TLS: kafkaesque_config.TLSConfig{
			Enabled: true,
			CAFile:  "missing-ca.pem",
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "read Kafka TLS CA file") {
		t.Fatalf("error = %v", err)
	}
}

func TestBrokerDialerUsesAddressOverride(t *testing.T) {
	var dialedAddress string
	dial := brokerDialerWithDial(
		map[string]string{"broker.internal:9092": "10.100.0.72:9094"},
		func(_ context.Context, _, address string) (net.Conn, error) {
			dialedAddress = address
			return nil, nil
		},
	)
	_, err := dial(context.Background(), "tcp", "broker.internal:9092")
	if err != nil {
		t.Fatal(err)
	}
	if dialedAddress != "10.100.0.72:9094" {
		t.Fatalf("dialed address = %q, want 10.100.0.72:9094", dialedAddress)
	}

	_, err = dial(context.Background(), "tcp", "other.internal:9092")
	if err != nil {
		t.Fatal(err)
	}
	if dialedAddress != "other.internal:9092" {
		t.Fatalf("unmapped dialed address = %q, want other.internal:9092", dialedAddress)
	}
}

func TestBrokerDialerErrorIncludesAdvertisedAndConnectAddresses(t *testing.T) {
	dial := brokerDialerWithDial(
		map[string]string{"broker.internal:9092": "127.0.0.1:0"},
		func(context.Context, string, string) (net.Conn, error) {
			return nil, errors.New("network failed")
		},
	)
	_, err := dial(context.Background(), "tcp", "broker.internal:9092")
	if err == nil {
		t.Fatal("expected dial error")
	}
	for _, address := range []string{"broker.internal:9092", "127.0.0.1:0"} {
		if !strings.Contains(err.Error(), address) {
			t.Fatalf("error %q does not contain %q", err, address)
		}
	}
}
