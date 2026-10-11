package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"time"

	kafkaesque_config "github.com/nikhil25803/kafkaesque/internals/config"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

type KafkaesqueConn struct {
	Client                 *kafkago.Client
	transport              *kafkago.Transport
	brokerAddressOverrides map[string]string
}

func Connect(ctx context.Context, cfg kafkaesque_config.KafkaConfig) (*KafkaesqueConn, error) {
	overrides := copyAddressOverrides(cfg.BrokerAddressOverrides)
	tlsConfig, err := buildTLSConfig(cfg.Security.TLS)
	if err != nil {
		return nil, err
	}
	saslMechanism, err := buildSASLMechanism(cfg.Security.SASL)
	if err != nil {
		return nil, err
	}
	transport := &kafkago.Transport{
		Dial:        brokerDialer(overrides, cfg.ConnectionTimeout),
		DialTimeout: cfg.ConnectionTimeout,
		TLS:         tlsConfig,
		SASL:        saslMechanism,
		Context:     ctx,
	}

	client := &kafkago.Client{
		Addr:      kafkago.TCP(cfg.BootstrapServers...),
		Timeout:   cfg.ConnectionTimeout,
		Transport: transport,
	}

	return &KafkaesqueConn{
		Client:                 client,
		transport:              transport,
		brokerAddressOverrides: overrides,
	}, nil
}

func (c *KafkaesqueConn) Close() error {
	if c.transport != nil {
		c.transport.CloseIdleConnections()
	}
	return nil
}

func buildTLSConfig(cfg kafkaesque_config.TLSConfig) (*tls.Config, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: cfg.ServerName}
	if cfg.CAFile != "" {
		ca, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read Kafka TLS CA file: %w", err)
		}
		roots, err := x509.SystemCertPool()
		if err != nil || roots == nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(ca) {
			return nil, fmt.Errorf("Kafka TLS CA file contains no certificates")
		}
		tlsConfig.RootCAs = roots
	}
	if cfg.ClientCertFile != "" {
		certificate, err := tls.LoadX509KeyPair(cfg.ClientCertFile, cfg.ClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load Kafka TLS client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}
	return tlsConfig, nil
}

func buildSASLMechanism(cfg kafkaesque_config.SASLConfig) (sasl.Mechanism, error) {
	switch cfg.Mechanism {
	case "":
		return nil, nil
	case "PLAIN":
		return plain.Mechanism{Username: cfg.Username, Password: cfg.Password}, nil
	case "SCRAM-SHA-256":
		return scram.Mechanism(scram.SHA256, cfg.Username, cfg.Password)
	case "SCRAM-SHA-512":
		return scram.Mechanism(scram.SHA512, cfg.Username, cfg.Password)
	default:
		return nil, fmt.Errorf("unsupported Kafka SASL mechanism %q", cfg.Mechanism)
	}
}

// ResolveBrokerAddress returns the configured connection address for an advertised broker address.
func (c *KafkaesqueConn) ResolveBrokerAddress(advertisedAddress string) string {
	if connectAddress, ok := c.brokerAddressOverrides[advertisedAddress]; ok {
		return connectAddress
	}
	return advertisedAddress
}

func brokerDialer(overrides map[string]string, timeout time.Duration) func(context.Context, string, string) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	return brokerDialerWithDial(overrides, dialer.DialContext)
}

func brokerDialerWithDial(
	overrides map[string]string,
	dial func(context.Context, string, string) (net.Conn, error),
) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, advertisedAddress string) (net.Conn, error) {
		connectAddress := advertisedAddress
		if override, ok := overrides[advertisedAddress]; ok {
			connectAddress = override
		}

		conn, err := dial(ctx, network, connectAddress)
		if err != nil && connectAddress != advertisedAddress {
			return nil, fmt.Errorf("dial Kafka broker advertised as %s via %s: %w", advertisedAddress, connectAddress, err)
		}
		return conn, err
	}
}

func copyAddressOverrides(overrides map[string]string) map[string]string {
	copy := make(map[string]string, len(overrides))
	for advertised, connect := range overrides {
		copy[advertised] = connect
	}
	return copy
}
