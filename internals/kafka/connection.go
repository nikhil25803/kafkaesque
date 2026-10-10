package kafka

import (
	"context"
	"fmt"
	"net"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

type KafkaesqueConn struct {
	Client                 *kafkago.Client
	Conn                   *kafkago.Conn
	transport              *kafkago.Transport
	brokerAddressOverrides map[string]string
}

func Connect(ctx context.Context, bootstrapServer string, brokerAddressOverrides map[string]string) (*KafkaesqueConn, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	overrides := copyAddressOverrides(brokerAddressOverrides)
	transport := &kafkago.Transport{
		Dial: brokerDialer(overrides),
	}

	// Kafka Client
	client := &kafkago.Client{
		Addr:      kafkago.TCP(bootstrapServer),
		Timeout:   10 * time.Second,
		Transport: transport,
	}

	// Kafka Connection
	conn, err := kafkago.DialContext(
		ctx,
		"tcp",
		bootstrapServer,
	)
	if err != nil {
		transport.CloseIdleConnections()
		return nil, err
	}

	return &KafkaesqueConn{
		Client:                 client,
		Conn:                   conn,
		transport:              transport,
		brokerAddressOverrides: overrides,
	}, nil
}

func (c *KafkaesqueConn) Close() error {
	if c.transport != nil {
		c.transport.CloseIdleConnections()
	}
	if c.Conn == nil {
		return nil
	}
	return c.Conn.Close()
}

// ResolveBrokerAddress returns the configured connection address for an advertised broker address.
func (c *KafkaesqueConn) ResolveBrokerAddress(advertisedAddress string) string {
	if connectAddress, ok := c.brokerAddressOverrides[advertisedAddress]; ok {
		return connectAddress
	}
	return advertisedAddress
}

func brokerDialer(overrides map[string]string) func(context.Context, string, string) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: 3 * time.Second}
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
