package kafka

import (
	"context"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

type KafkaesqueConn struct {
	Client *kafkago.Client
	Conn   *kafkago.Conn
}

func Connect(ctx context.Context, bootstrapServer string) (*KafkaesqueConn, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Kafka Client
	client := &kafkago.Client{
		Addr:    kafkago.TCP(bootstrapServer),
		Timeout: 10 * time.Second,
	}

	// Kafka Connection
	conn, err := kafkago.DialContext(
		ctx,
		"tcp",
		bootstrapServer,
	)
	if err != nil {
		return nil, err
	}

	return &KafkaesqueConn{
		Client: client,
		Conn:   conn,
	}, nil
}

func (c *KafkaesqueConn) Close() error {
	return c.Conn.Close()
}
