package kafka

import (
	"context"

	kafkago "github.com/segmentio/kafka-go"
)

// GetMetadata returns cluster metadata.
func (c *KafkaesqueConn) GetMetadata(ctx context.Context, topics []string) (*kafkago.MetadataResponse, error) {
	return c.Client.Metadata(ctx, &kafkago.MetadataRequest{Topics: topics})
}
