package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// MetadataSnapshot is one validated complete metadata response and its latency.
type MetadataSnapshot struct {
	Metadata *kafkago.MetadataResponse
	Latency  time.Duration
}

// GetMetadataSnapshot fetches and validates complete cluster metadata.
func (c *KafkaesqueConn) GetMetadataSnapshot(ctx context.Context) (*MetadataSnapshot, error) {
	started := time.Now()
	metadata, err := c.GetMetadata(ctx, nil)
	if err != nil {
		return nil, err
	}
	for _, topic := range metadata.Topics {
		if topic.Error != nil {
			return nil, fmt.Errorf("metadata for topic %s: %w", topic.Name, topic.Error)
		}
		for _, partition := range topic.Partitions {
			if partition.Error != nil && !errors.Is(partition.Error, kafkago.LeaderNotAvailable) && !errors.Is(partition.Error, kafkago.ReplicaNotAvailable) {
				return nil, fmt.Errorf("metadata for topic %s partition %d: %w", topic.Name, partition.ID, partition.Error)
			}
		}
	}
	return &MetadataSnapshot{Metadata: metadata, Latency: time.Since(started)}, nil
}
