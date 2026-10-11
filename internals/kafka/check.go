package kafka

import (
	"context"
	"fmt"
	"time"
)

// CheckResult contains the cluster information collected by a connection check.
type CheckResult struct {
	BrokerCount    int
	ControllerID   int
	TopicCount     int
	PartitionCount int
	Latency        time.Duration
}

// Check verifies authenticated access by requesting complete cluster metadata.
func (c *KafkaesqueConn) Check(ctx context.Context, timeout time.Duration) (*CheckResult, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	started := time.Now()
	metadata, err := c.GetMetadata(ctx, nil)
	latency := time.Since(started)
	if err != nil {
		return nil, err
	}

	partitions := 0
	for _, topic := range metadata.Topics {
		if topic.Error != nil {
			return nil, fmt.Errorf("metadata for topic %s: %w", topic.Name, topic.Error)
		}
		for _, partition := range topic.Partitions {
			if partition.Error != nil {
				return nil, fmt.Errorf("metadata for topic %s partition %d: %w", topic.Name, partition.ID, partition.Error)
			}
			partitions++
		}
	}

	return &CheckResult{
		BrokerCount:    len(metadata.Brokers),
		ControllerID:   metadata.Controller.ID,
		TopicCount:     len(metadata.Topics),
		PartitionCount: partitions,
		Latency:        latency,
	}, nil
}
