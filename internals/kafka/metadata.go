package kafka

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type ClusterInformation struct {
	ThrottleTimeMs int    `json:"throttle_time_ms"`
	ClusterID      string `json:"cluster_id"`
	ControllerID   int    `json:"controller_id"`
	Brokers        int    `json:"brokers"`
	Topics         int    `json:"topics"`
}

func (c *KafkaesqueConn) GetMetadata(ctx context.Context) (*ClusterInformation, error) {
	metadata, err := c.Client.Metadata(
		ctx,
		&kafka.MetadataRequest{},
	)
	if err != nil {
		return nil, err
	}

	return &ClusterInformation{
		ThrottleTimeMs: int(metadata.Throttle.Abs()),
		ClusterID:      metadata.ClusterID,
		ControllerID:   metadata.Controller.ID,
		Brokers:        len(metadata.Brokers),
		Topics:         len(metadata.Topics),
	}, nil
}
