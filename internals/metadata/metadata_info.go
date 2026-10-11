package metadata

import kafka "github.com/segmentio/kafka-go"

const statusConnected = "CONNECTED"

// MetadataInformation contains the cluster metadata exposed by the CLI.
type MetadataInformation struct {
	ClusterID    string `json:"cluster_id"`
	ControllerID int    `json:"controller_id"`
	BrokerCount  int    `json:"broker_count"`
	TopicCount   int    `json:"topic_count"`
	Status       string `json:"status"`
}

// GetMetadataInformation converts Kafka metadata into CLI metadata information.
func GetMetadataInformation(metadata *kafka.MetadataResponse) *MetadataInformation {
	return &MetadataInformation{
		ClusterID:    metadata.ClusterID,
		ControllerID: metadata.Controller.ID,
		BrokerCount:  len(metadata.Brokers),
		TopicCount:   len(metadata.Topics),
		Status:       statusConnected,
	}
}
