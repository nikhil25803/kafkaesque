package metadata

import kafka "github.com/segmentio/kafka-go"

const statusConnected = "CONNECTED"

// MetadataInformation contains the cluster metadata exposed by the CLI.
type MetadataInformation struct {
	ClusterID    string
	ControllerID int
	BrokerCount  int
	TopicCount   int
	Status       string
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
