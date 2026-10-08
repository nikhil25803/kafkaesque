package metadata

import kafka "github.com/segmentio/kafka-go"

// MetadataInformation contains the cluster metadata exposed by the CLI.
type MetadataInformation struct {
	ClusterID    string
	ControllerID int
	BrokerCount  int
	TopicCount   int
}

// GetMetadataInformation converts Kafka metadata into CLI metadata information.
func GetMetadataInformation(metadata *kafka.MetadataResponse) *MetadataInformation {
	return &MetadataInformation{
		ClusterID:    metadata.ClusterID,
		ControllerID: metadata.Controller.ID,
		BrokerCount:  len(metadata.Brokers),
		TopicCount:   len(metadata.Topics),
	}
}
