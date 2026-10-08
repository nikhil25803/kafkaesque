package brokers

import (
	"net"
	"strconv"

	kafka "github.com/segmentio/kafka-go"
)

// BrokerInformation contains the broker data exposed by the CLI.
type BrokerInformation struct {
	ID      int
	Address string
	Rack    string
}

// GetBrokerInformation returns brokers from cluster metadata.
func GetBrokerInformation(metadata *kafka.MetadataResponse) []BrokerInformation {
	brokers := make([]BrokerInformation, len(metadata.Brokers))
	for i, broker := range metadata.Brokers {
		brokers[i] = BrokerInformation{
			ID:      broker.ID,
			Address: net.JoinHostPort(broker.Host, strconv.Itoa(broker.Port)),
			Rack:    broker.Rack,
		}
	}
	return brokers
}
