package brokers

import (
	"net"
	"strconv"

	kafka "github.com/segmentio/kafka-go"
)

// BrokerInformation contains the broker data exposed by the CLI.
type BrokerInformation struct {
	ID             int
	Address        string
	ConnectAddress string
	Rack           string
}

// GetBrokerInformation returns brokers from cluster metadata.
func GetBrokerInformation(metadata *kafka.MetadataResponse, resolveAddress func(string) string) []BrokerInformation {
	brokers := make([]BrokerInformation, len(metadata.Brokers))
	for i, broker := range metadata.Brokers {
		address := net.JoinHostPort(broker.Host, strconv.Itoa(broker.Port))
		connectAddress := address
		if resolveAddress != nil {
			connectAddress = resolveAddress(address)
		}
		brokers[i] = BrokerInformation{
			ID:             broker.ID,
			Address:        address,
			ConnectAddress: connectAddress,
			Rack:           broker.Rack,
		}
	}
	return brokers
}
