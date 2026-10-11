package kafka

import (
	"context"
	"net"
	"testing"

	kafkago "github.com/segmentio/kafka-go"
	metadataAPI "github.com/segmentio/kafka-go/protocol/metadata"
)

type snapshotTransport struct {
	errorCode int16
	requests  int
}

func (t *snapshotTransport) RoundTrip(context.Context, net.Addr, kafkago.Request) (kafkago.Response, error) {
	t.requests++
	return &metadataAPI.Response{Topics: []metadataAPI.ResponseTopic{{
		Name: "orders", Partitions: []metadataAPI.ResponsePartition{{PartitionIndex: 0, ErrorCode: t.errorCode}},
	}}}, nil
}

func TestGetMetadataSnapshotValidation(t *testing.T) {
	for _, test := range []struct {
		name      string
		errorCode int16
		wantError bool
	}{
		{name: "complete"},
		{name: "degraded leader", errorCode: int16(kafkago.LeaderNotAvailable)},
		{name: "authorization", errorCode: int16(kafkago.TopicAuthorizationFailed), wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			transport := &snapshotTransport{errorCode: test.errorCode}
			conn := &KafkaesqueConn{Client: &kafkago.Client{Addr: kafkago.TCP("broker:9092"), Transport: transport}}
			snapshot, err := conn.GetMetadataSnapshot(context.Background())
			if (err != nil) != test.wantError {
				t.Fatalf("error = %v, wantError %v", err, test.wantError)
			}
			if !test.wantError && snapshot.Metadata == nil {
				t.Fatal("metadata is nil")
			}
			if transport.requests != 1 {
				t.Fatalf("metadata requests = %d, want 1", transport.requests)
			}
		})
	}
}
