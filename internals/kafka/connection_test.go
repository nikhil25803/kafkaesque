package kafka

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
)

func TestResolveBrokerAddress(t *testing.T) {
	conn := &KafkaesqueConn{brokerAddressOverrides: map[string]string{
		"broker.internal:9092": "10.100.0.72:9094",
		"[2001:db8::1]:9092":   "[2001:db8::2]:9094",
	}}

	tests := []struct {
		advertised string
		want       string
	}{
		{advertised: "broker.internal:9092", want: "10.100.0.72:9094"},
		{advertised: "[2001:db8::1]:9092", want: "[2001:db8::2]:9094"},
		{advertised: "other.internal:9092", want: "other.internal:9092"},
	}
	for _, test := range tests {
		if got := conn.ResolveBrokerAddress(test.advertised); got != test.want {
			t.Fatalf("ResolveBrokerAddress(%q) = %q, want %q", test.advertised, got, test.want)
		}
	}
}

func TestBrokerDialerUsesAddressOverride(t *testing.T) {
	var dialedAddress string
	dial := brokerDialerWithDial(
		map[string]string{"broker.internal:9092": "10.100.0.72:9094"},
		func(_ context.Context, _, address string) (net.Conn, error) {
			dialedAddress = address
			return nil, nil
		},
	)
	_, err := dial(context.Background(), "tcp", "broker.internal:9092")
	if err != nil {
		t.Fatal(err)
	}
	if dialedAddress != "10.100.0.72:9094" {
		t.Fatalf("dialed address = %q, want 10.100.0.72:9094", dialedAddress)
	}

	_, err = dial(context.Background(), "tcp", "other.internal:9092")
	if err != nil {
		t.Fatal(err)
	}
	if dialedAddress != "other.internal:9092" {
		t.Fatalf("unmapped dialed address = %q, want other.internal:9092", dialedAddress)
	}
}

func TestBrokerDialerErrorIncludesAdvertisedAndConnectAddresses(t *testing.T) {
	dial := brokerDialerWithDial(
		map[string]string{"broker.internal:9092": "127.0.0.1:0"},
		func(context.Context, string, string) (net.Conn, error) {
			return nil, errors.New("network failed")
		},
	)
	_, err := dial(context.Background(), "tcp", "broker.internal:9092")
	if err == nil {
		t.Fatal("expected dial error")
	}
	for _, address := range []string{"broker.internal:9092", "127.0.0.1:0"} {
		if !strings.Contains(err.Error(), address) {
			t.Fatalf("error %q does not contain %q", err, address)
		}
	}
}
