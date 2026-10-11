package ui

import (
	"net/http"
	"strings"
	"testing"
)

func TestBrokersPage(t *testing.T) {
	response := getUI(t, "/ui/brokers", false)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	body := response.Body.String()
	for _, text := range []string{
		"<!doctype html>", "Brokers", "Demo data", "Apply filters", "1–3 of 3 brokers",
		"Broker ID", "Advertised address", "Connect address", "Rack",
		"broker-1.kafka.local:9092", "10.0.0.11:9092", "rack-a",
		"broker-2.kafka.local:9092", "10.0.0.12:9092", "rack-b",
		"broker-3.kafka.local:9092", `aria-label="No rack configured">—`,
		`action="/ui/brokers" method="get"`, `href="/ui/brokers" aria-current="page"`,
	} {
		if !strings.Contains(body, text) {
			t.Errorf("brokers page does not contain %q", text)
		}
	}
	if got := strings.Count(body, `class="broker-row"`); got != 3 {
		t.Errorf("broker rows = %d, want 3", got)
	}
	if !(strings.Index(body, "broker-1.kafka.local") < strings.Index(body, "broker-2.kafka.local") &&
		strings.Index(body, "broker-2.kafka.local") < strings.Index(body, "broker-3.kafka.local")) {
		t.Error("brokers are not sorted by ID")
	}
}

func TestBrokersFilters(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want string
	}{
		{name: "advertised address", url: "/ui/brokers?q=BROKER-1", want: "broker-1.kafka.local:9092"},
		{name: "connect address", url: "/ui/brokers?q=10.0.0.12", want: "broker-2.kafka.local:9092"},
		{name: "rack", url: "/ui/brokers?q=RACK-B", want: "broker-2.kafka.local:9092"},
		{name: "no matches", url: "/ui/brokers?q=missing", want: "No brokers found"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := getUI(t, test.url, false).Body.String()
			if !strings.Contains(body, test.want) || !strings.Contains(body, "Clear filters") {
				t.Fatalf("response does not contain %q and Clear filters", test.want)
			}
		})
	}

	filtered := filterBrokers([]brokerView{{ID: 42, Address: "alpha", ConnectAddress: "beta"}}, "42")
	if len(filtered) != 1 || filtered[0].ID != 42 {
		t.Fatalf("broker ID search = %#v", filtered)
	}
}

func TestBrokersPaginationAndValidation(t *testing.T) {
	response := getUI(t, "/ui/brokers?page=2", false)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "0 of 3 brokers") {
		t.Fatalf("out-of-range response = %d %q", response.Code, response.Body.String())
	}
	for _, path := range []string{"/ui/brokers?page=0", "/ui/brokers?page=nope"} {
		response = getUI(t, path, false)
		if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "page must be a positive integer") {
			t.Fatalf("%s response = %d %q", path, response.Code, response.Body.String())
		}
	}
}

func TestBrokersHTMXFragment(t *testing.T) {
	response := getUI(t, "/ui/brokers?q=10.0.0.12", true)
	body := response.Body.String()
	if response.Code != http.StatusOK || !strings.Contains(body, `id="brokers-results"`) || !strings.Contains(body, "broker-2.kafka.local:9092") {
		t.Fatalf("fragment response = %d %q", response.Code, body)
	}
	for _, absent := range []string{"<!doctype html>", "Primary navigation", "Apply filters", "broker-1.kafka.local:9092"} {
		if strings.Contains(body, absent) {
			t.Errorf("fragment unexpectedly contains %q", absent)
		}
	}
}

func TestBrokersURLPreservesSearch(t *testing.T) {
	got := brokersURL("rack a", 2)
	want := "/ui/brokers?page=2&q=rack+a"
	if got != want {
		t.Fatalf("URL = %q, want %q", got, want)
	}
}
