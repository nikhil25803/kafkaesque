package ui

import (
	"net/http"
	"strings"
	"testing"
)

func TestTopicsPage(t *testing.T) {
	response := getUI(t, "/ui/topics", false)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	body := response.Body.String()
	for _, text := range []string{
		"<!doctype html>", "Topics", "Demo data", "Apply filters", "All levels",
		"1–10 of 24 topics", "__cluster_metadata", "billing-events", "External", "Internal",
		`action="/ui/topics" method="get"`, `src="/ui/assets/htmx-4.0.0.min.js"`,
		`href="/ui/topics" aria-current="page"`,
	} {
		if !strings.Contains(body, text) {
			t.Errorf("topics page does not contain %q", text)
		}
	}
	if strings.Contains(body, ">orders<") {
		t.Error("first page unexpectedly contains a page-two topic")
	}
	if strings.Index(body, "__cluster_metadata") > strings.Index(body, "analytics-events") {
		t.Error("topics are not sorted by name")
	}
}

func TestDemoTopicsAreConsistent(t *testing.T) {
	topics := demoTopics()
	internal, partitions := 0, 0
	names := map[string]bool{}
	for _, topic := range topics {
		partitions += topic.Partitions
		if topic.Level == "Internal" {
			internal++
		}
		names[topic.Name] = true
	}
	if len(topics) != 24 || internal != 3 || partitions != 186 {
		t.Fatalf("topics = %d, internal = %d, partitions = %d", len(topics), internal, partitions)
	}
	for _, name := range []string{"orders", "payments", "__consumer_offsets", "__transaction_state", "__cluster_metadata"} {
		if !names[name] {
			t.Errorf("demo topics do not contain %q", name)
		}
	}
}

func TestTopicsPagination(t *testing.T) {
	tests := []struct {
		path       string
		want       []string
		unwanted   string
		wantStatus int
	}{
		{path: "/ui/topics?page=2", want: []string{"11–20 of 24 topics", "inventory-events", "search-events", `aria-current="page">2`}, unwanted: "__cluster_metadata", wantStatus: http.StatusOK},
		{path: "/ui/topics?page=3", want: []string{"21–24 of 24 topics", "warehouse-events", "web-events"}, unwanted: "analytics-events", wantStatus: http.StatusOK},
		{path: "/ui/topics?page=9", want: []string{"0 of 24 topics"}, unwanted: "analytics-events", wantStatus: http.StatusOK},
	}
	for _, test := range tests {
		response := getUI(t, test.path, false)
		body := response.Body.String()
		if response.Code != test.wantStatus {
			t.Fatalf("%s status = %d, want %d", test.path, response.Code, test.wantStatus)
		}
		for _, text := range test.want {
			if !strings.Contains(body, text) {
				t.Errorf("%s does not contain %q", test.path, text)
			}
		}
		if strings.Contains(body, test.unwanted) {
			t.Errorf("%s unexpectedly contains %q", test.path, test.unwanted)
		}
	}
}

func TestTopicsFilters(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want string
	}{
		{name: "case-insensitive search", url: "/ui/topics?q=PAYMENTS", want: "1–2 of 2 topics"},
		{name: "external", url: "/ui/topics?level=external", want: "1–10 of 21 topics"},
		{name: "internal", url: "/ui/topics?level=internal", want: "1–3 of 3 topics"},
		{name: "combined", url: "/ui/topics?q=offsets&level=internal", want: "1–1 of 1 topics"},
		{name: "no matches", url: "/ui/topics?q=missing", want: "No topics found"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := getUI(t, test.url, false).Body.String()
			if !strings.Contains(body, test.want) || !strings.Contains(body, "Clear filters") {
				t.Fatalf("response does not contain %q and Clear filters", test.want)
			}
		})
	}
}

func TestTopicsRejectInvalidQueries(t *testing.T) {
	for _, test := range []struct {
		path string
		want string
	}{
		{path: "/ui/topics?page=0", want: "page must be a positive integer"},
		{path: "/ui/topics?page=nope", want: "page must be a positive integer"},
		{path: "/ui/topics?level=private", want: "level must be internal, external, or omitted"},
	} {
		response := getUI(t, test.path, false)
		if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), test.want) {
			t.Fatalf("%s response = %d %q", test.path, response.Code, response.Body.String())
		}
	}
}

func TestTopicsHTMXFragment(t *testing.T) {
	response := getUI(t, "/ui/topics?q=state&level=internal", true)
	body := response.Body.String()
	if response.Code != http.StatusOK || !strings.Contains(body, `id="topics-results"`) || !strings.Contains(body, "__transaction_state") {
		t.Fatalf("fragment response = %d %q", response.Code, body)
	}
	for _, absent := range []string{"<!doctype html>", "Primary navigation", "Apply filters"} {
		if strings.Contains(body, absent) {
			t.Errorf("fragment unexpectedly contains %q", absent)
		}
	}
}

func TestTopicsURLPreservesFilters(t *testing.T) {
	got := topicsURL("order events", "external", 2)
	want := "/ui/topics?level=external&page=2&q=order+events"
	if got != want {
		t.Fatalf("URL = %q, want %q", got, want)
	}
}
