package ui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDashboard(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/ui", nil)
	response := httptest.NewRecorder()
	NewHandler().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	if got := response.Header().Get("Content-Type"); got != "text/html; charset=utf-8" {
		t.Fatalf("Content-Type = %q", got)
	}
	if got := response.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q", got)
	}
	for _, text := range []string{
		"<main", "<nav", `aria-current="page"`,
		"Dashboard", "Demo data", "CONNECTED", "42ms", "SASL_SSL", "SCRAM-SHA-256",
		">3</strong>", ">24</strong>", ">186</strong>", ">17</strong>",
		"orders-service", "analytics", "notification", "payments",
		"DEGRADED", "Offline partitions", "Under-replicated", "Rebalancing groups", "Dead groups", "Lagging groups",
		"4 of 17", `href="/ui/consumer-groups">View all`,
	} {
		if !strings.Contains(response.Body.String(), text) {
			t.Errorf("dashboard does not contain %q", text)
		}
	}
	if strings.Contains(strings.ToLower(response.Body.String()), "htmx") {
		t.Error("phase-one dashboard unexpectedly loads HTMX")
	}
}

func TestDashboardSlashRedirects(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/ui/", nil)
	response := httptest.NewRecorder()
	NewHandler().ServeHTTP(response, request)

	if response.Code != http.StatusPermanentRedirect {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusPermanentRedirect)
	}
	if got := response.Header().Get("Location"); got != "/ui" {
		t.Fatalf("Location = %q, want /ui", got)
	}
}

func TestDashboardAssets(t *testing.T) {
	tests := []struct {
		path        string
		contentType string
		content     string
	}{
		{path: "/ui/assets/app.css", contentType: "text/css", content: "--canvas: #050505"},
		{path: "/ui/assets/app.js", contentType: "text/javascript", content: "event.key === 'Escape'"},
		{path: "/ui/assets/htmx-4.0.0.min.js", contentType: "text/javascript", content: "var htmx="},
		{path: "/ui/assets/htmx-4.0.0-LICENSE.txt", contentType: "text/plain", content: "Zero-Clause BSD"},
	}
	for _, test := range tests {
		t.Run(test.path, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, test.path, nil)
			response := httptest.NewRecorder()
			NewHandler().ServeHTTP(response, request)
			if response.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
			}
			if got := response.Header().Get("Content-Type"); !strings.HasPrefix(got, test.contentType) {
				t.Fatalf("Content-Type = %q, want prefix %q", got, test.contentType)
			}
			if !strings.Contains(response.Body.String(), test.content) {
				t.Fatalf("asset does not contain %q", test.content)
			}
		})
	}
}

func TestConsumerGroupsPage(t *testing.T) {
	response := getUI(t, "/ui/consumer-groups", false)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	body := response.Body.String()
	for _, text := range []string{
		"<!doctype html>", "Consumer Groups", "Demo data", "Apply filters", "All states",
		"1–10 of 17 groups", "analytics", "orders-service", "Healthy", "Warning", "Critical",
		`action="/ui/consumer-groups" method="get"`, `src="/ui/assets/htmx-4.0.0.min.js"`,
		`href="/ui/consumer-groups" aria-current="page"`,
	} {
		if !strings.Contains(body, text) {
			t.Errorf("consumer groups page does not contain %q", text)
		}
	}
	if strings.Contains(body, "payments") {
		t.Error("first page unexpectedly contains a page-two group")
	}
	if strings.Index(body, "analytics") > strings.Index(body, "audit-log") {
		t.Error("consumer groups are not sorted by name")
	}
}

func TestConsumerGroupsPagination(t *testing.T) {
	response := getUI(t, "/ui/consumer-groups?page=2", false)
	body := response.Body.String()
	for _, text := range []string{"11–17 of 17 groups", "payments", "warehouse-sync", `page=1`, `aria-current="page">2`} {
		if !strings.Contains(body, text) {
			t.Errorf("page two does not contain %q", text)
		}
	}
	if strings.Contains(body, "analytics") {
		t.Error("page two unexpectedly contains a page-one group")
	}

	response = getUI(t, "/ui/consumer-groups?page=9", false)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "0 of 17 groups") {
		t.Fatalf("out-of-range page = %d %q", response.Code, response.Body.String())
	}
}

func TestConsumerGroupsFilters(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want string
	}{
		{name: "case-insensitive search", url: "/ui/consumer-groups?q=SERVICE", want: "2 groups"},
		{name: "stable", url: "/ui/consumer-groups?state=stable", want: "billing-events"},
		{name: "rebalancing", url: "/ui/consumer-groups?state=Rebalancing", want: "recommendations"},
		{name: "empty", url: "/ui/consumer-groups?state=Empty", want: "email-dispatch"},
		{name: "dead", url: "/ui/consumer-groups?state=Dead", want: "search-indexer"},
		{name: "combined", url: "/ui/consumer-groups?q=search&state=dead", want: "1–1 of 1 groups"},
		{name: "no matches", url: "/ui/consumer-groups?q=missing", want: "No consumer groups found"},
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

func TestConsumerGroupsRejectsInvalidPage(t *testing.T) {
	for _, path := range []string{"/ui/consumer-groups?page=0", "/ui/consumer-groups?page=nope"} {
		response := getUI(t, path, false)
		if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "page must be a positive integer") {
			t.Fatalf("%s response = %d %q", path, response.Code, response.Body.String())
		}
	}
}

func TestConsumerGroupsHTMXFragment(t *testing.T) {
	response := getUI(t, "/ui/consumer-groups?q=service&state=Stable", true)
	body := response.Body.String()
	if response.Code != http.StatusOK || !strings.Contains(body, `id="consumer-groups-results"`) {
		t.Fatalf("fragment response = %d %q", response.Code, body)
	}
	for _, absent := range []string{"<!doctype html>", "Primary navigation", "Apply filters"} {
		if strings.Contains(body, absent) {
			t.Errorf("fragment unexpectedly contains %q", absent)
		}
	}
	if !strings.Contains(body, "orders-service") || !strings.Contains(body, "shipping-service") {
		t.Error("fragment does not contain filtered groups")
	}
}

func TestDemoLagClassificationBoundaries(t *testing.T) {
	tests := []struct {
		lag       int
		wantLabel string
	}{
		{lag: 100, wantLabel: "Healthy"},
		{lag: 101, wantLabel: "Warning"},
		{lag: 500, wantLabel: "Warning"},
		{lag: 501, wantLabel: "Critical"},
	}
	for _, test := range tests {
		if got := demoConsumerGroup("group", 1, 1, "Stable", test.lag).Health; got != test.wantLabel {
			t.Errorf("lag %d classified as %q, want %q", test.lag, got, test.wantLabel)
		}
	}
}

func TestConsumerGroupsURLPreservesFilters(t *testing.T) {
	got := consumerGroupsURL("order service", "Stable", 2)
	want := "/ui/consumer-groups?page=2&q=order+service&state=Stable"
	if got != want {
		t.Fatalf("URL = %q, want %q", got, want)
	}
}

func getUI(t *testing.T, path string, htmx bool) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, path, nil)
	if htmx {
		request.Header.Set("HX-Request", "true")
	}
	response := httptest.NewRecorder()
	NewHandler().ServeHTTP(response, request)
	return response
}
