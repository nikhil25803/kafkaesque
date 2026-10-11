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
		"<main", "<nav", `aria-current="page"`, `aria-disabled="true"`,
		"Dashboard", "Demo data", "CONNECTED", "42ms", "SASL_SSL", "SCRAM-SHA-256",
		">3</strong>", ">24</strong>", ">186</strong>", ">17</strong>",
		"orders-service", "analytics", "notification", "payments",
		"DEGRADED", "Offline partitions", "Under-replicated", "Rebalancing groups", "Dead groups", "Lagging groups",
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
