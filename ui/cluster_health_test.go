package ui

import (
	"net/http"
	"reflect"
	"strings"
	"testing"
)

func TestClusterHealthPage(t *testing.T) {
	response := getUI(t, "/ui/cluster-health", false)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	body := response.Body.String()
	for _, text := range []string{
		"<!doctype html>", "Cluster Health", "Demo data", "Overall status",
		"Cluster requires attention", "DEGRADED", "Cluster overview",
		">3</strong><span>Brokers", ">24</strong><span>Topics",
		">186</strong><span>Partitions", ">17</strong><span>Consumer Groups",
		"Partition health", "Offline partitions", "Under-replicated partitions",
		"Consumer health", "Rebalancing groups", "Dead groups", "Lagging groups",
		`status-label healthy`, `status-label warning`,
		`href="/ui/cluster-health" aria-current="page"`,
	} {
		if !strings.Contains(body, text) {
			t.Errorf("cluster health page does not contain %q", text)
		}
	}
	if strings.Contains(strings.ToLower(body), "htmx") {
		t.Error("cluster health page unexpectedly loads HTMX")
	}
}

func TestClusterHealthMatchesDashboardFixture(t *testing.T) {
	dashboard := demoDashboard()
	page := demoClusterHealth()
	if page.Status != dashboard.Health.Status || !reflect.DeepEqual(page.Metrics, dashboard.Metrics) {
		t.Fatalf("cluster health overview does not match Dashboard: %#v %#v", page, dashboard)
	}
	wantPartitions := []healthDiagnosticView{
		{Label: "Offline partitions", Value: dashboard.Health.Offline, Status: "HEALTHY", StatusClass: "healthy"},
		{Label: "Under-replicated partitions", Value: dashboard.Health.UnderReplicated, Status: "DEGRADED", StatusClass: "warning"},
	}
	wantConsumers := []healthDiagnosticView{
		{Label: "Rebalancing groups", Value: dashboard.Health.Rebalancing, Status: "DEGRADED", StatusClass: "warning"},
		{Label: "Dead groups", Value: dashboard.Health.Dead, Status: "HEALTHY", StatusClass: "healthy"},
		{Label: "Lagging groups", Value: dashboard.Health.Lagging, Status: "DEGRADED", StatusClass: "warning"},
	}
	if !reflect.DeepEqual(page.PartitionDiagnostics, wantPartitions) || !reflect.DeepEqual(page.ConsumerDiagnostics, wantConsumers) {
		t.Fatalf("cluster health diagnostics = %#v %#v", page.PartitionDiagnostics, page.ConsumerDiagnostics)
	}
}

func TestDashboardLinksToClusterHealth(t *testing.T) {
	body := getUI(t, "/ui", false).Body.String()
	if !strings.Contains(body, `href="/ui/cluster-health">View details</a>`) {
		t.Fatal("Dashboard does not link to Cluster Health")
	}
}
