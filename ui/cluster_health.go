package ui

import "net/http"

type clusterHealthPage struct {
	Navigation           navigationView
	Status               string
	StatusClass          string
	Metrics              []metricView
	PartitionDiagnostics []healthDiagnosticView
	ConsumerDiagnostics  []healthDiagnosticView
}

type healthDiagnosticView struct {
	Label       string
	Value       int
	Status      string
	StatusClass string
}

func renderClusterHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := dashboardTemplate.ExecuteTemplate(w, "cluster-health", demoClusterHealth()); err != nil {
		http.Error(w, "Unable to render cluster health", http.StatusInternalServerError)
	}
}

func demoClusterHealth() clusterHealthPage {
	dashboard := demoDashboard()
	health := dashboard.Health
	return clusterHealthPage{
		Navigation:  navigationView{Active: "cluster-health"},
		Status:      health.Status,
		StatusClass: "warning",
		Metrics:     dashboard.Metrics,
		PartitionDiagnostics: []healthDiagnosticView{
			diagnostic("Offline partitions", health.Offline),
			diagnostic("Under-replicated partitions", health.UnderReplicated),
		},
		ConsumerDiagnostics: []healthDiagnosticView{
			diagnostic("Rebalancing groups", health.Rebalancing),
			diagnostic("Dead groups", health.Dead),
			diagnostic("Lagging groups", health.Lagging),
		},
	}
}

func diagnostic(label string, value int) healthDiagnosticView {
	status, class := "HEALTHY", "healthy"
	if value > 0 {
		status, class = "DEGRADED", "warning"
	}
	return healthDiagnosticView{Label: label, Value: value, Status: status, StatusClass: class}
}
