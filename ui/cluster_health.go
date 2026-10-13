package ui

import (
	"net/http"

	kafkaesque_health "github.com/nikhil25803/kafkaesque/internals/health"
)

type clusterHealthPage struct {
	Navigation           navigationView
	Status               string
	StatusClass          string
	Headline             string
	Message              string
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

func renderClusterHealth(w http.ResponseWriter, r *http.Request) {
	health, err := sourceFrom(r).Health(r.Context())
	if err != nil {
		renderUpstreamError(w, r, "cluster-health", err)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := dashboardTemplate.ExecuteTemplate(w, "cluster-health", clusterHealthView(health, r)); err != nil {
		http.Error(w, "Unable to render cluster health", http.StatusInternalServerError)
	}
}

func clusterHealthView(health *kafkaesque_health.Information, requests ...*http.Request) clusterHealthPage {
	statusClass := "healthy"
	headline := "Cluster is healthy"
	message := "All monitored cluster health signals are healthy."
	if health.Status != kafkaesque_health.StatusHealthy {
		statusClass = "warning"
		headline = "Cluster requires attention"
		message = "The cluster is available, but one or more health signals require attention."
	}
	navigation := navigationView{Active: "cluster-health"}
	if len(requests) > 0 {
		navigation = navigationFor(requests[0], "cluster-health")
	}
	return clusterHealthPage{
		Navigation:  navigation,
		Status:      health.Status,
		StatusClass: statusClass,
		Headline:    headline,
		Message:     message,
		Metrics:     []metricView{{Label: "Brokers", Value: int64(health.BrokerCount)}, {Label: "Topics", Value: int64(health.TopicCount)}, {Label: "Partitions", Value: int64(health.PartitionCount)}, {Label: "Consumer Groups", Value: int64(health.ConsumerGroupCount)}},
		PartitionDiagnostics: []healthDiagnosticView{
			diagnostic("Offline partitions", health.OfflinePartitionCount, true),
			diagnostic("Under-replicated partitions", health.UnderReplicatedCount, false),
		},
		ConsumerDiagnostics: []healthDiagnosticView{
			diagnostic("Rebalancing groups", health.RebalancingGroupCount, false),
			diagnostic("Dead groups", health.DeadGroupCount, true),
			diagnostic("Lagging groups", health.LaggingGroupCount, false),
		},
	}
}

func diagnostic(label string, value int, critical bool) healthDiagnosticView {
	status, class := "HEALTHY", "healthy"
	if value > 0 {
		status, class = "DEGRADED", "warning"
		if critical {
			status, class = "UNHEALTHY", "critical"
		}
	}
	return healthDiagnosticView{Label: label, Value: value, Status: status, StatusClass: class}
}
