package ui

import (
	"fmt"
	"net/http"
	"strings"

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
	if health.Status != kafkaesque_health.StatusHealthy {
		statusClass = "warning"
		headline = "Cluster requires attention"
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
		Message:     clusterHealthMessage(health),
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

func clusterHealthMessage(health *kafkaesque_health.Information) string {
	if health.Status == kafkaesque_health.StatusHealthy {
		return "All monitored cluster health signals are healthy."
	}

	reasons := make([]string, 0, 5)
	addReason := func(count int, singular, plural string) {
		if count == 0 {
			return
		}
		label := plural
		if count == 1 {
			label = singular
		}
		reasons = append(reasons, fmt.Sprintf("%d %s", count, label))
	}

	addReason(health.OfflinePartitionCount, "offline partition", "offline partitions")
	addReason(health.UnderReplicatedCount, "under-replicated partition", "under-replicated partitions")
	addReason(health.RebalancingGroupCount, "rebalancing consumer group", "rebalancing consumer groups")
	addReason(health.DeadGroupCount, "dead consumer group", "dead consumer groups")
	addReason(health.LaggingGroupCount, "lagging consumer group", "lagging consumer groups")

	if len(reasons) == 0 {
		return "The cluster is available, but one or more health signals require attention. Review the diagnostics below."
	}

	var summary string
	switch len(reasons) {
	case 1:
		summary = reasons[0]
	case 2:
		summary = strings.Join(reasons, " and ")
	default:
		summary = strings.Join(reasons[:len(reasons)-1], ", ") + ", and " + reasons[len(reasons)-1]
	}
	return "Detected " + summary + ". Review the diagnostics below."
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
