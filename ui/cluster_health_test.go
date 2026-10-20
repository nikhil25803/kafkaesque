package ui

import (
	"testing"

	kafkaesque_health "github.com/nikhil25803/kafkaesque/internals/health"
)

func TestClusterHealthMessage(t *testing.T) {
	tests := []struct {
		name   string
		health kafkaesque_health.Information
		want   string
	}{
		{
			name:   "healthy",
			health: kafkaesque_health.Information{Status: kafkaesque_health.StatusHealthy},
			want:   "All monitored cluster health signals are healthy.",
		},
		{
			name: "one singular reason",
			health: kafkaesque_health.Information{
				Status:                kafkaesque_health.StatusDegraded,
				OfflinePartitionCount: 1,
			},
			want: "Detected 1 offline partition. Review the diagnostics below.",
		},
		{
			name: "two reasons",
			health: kafkaesque_health.Information{
				Status:            kafkaesque_health.StatusDegraded,
				DeadGroupCount:    1,
				LaggingGroupCount: 2,
			},
			want: "Detected 1 dead consumer group and 2 lagging consumer groups. Review the diagnostics below.",
		},
		{
			name: "multiple reasons",
			health: kafkaesque_health.Information{
				Status:                kafkaesque_health.StatusDegraded,
				UnderReplicatedCount:  3,
				RebalancingGroupCount: 1,
				LaggingGroupCount:     2,
			},
			want: "Detected 3 under-replicated partitions, 1 rebalancing consumer group, and 2 lagging consumer groups. Review the diagnostics below.",
		},
		{
			name: "all reasons in display order",
			health: kafkaesque_health.Information{
				Status:                kafkaesque_health.StatusDegraded,
				OfflinePartitionCount: 1,
				UnderReplicatedCount:  2,
				RebalancingGroupCount: 3,
				DeadGroupCount:        4,
				LaggingGroupCount:     5,
			},
			want: "Detected 1 offline partition, 2 under-replicated partitions, 3 rebalancing consumer groups, 4 dead consumer groups, and 5 lagging consumer groups. Review the diagnostics below.",
		},
		{
			name:   "degraded fallback",
			health: kafkaesque_health.Information{Status: kafkaesque_health.StatusDegraded},
			want:   "The cluster is available, but one or more health signals require attention. Review the diagnostics below.",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := clusterHealthMessage(&test.health); got != test.want {
				t.Fatalf("message = %q, want %q", got, test.want)
			}
		})
	}
}
