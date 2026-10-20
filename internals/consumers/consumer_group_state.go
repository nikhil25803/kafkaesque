package consumers

import "strings"

const (
	GroupStateStable      = "Stable"
	GroupStateRebalancing = "Rebalancing"
	GroupStateEmpty       = "Empty"
	GroupStateDead        = "Dead"
	GroupStateUnknown     = "Unknown"
)

// GroupStateCategory converts Kafka group states into stable display categories.
func GroupStateCategory(state string) string {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "stable":
		return GroupStateStable
	case "preparingrebalance", "completingrebalance", "assigning", "reconciling":
		return GroupStateRebalancing
	case "empty":
		return GroupStateEmpty
	case "dead":
		return GroupStateDead
	default:
		return GroupStateUnknown
	}
}

// IsGroupStateCategory reports whether a filter is a supported state category.
func IsGroupStateCategory(state string) bool {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "stable", "rebalancing", "empty", "dead", "unknown":
		return true
	default:
		return false
	}
}
