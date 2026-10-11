package ui

import (
	"net/http"
	"strings"
	"testing"
)

func TestConsumerGroupLinks(t *testing.T) {
	dashboard := getUI(t, "/ui", false).Body.String()
	if !strings.Contains(dashboard, `href="/ui/consumer-groups/orders-service"`) {
		t.Fatal("Dashboard consumer group is not linked")
	}
	list := getUI(t, "/ui/consumer-groups", false).Body.String() + getUI(t, "/ui/consumer-groups?page=2", false).Body.String()
	for _, group := range demoConsumerGroups() {
		if !strings.Contains(list, `href="/ui/consumer-groups/`+group.Name+`"`) {
			t.Errorf("consumer group %q is not linked", group.Name)
		}
	}
}

func TestEveryConsumerGroupDetail(t *testing.T) {
	for _, group := range demoConsumerGroups() {
		t.Run(group.Name, func(t *testing.T) {
			response := getUI(t, "/ui/consumer-groups/"+group.Name, false)
			if response.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
			}
			detail, found := demoConsumerGroupDetail(group.Name)
			if !found {
				t.Fatal("detail fixture not found")
			}
			summary := summarizeDetail(detail)
			if summary.TopicCount != group.Topics || summary.MemberCount != group.Members || summary.TotalLag != group.TotalLag {
				t.Fatalf("detail totals = topics %d, members %d, lag %d; list totals = %d, %d, %d",
					summary.TopicCount, summary.MemberCount, summary.TotalLag, group.Topics, group.Members, group.TotalLag)
			}
			body := response.Body.String()
			for _, text := range []string{group.Name, "Consumer Groups", "Lag summary", "Partition lag", "Topic summary", "Members and assignments"} {
				if !strings.Contains(body, text) {
					t.Errorf("detail does not contain %q", text)
				}
			}
		})
	}
}

func TestOrdersServiceDetail(t *testing.T) {
	response := getUI(t, "/ui/consumer-groups/orders-service", false)
	body := response.Body.String()
	for _, text := range []string{
		"orders-service", "Demo data", "Stable", "Total Lag", "136", "Maximum Partition Lag", "124",
		"22.7", "Healthy Partitions", "5", "Warning Partitions", "1", "Unhealthy Partitions",
		`option value="orders" selected`, "payments", "consumer-1", "consumer-2", "consumer-3",
		"order-worker", "10.0.0.12", "orders:0; payments:0", "129932", "130056",
	} {
		if !strings.Contains(body, text) {
			t.Errorf("orders-service detail does not contain %q", text)
		}
	}
}

func TestConsumerGroupTopicSelectionAndFragment(t *testing.T) {
	response := getUI(t, "/ui/consumer-groups/orders-service?topic=payments", false)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "Partition lag · payments") {
		t.Fatalf("payments response = %d %q", response.Code, response.Body.String())
	}

	response = getUI(t, "/ui/consumer-groups/orders-service?topic=payments&group_by=member", true)
	body := response.Body.String()
	if response.Code != http.StatusOK || !strings.Contains(body, `id="partition-lag"`) {
		t.Fatalf("fragment response = %d %q", response.Code, body)
	}
	for _, absent := range []string{"<!doctype html>", "Primary navigation", "Lag summary", "Members and assignments"} {
		if strings.Contains(body, absent) {
			t.Errorf("fragment unexpectedly contains %q", absent)
		}
	}
	for _, member := range []string{"consumer-1 · order-worker", "consumer-2 · order-worker", "consumer-3 · order-worker"} {
		if !strings.Contains(body, member) {
			t.Errorf("grouped fragment does not contain %q", member)
		}
	}
}

func TestPartitionLagSorting(t *testing.T) {
	detail, _ := demoConsumerGroupDetail("orders-service")
	tests := []struct {
		name      string
		sort      string
		direction string
		want      []int
	}{
		{name: "partition default", sort: "partition", direction: "asc", want: []int{124, 0, 12}},
		{name: "highest lag", sort: "lag", direction: "desc", want: []int{124, 12, 0}},
		{name: "lowest lag", sort: "lag", direction: "asc", want: []int{0, 12, 124}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			section := buildPartitionSection(detail, "orders", "", test.sort, test.direction)
			for i, want := range test.want {
				if got := section.Groups[0].Partitions[i].Lag; got != want {
					t.Fatalf("lag[%d] = %d, want %d", i, got, want)
				}
			}
		})
	}
	descending := buildPartitionSection(detail, "orders", "", "lag", "desc")
	if !strings.Contains(descending.LagSortURL, "direction=asc") || descending.LagSortIcon != "↓" {
		t.Fatalf("descending sort toggle = %q %q", descending.LagSortURL, descending.LagSortIcon)
	}
}

func TestPartitionGroupingIncludesUnassigned(t *testing.T) {
	detail, _ := demoConsumerGroupDetail("search-indexer")
	section := buildPartitionSection(detail, detail.Topics[0].Name, "member", "partition", "asc")
	if len(section.Groups) != 1 || section.Groups[0].Name != "Unassigned" || len(section.Groups[0].Partitions) != 3 {
		t.Fatalf("unexpected groups: %+v", section.Groups)
	}
}

func TestConsumerGroupDetailErrors(t *testing.T) {
	tests := []struct {
		path   string
		status int
		text   string
	}{
		{path: "/ui/consumer-groups/missing", status: http.StatusNotFound, text: "Consumer group not found"},
		{path: "/ui/consumer-groups/orders-service?topic=missing", status: http.StatusNotFound, text: "Consumer topic not found"},
		{path: "/ui/consumer-groups/orders-service?group_by=broker", status: http.StatusBadRequest, text: "group_by must be member or omitted"},
		{path: "/ui/consumer-groups/orders-service?sort=offset", status: http.StatusBadRequest, text: "sort must be partition or lag"},
		{path: "/ui/consumer-groups/orders-service?direction=sideways", status: http.StatusBadRequest, text: "direction must be asc or desc"},
	}
	for _, test := range tests {
		t.Run(test.path, func(t *testing.T) {
			response := getUI(t, test.path, false)
			if response.Code != test.status || response.Header().Get("Content-Type") != "text/html; charset=utf-8" || !strings.Contains(response.Body.String(), test.text) {
				t.Fatalf("response = %d %q", response.Code, response.Body.String())
			}
		})
	}
}
