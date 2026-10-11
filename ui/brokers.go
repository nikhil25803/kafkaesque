package ui

import (
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

type brokerView struct {
	ID             int
	Address        string
	ConnectAddress string
	Rack           string
}

type brokersPage struct {
	Navigation navigationView
	Query      string
	Results    brokersResults
}

type brokersResults struct {
	Brokers     []brokerView
	Range       string
	Pages       []pageLink
	PreviousURL string
	NextURL     string
	HasFilters  bool
}

func renderBrokers(w http.ResponseWriter, r *http.Request) {
	page, err := positivePage(r.URL.Query().Get("page"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	results := paginateBrokers(filterBrokers(demoBrokers(), query), query, page)

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if strings.EqualFold(r.Header.Get("HX-Request"), "true") {
		if err := dashboardTemplate.ExecuteTemplate(w, "brokers-results", results); err != nil {
			http.Error(w, "Unable to render brokers", http.StatusInternalServerError)
		}
		return
	}
	view := brokersPage{
		Navigation: navigationView{Active: "brokers"},
		Query:      query,
		Results:    results,
	}
	if err := dashboardTemplate.ExecuteTemplate(w, "brokers", view); err != nil {
		http.Error(w, "Unable to render brokers", http.StatusInternalServerError)
	}
}

func filterBrokers(brokers []brokerView, query string) []brokerView {
	query = strings.ToLower(query)
	filtered := make([]brokerView, 0, len(brokers))
	for _, broker := range brokers {
		if strings.Contains(strconv.Itoa(broker.ID), query) ||
			strings.Contains(strings.ToLower(broker.Address), query) ||
			strings.Contains(strings.ToLower(broker.ConnectAddress), query) ||
			strings.Contains(strings.ToLower(broker.Rack), query) {
			filtered = append(filtered, broker)
		}
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].ID < filtered[j].ID })
	return filtered
}

func paginateBrokers(brokers []brokerView, query string, page int) brokersResults {
	const pageSize = 10
	total := len(brokers)
	totalPages := (total + pageSize - 1) / pageSize
	start := (page - 1) * pageSize
	if start > total {
		start = total
	}
	end := min(start+pageSize, total)
	pages := make([]pageLink, 0, totalPages)
	for number := 1; number <= totalPages; number++ {
		pages = append(pages, pageLink{Number: number, URL: brokersURL(query, number), Active: number == page})
	}
	rangeText := "0 brokers"
	if start < end {
		rangeText = strconv.Itoa(start+1) + "–" + strconv.Itoa(end) + " of " + strconv.Itoa(total) + " brokers"
	} else if total > 0 {
		rangeText = "0 of " + strconv.Itoa(total) + " brokers"
	}
	results := brokersResults{
		Brokers: brokers[start:end], Range: rangeText, Pages: pages, HasFilters: query != "",
	}
	if page > 1 && page <= totalPages {
		results.PreviousURL = brokersURL(query, page-1)
	}
	if page < totalPages {
		results.NextURL = brokersURL(query, page+1)
	}
	return results
}

func brokersURL(query string, page int) string {
	values := url.Values{}
	if query != "" {
		values.Set("q", query)
	}
	values.Set("page", strconv.Itoa(page))
	return "/ui/brokers?" + values.Encode()
}

func demoBrokers() []brokerView {
	return []brokerView{
		{ID: 1, Address: "broker-1.kafka.local:9092", ConnectAddress: "10.0.0.11:9092", Rack: "rack-a"},
		{ID: 2, Address: "broker-2.kafka.local:9092", ConnectAddress: "10.0.0.12:9092", Rack: "rack-b"},
		{ID: 3, Address: "broker-3.kafka.local:9092", ConnectAddress: "broker-3.kafka.local:9092"},
	}
}
