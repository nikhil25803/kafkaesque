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
		renderUIError(w, r, http.StatusBadRequest, "brokers", "Invalid brokers request", err.Error())
		return
	}
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	information, err := sourceFrom(r).Brokers(r.Context())
	if err != nil {
		renderUpstreamError(w, r, "brokers", err)
		return
	}
	brokers := make([]brokerView, 0, len(information))
	for _, broker := range information {
		brokers = append(brokers, brokerView{ID: broker.ID, Address: broker.Address, ConnectAddress: broker.ConnectAddress, Rack: broker.Rack})
	}
	results := paginateBrokers(filterBrokers(brokers, query), query, page)

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
