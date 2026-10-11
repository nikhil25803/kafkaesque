// Package ui serves Kafkaesque's embedded web interface.
package ui

import (
	"embed"
	"html/template"
	"io/fs"
	"net/http"
)

//go:embed templates/*.html static/*
var assets embed.FS

var dashboardTemplate = template.Must(template.ParseFS(assets, "templates/*.html"))

type dashboardPage struct {
	Connection     connectionView
	Metrics        []metricView
	ConsumerGroups []consumerGroupView
	Health         healthView
}

type connectionView struct {
	Status    string
	Latency   string
	Protocol  string
	Mechanism string
}

type metricView struct {
	Label string
	Value int
}

type consumerGroupView struct {
	Name        string
	Topics      int
	Members     int
	State       string
	TotalLag    int
	Health      string
	HealthClass string
}

type healthView struct {
	Status          string
	Offline         int
	UnderReplicated int
	Rebalancing     int
	Dead            int
	Lagging         int
}

// NewHandler returns the embedded UI handler.
func NewHandler() http.Handler {
	static, err := fs.Sub(assets, "static")
	if err != nil {
		panic(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /ui", renderDashboard)
	mux.HandleFunc("GET /ui/{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui", http.StatusPermanentRedirect)
	})
	mux.Handle("GET /ui/assets/", http.StripPrefix("/ui/assets/", http.FileServerFS(static)))
	return withHeaders(mux)
}

func renderDashboard(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := dashboardTemplate.ExecuteTemplate(w, "dashboard", demoDashboard()); err != nil {
		http.Error(w, "Unable to render Dashboard", http.StatusInternalServerError)
	}
}

func withHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; img-src 'self'; style-src 'self'; script-src 'self'; base-uri 'none'; frame-ancestors 'none'")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		next.ServeHTTP(w, r)
	})
}

func demoDashboard() dashboardPage {
	return dashboardPage{
		Connection: connectionView{
			Status: "CONNECTED", Latency: "42ms", Protocol: "SASL_SSL", Mechanism: "SCRAM-SHA-256",
		},
		Metrics: []metricView{
			{Label: "Brokers", Value: 3},
			{Label: "Topics", Value: 24},
			{Label: "Partitions", Value: 186},
			{Label: "Consumer Groups", Value: 17},
		},
		ConsumerGroups: []consumerGroupView{
			{Name: "orders-service", Topics: 2, Members: 3, State: "Stable", TotalLag: 136, Health: "Warning", HealthClass: "warning"},
			{Name: "analytics", Topics: 5, Members: 2, State: "Stable", TotalLag: 31, Health: "Healthy", HealthClass: "healthy"},
			{Name: "notification", Topics: 1, Members: 1, State: "Rebalancing", TotalLag: 842, Health: "Critical", HealthClass: "critical"},
			{Name: "payments", Topics: 2, Members: 4, State: "Stable", TotalLag: 0, Health: "Healthy", HealthClass: "healthy"},
		},
		Health: healthView{
			Status: "DEGRADED", Offline: 0, UnderReplicated: 3, Rebalancing: 1, Dead: 0, Lagging: 2,
		},
	}
}
