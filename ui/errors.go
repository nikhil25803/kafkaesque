package ui

import (
	"net/http"
	"strings"

	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
)

func renderUpstreamError(w http.ResponseWriter, r *http.Request, active string, err error) {
	status, title, message := http.StatusBadGateway, "Kafka unavailable", "Kafka could not complete the request."
	if code, _ := kafkaesque.ClassifyConnectionError(err); code == 3 {
		status = http.StatusGatewayTimeout
		title = "Kafka request timed out"
		message = "Kafka did not respond before the configured timeout."
	}

	renderUIError(w, r, status, active, title, message)
}

func renderUIError(w http.ResponseWriter, r *http.Request, status int, active, title, message string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	templateName := "ui-error"
	if strings.EqualFold(r.Header.Get("HX-Request"), "true") {
		templateName = "ui-error-fragment"
	}

	_ = dashboardTemplate.ExecuteTemplate(w, templateName, struct {
		Navigation     navigationView
		Status         int
		Title, Message string
	}{
		Navigation: navigationFor(r, active),
		Status:     status,
		Title:      title,
		Message:    message,
	})
}
