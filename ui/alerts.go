package ui

import (
	"errors"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"

	kafkaesque_alerts "github.com/nikhil25803/kafkaesque/internals/alerts"
	kafkaesque_auth "github.com/nikhil25803/kafkaesque/internals/auth"
)

type alertsPage struct {
	Navigation navigationView
	Tabs       []settingsTab
	Alerts     []alertView
	Success    string
	CanCreate  bool
	CanEdit    bool
	CanDelete  bool
}

type alertView struct {
	ID              string
	ConsumerGroup   string
	Topic           string
	Metric          string
	Threshold       int64
	NotifyOnResolve bool
	State           string
	StateClass      string
	LastValue       string
	LastNotified    string
}

type alertFormPage struct {
	Navigation navigationView
	Tabs       []settingsTab
	Form       alertFormView
	Groups     []string
	Topics     []string
}

type alertFormView struct {
	ID                string
	ConsumerGroup     string
	Topic             string
	Metric            string
	LagThreshold      string
	NotifyOnResolve   bool
	WebhookURL        string
	WebhookConfigured bool
	Editing           bool
	Error             string
}

func registerAlertRoutes(
	mux *http.ServeMux,
	protect func(http.HandlerFunc) http.HandlerFunc,
	manager kafkaesque_alerts.Manager,
	logger *slog.Logger,
) {
	mux.HandleFunc("GET /ui/settings/alerts", protect(authorizeUI(kafkaesque_auth.PermissionAlertView, renderAlerts(manager, logger))))
	mux.HandleFunc("GET /ui/settings/alerts/new", protect(authorizeUI(kafkaesque_auth.PermissionAlertCreate, renderNewAlert(manager))))
	mux.HandleFunc("POST /ui/settings/alerts", protect(authorizeUI(kafkaesque_auth.PermissionAlertCreate, createAlertUI(manager, logger))))
	mux.HandleFunc("GET /ui/settings/alerts/{id}/edit", protect(authorizeUI(kafkaesque_auth.PermissionAlertEdit, renderEditAlert(manager))))
	mux.HandleFunc("POST /ui/settings/alerts/{id}", protect(authorizeUI(kafkaesque_auth.PermissionAlertEdit, updateAlertUI(manager, logger))))
	mux.HandleFunc("POST /ui/settings/alerts/{id}/delete", protect(authorizeUI(kafkaesque_auth.PermissionAlertDelete, deleteAlertUI(manager, logger))))
}

func renderAlerts(manager kafkaesque_alerts.Manager, logger *slog.Logger) http.HandlerFunc {
	logger = uiLogger(logger)
	return func(w http.ResponseWriter, r *http.Request) {
		actor, _ := kafkaesque_auth.UserFromContext(r.Context())
		subscriptions, err := manager.List(r.Context(), actor)
		if err != nil {
			logger.Error("alert_list_failed", "reason", "database_unavailable")
			renderAlertError(w, r, err)
			return
		}
		views := make([]alertView, 0, len(subscriptions))
		for _, subscription := range subscriptions {
			views = append(views, newAlertView(subscription))
		}
		view := alertsPage{
			Navigation: navigationFor(r, "settings"), Tabs: settingsTabs(r, "alerts"), Alerts: views,
			CanCreate: kafkaesque_auth.HasPermission(actor, kafkaesque_auth.PermissionAlertCreate),
			CanEdit:   kafkaesque_auth.HasPermission(actor, kafkaesque_auth.PermissionAlertEdit),
			CanDelete: kafkaesque_auth.HasPermission(actor, kafkaesque_auth.PermissionAlertDelete),
		}
		switch r.URL.Query().Get("saved") {
		case "created":
			view.Success = "Slack alert created successfully."
		case "updated":
			view.Success = "Slack alert updated successfully."
		case "deleted":
			view.Success = "Slack alert deleted successfully."
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_ = dashboardTemplate.ExecuteTemplate(w, "alerts", view)
	}
}

func renderNewAlert(manager kafkaesque_alerts.Manager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		renderAlertForm(w, r, manager, http.StatusOK, alertFormView{
			Metric: kafkaesque_alerts.MetricAverage, NotifyOnResolve: true,
		})
	}
}

func renderEditAlert(manager kafkaesque_alerts.Manager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		actor, _ := kafkaesque_auth.UserFromContext(r.Context())
		subscription, err := manager.Get(r.Context(), actor, r.PathValue("id"))
		if err != nil {
			renderAlertError(w, r, err)
			return
		}
		renderAlertForm(w, r, manager, http.StatusOK, alertFormView{
			ID: subscription.ID, ConsumerGroup: subscription.ConsumerGroup, Topic: subscription.Topic,
			Metric: subscription.Metric, LagThreshold: strconv.FormatInt(subscription.LagThreshold, 10),
			NotifyOnResolve:   subscription.NotifyOnResolve,
			WebhookConfigured: subscription.WebhookConfigured, Editing: true,
		})
	}
}

func createAlertUI(manager kafkaesque_alerts.Manager, logger *slog.Logger) http.HandlerFunc {
	return saveAlertUI(manager, logger, false)
}

func updateAlertUI(manager kafkaesque_alerts.Manager, logger *slog.Logger) http.HandlerFunc {
	return saveAlertUI(manager, logger, true)
}

func saveAlertUI(manager kafkaesque_alerts.Manager, logger *slog.Logger, editing bool) http.HandlerFunc {
	logger = uiLogger(logger)
	return func(w http.ResponseWriter, r *http.Request) {
		form, input, err := parseAlertForm(r, editing)
		if err != nil {
			form.Error = err.Error()
			renderAlertForm(w, r, manager, http.StatusBadRequest, form)
			return
		}
		actor, _ := kafkaesque_auth.UserFromContext(r.Context())
		if editing {
			_, err = manager.Update(r.Context(), actor, r.PathValue("id"), input)
		} else {
			_, err = manager.Create(r.Context(), actor, input)
		}
		if err != nil {
			form.Error = alertErrorMessage(err)
			status := http.StatusBadRequest
			if !isAlertInputError(err) {
				status = http.StatusServiceUnavailable
			}
			renderAlertForm(w, r, manager, status, form)
			return
		}
		logger.Info("alert_subscription_saved", "actor_user_id", actor.ID)
		result := "created"
		if editing {
			result = "updated"
		}
		http.Redirect(w, r, "/ui/settings/alerts?saved="+result, http.StatusSeeOther)
	}
}

func deleteAlertUI(manager kafkaesque_alerts.Manager, logger *slog.Logger) http.HandlerFunc {
	logger = uiLogger(logger)
	return func(w http.ResponseWriter, r *http.Request) {
		actor, _ := kafkaesque_auth.UserFromContext(r.Context())
		if err := manager.Delete(r.Context(), actor, r.PathValue("id")); err != nil {
			renderAlertError(w, r, err)
			return
		}
		logger.Info("alert_subscription_deleted", "actor_user_id", actor.ID, "alert_id", r.PathValue("id"))
		http.Redirect(w, r, "/ui/settings/alerts?saved=deleted", http.StatusSeeOther)
	}
}

func parseAlertForm(r *http.Request, editing bool) (alertFormView, kafkaesque_alerts.Input, error) {
	if err := r.ParseForm(); err != nil {
		return alertFormView{Editing: editing}, kafkaesque_alerts.Input{}, errors.New("Invalid alert request.")
	}
	form := alertFormView{
		ID: r.PathValue("id"), ConsumerGroup: strings.TrimSpace(r.FormValue("consumer_group")),
		Topic: strings.TrimSpace(r.FormValue("topic")), Metric: r.FormValue("metric"),
		LagThreshold:    strings.TrimSpace(r.FormValue("lag_threshold")),
		NotifyOnResolve: r.FormValue("notify_on_resolve") == "true", WebhookURL: strings.TrimSpace(r.FormValue("webhook_url")),
		WebhookConfigured: editing, Editing: editing,
	}
	threshold, err := strconv.ParseInt(form.LagThreshold, 10, 64)
	if err != nil || threshold < 0 {
		return form, kafkaesque_alerts.Input{}, errors.New("Lag threshold must be a non-negative whole number.")
	}
	return form, kafkaesque_alerts.Input{
		ConsumerGroup: form.ConsumerGroup, Topic: form.Topic, Metric: form.Metric,
		LagThreshold: threshold, NotifyOnResolve: form.NotifyOnResolve,
		WebhookURL: form.WebhookURL,
	}, nil
}

func renderAlertForm(w http.ResponseWriter, r *http.Request, manager kafkaesque_alerts.Manager, status int, form alertFormView) {
	groups, err := sourceFrom(r).ConsumerGroups(r.Context())
	if err != nil {
		renderUpstreamError(w, r, "settings", err)
		return
	}
	groupNames := make([]string, 0, len(groups))
	topicSet := make(map[string]struct{})
	for _, group := range groups {
		groupNames = append(groupNames, group.GroupName)
		for _, topic := range group.Topics {
			topicSet[topic.Topic] = struct{}{}
		}
	}
	sort.Strings(groupNames)
	topics := make([]string, 0, len(topicSet))
	for topic := range topicSet {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	view := alertFormPage{
		Navigation: navigationFor(r, "settings"), Tabs: settingsTabs(r, "alerts"),
		Form: form, Groups: groupNames, Topics: topics,
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_ = dashboardTemplate.ExecuteTemplate(w, "alert-form", view)
}

func newAlertView(subscription kafkaesque_alerts.Subscription) alertView {
	metric := "Average lag"
	if subscription.Metric == kafkaesque_alerts.MetricPartition {
		metric = "Partition-wise lag"
	}
	lastValue, lastNotified := "—", "Never"
	if subscription.LastValue != nil {
		lastValue = strconv.FormatFloat(*subscription.LastValue, 'f', 2, 64)
	}
	if subscription.LastNotifiedAt != nil {
		lastNotified = subscription.LastNotifiedAt.UTC().Format("02 Jan 2006, 15:04 UTC")
	}
	stateClass := "healthy"
	if subscription.State != kafkaesque_alerts.StateHealthy {
		stateClass = "critical"
	}
	return alertView{
		ID: subscription.ID, ConsumerGroup: subscription.ConsumerGroup, Topic: subscription.Topic,
		Metric: metric, Threshold: subscription.LagThreshold,
		NotifyOnResolve: subscription.NotifyOnResolve, State: strings.ToUpper(subscription.State),
		StateClass: stateClass, LastValue: lastValue, LastNotified: lastNotified,
	}
}

func renderAlertError(w http.ResponseWriter, r *http.Request, err error) {
	status := http.StatusServiceUnavailable
	title, message := "Alerts unavailable", "The alert database is unavailable."
	switch {
	case errors.Is(err, kafkaesque_alerts.ErrForbidden):
		status, title, message = http.StatusForbidden, "Permission denied", "You do not have permission to manage this alert."
	case errors.Is(err, kafkaesque_alerts.ErrNotFound):
		status, title, message = http.StatusNotFound, "Alert not found", "The requested alert subscription does not exist."
	}
	renderUIError(w, r, status, "settings", title, message)
}

func isAlertInputError(err error) bool {
	var validation *kafkaesque_alerts.ValidationError
	return errors.As(err, &validation)
}

func alertErrorMessage(err error) string {
	var validation *kafkaesque_alerts.ValidationError
	if errors.As(err, &validation) {
		return validation.Error()
	}
	return "Unable to save the Slack alert."
}
