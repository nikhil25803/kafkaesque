package ui

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	kafkaesque_alerts "github.com/nikhil25803/kafkaesque/internals/alerts"
	kafkaesque_auth "github.com/nikhil25803/kafkaesque/internals/auth"
)

type fakeAlertManager struct {
	alerts      []kafkaesque_alerts.Subscription
	created     kafkaesque_alerts.Input
	createCalls int
}

func (f *fakeAlertManager) List(context.Context, kafkaesque_auth.User) ([]kafkaesque_alerts.Subscription, error) {
	return f.alerts, nil
}
func (f *fakeAlertManager) Get(_ context.Context, _ kafkaesque_auth.User, id string) (kafkaesque_alerts.Subscription, error) {
	for _, alert := range f.alerts {
		if alert.ID == id {
			return alert, nil
		}
	}
	return kafkaesque_alerts.Subscription{}, kafkaesque_alerts.ErrNotFound
}
func (f *fakeAlertManager) Create(_ context.Context, _ kafkaesque_auth.User, input kafkaesque_alerts.Input) (kafkaesque_alerts.Subscription, error) {
	f.created, f.createCalls = input, f.createCalls+1
	return kafkaesque_alerts.Subscription{ID: "alert-1"}, nil
}
func (f *fakeAlertManager) Update(context.Context, kafkaesque_auth.User, string, kafkaesque_alerts.Input) (kafkaesque_alerts.Subscription, error) {
	return kafkaesque_alerts.Subscription{}, nil
}
func (f *fakeAlertManager) Delete(context.Context, kafkaesque_auth.User, string) error { return nil }

func TestSettingsAlertsListAndPermissions(t *testing.T) {
	lastValue := 124.0
	lastNotified := time.Date(2026, time.October, 30, 12, 0, 0, 0, time.UTC)
	manager := &fakeAlertManager{alerts: []kafkaesque_alerts.Subscription{{
		ID: "alert-1", ConsumerGroup: "orders-service", Topic: "orders",
		Metric: kafkaesque_alerts.MetricPartition, LagThreshold: 100,
		NotifyOnResolve: true, State: kafkaesque_alerts.StateFiring,
		LastValue: &lastValue, LastNotifiedAt: &lastNotified,
	}}}
	authentication := &fakeAuthManager{user: kafkaesque_auth.User{ID: "super-1", Role: "super_admin"}, csrfValid: true}
	handler := newHandlerWithAuth(&fakeDataSource{}, authentication, false, nil, manager)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, authenticatedUIRequest(http.MethodGet, "/ui/settings/alerts", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	requireContains(t, response.Body.String(), ">Settings</span>", ">Alerts</a>", "+ Add Slack alert", "orders-service", "Partition-wise lag &gt; 100", "FIRING", "Delete this Slack alert?")

	viewer := &fakeAuthManager{user: kafkaesque_auth.User{
		ID: "viewer-1", Role: "custom", Permissions: []string{kafkaesque_auth.PermissionAlertView},
	}}
	handler = newHandlerWithAuth(&fakeDataSource{}, viewer, false, nil, manager)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, authenticatedUIRequest(http.MethodGet, "/ui/settings/alerts", nil))
	if response.Code != http.StatusOK || strings.Contains(response.Body.String(), "+ Add Slack alert") || strings.Contains(response.Body.String(), "Delete this Slack alert?") {
		t.Fatalf("viewer response=%d body=%s", response.Code, response.Body.String())
	}

	response = httptest.NewRecorder()
	handler.ServeHTTP(response, authenticatedUIRequest(http.MethodGet, "/ui/settings/alerts/new", nil))
	if response.Code != http.StatusForbidden {
		t.Fatalf("viewer create status=%d", response.Code)
	}
}

func TestCreateSlackAlertForm(t *testing.T) {
	manager := &fakeAlertManager{}
	authentication := &fakeAuthManager{user: kafkaesque_auth.User{ID: "super-1", Role: "super_admin"}, csrfValid: true}
	handler := newHandlerWithAuth(&fakeDataSource{}, authentication, false, nil, manager)

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, authenticatedUIRequest(http.MethodGet, "/ui/settings/alerts/new", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("form status=%d body=%s", response.Code, response.Body.String())
	}
	requireContains(t, response.Body.String(), "Consumer group", "orders-service", "Average lag", "Partition-wise lag", "Alert when resolved", "Slack Webhook URL", "Webhook URLs are encrypted", "/services/", "/triggers/", "Block Kit")
	if strings.Contains(response.Body.String(), "cooldown") {
		t.Fatalf("form still contains cooldown controls: %s", response.Body.String())
	}

	form := url.Values{
		"consumer_group": {"orders-service"}, "topic": {"orders"}, "metric": {"average"},
		"lag_threshold":     {"100"},
		"notify_on_resolve": {"true"}, "webhook_url": {"https://hooks.slack.com/triggers/T/1/secret"},
		"csrf_token": {"csrf"},
	}
	request := authenticatedUIRequest(http.MethodPost, "/ui/settings/alerts", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusSeeOther || response.Header().Get("Location") != "/ui/settings/alerts?saved=created" {
		t.Fatalf("save=%d location=%q body=%s", response.Code, response.Header().Get("Location"), response.Body.String())
	}
	if manager.createCalls != 1 || !manager.created.NotifyOnResolve {
		t.Fatalf("create calls=%d input=%+v", manager.createCalls, manager.created)
	}
}

func TestEditSlackAlertNeverRendersWebhook(t *testing.T) {
	manager := &fakeAlertManager{alerts: []kafkaesque_alerts.Subscription{{
		ID: "alert-1", ConsumerGroup: "orders-service", Topic: "orders",
		Metric: kafkaesque_alerts.MetricAverage, LagThreshold: 100,
		NotifyOnResolve: true, WebhookConfigured: true,
	}}}
	authentication := &fakeAuthManager{user: kafkaesque_auth.User{ID: "super-1", Role: "super_admin"}}
	handler := newHandlerWithAuth(&fakeDataSource{}, authentication, false, nil, manager)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, authenticatedUIRequest(http.MethodGet, "/ui/settings/alerts/alert-1/edit", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	body := response.Body.String()
	if strings.Contains(body, "/triggers/T/1/") {
		t.Fatalf("edit form rendered a webhook URL: %s", body)
	}
	requireContains(t, body, "Leave blank to keep the current webhook")
}

func TestSettingsRootAndLegacyRedirects(t *testing.T) {
	authentication := &fakeAuthManager{user: kafkaesque_auth.User{ID: "super-1", Role: "super_admin"}}
	handler := newHandlerWithAuth(&fakeDataSource{}, authentication, false, nil)

	for _, test := range []struct {
		path string
		code int
		want string
	}{
		{path: "/ui/settings", code: http.StatusSeeOther, want: "/ui/settings/users"},
		{path: "/ui/users?page=2", code: http.StatusPermanentRedirect, want: "/ui/settings/users?page=2"},
		{path: "/ui/roles/new", code: http.StatusPermanentRedirect, want: "/ui/settings/roles/new"},
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, authenticatedUIRequest(http.MethodGet, test.path, nil))
		if response.Code != test.code || response.Header().Get("Location") != test.want {
			t.Fatalf("%s status=%d location=%q", test.path, response.Code, response.Header().Get("Location"))
		}
	}
}
