package ui

import (
	"net/http"
	"strings"

	kafkaesque_auth "github.com/nikhil25803/kafkaesque/internals/auth"
)

type settingsTab struct {
	Label  string
	URL    string
	Active bool
}

func settingsTabs(r *http.Request, active string) []settingsTab {
	user, _ := kafkaesque_auth.UserFromContext(r.Context())
	tabs := make([]settingsTab, 0, 3)
	if kafkaesque_auth.HasPermission(user, kafkaesque_auth.PermissionUserView) {
		tabs = append(tabs, settingsTab{Label: "Users", URL: "/ui/settings/users", Active: active == "users"})
	}
	if user.Role == "super_admin" {
		tabs = append(tabs, settingsTab{Label: "Roles", URL: "/ui/settings/roles", Active: active == "roles"})
	}
	if kafkaesque_auth.HasPermission(user, kafkaesque_auth.PermissionAlertView) {
		tabs = append(tabs, settingsTab{Label: "Alerts", URL: "/ui/settings/alerts", Active: active == "alerts"})
	}
	return tabs
}

func renderSettingsRoot(w http.ResponseWriter, r *http.Request) {
	tabs := settingsTabs(r, "")
	if len(tabs) == 0 {
		renderUIError(w, r, http.StatusForbidden, "settings", "Permission denied", "You do not have permission to access Settings.")
		return
	}
	http.Redirect(w, r, tabs[0].URL, http.StatusSeeOther)
}

func legacySettingsRedirect(section string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		suffix := strings.TrimPrefix(r.URL.Path, "/ui/"+section)
		target := "/ui/settings/" + section + suffix
		if r.URL.RawQuery != "" {
			target += "?" + r.URL.RawQuery
		}
		http.Redirect(w, r, target, http.StatusPermanentRedirect)
	}
}
