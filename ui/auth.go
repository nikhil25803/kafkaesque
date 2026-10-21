package ui

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"

	kafkaesque_auth "github.com/nikhil25803/kafkaesque/internals/auth"
)

type loginPage struct {
	Error string
	Next  string
}

func navigationFor(r *http.Request, active string) navigationView {
	user, _ := kafkaesque_auth.UserFromContext(r.Context())
	return navigationView{
		Active:    active,
		Username:  user.Username,
		Role:      roleLabel(user.Role),
		CSRFToken: kafkaesque_auth.CookieValue(r, kafkaesque_auth.CSRFCookie),
		ShowUsers: kafkaesque_auth.HasPermission(user, kafkaesque_auth.PermissionUserView),
		ShowRoles: user.Role == "super_admin",
	}
}

// authorizeUI requires one account-management permission.
func authorizeUI(permission string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user, ok := kafkaesque_auth.UserFromContext(r.Context())
		if !ok || !kafkaesque_auth.HasPermission(user, permission) {
			renderUIError(
				w,
				r,
				http.StatusForbidden,
				"",
				"Permission denied",
				"You do not have permission to access this page.",
			)
			return
		}

		next(w, r)
	}
}

// superAdminUI restricts role administration to the configured Super Admin.
func superAdminUI(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user, ok := kafkaesque_auth.UserFromContext(r.Context())
		if !ok || user.Role != "super_admin" {
			renderUIError(
				w,
				r,
				http.StatusForbidden,
				"",
				"Permission denied",
				"Only the Super Admin can manage roles.",
			)
			return
		}

		next(w, r)
	}
}

func roleLabel(role string) string {
	switch role {
	case "super_admin":
		return "Super Admin"
	case "admin":
		return "Admin"
	case "user":
		return "User"
	default:
		return role
	}
}

// authenticateUI validates browser sessions before rendering protected pages.
func authenticateUI(
	service kafkaesque_auth.Manager,
	secure bool,
	logger *slog.Logger,
	next http.HandlerFunc,
) http.HandlerFunc {
	logger = uiLogger(logger)
	return func(w http.ResponseWriter, r *http.Request) {
		token := kafkaesque_auth.CookieValue(r, kafkaesque_auth.AccessCookie)
		user, err := service.AuthenticateAccess(r.Context(), token)
		if errors.Is(err, kafkaesque_auth.ErrInvalidToken) {
			refreshToken := kafkaesque_auth.CookieValue(r, kafkaesque_auth.RefreshCookie)
			csrfToken := kafkaesque_auth.CookieValue(r, kafkaesque_auth.CSRFCookie)
			validCSRF, csrfErr := service.ValidateRefreshCSRF(r.Context(), refreshToken, csrfToken)
			if csrfErr != nil {
				err = csrfErr
			}
			if validCSRF {
				if refreshed, refreshErr := service.Refresh(r.Context(), refreshToken); refreshErr == nil {
					kafkaesque_auth.SetCookies(w, refreshed, secure)
					user = refreshed.User
					token = refreshed.AccessToken
					err = nil
					logger.Info("refresh_succeeded", "user_id", refreshed.User.ID, "source", "browser")
				} else {
					err = refreshErr
				}
			}
		}
		if err != nil {
			if !errors.Is(err, kafkaesque_auth.ErrInvalidToken) {
				logger.Error("authentication_failed", "reason", "database_unavailable")
				renderUIError(
					w,
					r,
					http.StatusServiceUnavailable,
					"",
					"Authentication unavailable",
					"The authentication database is unavailable.",
				)
				return
			}

			logger.Warn("authentication_rejected", "reason", "invalid_token")
			target := "/ui/login?next=" + url.QueryEscape(safeNext(r.URL.RequestURI()))
			if strings.EqualFold(r.Header.Get("HX-Request"), "true") {
				w.Header().Set("HX-Redirect", target)
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			http.Redirect(w, r, target, http.StatusSeeOther)
			return
		}
		if r.Method != "GET" && r.Method != "HEAD" {
			_ = r.ParseForm()
			csrf := r.Form.Get("csrf_token")
			validCSRF, csrfErr := service.ValidateCSRF(r.Context(), token, csrf)
			if csrfErr != nil {
				logger.Error("csrf_validation_failed", "reason", "database_unavailable")
				renderUIError(
					w,
					r,
					http.StatusServiceUnavailable,
					"",
					"Authentication unavailable",
					"The authentication database is unavailable.",
				)
				return
			}

			if csrf == "" || csrf != kafkaesque_auth.CookieValue(r, kafkaesque_auth.CSRFCookie) || !validCSRF {
				logger.Warn("csrf_rejected", "user_id", user.ID)
				renderUIError(w, r, http.StatusForbidden, "", "Request rejected", "CSRF validation failed.")
				return
			}
		}

		kafkaesque_auth.RecordAuthenticatedUser(w, user.ID)
		next(w, r.WithContext(kafkaesque_auth.WithUser(r.Context(), user)))
	}
}

func renderLogin(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = dashboardTemplate.ExecuteTemplate(w, "login", loginPage{Next: safeNext(r.URL.Query().Get("next"))})
}

// loginUI validates an account and starts a browser session.
func loginUI(
	service kafkaesque_auth.Manager,
	limiter *kafkaesque_auth.Limiter,
	secure bool,
	logger *slog.Logger,
) http.HandlerFunc {
	logger = uiLogger(logger)
	return func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			renderLoginFailure(w, http.StatusBadRequest, "Invalid login request", r.FormValue("next"))
			return
		}
		username, _ := kafkaesque_auth.NormalizeUsername(r.FormValue("username"))
		clientAddress := kafkaesque_auth.ClientAddress(r.RemoteAddr)
		if !limiter.Allow(username, r.RemoteAddr) {
			logger.Warn("login_throttled", "username", username, "client_address", clientAddress)
			renderLoginFailure(
				w,
				http.StatusTooManyRequests,
				"Too many login attempts; try again later",
				r.FormValue("next"),
			)
			return
		}
		tokens, err := service.Login(r.Context(), username, r.FormValue("password"))
		if err != nil {
			if !errors.Is(err, kafkaesque_auth.ErrInvalidCredentials) {
				logger.Error("login_failed", "reason", "database_unavailable", "client_address", clientAddress)
				renderLoginFailure(
					w,
					http.StatusServiceUnavailable,
					"Authentication is temporarily unavailable",
					r.FormValue("next"),
				)
				return
			}
			limiter.Failure(username, r.RemoteAddr)
			logger.Warn("login_failed", "username", username, "client_address", clientAddress)
			renderLoginFailure(w, http.StatusUnauthorized, "Invalid username or password", r.FormValue("next"))
			return
		}
		limiter.Success(username, r.RemoteAddr)
		logger.Info("login_succeeded", "user_id", tokens.User.ID, "client_address", clientAddress)
		kafkaesque_auth.SetCookies(w, tokens, secure)
		http.Redirect(w, r, safeNext(r.FormValue("next")), http.StatusSeeOther)
	}
}

func renderLoginFailure(w http.ResponseWriter, status int, message, next string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_ = dashboardTemplate.ExecuteTemplate(w, "login", loginPage{Error: message, Next: safeNext(next)})
}

// logoutUI revokes the current browser session and clears its cookies.
func logoutUI(service kafkaesque_auth.Manager, secure bool, logger *slog.Logger) http.HandlerFunc {
	logger = uiLogger(logger)
	return func(w http.ResponseWriter, r *http.Request) {
		err := service.Logout(r.Context(), kafkaesque_auth.CookieValue(r, kafkaesque_auth.AccessCookie))
		if err != nil && !errors.Is(err, kafkaesque_auth.ErrInvalidToken) {
			logger.Error("logout_failed", "reason", "database_unavailable")
			renderUIError(
				w,
				r,
				http.StatusServiceUnavailable,
				"",
				"Authentication unavailable",
				"The authentication database is unavailable.",
			)
			return
		}
		if errors.Is(err, kafkaesque_auth.ErrInvalidToken) {
			logger.Warn("logout_rejected", "reason", "invalid_token")
		}
		logger.Info("logout_succeeded")
		kafkaesque_auth.ClearCookies(w, secure)
		http.Redirect(w, r, "/ui/login", http.StatusSeeOther)
	}
}

func uiLogger(logger *slog.Logger) *slog.Logger {
	if logger != nil {
		return logger
	}
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

// safeNext accepts only local UI destinations after login.
func safeNext(value string) string {
	if value == "" {
		return "/ui"
	}
	parsed, err := url.Parse(value)
	unsafe := err != nil ||
		parsed.IsAbs() ||
		parsed.Host != "" ||
		!strings.HasPrefix(parsed.Path, "/ui") ||
		strings.HasPrefix(parsed.Path, "//")
	if unsafe {
		return "/ui"
	}

	return parsed.RequestURI()
}
