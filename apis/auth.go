package apis

import (
	"encoding/json"
	"errors"
	"net/http"

	kafkaesque_auth "github.com/nikhil25803/kafkaesque/internals/auth"
)

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type refreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

// login validates credentials and creates an API and browser session.
func (s *server) login(w http.ResponseWriter, r *http.Request) {
	var request loginRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&request); err != nil {
		writeInvalid(w, "invalid login request")
		return
	}
	request.Username, _ = kafkaesque_auth.NormalizeUsername(request.Username)
	clientAddress := kafkaesque_auth.ClientAddress(r.RemoteAddr)
	if !s.limiter.Allow(request.Username, r.RemoteAddr) {
		s.logger.Warn("login_throttled", "username", request.Username, "client_address", clientAddress)
		writeError(w, http.StatusTooManyRequests, "too_many_attempts", "too many login attempts; try again later")
		return
	}
	tokens, err := s.auth.Login(r.Context(), request.Username, request.Password)
	if err != nil {
		if errors.Is(err, kafkaesque_auth.ErrInvalidCredentials) {
			s.limiter.Failure(request.Username, r.RemoteAddr)
			s.logger.Warn("login_failed", "username", request.Username, "client_address", clientAddress)
			writeError(w, http.StatusUnauthorized, "invalid_credentials", "invalid username or password")
			return
		}
		s.logger.Error("login_failed", "reason", "database_unavailable", "client_address", clientAddress)
		writeError(w, http.StatusServiceUnavailable, "database_unavailable", "authentication service unavailable")
		return
	}
	s.limiter.Success(request.Username, r.RemoteAddr)
	s.logger.Info("login_succeeded", "user_id", tokens.User.ID, "client_address", clientAddress)
	kafkaesque_auth.SetCookies(w, tokens, s.config.Authentication.CookieSecure)
	writeSuccess(w, http.StatusOK, tokens, map[string]any{})
}

// refresh rotates a valid refresh token and its session cookies.
func (s *server) refresh(w http.ResponseWriter, r *http.Request) {
	token := kafkaesque_auth.CookieValue(r, kafkaesque_auth.RefreshCookie)
	if token != "" {
		csrf := r.Header.Get("X-CSRF-Token")
		validCSRF, csrfErr := s.auth.ValidateRefreshCSRF(r.Context(), token, csrf)
		if csrfErr != nil {
			s.logger.Error("csrf_validation_failed", "reason", "database_unavailable")
			writeError(w, http.StatusServiceUnavailable, "database_unavailable", "authentication service unavailable")
			return
		}
		if csrf == "" || csrf != kafkaesque_auth.CookieValue(r, kafkaesque_auth.CSRFCookie) || !validCSRF {
			s.logger.Warn("csrf_rejected", "operation", "refresh")
			writeError(w, http.StatusForbidden, "csrf_failed", "CSRF validation failed")
			return
		}
	}
	if token == "" {
		var request refreshRequest
		if json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&request) == nil {
			token = request.RefreshToken
		}
	}
	tokens, err := s.auth.Refresh(r.Context(), token)
	if err != nil {
		if errors.Is(err, kafkaesque_auth.ErrInvalidToken) {
			s.logger.Warn("refresh_rejected", "reason", "invalid_token")
			writeError(w, http.StatusUnauthorized, "invalid_token", "invalid or expired refresh token")
			return
		}
		s.logger.Error("refresh_failed", "reason", "database_unavailable")
		writeError(w, http.StatusServiceUnavailable, "database_unavailable", "authentication service unavailable")
		return
	}
	kafkaesque_auth.SetCookies(w, tokens, s.config.Authentication.CookieSecure)
	s.logger.Info("refresh_succeeded", "user_id", tokens.User.ID)
	writeSuccess(w, http.StatusOK, tokens, map[string]any{})
}

// logout revokes the session identified by a bearer token or cookie.
func (s *server) logout(w http.ResponseWriter, r *http.Request) {
	token := kafkaesque_auth.BearerToken(r)
	if token == "" {
		token = kafkaesque_auth.CookieValue(r, kafkaesque_auth.AccessCookie)
		usingRefresh := token == ""
		if usingRefresh {
			token = kafkaesque_auth.CookieValue(r, kafkaesque_auth.RefreshCookie)
		}
		csrf := r.Header.Get("X-CSRF-Token")
		if csrf == "" {
			_ = r.ParseForm()
			csrf = r.Form.Get("csrf_token")
		}
		validCSRF, csrfErr := s.auth.ValidateCSRF(r.Context(), token, csrf)
		if usingRefresh {
			validCSRF, csrfErr = s.auth.ValidateRefreshCSRF(r.Context(), token, csrf)
		}
		if csrfErr != nil {
			s.logger.Error("csrf_validation_failed", "reason", "database_unavailable")
			writeError(w, http.StatusServiceUnavailable, "database_unavailable", "authentication service unavailable")
			return
		}
		if csrf == "" || csrf != kafkaesque_auth.CookieValue(r, kafkaesque_auth.CSRFCookie) || !validCSRF {
			s.logger.Warn("csrf_rejected", "operation", "logout")
			writeError(w, http.StatusForbidden, "csrf_failed", "CSRF validation failed")
			return
		}
	}
	if err := s.auth.Logout(r.Context(), token); err != nil && !errors.Is(err, kafkaesque_auth.ErrInvalidToken) {
		s.logger.Error("logout_failed", "reason", "database_unavailable")
		writeError(w, http.StatusServiceUnavailable, "database_unavailable", "authentication service unavailable")
		return
	} else if errors.Is(err, kafkaesque_auth.ErrInvalidToken) {
		s.logger.Warn("logout_rejected", "reason", "invalid_token")
		writeError(w, http.StatusUnauthorized, "invalid_token", "invalid or expired token")
		return
	}
	kafkaesque_auth.ClearCookies(w, s.config.Authentication.CookieSecure)
	s.logger.Info("logout_succeeded")
	writeSuccess(w, http.StatusOK, map[string]string{"status": "LOGGED_OUT"}, map[string]any{})
}

func (s *server) me(w http.ResponseWriter, r *http.Request) {
	user, _ := kafkaesque_auth.UserFromContext(r.Context())
	writeSuccess(w, http.StatusOK, user, map[string]any{})
}

// authenticate validates a bearer or browser session before calling a handler.
func (s *server) authenticate(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := kafkaesque_auth.BearerToken(r)
		cookieAuth := token == ""
		if cookieAuth {
			token = kafkaesque_auth.CookieValue(r, kafkaesque_auth.AccessCookie)
		}
		user, err := s.auth.AuthenticateAccess(r.Context(), token)
		if errors.Is(err, kafkaesque_auth.ErrInvalidToken) && cookieAuth {
			refreshToken := kafkaesque_auth.CookieValue(r, kafkaesque_auth.RefreshCookie)
			csrfToken := kafkaesque_auth.CookieValue(r, kafkaesque_auth.CSRFCookie)
			validCSRF, csrfErr := s.auth.ValidateRefreshCSRF(r.Context(), refreshToken, csrfToken)
			if csrfErr != nil {
				err = csrfErr
			}
			if validCSRF {
				if refreshed, refreshErr := s.auth.Refresh(r.Context(), refreshToken); refreshErr == nil {
					kafkaesque_auth.SetCookies(w, refreshed, s.config.Authentication.CookieSecure)
					user = refreshed.User
					token = refreshed.AccessToken
					err = nil
					s.logger.Info("refresh_succeeded", "user_id", refreshed.User.ID, "source", "browser")
				} else {
					err = refreshErr
				}
			}
		}
		if err != nil {
			if !errors.Is(err, kafkaesque_auth.ErrInvalidToken) {
				s.logger.Error("authentication_failed", "reason", "database_unavailable")
				writeError(w, http.StatusServiceUnavailable, "database_unavailable", "authentication service unavailable")
				return
			}
			s.logger.Warn("authentication_rejected", "reason", "invalid_token")
			writeError(w, http.StatusUnauthorized, "unauthorized", "authentication required")
			return
		}
		if cookieAuth && r.Method != "GET" && r.Method != "HEAD" && r.Method != "OPTIONS" {
			csrf := r.Header.Get("X-CSRF-Token")
			if csrf == "" {
				_ = r.ParseForm()
				csrf = r.Form.Get("csrf_token")
			}
			validCSRF, csrfErr := s.auth.ValidateCSRF(r.Context(), token, csrf)
			if csrfErr != nil {
				s.logger.Error("csrf_validation_failed", "reason", "database_unavailable")
				writeError(w, http.StatusServiceUnavailable, "database_unavailable", "authentication service unavailable")
				return
			}
			if csrf == "" || csrf != kafkaesque_auth.CookieValue(r, kafkaesque_auth.CSRFCookie) || !validCSRF {
				s.logger.Warn("csrf_rejected", "user_id", user.ID)
				writeError(w, http.StatusForbidden, "csrf_failed", "CSRF validation failed")
				return
			}
		}
		kafkaesque_auth.RecordAuthenticatedUser(w, user.ID)
		next(w, r.WithContext(kafkaesque_auth.WithUser(r.Context(), user)))
	}
}
