package apis

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	kafkaesque_auth "github.com/nikhil25803/kafkaesque/internals/auth"
	kafkaesque_config "github.com/nikhil25803/kafkaesque/internals/config"
	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
	kafka "github.com/segmentio/kafka-go"
)

type fakeAuthManager struct {
	loginTokens   kafkaesque_auth.Tokens
	refreshTokens kafkaesque_auth.Tokens
	user          kafkaesque_auth.User
	loginErr      error
	authErr       error
	refreshErr    error
	logoutErr     error
	csrfErr       error
	csrfValid     bool
	loginCalls    int
	authCalls     int
	refreshCalls  int
	logoutCalls   int
	listedUsers   []kafkaesque_auth.ManagedUser
	usersTotal    int
	listUsersErr  error
	createdUser   kafkaesque_auth.ManagedUser
	createUserErr error
	createInput   kafkaesque_auth.CreateUserInput
	listCalls     int
	createCalls   int
}

func (f *fakeAuthManager) Login(context.Context, string, string) (kafkaesque_auth.Tokens, error) {
	f.loginCalls++
	return f.loginTokens, f.loginErr
}

func (f *fakeAuthManager) AuthenticateAccess(context.Context, string) (kafkaesque_auth.User, error) {
	f.authCalls++
	return f.user, f.authErr
}

func (f *fakeAuthManager) Refresh(context.Context, string) (kafkaesque_auth.Tokens, error) {
	f.refreshCalls++
	return f.refreshTokens, f.refreshErr
}

func (f *fakeAuthManager) Logout(context.Context, string) error {
	f.logoutCalls++
	return f.logoutErr
}

func (f *fakeAuthManager) ValidateCSRF(context.Context, string, string) (bool, error) {
	return f.csrfValid, f.csrfErr
}

func (f *fakeAuthManager) ValidateRefreshCSRF(context.Context, string, string) (bool, error) {
	return f.csrfValid, f.csrfErr
}

func (f *fakeAuthManager) ListUsers(context.Context, int, int) ([]kafkaesque_auth.ManagedUser, int, error) {
	f.listCalls++
	return f.listedUsers, f.usersTotal, f.listUsersErr
}

func (f *fakeAuthManager) CreateUser(_ context.Context, input kafkaesque_auth.CreateUserInput) (kafkaesque_auth.ManagedUser, error) {
	f.createCalls++
	f.createInput = input
	return f.createdUser, f.createUserErr
}

func authenticatedTestHandler(authentication kafkaesque_auth.RuntimeManager, logger *slog.Logger) (http.Handler, *apiTransport) {
	transport := &apiTransport{}
	cfg := &kafkaesque_config.Config{
		Kafka: kafkaesque_config.KafkaConfig{
			BootstrapServers:  []string{"bootstrap:9092"},
			ConnectionTimeout: time.Second,
		},
		Lag:            kafkaesque_config.LagConfig{WarningThreshold: 100, UnhealthyThreshold: 500},
		Authentication: kafkaesque_config.AuthenticationConfig{CookieSecure: true},
	}
	conn := &kafkaesque.KafkaesqueConn{
		Client: &kafka.Client{Addr: kafka.TCP("bootstrap:9092"), Transport: transport},
	}
	return NewHandler(conn, cfg, authentication, logger), transport
}

func TestAuthenticationRunsBeforeKafka(t *testing.T) {
	authentication := &fakeAuthManager{authErr: kafkaesque_auth.ErrInvalidToken}
	handler, transport := authenticatedTestHandler(authentication, nil)
	response := request(t, handler, "/api/v1/metadata/cluster")

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", response.Code)
	}
	if transport.metadataRequests != 0 {
		t.Fatalf("metadata requests = %d, want 0", transport.metadataRequests)
	}
}

func TestLoginSetsSecureCookiesAndRedactsLogs(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	user := kafkaesque_auth.User{ID: "user-1", Username: "super_admin", Role: "super_admin"}
	authentication := &fakeAuthManager{loginTokens: kafkaesque_auth.Tokens{
		AccessToken:      "access-secret",
		RefreshToken:     "refresh-secret",
		CSRFToken:        "csrf-secret",
		AccessExpiresAt:  time.Now().Add(time.Minute),
		RefreshExpiresAt: time.Now().Add(time.Hour),
		User:             user,
	}}
	handler, _ := authenticatedTestHandler(authentication, logger)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", strings.NewReader(`{"username":"super_admin","password":"a-private-password"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK || len(response.Result().Cookies()) != 3 {
		t.Fatalf("login response = %d, cookies = %d", response.Code, len(response.Result().Cookies()))
	}
	for _, cookie := range response.Result().Cookies() {
		if !cookie.Secure {
			t.Fatalf("cookie %s must be secure", cookie.Name)
		}
		if !cookie.HttpOnly && cookie.Name != kafkaesque_auth.CSRFCookie {
			t.Fatalf("cookie %s must be HttpOnly", cookie.Name)
		}
		if cookie.SameSite != http.SameSiteStrictMode || cookie.Path != "/" {
			t.Fatalf("unsafe cookie: %+v", cookie)
		}
	}
	logOutput := logs.String()
	if !strings.Contains(logOutput, `"msg":"login_succeeded"`) {
		t.Fatalf("missing login log: %s", logOutput)
	}
	for _, secret := range []string{"a-private-password", "access-secret", "refresh-secret", "csrf-secret"} {
		if strings.Contains(logOutput, secret) {
			t.Fatalf("logs contain secret %q", secret)
		}
	}
}

func TestLoginFailuresAndThrottle(t *testing.T) {
	authentication := &fakeAuthManager{loginErr: kafkaesque_auth.ErrInvalidCredentials}
	handler, _ := authenticatedTestHandler(authentication, nil)
	for attempt := 1; attempt <= 6; attempt++ {
		request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", strings.NewReader(`{"username":"super_admin","password":"wrong-password"}`))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		want := http.StatusUnauthorized
		if attempt == 6 {
			want = http.StatusTooManyRequests
		}
		if response.Code != want {
			t.Fatalf("attempt %d status = %d, want %d", attempt, response.Code, want)
		}
		if strings.Contains(response.Body.String(), "super_admin") {
			t.Fatalf("credential response exposed username: %s", response.Body.String())
		}
	}
}

func TestBrowserAccessRefreshesExpiredSession(t *testing.T) {
	user := kafkaesque_auth.User{ID: "user-1", Username: "super_admin", Role: "super_admin"}
	authentication := &fakeAuthManager{
		authErr:   kafkaesque_auth.ErrInvalidToken,
		csrfValid: true,
		refreshTokens: kafkaesque_auth.Tokens{
			AccessToken: "new-access", RefreshToken: "new-refresh", CSRFToken: "new-csrf",
			AccessExpiresAt: time.Now().Add(time.Minute), RefreshExpiresAt: time.Now().Add(time.Hour), User: user,
		},
	}
	handler, transport := authenticatedTestHandler(authentication, nil)
	request := httptest.NewRequest(http.MethodGet, "/api/v1/metadata/cluster", nil)
	request.AddCookie(&http.Cookie{Name: kafkaesque_auth.RefreshCookie, Value: "old-refresh"})
	request.AddCookie(&http.Cookie{Name: kafkaesque_auth.CSRFCookie, Value: "old-csrf"})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK || authentication.refreshCalls != 1 || transport.metadataRequests != 1 {
		t.Fatalf("status=%d refreshes=%d metadata=%d", response.Code, authentication.refreshCalls, transport.metadataRequests)
	}
}

func TestBearerRefreshRotatesTokens(t *testing.T) {
	user := kafkaesque_auth.User{ID: "user-1", Username: "super_admin", Role: "super_admin"}
	authentication := &fakeAuthManager{
		refreshTokens: kafkaesque_auth.Tokens{
			AccessToken:      "new-access",
			RefreshToken:     "new-refresh",
			CSRFToken:        "new-csrf",
			AccessExpiresAt:  time.Now().Add(time.Minute),
			RefreshExpiresAt: time.Now().Add(time.Hour),
			User:             user,
		},
	}
	handler, _ := authenticatedTestHandler(authentication, nil)
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/auth/refresh",
		strings.NewReader(`{"refresh_token":"old-refresh"}`),
	)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK || authentication.refreshCalls != 1 {
		t.Fatalf("status = %d, refresh calls = %d", response.Code, authentication.refreshCalls)
	}
	if !strings.Contains(response.Body.String(), `"access_token":"new-access"`) {
		t.Fatalf("unexpected refresh response: %s", response.Body.String())
	}
}

func TestCookieRefreshRequiresCSRF(t *testing.T) {
	authentication := &fakeAuthManager{}
	handler, _ := authenticatedTestHandler(authentication, nil)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/refresh", nil)
	request.AddCookie(&http.Cookie{Name: kafkaesque_auth.RefreshCookie, Value: "refresh"})
	request.AddCookie(&http.Cookie{Name: kafkaesque_auth.CSRFCookie, Value: "cookie-csrf"})
	request.Header.Set("X-CSRF-Token", "different-csrf")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusForbidden || authentication.refreshCalls != 0 {
		t.Fatalf("status = %d, refresh calls = %d", response.Code, authentication.refreshCalls)
	}
}

func TestBearerLogoutAndMe(t *testing.T) {
	user := kafkaesque_auth.User{ID: "user-1", Username: "super_admin", Role: "super_admin"}
	authentication := &fakeAuthManager{user: user}
	handler, _ := authenticatedTestHandler(authentication, nil)

	meRequest := httptest.NewRequest(http.MethodGet, "/api/v1/auth/me", nil)
	meRequest.Header.Set("Authorization", "Bearer access-token")
	meResponse := httptest.NewRecorder()
	handler.ServeHTTP(meResponse, meRequest)
	if meResponse.Code != http.StatusOK ||
		!strings.Contains(meResponse.Body.String(), `"username":"super_admin"`) ||
		!strings.Contains(meResponse.Body.String(), `"role":"super_admin"`) {
		t.Fatalf("me response = %d %s", meResponse.Code, meResponse.Body.String())
	}

	logoutRequest := httptest.NewRequest(http.MethodPost, "/api/v1/auth/logout", nil)
	logoutRequest.Header.Set("Authorization", "Bearer access-token")
	logoutResponse := httptest.NewRecorder()
	handler.ServeHTTP(logoutResponse, logoutRequest)
	if logoutResponse.Code != http.StatusOK || authentication.logoutCalls != 1 {
		t.Fatalf("logout response = %d, calls = %d", logoutResponse.Code, authentication.logoutCalls)
	}
}

func TestCookieLogoutRequiresCSRF(t *testing.T) {
	authentication := &fakeAuthManager{}
	handler, _ := authenticatedTestHandler(authentication, nil)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/logout", nil)
	request.AddCookie(&http.Cookie{Name: kafkaesque_auth.AccessCookie, Value: "access"})
	request.AddCookie(&http.Cookie{Name: kafkaesque_auth.CSRFCookie, Value: "csrf"})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusForbidden || authentication.logoutCalls != 0 {
		t.Fatalf("status = %d, logout calls = %d", response.Code, authentication.logoutCalls)
	}
}

func TestAuthenticationDatabaseFailuresReturnUnavailable(t *testing.T) {
	authentication := &fakeAuthManager{authErr: errors.New("database unavailable")}
	handler, _ := authenticatedTestHandler(authentication, nil)
	response := request(t, handler, "/api/v1/auth/me")
	if response.Code != http.StatusServiceUnavailable || strings.Contains(response.Body.String(), "database unavailable") {
		t.Fatalf("status/body = %d %s", response.Code, response.Body.String())
	}
}

func TestPublicAndUnknownRoutesSkipAuthentication(t *testing.T) {
	authentication := &fakeAuthManager{authErr: kafkaesque_auth.ErrInvalidToken}
	handler, _ := authenticatedTestHandler(authentication, nil)
	if response := request(t, handler, "/api/v1/health"); response.Code != http.StatusOK {
		t.Fatalf("health status = %d", response.Code)
	}
	if response := request(t, handler, "/api/v1/not-found"); response.Code != http.StatusNotFound {
		t.Fatalf("unknown status = %d", response.Code)
	}
	if authentication.authCalls != 0 {
		t.Fatalf("authentication calls = %d, want 0", authentication.authCalls)
	}
}
