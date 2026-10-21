package ui

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	kafkaesque_auth "github.com/nikhil25803/kafkaesque/internals/auth"
)

type fakeAuthManager struct {
	user        kafkaesque_auth.User
	tokens      kafkaesque_auth.Tokens
	loginErr    error
	authErr     error
	refreshErr  error
	logoutErr   error
	csrfValid   bool
	users       []kafkaesque_auth.ManagedUser
	usersTotal  int
	listErr     error
	createErr   error
	created     kafkaesque_auth.ManagedUser
	createInput kafkaesque_auth.CreateUserInput
	listCalls   int
	createCalls int
	listPage    int
	listSize    int
}

func (f *fakeAuthManager) Login(context.Context, string, string) (kafkaesque_auth.Tokens, error) {
	return f.tokens, f.loginErr
}

func (f *fakeAuthManager) AuthenticateAccess(context.Context, string) (kafkaesque_auth.User, error) {
	return f.user, f.authErr
}

func (f *fakeAuthManager) Refresh(context.Context, string) (kafkaesque_auth.Tokens, error) {
	return f.tokens, f.refreshErr
}

func (f *fakeAuthManager) Logout(context.Context, string) error { return f.logoutErr }

func (f *fakeAuthManager) ValidateCSRF(context.Context, string, string) (bool, error) {
	return f.csrfValid, nil
}

func (f *fakeAuthManager) ValidateRefreshCSRF(context.Context, string, string) (bool, error) {
	return f.csrfValid, nil
}

func (f *fakeAuthManager) ListUsers(_ context.Context, page, pageSize int) ([]kafkaesque_auth.ManagedUser, int, error) {
	f.listCalls++
	f.listPage = page
	f.listSize = pageSize
	return f.users, f.usersTotal, f.listErr
}

func (f *fakeAuthManager) CreateUser(_ context.Context, input kafkaesque_auth.CreateUserInput) (kafkaesque_auth.ManagedUser, error) {
	f.createCalls++
	f.createInput = input
	return f.created, f.createErr
}

func TestSafeNext(t *testing.T) {
	tests := map[string]string{
		"":                        "/ui",
		"/ui/topics?page=2":       "/ui/topics?page=2",
		"https://evil.example/ui": "/ui",
		"//evil.example/ui":       "/ui",
		"/api/v1/health":          "/ui",
	}
	for input, want := range tests {
		if got := safeNext(input); got != want {
			t.Errorf("safeNext(%q)=%q, want %q", input, got, want)
		}
	}
}

func TestLoginPageAndGenericFailure(t *testing.T) {
	authentication := &fakeAuthManager{loginErr: kafkaesque_auth.ErrInvalidCredentials}
	handler := newHandlerWithAuth(&fakeDataSource{}, authentication, false, nil)
	response := requestUI(t, handler, "/ui/login", false)
	if response.Code != http.StatusOK {
		t.Fatalf("login status = %d", response.Code)
	}
	requireContains(
		t,
		response.Body.String(),
		`class="wordmark login-wordmark"`,
		`id="login-title">Sign in</h1>`,
		"Read. Understand. Monitor. Alert.",
		"A lightweight, read-only Kafka observability tool",
		"/ui/assets/KafkaesqueLogo.png",
	)

	form := url.Values{"username": {"unknown"}, "password": {"wrong-password"}}
	request := httptest.NewRequest(http.MethodPost, "/ui/login", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("failure status = %d", response.Code)
	}
	requireContains(t, response.Body.String(), "Invalid username or password")
	if strings.Contains(response.Body.String(), "unknown") {
		t.Fatal("login failure exposed the submitted username")
	}
}

func TestLoginAndSidebarWordmarksUseSeparateResponsiveStyles(t *testing.T) {
	handler := newHandlerWithAuth(&fakeDataSource{}, &fakeAuthManager{}, false, nil)
	response := requestUI(t, handler, "/ui/assets/app.css", false)

	if response.Code != http.StatusOK {
		t.Fatalf("stylesheet status = %d", response.Code)
	}

	stylesheet := response.Body.String()
	requireContains(t, stylesheet, ".sidebar .wordmark {", ".login-wordmark {")
	if strings.Contains(stylesheet, "  .wordmark { width: 36px;") {
		t.Fatal("tablet sidebar rule also affects the login wordmark")
	}
}

func TestLoginLogoAsset(t *testing.T) {
	handler := newHandlerWithAuth(&fakeDataSource{}, &fakeAuthManager{}, false, nil)
	response := requestUI(t, handler, "/ui/assets/KafkaesqueLogo.png", false)

	if response.Code != http.StatusOK {
		t.Fatalf("logo status = %d", response.Code)
	}
	if contentType := response.Header().Get("Content-Type"); contentType != "image/png" {
		t.Fatalf("logo content type = %q", contentType)
	}
	if response.Body.Len() == 0 {
		t.Fatal("logo response was empty")
	}
}

func TestLoginThrottlesRepeatedFailures(t *testing.T) {
	authentication := &fakeAuthManager{loginErr: kafkaesque_auth.ErrInvalidCredentials}
	handler := newHandlerWithAuth(&fakeDataSource{}, authentication, false, nil)
	form := url.Values{"username": {"super_admin"}, "password": {"wrong-password"}}

	for attempt := 1; attempt <= 6; attempt++ {
		request := httptest.NewRequest(http.MethodPost, "/ui/login", strings.NewReader(form.Encode()))
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)

		want := http.StatusUnauthorized
		if attempt == 6 {
			want = http.StatusTooManyRequests
		}
		if response.Code != want {
			t.Fatalf("attempt %d status = %d, want %d", attempt, response.Code, want)
		}
	}
}

func TestProtectedUIRedirectsAndHTMXExpiry(t *testing.T) {
	authentication := &fakeAuthManager{authErr: kafkaesque_auth.ErrInvalidToken}
	handler := newHandlerWithAuth(&fakeDataSource{}, authentication, false, nil)
	response := requestUI(t, handler, "/ui/topics", false)
	if response.Code != http.StatusSeeOther || !strings.HasPrefix(response.Header().Get("Location"), "/ui/login?") {
		t.Fatalf("redirect = %d %q", response.Code, response.Header().Get("Location"))
	}
	response = requestUI(t, handler, "/ui/topics", true)
	if response.Code != http.StatusUnauthorized || !strings.HasPrefix(response.Header().Get("HX-Redirect"), "/ui/login?") {
		t.Fatalf("HTMX redirect = %d %q", response.Code, response.Header().Get("HX-Redirect"))
	}
}

func TestAuthenticatedShellAndLogout(t *testing.T) {
	user := kafkaesque_auth.User{ID: "user-1", Username: "viewer", Role: "user"}
	authentication := &fakeAuthManager{user: user, csrfValid: true}
	handler := newHandlerWithAuth(&fakeDataSource{}, authentication, false, nil)
	request := httptest.NewRequest(http.MethodGet, "/ui", nil)
	request.AddCookie(&http.Cookie{Name: kafkaesque_auth.AccessCookie, Value: "access"})
	request.AddCookie(&http.Cookie{Name: kafkaesque_auth.CSRFCookie, Value: "csrf"})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("dashboard status = %d", response.Code)
	}
	requireContains(t, response.Body.String(), "Logged in as", "Role: User", ">Users</span>", "Log out")

	form := url.Values{"csrf_token": {"csrf"}}
	request = httptest.NewRequest(http.MethodPost, "/ui/logout", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.AddCookie(&http.Cookie{Name: kafkaesque_auth.AccessCookie, Value: "access"})
	request.AddCookie(&http.Cookie{Name: kafkaesque_auth.CSRFCookie, Value: "csrf"})
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusSeeOther || response.Header().Get("Location") != "/ui/login" {
		t.Fatalf("logout = %d %q", response.Code, response.Header().Get("Location"))
	}
}

func TestUILogoutRejectsMissingCSRF(t *testing.T) {
	user := kafkaesque_auth.User{ID: "user-1", Username: "super_admin", Role: "super_admin"}
	authentication := &fakeAuthManager{user: user}
	handler := newHandlerWithAuth(&fakeDataSource{}, authentication, false, nil)
	request := httptest.NewRequest(http.MethodPost, "/ui/logout", nil)
	request.AddCookie(&http.Cookie{Name: kafkaesque_auth.AccessCookie, Value: "access"})
	request.AddCookie(&http.Cookie{Name: kafkaesque_auth.CSRFCookie, Value: "csrf"})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusForbidden {
		t.Fatalf("logout status = %d, want 403", response.Code)
	}
	requireContains(t, response.Body.String(), "CSRF validation failed")
}

func TestUILoginDatabaseFailureIsUnavailable(t *testing.T) {
	authentication := &fakeAuthManager{loginErr: errors.New("private database error")}
	handler := newHandlerWithAuth(&fakeDataSource{}, authentication, false, nil)
	form := url.Values{"username": {"super_admin"}, "password": {"password-not-logged"}}
	request := httptest.NewRequest(http.MethodPost, "/ui/login", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusServiceUnavailable || strings.Contains(response.Body.String(), "private database error") {
		t.Fatalf("status/body = %d %s", response.Code, response.Body.String())
	}
	requireContains(t, response.Body.String(), `class="login-error"`)
}

func TestUIDatabaseFailureIsUnavailable(t *testing.T) {
	authentication := &fakeAuthManager{authErr: errors.New("private database error")}
	handler := newHandlerWithAuth(&fakeDataSource{}, authentication, false, nil)
	response := requestUI(t, handler, "/ui", false)
	if response.Code != http.StatusServiceUnavailable || strings.Contains(response.Body.String(), "private database error") {
		t.Fatalf("status/body = %d %s", response.Code, response.Body.String())
	}
}

func TestBrowserSessionRefreshes(t *testing.T) {
	user := kafkaesque_auth.User{ID: "user-1", Username: "super_admin", Role: "super_admin"}
	authentication := &fakeAuthManager{
		authErr:   kafkaesque_auth.ErrInvalidToken,
		csrfValid: true,
		tokens: kafkaesque_auth.Tokens{
			AccessToken: "new-access", RefreshToken: "new-refresh", CSRFToken: "new-csrf",
			AccessExpiresAt: time.Now().Add(time.Minute), RefreshExpiresAt: time.Now().Add(time.Hour), User: user,
		},
	}
	handler := newHandlerWithAuth(&fakeDataSource{}, authentication, false, nil)
	request := httptest.NewRequest(http.MethodGet, "/ui", nil)
	request.AddCookie(&http.Cookie{Name: kafkaesque_auth.RefreshCookie, Value: "refresh"})
	request.AddCookie(&http.Cookie{Name: kafkaesque_auth.CSRFCookie, Value: "csrf"})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || len(response.Result().Cookies()) != 3 {
		t.Fatalf("refresh status = %d, cookies = %d", response.Code, len(response.Result().Cookies()))
	}
}
