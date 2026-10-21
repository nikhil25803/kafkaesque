package apis

import (
	"bytes"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	kafkaesque_auth "github.com/nikhil25803/kafkaesque/internals/auth"
)

func TestUserRoutesAllowEveryRoleWithoutCheckingKafka(t *testing.T) {
	for _, role := range []string{"super_admin", "admin", "user"} {
		authentication := &fakeAuthManager{
			user: kafkaesque_auth.User{ID: role + "-1", Username: role, Role: role},
		}
		handler, transport := authenticatedTestHandler(authentication, nil)
		request := httptest.NewRequest(http.MethodGet, "/api/v1/users", nil)
		request.Header.Set("Authorization", "Bearer token")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)

		if response.Code != http.StatusOK || authentication.listCalls != 1 {
			t.Fatalf("role=%s response=%d list=%d body=%s", role, response.Code, authentication.listCalls, response.Body.String())
		}
		if transport.metadataRequests != 0 {
			t.Fatalf("role=%s metadata requests=%d", role, transport.metadataRequests)
		}
	}
}

func TestUserPermissionsAndRoleAdministration(t *testing.T) {
	tests := []struct {
		name   string
		user   kafkaesque_auth.User
		method string
		path   string
		want   int
	}{
		{
			name: "missing view", user: kafkaesque_auth.User{ID: "custom-1", Role: "custom"},
			method: http.MethodGet, path: "/api/v1/users", want: http.StatusForbidden,
		},
		{
			name: "admin cannot delete", user: kafkaesque_auth.User{ID: "admin-1", Role: "admin"},
			method: http.MethodDelete, path: "/api/v1/users/user-1", want: http.StatusForbidden,
		},
		{
			name: "user cannot manage roles", user: kafkaesque_auth.User{ID: "user-1", Role: "user"},
			method: http.MethodGet, path: "/api/v1/roles", want: http.StatusForbidden,
		},
		{
			name: "super admin lists roles", user: kafkaesque_auth.User{ID: "super-1", Role: "super_admin"},
			method: http.MethodGet, path: "/api/v1/roles", want: http.StatusOK,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authentication := &fakeAuthManager{user: test.user}
			handler, transport := authenticatedTestHandler(authentication, nil)
			request := httptest.NewRequest(test.method, test.path, nil)
			request.Header.Set("Authorization", "Bearer token")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.want {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			if transport.metadataRequests != 0 {
				t.Fatalf("metadata requests=%d", transport.metadataRequests)
			}
		})
	}
}

func TestUserRoutesRequireAuthenticationAndCookieCSRF(t *testing.T) {
	authentication := &fakeAuthManager{
		user: kafkaesque_auth.User{ID: "super-1", Role: "super_admin"},
	}
	handler, _ := authenticatedTestHandler(authentication, nil)

	unauthenticated := httptest.NewRequest(http.MethodGet, "/api/v1/users", nil)
	authentication.authErr = kafkaesque_auth.ErrInvalidToken
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, unauthenticated)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d", response.Code)
	}

	authentication.authErr = nil
	request := httptest.NewRequest(http.MethodPost, "/api/v1/users", strings.NewReader(`{}`))
	request.AddCookie(&http.Cookie{Name: kafkaesque_auth.AccessCookie, Value: "access"})
	request.AddCookie(&http.Cookie{Name: kafkaesque_auth.CSRFCookie, Value: "csrf"})
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden || authentication.createCalls != 0 {
		t.Fatalf("CSRF response = %d, create calls = %d", response.Code, authentication.createCalls)
	}
}

func TestListUsersReturnsPaginatedAccounts(t *testing.T) {
	createdAt := time.Date(2026, 10, 21, 1, 2, 3, 0, time.UTC)
	authentication := &fakeAuthManager{
		user: kafkaesque_auth.User{ID: "user-1", Username: "viewer", Role: "user"},
		listedUsers: []kafkaesque_auth.ManagedUser{{
			ID: "user-1", Name: "Jane Doe", Username: "jane", Email: "jane@example.com",
			Role: "user", CreatedAt: createdAt, UpdatedAt: createdAt,
		}},
		usersTotal: 26,
	}
	handler, transport := authenticatedTestHandler(authentication, nil)
	request := httptest.NewRequest(http.MethodGet, "/api/v1/users?page=2&page_size=10", nil)
	request.Header.Set("Authorization", "Bearer token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	body := response.Body.String()
	for _, expected := range []string{
		`"success":true`,
		`"status":200`,
		`"username":"jane"`,
		`"email":"jane@example.com"`,
		`"page":2`,
		`"page_size":10`,
		`"total_items":26`,
		`"total_pages":3`,
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("response missing %s: %s", expected, body)
		}
	}
	if strings.Contains(body, `"active"`) {
		t.Fatalf("response exposes inactive-account state: %s", body)
	}
	if transport.metadataRequests != 0 || authentication.listCalls != 1 {
		t.Fatalf("metadata=%d list=%d", transport.metadataRequests, authentication.listCalls)
	}
}

func TestCreateUserReturnsCreatedAccountAndSafeLog(t *testing.T) {
	var logs bytes.Buffer
	createdAt := time.Date(2026, 10, 21, 1, 2, 3, 0, time.UTC)
	authentication := &fakeAuthManager{
		user: kafkaesque_auth.User{ID: "admin-1", Username: "admin", Role: "admin"},
		createdUser: kafkaesque_auth.ManagedUser{
			ID: "user-1", Name: "Jane Doe", Username: "jane", Email: "jane@example.com",
			Role: "user", CreatedAt: createdAt, UpdatedAt: createdAt,
		},
	}
	handler, transport := authenticatedTestHandler(
		authentication,
		slog.New(slog.NewJSONHandler(&logs, nil)),
	)
	password := userTestPassword()
	body := `{"name":"Jane Doe","username":"jane","email":"jane@example.com","password":"` + password + `"}`
	request := httptest.NewRequest(http.MethodPost, "/api/v1/users", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusCreated || !strings.Contains(response.Body.String(), `"status":201`) {
		t.Fatalf("response = %d %s", response.Code, response.Body.String())
	}
	if authentication.createCalls != 1 || authentication.createInput.Password != password {
		t.Fatalf("create calls/input = %d %+v", authentication.createCalls, authentication.createInput)
	}
	if transport.metadataRequests != 0 {
		t.Fatalf("metadata requests = %d", transport.metadataRequests)
	}
	logOutput := logs.String()
	if !strings.Contains(logOutput, `"msg":"user_created"`) || strings.Contains(logOutput, password) || strings.Contains(logOutput, "jane@example.com") {
		t.Fatalf("unsafe or missing creation log: %s", logOutput)
	}
}

func TestCreateUserRejectsInvalidJSON(t *testing.T) {
	authentication := &fakeAuthManager{
		user: kafkaesque_auth.User{ID: "super-1", Role: "super_admin"},
	}
	handler, _ := authenticatedTestHandler(authentication, nil)

	for _, body := range []string{
		`{"name":"Jane","unknown":true}`,
		`{} {}`,
		`{"name":`,
		`{"name":"` + strings.Repeat("x", 1<<20) + `"}`,
	} {
		request := httptest.NewRequest(http.MethodPost, "/api/v1/users", strings.NewReader(body))
		request.Header.Set("Authorization", "Bearer token")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)

		if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), `"code":"invalid_request"`) {
			t.Fatalf("body %q response = %d %s", body, response.Code, response.Body.String())
		}
	}
	if authentication.createCalls != 0 {
		t.Fatalf("create calls = %d", authentication.createCalls)
	}
}

func TestUpdateUserRejectsInactiveState(t *testing.T) {
	authentication := &fakeAuthManager{
		user: kafkaesque_auth.User{ID: "super-1", Role: "super_admin"},
	}
	handler, _ := authenticatedTestHandler(authentication, nil)
	request := httptest.NewRequest(http.MethodPatch, "/api/v1/users/user-1", strings.NewReader(`{"active":false}`))
	request.Header.Set("Authorization", "Bearer token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), `"code":"invalid_request"`) {
		t.Fatalf("response = %d %s", response.Code, response.Body.String())
	}
}

func TestCreateUserMapsServiceErrors(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{name: "validation", err: &kafkaesque_auth.ValidationError{Field: "role", Message: "must be admin or user"}, status: 400, code: "invalid_request"},
		{name: "username", err: kafkaesque_auth.ErrUsernameExists, status: 409, code: "username_exists"},
		{name: "email", err: kafkaesque_auth.ErrEmailExists, status: 409, code: "email_exists"},
		{name: "database", err: errors.New("private database error"), status: 503, code: "database_unavailable"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authentication := &fakeAuthManager{
				user:          kafkaesque_auth.User{ID: "super-1", Role: "super_admin"},
				createUserErr: test.err,
			}
			handler, _ := authenticatedTestHandler(authentication, nil)
			body := `{"name":"Jane Doe","username":"jane","email":"jane@example.com","password":"` +
				userTestPassword() + `"}`
			request := httptest.NewRequest(http.MethodPost, "/api/v1/users", strings.NewReader(body))
			request.Header.Set("Authorization", "Bearer token")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)

			if response.Code != test.status || !strings.Contains(response.Body.String(), `"code":"`+test.code+`"`) {
				t.Fatalf("response = %d %s", response.Code, response.Body.String())
			}
			if strings.Contains(response.Body.String(), "private database error") {
				t.Fatalf("response exposed database error: %s", response.Body.String())
			}
		})
	}
}

func userTestPassword() string {
	return string([]rune{'A', 'a', '1', '_'}) + strings.Repeat("x", 8)
}

func TestListUsersMapsDatabaseFailure(t *testing.T) {
	authentication := &fakeAuthManager{
		user:         kafkaesque_auth.User{ID: "super-1", Role: "super_admin"},
		listUsersErr: errors.New("private database error"),
	}
	handler, _ := authenticatedTestHandler(authentication, nil)
	request := httptest.NewRequest(http.MethodGet, "/api/v1/users", nil)
	request.Header.Set("Authorization", "Bearer token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusServiceUnavailable || strings.Contains(response.Body.String(), "private database error") {
		t.Fatalf("response = %d %s", response.Code, response.Body.String())
	}
}
