package ui

import (
	"bytes"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	kafkaesque_auth "github.com/nikhil25803/kafkaesque/internals/auth"
)

func TestUsersPageRendersAccountsAndNavigation(t *testing.T) {
	createdAt := time.Date(2026, 10, 21, 1, 2, 3, 0, time.UTC)
	authentication := &fakeAuthManager{
		user: kafkaesque_auth.User{ID: "super-1", Username: "super_admin", Role: "super_admin"},
		users: []kafkaesque_auth.ManagedUser{{
			ID: "super-1", Name: "Super Admin", Username: "super_admin",
			Email: "super-admin@example.com", Role: "super_admin",
			CreatedAt: createdAt, UpdatedAt: createdAt,
		}, {
			ID: "user-1", Name: "Jane Doe", Username: "jane",
			Email: "jane@example.com", Role: "user",
			CreatedAt: createdAt, UpdatedAt: createdAt,
		}},
		usersTotal: 2,
	}
	source := &fakeDataSource{}
	handler := newHandlerWithAuth(source, authentication, false, nil)
	request := authenticatedUIRequest(http.MethodGet, "/ui/settings/users?created=1", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	requireContains(
		t,
		response.Body.String(),
		`aria-current="page"`,
		">Settings</span>",
		`href="/ui/settings/users/new"`,
		"+ Add user",
		"All users",
		"User created successfully.",
		"Super Admin",
		"super-admin@example.com",
		`datetime="2026-10-21T01:02:03Z"`,
		"21 Oct 2026, 01:02 UTC",
		`<span class="visually-hidden">Actions</span>`,
		`class="action-menu-trigger"`,
		`popover="auto"`,
		`aria-label="Actions for Jane Doe"`,
		"Permanently delete this user?",
	)
	if strings.Contains(response.Body.String(), `<th scope="col">Status</th>`) || strings.Contains(response.Body.String(), "Active account") {
		t.Fatal("users page exposes inactive-account controls")
	}
	if strings.Contains(response.Body.String(), `data-user-form`) || strings.Contains(response.Body.String(), "Accounts are provisioned only") {
		t.Fatal("users list unexpectedly contains the creation form")
	}
	if authentication.listPage != 1 || authentication.listSize != usersPageSize {
		t.Fatalf("page=%d size=%d", authentication.listPage, authentication.listSize)
	}
	if source.dashboardCalls != 0 || source.groupCalls != 0 {
		t.Fatalf("Kafka data source calls: dashboard=%d group=%d", source.dashboardCalls, source.groupCalls)
	}
}

func TestNewUserPageRendersCenteredFormWithoutListingUsers(t *testing.T) {
	authentication := &fakeAuthManager{
		user: kafkaesque_auth.User{ID: "super-1", Username: "super_admin", Role: "super_admin"},
	}
	handler := newHandlerWithAuth(&fakeDataSource{}, authentication, false, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, authenticatedUIRequest(http.MethodGet, "/ui/settings/users/new", nil))

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	body := response.Body.String()
	requireContains(
		t,
		body,
		`class="user-create-shell"`,
		`action="/ui/settings/users"`,
		`href="/ui/settings/users">Cancel</a>`,
		"Create an account that can sign in immediately",
		"Use email as username",
		"8–99 characters",
		"One uppercase letter",
		"One lowercase letter",
		"One number",
		"One of !@#$%^&amp;*()_",
		`role="progressbar"`,
		`aria-valuenow="0"`,
		`aria-valuetext="0 of 5 password requirements met"`,
	)

	orderedFields := []string{
		`name="name"`,
		`name="email"`,
		`name="username"`,
		`name="use_email_as_username"`,
		`name="password"`,
		`data-password-meter`,
		`name="role"`,
		`<button class="primary-button" type="submit">Create user</button>`,
	}
	previous := -1
	for _, field := range orderedFields {
		index := strings.Index(body, field)
		if index <= previous {
			t.Fatalf("field %q is out of order", field)
		}
		previous = index
	}
	if strings.Count(body, `class="password-meter-segment critical"`) != 5 {
		t.Fatal("password meter does not start with five unmet segments")
	}
	if strings.Count(body, `data-rule-icon aria-hidden="true">×</span>`) != 5 {
		t.Fatal("password requirements do not start with five cross icons")
	}
	if strings.Contains(body, `<details class="password-policy" id="password-policy" data-password-policy open`) {
		t.Fatal("password requirements should be collapsed initially")
	}
	if authentication.listCalls != 0 {
		t.Fatalf("new user page listed users %d times", authentication.listCalls)
	}
}

func TestUserPasswordFeedbackUpdatesEveryRuleAndProgress(t *testing.T) {
	script, err := assets.ReadFile("static/app.js")
	if err != nil {
		t.Fatal(err)
	}

	requireContains(
		t,
		string(script),
		"length: length >= 8 && length <= 99",
		"uppercase: /[A-Z]/.test(value)",
		"lowercase: /[a-z]/.test(value)",
		"number: /[0-9]/.test(value)",
		"special: /[!@#$%^&*()_]/.test(value)",
		"item.querySelector('[data-rule-icon]').textContent = met ? '✓' : '×'",
		"segment.classList.toggle('healthy', met)",
		"meter.setAttribute('aria-valuenow', String(metCount))",
		"meter.setAttribute('aria-valuetext', progress)",
	)
}

func TestActionMenusSupportDismissal(t *testing.T) {
	script, err := assets.ReadFile("static/app.js")
	if err != nil {
		t.Fatal(err)
	}

	requireContains(
		t,
		string(script),
		"const initializeActionMenus",
		"event.target.matches('.action-menu-items[popover]')",
		"event.newState !== 'open'",
		"trigger.getBoundingClientRect()",
		"window.innerHeight - padding",
		"window.innerWidth - menuBounds.width - padding",
		".action-menu-items:popover-open",
		"function syncFilterPageSize",
		`.filter-form input[name="page_size"]`,
	)
}

func TestUsersPagePaginationAndFragment(t *testing.T) {
	authentication := &fakeAuthManager{
		user: kafkaesque_auth.User{ID: "super-1", Role: "super_admin"},
		users: []kafkaesque_auth.ManagedUser{{
			ID: "user-26", Name: "Old User", Username: "old-user", Email: "old@example.com", Role: "user",
		}},
		usersTotal: 26,
	}
	handler := newHandlerWithAuth(&fakeDataSource{}, authentication, false, nil)
	request := authenticatedUIRequest(http.MethodGet, "/ui/settings/users?page=2", nil)
	request.Header.Set("HX-Request", "true")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	body := response.Body.String()
	if response.Code != http.StatusOK || authentication.listPage != 2 || !strings.Contains(body, "26–26 of 26 users") {
		t.Fatalf("status=%d page=%d body=%s", response.Code, authentication.listPage, body)
	}
	if strings.Contains(body, "<!doctype html>") || strings.Contains(body, "Create user") {
		t.Fatalf("fragment contains full page: %s", body)
	}
	requireContains(t, body, "/ui/settings/users?page=1", "Old User")

	authentication.users = []kafkaesque_auth.ManagedUser{}
	request = authenticatedUIRequest(http.MethodGet, "/ui/settings/users?page=99", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "0 of 26 users") {
		t.Fatalf("out-of-range response=%d body=%s", response.Code, response.Body.String())
	}
}

func TestUsersPageSize(t *testing.T) {
	authentication := &fakeAuthManager{
		user: kafkaesque_auth.User{ID: "super-1", Role: "super_admin"},
		users: []kafkaesque_auth.ManagedUser{{
			ID: "user-1", Name: "Jane Doe", Username: "jane", Email: "jane@example.com", Role: "user",
		}},
		usersTotal: 1,
	}
	handler := newHandlerWithAuth(&fakeDataSource{}, authentication, false, nil)

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, authenticatedUIRequest(http.MethodGet, "/ui/settings/users?page_size=50", nil))
	if response.Code != http.StatusOK || authentication.listSize != 50 {
		t.Fatalf("status=%d page size=%d body=%s", response.Code, authentication.listSize, response.Body.String())
	}
	requireContains(t, response.Body.String(), `name="page_size"`, `<option value="50" selected>50</option>`, `name="page" value="1"`)

	response = httptest.NewRecorder()
	handler.ServeHTTP(response, authenticatedUIRequest(http.MethodGet, "/ui/settings/users?page_size=20", nil))
	if response.Code != http.StatusBadRequest {
		t.Fatalf("invalid page size status=%d", response.Code)
	}
}

func TestUsersPageEmptyState(t *testing.T) {
	authentication := &fakeAuthManager{
		user:  kafkaesque_auth.User{ID: "super-1", Role: "super_admin"},
		users: []kafkaesque_auth.ManagedUser{},
	}
	handler := newHandlerWithAuth(&fakeDataSource{}, authentication, false, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, authenticatedUIRequest(http.MethodGet, "/ui/settings/users", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
	requireContains(t, response.Body.String(), "No users found", "0 users")
}

func TestUsersPageRejectsInvalidPageAndAllowsEveryRole(t *testing.T) {
	t.Run("invalid page", func(t *testing.T) {
		authentication := &fakeAuthManager{user: kafkaesque_auth.User{ID: "super-1", Role: "super_admin"}}
		handler := newHandlerWithAuth(&fakeDataSource{}, authentication, false, nil)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, authenticatedUIRequest(http.MethodGet, "/ui/settings/users?page=0", nil))
		if response.Code != http.StatusBadRequest || authentication.listCalls != 0 {
			t.Fatalf("status=%d list calls=%d", response.Code, authentication.listCalls)
		}
	})

	for _, role := range []string{"super_admin", "admin", "user"} {
		t.Run(role, func(t *testing.T) {
			authentication := &fakeAuthManager{user: kafkaesque_auth.User{ID: role + "-1", Role: role}}
			handler := newHandlerWithAuth(&fakeDataSource{}, authentication, false, nil)

			response := httptest.NewRecorder()
			handler.ServeHTTP(response, authenticatedUIRequest(http.MethodGet, "/ui/settings/users", nil))
			if response.Code != http.StatusOK || authentication.listCalls != 1 {
				t.Fatalf("role=%s status=%d list calls=%d", role, response.Code, authentication.listCalls)
			}
			requireContains(t, response.Body.String(), ">Settings</span>", ">Users</a>")

			response = httptest.NewRecorder()
			handler.ServeHTTP(response, authenticatedUIRequest(http.MethodGet, "/ui/settings/users/new", nil))
			want := http.StatusOK
			if role == "user" {
				want = http.StatusForbidden
			}
			if response.Code != want || authentication.listCalls != 1 {
				t.Fatalf("new page role=%s status=%d list calls=%d", role, response.Code, authentication.listCalls)
			}
		})
	}
}

func TestCreateUserRedirectsAndSupportsEmailUsername(t *testing.T) {
	var logs bytes.Buffer
	authentication := &fakeAuthManager{
		user:      kafkaesque_auth.User{ID: "admin-1", Role: "admin"},
		csrfValid: true,
		created:   kafkaesque_auth.ManagedUser{ID: "user-1", Role: "user"},
	}
	handler := newHandlerWithAuth(
		&fakeDataSource{},
		authentication,
		false,
		slog.New(slog.NewJSONHandler(&logs, nil)),
	)
	password := userTestPassword()
	form := url.Values{
		"csrf_token":            {"csrf"},
		"name":                  {"Jane Doe"},
		"email":                 {" JANE@EXAMPLE.COM "},
		"username":              {"ignored"},
		"password":              {password},
		"use_email_as_username": {"true"},
	}
	request := authenticatedUIRequest(http.MethodPost, "/ui/settings/users", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusSeeOther || response.Header().Get("Location") != "/ui/settings/users?created=1" {
		t.Fatalf("response=%d location=%q", response.Code, response.Header().Get("Location"))
	}
	if authentication.createCalls != 1 || authentication.createInput.Username != "jane@example.com" || authentication.createInput.Role != "user" {
		t.Fatalf("create input = %+v", authentication.createInput)
	}
	if authentication.createInput.Password != password {
		t.Fatal("password was not passed to the service")
	}
	if strings.Contains(logs.String(), password) || strings.Contains(logs.String(), "jane@example.com") {
		t.Fatalf("creation log exposed submitted values: %s", logs.String())
	}

	form = url.Values{
		"csrf_token": {"csrf"}, "name": {"Admin User"}, "username": {"admin-user"},
		"email": {"admin@example.com"}, "password": {password}, "role": {"admin"},
	}
	authentication.user = kafkaesque_auth.User{ID: "super-1", Role: "super_admin"}
	request = authenticatedUIRequest(http.MethodPost, "/ui/settings/users", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusSeeOther || authentication.createInput.Role != "admin" {
		t.Fatalf("Admin creation response=%d input=%+v", response.Code, authentication.createInput)
	}
}

func TestCreateUserPreservesSafeValuesButClearsPassword(t *testing.T) {
	password := userTestPassword()
	tests := []struct {
		name   string
		err    error
		status int
		text   string
	}{
		{name: "validation", err: &kafkaesque_auth.ValidationError{Field: "role", Message: "must be admin or user"}, status: 400, text: "role must be admin or user"},
		{name: "username conflict", err: kafkaesque_auth.ErrUsernameExists, status: 409, text: "Username already exists."},
		{name: "email conflict", err: kafkaesque_auth.ErrEmailExists, status: 409, text: "Email already exists."},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authentication := &fakeAuthManager{
				user:       kafkaesque_auth.User{ID: "super-1", Role: "super_admin"},
				csrfValid:  true,
				createErr:  test.err,
				users:      []kafkaesque_auth.ManagedUser{},
				usersTotal: 0,
			}
			handler := newHandlerWithAuth(&fakeDataSource{}, authentication, false, nil)
			form := url.Values{
				"csrf_token": {"csrf"}, "name": {"Jane Doe"}, "username": {"jane"},
				"email": {"jane@example.com"}, "password": {password}, "role": {"admin"},
			}
			request := authenticatedUIRequest(http.MethodPost, "/ui/settings/users", strings.NewReader(form.Encode()))
			request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)

			body := response.Body.String()
			if response.Code != test.status || !strings.Contains(body, test.text) {
				t.Fatalf("response=%d body=%s", response.Code, body)
			}
			requireContains(t, body, `value="Jane Doe"`, `value="jane"`, `value="jane@example.com"`)
			if strings.Contains(body, password) {
				t.Fatal("validation response exposed the submitted password")
			}
			if authentication.listCalls != 0 {
				t.Fatalf("validation response listed users %d times", authentication.listCalls)
			}
		})
	}
}

func TestUsersDatabaseFailuresAreUnavailable(t *testing.T) {
	t.Run("list", func(t *testing.T) {
		authentication := &fakeAuthManager{
			user:    kafkaesque_auth.User{ID: "super-1", Role: "super_admin"},
			listErr: errors.New("private database error"),
		}
		handler := newHandlerWithAuth(&fakeDataSource{}, authentication, false, nil)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, authenticatedUIRequest(http.MethodGet, "/ui/settings/users", nil))
		if response.Code != http.StatusServiceUnavailable || strings.Contains(response.Body.String(), "private database error") {
			t.Fatalf("response=%d body=%s", response.Code, response.Body.String())
		}
	})

	t.Run("create", func(t *testing.T) {
		authentication := &fakeAuthManager{
			user:      kafkaesque_auth.User{ID: "super-1", Role: "super_admin"},
			csrfValid: true,
			createErr: errors.New("private database error"),
		}
		handler := newHandlerWithAuth(&fakeDataSource{}, authentication, false, nil)
		form := url.Values{"csrf_token": {"csrf"}}
		request := authenticatedUIRequest(http.MethodPost, "/ui/settings/users", strings.NewReader(form.Encode()))
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusServiceUnavailable || strings.Contains(response.Body.String(), "private database error") {
			t.Fatalf("response=%d body=%s", response.Code, response.Body.String())
		}
	})
}

func TestUserCreationRequiresCSRF(t *testing.T) {
	authentication := &fakeAuthManager{
		user: kafkaesque_auth.User{ID: "super-1", Role: "super_admin"},
	}
	handler := newHandlerWithAuth(&fakeDataSource{}, authentication, false, nil)
	request := authenticatedUIRequest(http.MethodPost, "/ui/settings/users", strings.NewReader(url.Values{}.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden || authentication.createCalls != 0 {
		t.Fatalf("response=%d create calls=%d", response.Code, authentication.createCalls)
	}
}

func authenticatedUIRequest(method, target string, body *strings.Reader) *http.Request {
	var request *http.Request
	if body == nil {
		request = httptest.NewRequest(method, target, nil)
	} else {
		request = httptest.NewRequest(method, target, body)
	}
	request.AddCookie(&http.Cookie{Name: kafkaesque_auth.AccessCookie, Value: "access"})
	request.AddCookie(&http.Cookie{Name: kafkaesque_auth.CSRFCookie, Value: "csrf"})
	return request
}

func userTestPassword() string {
	return string([]rune{'A', 'a', '1', '_'}) + strings.Repeat("x", 8)
}
