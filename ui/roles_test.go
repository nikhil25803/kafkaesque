package ui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	kafkaesque_auth "github.com/nikhil25803/kafkaesque/internals/auth"
)

func TestRolesPageIsSuperAdminOnly(t *testing.T) {
	for _, test := range []struct {
		role string
		want int
	}{
		{role: "super_admin", want: http.StatusOK},
		{role: "admin", want: http.StatusForbidden},
		{role: "user", want: http.StatusForbidden},
	} {
		authentication := &fakeAuthManager{user: kafkaesque_auth.User{ID: test.role + "-1", Role: test.role}}
		handler := newHandlerWithAuth(&fakeDataSource{}, authentication, false, nil)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, authenticatedUIRequest(http.MethodGet, "/ui/roles", nil))
		if response.Code != test.want {
			t.Fatalf("role=%s status=%d body=%s", test.role, response.Code, response.Body.String())
		}
		if test.want == http.StatusOK {
			body := response.Body.String()
			requireContains(t, body, "Roles", "System", "+ Add role", "User", "List Users", "Create Users", "Edit Users", "Delete Users", "View permissions (3)", "View permissions (4)", `class="permission-disclosure"`, `class="action-menu-trigger"`, `popover="auto"`, `aria-label="Actions for support"`, "/ui/roles/support-role/edit", "/ui/roles/support-role/delete")
			if strings.Contains(body, "Locked") || strings.Contains(body, "user:account:") {
				t.Fatal("system role renders a locked action")
			}
			if strings.Count(body, `class="action-menu-trigger"`) != 1 {
				t.Fatalf("action menu count = %d", strings.Count(body, `class="action-menu-trigger"`))
			}
			if strings.Contains(body, `class="permission-disclosure" open`) {
				t.Fatal("permission disclosure starts expanded")
			}
		}
	}
}

func TestRoleFormGroupsUserPermissions(t *testing.T) {
	authentication := &fakeAuthManager{user: kafkaesque_auth.User{ID: "super-1", Role: "super_admin"}}
	handler := newHandlerWithAuth(&fakeDataSource{}, authentication, false, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, authenticatedUIRequest(http.MethodGet, "/ui/roles/new", nil))

	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	requireContains(t, response.Body.String(), "Permissions", "User", "List Users", "Create Users", "Edit Users")
}
