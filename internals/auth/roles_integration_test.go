package auth

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestPostgreSQLAccountRBACLifecycle(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	resetAuthenticationDatabase(t, databaseURL)
	ctx := context.Background()
	service := openTestService(t, ctx, testAuthenticationConfig(databaseURL))
	defer service.Close()

	superTokens, err := service.Login(ctx, "super_admin", integrationPassword("first"))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(superTokens.User.Permissions, accountPermissions) {
		t.Fatalf("Super Admin permissions = %v", superTokens.User.Permissions)
	}
	if _, err := service.DeleteUser(ctx, superTokens.User, superTokens.User.ID); !errors.Is(err, ErrForbidden) {
		t.Fatalf("Super Admin deletion error = %v", err)
	}

	role, err := service.CreateRole(ctx, superTokens.User, RoleInput{
		Name:        "support_team",
		Permissions: []string{PermissionUserView, PermissionUserCreate},
	})
	if err != nil {
		t.Fatal(err)
	}
	if role.System || role.Name != "support_team" || role.UserCount != 0 {
		t.Fatalf("created role = %+v", role)
	}

	customUser, err := service.CreateUser(ctx, superTokens.User, CreateUserInput{
		Name: "Support User", Username: "support", Email: "support@example.com",
		Password: integrationPassword("support"), Role: role.Name,
	})
	if err != nil {
		t.Fatal(err)
	}
	customTokens, err := service.Login(ctx, customUser.Username, integrationPassword("support"))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(customTokens.User.Permissions, []string{PermissionUserCreate, PermissionUserView}) {
		t.Fatalf("custom permissions = %v", customTokens.User.Permissions)
	}
	if err := service.DeleteRole(ctx, superTokens.User, role.ID); !errors.Is(err, ErrRoleAssigned) {
		t.Fatalf("assigned role deletion error = %v", err)
	}

	created, err := service.CreateUser(ctx, customTokens.User, CreateUserInput{
		Name: "Managed User", Username: "managed", Email: "managed@example.com",
		Password: integrationPassword("managed"), Role: "user",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateUser(ctx, customTokens.User, CreateUserInput{
		Name: "Escalated User", Username: "escalated", Email: "escalated@example.com",
		Password: integrationPassword("escalated"), Role: "admin",
	}); !errors.Is(err, ErrForbidden) {
		t.Fatalf("role escalation error = %v", err)
	}

	role, err = service.UpdateRole(ctx, superTokens.User, role.ID, RoleInput{
		Name:        "support_team",
		Permissions: []string{PermissionUserView, PermissionUserEdit},
	})
	if err != nil {
		t.Fatal(err)
	}
	currentCustom, err := service.AuthenticateAccess(ctx, customTokens.AccessToken)
	if err != nil || !reflect.DeepEqual(currentCustom.Permissions, []string{PermissionUserEdit, PermissionUserView}) {
		t.Fatalf("updated access user=%+v error=%v", currentCustom, err)
	}
	name := "Managed User Updated"
	if _, err := service.UpdateUser(ctx, currentCustom, created.ID, UpdateUserInput{Name: &name}); err != nil {
		t.Fatal(err)
	}

	managedTokens, err := service.Login(ctx, created.Username, integrationPassword("managed"))
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := service.DeleteUser(ctx, superTokens.User, created.ID)
	if err != nil || deleted.ID != created.ID {
		t.Fatalf("deleted user=%+v error=%v", deleted, err)
	}
	if _, err := service.AuthenticateAccess(ctx, managedTokens.AccessToken); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("revoked access error = %v", err)
	}
	if _, err := service.GetUser(ctx, created.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted user lookup error = %v", err)
	}
	for table, query := range map[string]string{
		"sessions":     "SELECT count(*) FROM auth_sessions WHERE user_id=$1",
		"role links":   "SELECT count(*) FROM user_roles WHERE user_id=$1",
		"user records": "SELECT count(*) FROM users WHERE id=$1",
	} {
		var count int
		if err := service.pool.QueryRow(ctx, query, created.ID).Scan(&count); err != nil || count != 0 {
			t.Fatalf("%s after deletion=%d error=%v", table, count, err)
		}
	}

	if _, err := service.DeleteUser(ctx, superTokens.User, customUser.ID); err != nil {
		t.Fatal(err)
	}
	if storedRole, err := service.GetRole(ctx, role.ID); err != nil || storedRole.Name != role.Name {
		t.Fatalf("custom role after user deletion=%+v error=%v", storedRole, err)
	}
	if err := service.DeleteRole(ctx, superTokens.User, role.ID); err != nil {
		t.Fatal(err)
	}
	var permissionLinks int
	if err := service.pool.QueryRow(ctx, "SELECT count(*) FROM role_permissions WHERE role_id=$1", role.ID).Scan(&permissionLinks); err != nil || permissionLinks != 0 {
		t.Fatalf("role permission links after deletion=%d error=%v", permissionLinks, err)
	}
}

func TestPostgreSQLSystemRolesAreImmutable(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	resetAuthenticationDatabase(t, databaseURL)
	ctx := context.Background()
	service := openTestService(t, ctx, testAuthenticationConfig(databaseURL))
	defer service.Close()

	if _, err := service.pool.Exec(ctx, "UPDATE roles SET name='changed' WHERE name='admin'"); err == nil {
		t.Fatal("system role update succeeded")
	}
	if _, err := service.pool.Exec(ctx, "DELETE FROM role_permissions WHERE role_id=(SELECT id FROM roles WHERE name='user')"); err == nil {
		t.Fatal("system role grant deletion succeeded")
	}
	if _, err := service.pool.Exec(ctx, "UPDATE permissions SET action='changed' WHERE action='view'"); err == nil {
		t.Fatal("permission catalog update succeeded")
	}
	if _, err := service.pool.Exec(ctx, "ALTER TABLE roles DISABLE TRIGGER protect_system_role_update"); err != nil {
		t.Fatal(err)
	}
	if err := verifySchema(ctx, service.pool); err == nil {
		t.Fatal("schema verification accepted a disabled system-role guard")
	}
}
