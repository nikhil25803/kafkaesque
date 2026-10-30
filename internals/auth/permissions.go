package auth

import "slices"

const (
	PermissionUserView    = "user:account:view"
	PermissionUserCreate  = "user:account:create"
	PermissionUserEdit    = "user:account:edit"
	PermissionUserDelete  = "user:account:delete"
	PermissionAlertView   = "alert:subscription:view"
	PermissionAlertCreate = "alert:subscription:create"
	PermissionAlertEdit   = "alert:subscription:edit"
	PermissionAlertDelete = "alert:subscription:delete"
)

var accountPermissions = []string{
	PermissionAlertCreate,
	PermissionAlertDelete,
	PermissionAlertEdit,
	PermissionAlertView,
	PermissionUserCreate,
	PermissionUserDelete,
	PermissionUserEdit,
	PermissionUserView,
}

// HasPermission reports whether an authenticated user has a permission.
func HasPermission(user User, permission string) bool {
	return slices.Contains(user.Permissions, permission)
}

func isSuperAdmin(user User) bool {
	return user.Role == "super_admin"
}
