package auth

import "slices"

const (
	PermissionUserView   = "user:account:view"
	PermissionUserCreate = "user:account:create"
	PermissionUserEdit   = "user:account:edit"
	PermissionUserDelete = "user:account:delete"
)

var accountPermissions = []string{
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
