package ui

import (
	"errors"
	"log/slog"
	"net/http"
	"strings"

	kafkaesque_auth "github.com/nikhil25803/kafkaesque/internals/auth"
)

type rolesPage struct {
	Navigation navigationView
	Roles      []roleView
	Success    string
}

type roleView struct {
	kafkaesque_auth.ManagedRole
	PermissionGroups []permissionGroupView
}

type permissionGroupView struct {
	Name        string
	Permissions []string
}

type roleFormPage struct {
	Navigation navigationView
	Role       kafkaesque_auth.ManagedRole
	Editing    bool
	Error      string
}

func renderRoles(manager kafkaesque_auth.RoleManager, logger *slog.Logger) http.HandlerFunc {
	logger = uiLogger(logger)
	return func(w http.ResponseWriter, r *http.Request) {
		roles, err := manager.ListRoles(r.Context())
		if err != nil {
			logger.Error("role_list_failed", "reason", "database_unavailable")
			renderRoleError(w, r, err)
			return
		}
		views := make([]roleView, 0, len(roles))
		for _, role := range roles {
			views = append(views, roleView{
				ManagedRole:      role,
				PermissionGroups: rolePermissionGroups(role.Permissions),
			})
		}
		view := rolesPage{Navigation: navigationFor(r, "roles"), Roles: views}
		if r.URL.Query().Get("saved") == "1" {
			view.Success = "Role saved successfully."
		}
		if r.URL.Query().Get("deleted") == "1" {
			view.Success = "Role deleted successfully."
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_ = dashboardTemplate.ExecuteTemplate(w, "roles", view)
	}
}

func renderNewRole(w http.ResponseWriter, r *http.Request) {
	renderRoleForm(w, r, http.StatusOK, roleFormPage{})
}

func renderEditRole(manager kafkaesque_auth.RoleManager, _ *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		role, err := manager.GetRole(r.Context(), r.PathValue("id"))
		if err != nil {
			renderRoleError(w, r, err)
			return
		}
		if role.System {
			renderRoleError(w, r, kafkaesque_auth.ErrForbidden)
			return
		}
		renderRoleForm(w, r, http.StatusOK, roleFormPage{Role: role, Editing: true})
	}
}

func createRoleUI(manager kafkaesque_auth.RoleManager, logger *slog.Logger) http.HandlerFunc {
	return saveRoleUI(manager, logger, false)
}

func updateRoleUI(manager kafkaesque_auth.RoleManager, logger *slog.Logger) http.HandlerFunc {
	return saveRoleUI(manager, logger, true)
}

func saveRoleUI(manager kafkaesque_auth.RoleManager, logger *slog.Logger, editing bool) http.HandlerFunc {
	logger = uiLogger(logger)
	return func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			renderRoleForm(w, r, http.StatusBadRequest, roleFormPage{Editing: editing, Error: "Invalid role request."})
			return
		}
		actor, _ := kafkaesque_auth.UserFromContext(r.Context())
		input := kafkaesque_auth.RoleInput{Name: r.FormValue("name"), Permissions: r.Form["permissions"]}
		var role kafkaesque_auth.ManagedRole
		var err error
		if editing {
			role, err = manager.UpdateRole(r.Context(), actor, r.PathValue("id"), input)
		} else {
			role, err = manager.CreateRole(r.Context(), actor, input)
		}
		if err != nil {
			logger.Warn("role_save_rejected", "actor_user_id", actor.ID, "reason", "invalid_or_forbidden")
			status, message := roleUIError(err)
			view := roleFormPage{
				Role:    kafkaesque_auth.ManagedRole{ID: r.PathValue("id"), Name: strings.TrimSpace(input.Name), Permissions: input.Permissions},
				Editing: editing, Error: message,
			}
			renderRoleForm(w, r, status, view)
			return
		}
		logger.Info("role_saved", "actor_user_id", actor.ID, "role_id", role.ID, "role", role.Name)
		http.Redirect(w, r, "/ui/roles?saved=1", http.StatusSeeOther)
	}
}

func deleteRoleUI(manager kafkaesque_auth.RoleManager, logger *slog.Logger) http.HandlerFunc {
	logger = uiLogger(logger)
	return func(w http.ResponseWriter, r *http.Request) {
		actor, _ := kafkaesque_auth.UserFromContext(r.Context())
		id := r.PathValue("id")
		if err := manager.DeleteRole(r.Context(), actor, id); err != nil {
			renderRoleError(w, r, err)
			return
		}
		logger.Info("role_deleted", "actor_user_id", actor.ID, "role_id", id)
		http.Redirect(w, r, "/ui/roles?deleted=1", http.StatusSeeOther)
	}
}

func renderRoleForm(w http.ResponseWriter, r *http.Request, status int, view roleFormPage) {
	view.Navigation = navigationFor(r, "roles")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_ = dashboardTemplate.ExecuteTemplate(w, "role-form", view)
}

func renderRoleError(w http.ResponseWriter, r *http.Request, err error) {
	status, message := roleUIError(err)
	title := "Roles unavailable"
	if status == http.StatusNotFound {
		title = "Role not found"
	} else if status == http.StatusForbidden {
		title = "Permission denied"
	} else if status == http.StatusConflict {
		title = "Role conflict"
	}
	renderUIError(w, r, status, "roles", title, message)
}

func roleUIError(err error) (int, string) {
	var validationError *kafkaesque_auth.ValidationError
	switch {
	case errors.As(err, &validationError):
		return http.StatusBadRequest, validationError.Error()
	case errors.Is(err, kafkaesque_auth.ErrForbidden):
		return http.StatusForbidden, "Built-in roles cannot be changed."
	case errors.Is(err, kafkaesque_auth.ErrNotFound):
		return http.StatusNotFound, "The requested role does not exist."
	case errors.Is(err, kafkaesque_auth.ErrRoleExists):
		return http.StatusConflict, "A role with this name already exists."
	case errors.Is(err, kafkaesque_auth.ErrRoleAssigned):
		return http.StatusConflict, "Reassign this role's users before deleting it."
	default:
		return http.StatusServiceUnavailable, "The role database is unavailable."
	}
}

func permissionChecked(role kafkaesque_auth.ManagedRole, permission string) bool {
	for _, value := range role.Permissions {
		if value == permission {
			return true
		}
	}
	return false
}

func rolePermissionGroups(permissions []string) []permissionGroupView {
	labels := []struct {
		key   string
		label string
	}{
		{key: kafkaesque_auth.PermissionUserView, label: "List Users"},
		{key: kafkaesque_auth.PermissionUserCreate, label: "Create Users"},
		{key: kafkaesque_auth.PermissionUserEdit, label: "Edit Users"},
		{key: kafkaesque_auth.PermissionUserDelete, label: "Delete Users"},
	}

	group := permissionGroupView{Name: "User"}
	for _, item := range labels {
		for _, permission := range permissions {
			if permission == item.key {
				group.Permissions = append(group.Permissions, item.label)
				break
			}
		}
	}
	if len(group.Permissions) == 0 {
		return nil
	}
	return []permissionGroupView{group}
}
