package ui

import (
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	kafkaesque_auth "github.com/nikhil25803/kafkaesque/internals/auth"
	"github.com/nikhil25803/kafkaesque/internals/credentials"
)

const usersPageSize = 25

type usersPage struct {
	Navigation navigationView
	Results    usersResults
	Success    string
	CanCreate  bool
}

type newUserPage struct {
	Navigation navigationView
	Form       userFormView
}

type userFormView struct {
	ID                 string
	Name               string
	Username           string
	Email              string
	Role               string
	UseEmailAsUsername bool
	Error              string
	CanManageRole      bool
	RoleOptions        []roleOption
	Editing            bool
}

type roleOption struct {
	Value    string
	Label    string
	Selected bool
}

type userView struct {
	ID        string
	Name      string
	Username  string
	Email     string
	Role      string
	Added     string
	AddedAt   string
	CanEdit   bool
	CanDelete bool
}

type usersResults struct {
	Users       []userView
	Range       string
	Pages       []pageLink
	PreviousURL string
	NextURL     string
	CSRFToken   string
	Pagination  pageSizeControl
}

func renderUsers(manager kafkaesque_auth.UserManager, logger *slog.Logger) http.HandlerFunc {
	logger = uiLogger(logger)
	return func(w http.ResponseWriter, r *http.Request) {
		page, err := positivePage(r.URL.Query().Get("page"))
		if err != nil {
			renderUIError(w, r, http.StatusBadRequest, "users", "Invalid users request", err.Error())
			return
		}
		pageSize, err := parsePageSize(r.URL.Query().Get("page_size"), usersPageSize)
		if err != nil {
			renderUIError(w, r, http.StatusBadRequest, "users", "Invalid users request", err.Error())
			return
		}
		results, err := loadUsers(r, manager, page, pageSize)
		if err != nil {
			logger.Error("user_list_failed", "reason", "database_unavailable")
			renderUIError(w, r, http.StatusServiceUnavailable, "users", "Users unavailable", "The user database is unavailable.")
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if strings.EqualFold(r.Header.Get("HX-Request"), "true") {
			if err := dashboardTemplate.ExecuteTemplate(w, "users-results", results); err != nil {
				http.Error(w, "Unable to render users", http.StatusInternalServerError)
			}
			return
		}
		actor, _ := kafkaesque_auth.UserFromContext(r.Context())
		view := usersPage{
			Navigation: navigationFor(r, "users"), Results: results,
			CanCreate: kafkaesque_auth.HasPermission(actor, kafkaesque_auth.PermissionUserCreate),
		}
		switch {
		case r.URL.Query().Get("created") == "1":
			view.Success = "User created successfully."
		case r.URL.Query().Get("updated") == "1":
			view.Success = "User updated successfully."
		case r.URL.Query().Get("deleted") == "1":
			view.Success = "User deleted successfully."
		}
		if err := dashboardTemplate.ExecuteTemplate(w, "users", view); err != nil {
			http.Error(w, "Unable to render users", http.StatusInternalServerError)
		}
	}
}

func renderNewUser(manager kafkaesque_auth.RuntimeManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		form, err := userForm(r, manager, kafkaesque_auth.ManagedUser{Role: "user"})
		if err != nil {
			renderUserServiceError(w, r, err)
			return
		}
		renderNewUserPage(w, r, http.StatusOK, form)
	}
}

func createUserUI(manager kafkaesque_auth.RuntimeManager, logger *slog.Logger) http.HandlerFunc {
	logger = uiLogger(logger)
	return func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			renderNewUserPage(w, r, http.StatusBadRequest, userFormView{Role: "user", Error: "Invalid user request."})
			return
		}
		form := userFormView{
			Name:               strings.TrimSpace(r.FormValue("name")),
			Username:           strings.TrimSpace(r.FormValue("username")),
			Email:              strings.TrimSpace(r.FormValue("email")),
			Role:               strings.ToLower(strings.TrimSpace(r.FormValue("role"))),
			UseEmailAsUsername: r.FormValue("use_email_as_username") == "true",
		}
		if form.Role == "" {
			form.Role = "user"
		}
		if form.UseEmailAsUsername {
			form.Username = strings.ToLower(form.Email)
			if normalized, valid := credentials.NormalizeEmail(form.Email); valid {
				form.Username = normalized
			}
		}
		actor, _ := kafkaesque_auth.UserFromContext(r.Context())
		created, err := manager.CreateUser(r.Context(), actor, kafkaesque_auth.CreateUserInput{
			Name:     form.Name,
			Username: form.Username,
			Email:    form.Email,
			Password: r.FormValue("password"),
			Role:     form.Role,
		})
		if err != nil {
			prepared, prepareErr := userForm(r, manager, kafkaesque_auth.ManagedUser{
				Name:     form.Name,
				Username: form.Username,
				Email:    form.Email,
				Role:     form.Role,
			})
			if prepareErr == nil {
				form.RoleOptions = prepared.RoleOptions
				form.CanManageRole = prepared.CanManageRole
			}
			handleUserCreationError(w, r, logger, form, err)
			return
		}
		logger.Info("user_created", "actor_user_id", actor.ID, "user_id", created.ID, "role", created.Role)
		http.Redirect(w, r, "/ui/users?created=1", http.StatusSeeOther)
	}
}

func renderEditUser(manager kafkaesque_auth.RuntimeManager, logger *slog.Logger) http.HandlerFunc {
	logger = uiLogger(logger)
	return func(w http.ResponseWriter, r *http.Request) {
		user, err := manager.GetUser(r.Context(), r.PathValue("id"))
		if err != nil {
			renderUserServiceError(w, r, err)
			return
		}
		actor, _ := kafkaesque_auth.UserFromContext(r.Context())
		if user.Role == "super_admin" || (actor.Role != "super_admin" && user.Role != "user") {
			renderUserServiceError(w, r, kafkaesque_auth.ErrForbidden)
			return
		}
		form, err := userForm(r, manager, user)
		if err != nil {
			logger.Error("user_edit_failed", "reason", "database_unavailable")
			renderUserServiceError(w, r, err)
			return
		}
		form.Editing = true
		renderNewUserPage(w, r, http.StatusOK, form)
	}
}

func updateUserUI(manager kafkaesque_auth.UserManager, logger *slog.Logger) http.HandlerFunc {
	logger = uiLogger(logger)
	return func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			renderUIError(w, r, http.StatusBadRequest, "users", "Invalid user request", "The submitted form is invalid.")
			return
		}
		name, username, email := r.FormValue("name"), r.FormValue("username"), r.FormValue("email")
		input := kafkaesque_auth.UpdateUserInput{Name: &name, Username: &username, Email: &email}
		actor, _ := kafkaesque_auth.UserFromContext(r.Context())
		if actor.Role == "super_admin" {
			role := r.FormValue("role")
			input.Role = &role
		}
		user, err := manager.UpdateUser(r.Context(), actor, r.PathValue("id"), input)
		if err != nil {
			logger.Warn("user_update_rejected", "actor_user_id", actor.ID, "reason", "invalid_or_forbidden")
			renderUserServiceError(w, r, err)
			return
		}
		logger.Info("user_updated", "actor_user_id", actor.ID, "user_id", user.ID, "role", user.Role)
		http.Redirect(w, r, "/ui/users?updated=1", http.StatusSeeOther)
	}
}

func deleteUserUI(manager kafkaesque_auth.UserManager, logger *slog.Logger) http.HandlerFunc {
	logger = uiLogger(logger)
	return func(w http.ResponseWriter, r *http.Request) {
		actor, _ := kafkaesque_auth.UserFromContext(r.Context())
		user, err := manager.DeleteUser(r.Context(), actor, r.PathValue("id"))
		if err != nil {
			renderUserServiceError(w, r, err)
			return
		}
		logger.Info("user_deleted", "actor_user_id", actor.ID, "user_id", user.ID, "role", user.Role)
		http.Redirect(w, r, "/ui/users?deleted=1", http.StatusSeeOther)
	}
}

func userForm(
	r *http.Request,
	manager kafkaesque_auth.RuntimeManager,
	user kafkaesque_auth.ManagedUser,
) (userFormView, error) {
	actor, _ := kafkaesque_auth.UserFromContext(r.Context())
	form := userFormView{
		ID:            user.ID,
		Name:          user.Name,
		Username:      user.Username,
		Email:         user.Email,
		Role:          user.Role,
		CanManageRole: actor.Role == "super_admin",
	}
	if !form.CanManageRole {
		form.RoleOptions = []roleOption{{Value: "user", Label: "User", Selected: true}}
		return form, nil
	}
	roles, err := manager.ListRoles(r.Context())
	if err != nil {
		return userFormView{}, err
	}
	for _, role := range roles {
		if role.Name != "super_admin" {
			form.RoleOptions = append(form.RoleOptions, roleOption{
				Value:    role.Name,
				Label:    roleLabel(role.Name),
				Selected: role.Name == form.Role,
			})
		}
	}
	return form, nil
}

func handleUserCreationError(
	w http.ResponseWriter,
	r *http.Request,
	logger *slog.Logger,
	form userFormView,
	err error,
) {
	actor, _ := kafkaesque_auth.UserFromContext(r.Context())
	var validationError *kafkaesque_auth.ValidationError
	switch {
	case errors.As(err, &validationError):
		logger.Warn("user_creation_rejected", "actor_user_id", actor.ID, "field", validationError.Field)
		form.Error = validationError.Error()
		renderNewUserPage(w, r, http.StatusBadRequest, form)
	case errors.Is(err, kafkaesque_auth.ErrForbidden):
		renderUIError(w, r, http.StatusForbidden, "users", "Permission denied", "You cannot assign the selected role.")
	case errors.Is(err, kafkaesque_auth.ErrUsernameExists):
		form.Error = "Username already exists."
		renderNewUserPage(w, r, http.StatusConflict, form)
	case errors.Is(err, kafkaesque_auth.ErrEmailExists):
		form.Error = "Email already exists."
		renderNewUserPage(w, r, http.StatusConflict, form)
	default:
		logger.Error("user_creation_failed", "actor_user_id", actor.ID, "reason", "database_unavailable")
		renderUIError(
			w,
			r,
			http.StatusServiceUnavailable,
			"users",
			"User creation unavailable",
			"The user database is unavailable.",
		)
	}
}

func renderUserServiceError(w http.ResponseWriter, r *http.Request, err error) {
	var validationError *kafkaesque_auth.ValidationError
	switch {
	case errors.As(err, &validationError):
		renderUIError(w, r, http.StatusBadRequest, "users", "Invalid user request", validationError.Error())
	case errors.Is(err, kafkaesque_auth.ErrNotFound):
		renderUIError(w, r, http.StatusNotFound, "users", "User not found", "The requested user does not exist.")
	case errors.Is(err, kafkaesque_auth.ErrForbidden):
		renderUIError(w, r, http.StatusForbidden, "users", "Permission denied", "You cannot manage this account.")
	case errors.Is(err, kafkaesque_auth.ErrUsernameExists), errors.Is(err, kafkaesque_auth.ErrEmailExists):
		renderUIError(w, r, http.StatusConflict, "users", "User conflict", "The username or email is already in use.")
	default:
		renderUIError(w, r, http.StatusServiceUnavailable, "users", "Users unavailable", "The user database is unavailable.")
	}
}

func renderNewUserPage(w http.ResponseWriter, r *http.Request, status int, form userFormView) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_ = dashboardTemplate.ExecuteTemplate(w, "new-user", newUserPage{Navigation: navigationFor(r, "users"), Form: form})
}

func loadUsers(r *http.Request, manager kafkaesque_auth.UserManager, page, pageSize int) (usersResults, error) {
	users, total, err := manager.ListUsers(r.Context(), page, pageSize)
	if err != nil {
		return usersResults{}, err
	}
	actor, _ := kafkaesque_auth.UserFromContext(r.Context())
	views := make([]userView, 0, len(users))
	for _, user := range users {
		createdAt := user.CreatedAt.UTC()
		canEdit := kafkaesque_auth.HasPermission(actor, kafkaesque_auth.PermissionUserEdit) &&
			user.Role != "super_admin" &&
			(actor.Role == "super_admin" || user.Role == "user")
		canDelete := actor.Role == "super_admin" &&
			kafkaesque_auth.HasPermission(actor, kafkaesque_auth.PermissionUserDelete) &&
			user.Role != "super_admin"

		views = append(views, userView{
			ID:        user.ID,
			Name:      user.Name,
			Username:  user.Username,
			Email:     user.Email,
			Role:      roleLabel(user.Role),
			Added:     createdAt.Format("02 Jan 2006, 15:04 UTC"),
			AddedAt:   createdAt.Format(time.RFC3339),
			CanEdit:   canEdit,
			CanDelete: canDelete,
		})
	}
	results := paginateUsers(views, total, page, pageSize)
	results.CSRFToken = kafkaesque_auth.CookieValue(r, kafkaesque_auth.CSRFCookie)
	return results, nil
}

func paginateUsers(users []userView, total, page, pageSize int) usersResults {
	totalPages := (total + pageSize - 1) / pageSize
	start := (page - 1) * pageSize
	end := min(start+len(users), total)
	pages := make([]pageLink, 0, totalPages)
	for number := 1; number <= totalPages; number++ {
		pages = append(pages, pageLink{
			Number: number,
			URL:    usersURL(number, pageSize),
			Active: number == page,
		})
	}
	rangeText := "0 users"
	if len(users) > 0 {
		rangeText = strconv.Itoa(start+1) + "–" + strconv.Itoa(end) + " of " + strconv.Itoa(total) + " users"
	} else if total > 0 {
		rangeText = "0 of " + strconv.Itoa(total) + " users"
	}
	results := usersResults{
		Users:      users,
		Range:      rangeText,
		Pages:      pages,
		Pagination: newPageSizeControl("/ui/users", "#users-results", pageSize),
	}
	if page > 1 && page <= totalPages {
		results.PreviousURL = usersURL(page-1, pageSize)
	}
	if page < totalPages {
		results.NextURL = usersURL(page+1, pageSize)
	}
	return results
}

func usersURL(page, pageSize int) string {
	return "/ui/users?page=" + strconv.Itoa(page) + "&page_size=" + strconv.Itoa(pageSize)
}
