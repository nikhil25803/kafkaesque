package auth

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/nikhil25803/kafkaesque/internals/credentials"
)

// ErrUsernameExists indicates that a normalized username is already stored.
var ErrUsernameExists = errors.New("username already exists")

// ErrEmailExists indicates that a normalized email is already stored.
var ErrEmailExists = errors.New("email already exists")

// ManagedUser is an account returned by user-management operations.
type ManagedUser struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Username  string    `json:"username"`
	Email     string    `json:"email"`
	Role      string    `json:"role"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// CreateUserInput contains fields accepted when creating an account.
type CreateUserInput struct {
	Name     string
	Username string
	Email    string
	Password string
	Role     string
}

// UpdateUserInput contains optional fields accepted when updating an account.
type UpdateUserInput struct {
	Name     *string
	Username *string
	Email    *string
	Role     *string
}

// ValidationError identifies an invalid user field without exposing its value.
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("%s %s", e.Field, e.Message)
}

// ListUsers returns one page of accounts ordered by newest first.
func (s *Service) ListUsers(ctx context.Context, page, pageSize int) ([]ManagedUser, int, error) {
	var total int
	if err := s.pool.QueryRow(ctx, "SELECT count(*) FROM users").Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count users: %w", err)
	}

	offset := int64(page-1) * int64(pageSize)
	const query = `
		SELECT u.id, u.name, u.username, u.email, r.name, u.created_at, u.updated_at
		FROM users u
		JOIN user_roles ur ON ur.user_id = u.id
		JOIN roles r ON r.id = ur.role_id
		ORDER BY u.created_at DESC, u.id DESC
		LIMIT $1 OFFSET $2`
	rows, err := s.pool.Query(ctx, query, pageSize, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("list users: %w", err)
	}
	defer rows.Close()

	users := make([]ManagedUser, 0)
	for rows.Next() {
		var user ManagedUser
		if err := rows.Scan(
			&user.ID,
			&user.Name,
			&user.Username,
			&user.Email,
			&user.Role,
			&user.CreatedAt,
			&user.UpdatedAt,
		); err != nil {
			return nil, 0, fmt.Errorf("scan user: %w", err)
		}
		users = append(users, user)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("list users: %w", err)
	}

	return users, total, nil
}

// GetUser returns one managed account by ID.
func (s *Service) GetUser(ctx context.Context, id string) (ManagedUser, error) {
	const query = `
		SELECT u.id, u.name, u.username, u.email, r.name, u.created_at, u.updated_at
		FROM users u
		JOIN user_roles ur ON ur.user_id = u.id
		JOIN roles r ON r.id = ur.role_id
		WHERE u.id::text = $1`
	var user ManagedUser
	err := s.pool.QueryRow(ctx, query, id).Scan(
		&user.ID, &user.Name, &user.Username, &user.Email, &user.Role,
		&user.CreatedAt, &user.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return ManagedUser{}, ErrNotFound
	}
	if err != nil {
		return ManagedUser{}, fmt.Errorf("get user: %w", err)
	}
	return user, nil
}

// CreateUser validates and creates one active account within the actor's scope.
func (s *Service) CreateUser(ctx context.Context, actor User, input CreateUserInput) (ManagedUser, error) {
	input, err := validateCreateUser(input)
	if err != nil {
		return ManagedUser{}, err
	}
	if !HasPermission(actor, PermissionUserCreate) {
		return ManagedUser{}, ErrForbidden
	}
	if !isSuperAdmin(actor) && input.Role != "user" {
		return ManagedUser{}, ErrForbidden
	}
	if input.Role == "super_admin" {
		return ManagedUser{}, ErrForbidden
	}

	passwordHash, err := HashPassword(input.Password)
	if err != nil {
		return ManagedUser{}, fmt.Errorf("hash user password: %w", err)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ManagedUser{}, fmt.Errorf("begin user creation: %w", err)
	}
	defer tx.Rollback(ctx)

	var roleID string
	err = tx.QueryRow(ctx, "SELECT id FROM roles WHERE name=$1", input.Role).Scan(&roleID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ManagedUser{}, &ValidationError{Field: "role", Message: "does not exist"}
	} else if err != nil {
		return ManagedUser{}, fmt.Errorf("find user role: %w", err)
	}

	user := ManagedUser{
		Name:     input.Name,
		Username: input.Username,
		Email:    input.Email,
		Role:     input.Role,
	}
	const insertUser = `
		INSERT INTO users(name, username, email, password_hash)
		VALUES($1, $2, $3, $4)
		RETURNING id, created_at, updated_at`
	err = tx.QueryRow(
		ctx,
		insertUser,
		input.Name,
		input.Username,
		input.Email,
		passwordHash,
	).Scan(&user.ID, &user.CreatedAt, &user.UpdatedAt)
	if err != nil {
		return ManagedUser{}, classifyUserConflict(err)
	}

	if _, err := tx.Exec(
		ctx,
		"INSERT INTO user_roles(user_id, role_id) VALUES($1, $2)",
		user.ID,
		roleID,
	); err != nil {
		return ManagedUser{}, fmt.Errorf("assign user role: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return ManagedUser{}, fmt.Errorf("commit user creation: %w", err)
	}

	return user, nil
}

// UpdateUser changes an account within the actor's management scope.
func (s *Service) UpdateUser(ctx context.Context, actor User, id string, input UpdateUserInput) (ManagedUser, error) {
	if !HasPermission(actor, PermissionUserEdit) {
		return ManagedUser{}, ErrForbidden
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ManagedUser{}, fmt.Errorf("begin user update: %w", err)
	}
	defer tx.Rollback(ctx)

	current, err := getUserForUpdate(ctx, tx, id)
	if err != nil {
		return ManagedUser{}, err
	}
	if current.Role == "super_admin" {
		return ManagedUser{}, ErrForbidden
	}
	if !isSuperAdmin(actor) {
		if current.Role != "user" || input.Role != nil {
			return ManagedUser{}, ErrForbidden
		}
	}

	next, roleChanged, err := validateUpdateUser(current, input)
	if err != nil {
		return ManagedUser{}, err
	}
	if roleChanged {
		var roleID string
		if next.Role == "super_admin" {
			return ManagedUser{}, ErrForbidden
		}

		err = tx.QueryRow(ctx, "SELECT id FROM roles WHERE name=$1", next.Role).Scan(&roleID)
		if errors.Is(err, pgx.ErrNoRows) {
			return ManagedUser{}, &ValidationError{Field: "role", Message: "does not exist"}
		} else if err != nil {
			return ManagedUser{}, fmt.Errorf("find user role: %w", err)
		}

		const updateRole = `
			UPDATE user_roles
			SET role_id=$1, assigned_at=now()
			WHERE user_id=$2`
		if _, err := tx.Exec(ctx, updateRole, roleID, id); err != nil {
			return ManagedUser{}, fmt.Errorf("update user role: %w", err)
		}
	}

	const update = `
		UPDATE users
		SET name=$1, username=$2, email=$3, updated_at=now()
		WHERE id=$4
		RETURNING updated_at`
	err = tx.QueryRow(ctx, update, next.Name, next.Username, next.Email, id).Scan(&next.UpdatedAt)
	if err != nil {
		return ManagedUser{}, classifyUserConflict(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return ManagedUser{}, fmt.Errorf("commit user update: %w", err)
	}
	return next, nil
}

// DeleteUser permanently removes an account and its sessions.
func (s *Service) DeleteUser(ctx context.Context, actor User, id string) (ManagedUser, error) {
	if !isSuperAdmin(actor) || !HasPermission(actor, PermissionUserDelete) {
		return ManagedUser{}, ErrForbidden
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ManagedUser{}, fmt.Errorf("begin user deletion: %w", err)
	}
	defer tx.Rollback(ctx)

	user, err := getUserForUpdate(ctx, tx, id)
	if err != nil {
		return ManagedUser{}, err
	}
	if user.Role == "super_admin" {
		return ManagedUser{}, ErrForbidden
	}
	if _, err := tx.Exec(ctx, "DELETE FROM users WHERE id=$1", id); err != nil {
		return ManagedUser{}, fmt.Errorf("delete user: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return ManagedUser{}, fmt.Errorf("commit user deletion: %w", err)
	}
	return user, nil
}

func getUserForUpdate(ctx context.Context, tx pgx.Tx, id string) (ManagedUser, error) {
	const query = `
		SELECT u.id, u.name, u.username, u.email, r.name, u.created_at, u.updated_at
		FROM users u
		JOIN user_roles ur ON ur.user_id = u.id
		JOIN roles r ON r.id = ur.role_id
		WHERE u.id::text=$1
		FOR UPDATE OF u, ur`
	var user ManagedUser
	err := tx.QueryRow(ctx, query, id).Scan(
		&user.ID, &user.Name, &user.Username, &user.Email, &user.Role,
		&user.CreatedAt, &user.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return ManagedUser{}, ErrNotFound
	}
	if err != nil {
		return ManagedUser{}, fmt.Errorf("get user for update: %w", err)
	}
	return user, nil
}

func validateUpdateUser(current ManagedUser, input UpdateUserInput) (ManagedUser, bool, error) {
	next := current
	if input.Name != nil {
		value, valid := credentials.NormalizeName(*input.Name)
		if !valid {
			return ManagedUser{}, false, &ValidationError{
				Field:   "name",
				Message: "must contain 1-100 characters without line breaks",
			}
		}
		next.Name = value
	}
	if input.Username != nil {
		value, valid := credentials.NormalizeUsername(*input.Username)
		if !valid {
			return ManagedUser{}, false, &ValidationError{Field: "username", Message: "is invalid"}
		}
		next.Username = value
	}
	if input.Email != nil {
		value, valid := credentials.NormalizeEmail(*input.Email)
		if !valid {
			return ManagedUser{}, false, &ValidationError{Field: "email", Message: "is invalid"}
		}
		next.Email = value
	}
	if usernameEmail, isEmail := credentials.NormalizeEmail(next.Username); isEmail && usernameEmail != next.Email {
		return ManagedUser{}, false, &ValidationError{
			Field:   "username",
			Message: "must match email when it is an email address",
		}
	}
	if input.Role != nil {
		next.Role = strings.ToLower(strings.TrimSpace(*input.Role))
		if next.Role == "" {
			return ManagedUser{}, false, &ValidationError{Field: "role", Message: "is required"}
		}
	}
	return next, next.Role != current.Role, nil
}

func validateCreateUser(input CreateUserInput) (CreateUserInput, error) {
	var valid bool
	input.Name, valid = credentials.NormalizeName(input.Name)
	if !valid {
		return CreateUserInput{}, &ValidationError{
			Field:   "name",
			Message: "must contain 1-100 characters without line breaks",
		}
	}

	input.Username, valid = credentials.NormalizeUsername(input.Username)
	if !valid {
		return CreateUserInput{}, &ValidationError{Field: "username", Message: "is invalid"}
	}

	input.Email, valid = credentials.NormalizeEmail(input.Email)
	if !valid {
		return CreateUserInput{}, &ValidationError{Field: "email", Message: "is invalid"}
	}
	if usernameEmail, isEmail := credentials.NormalizeEmail(input.Username); isEmail && usernameEmail != input.Email {
		return CreateUserInput{}, &ValidationError{
			Field:   "username",
			Message: "must match email when it is an email address",
		}
	}

	if err := credentials.ValidatePassword(input.Password); err != nil {
		return CreateUserInput{}, &ValidationError{Field: "password", Message: err.Error()}
	}

	input.Role = strings.ToLower(strings.TrimSpace(input.Role))
	if input.Role == "" {
		input.Role = "user"
	}
	if input.Role == "super_admin" {
		return CreateUserInput{}, &ValidationError{Field: "role", Message: "cannot be super_admin"}
	}

	return input, nil
}

func classifyUserConflict(err error) error {
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "23505" {
		return fmt.Errorf("create user: %w", err)
	}

	switch postgresError.ConstraintName {
	case "users_username_key":
		return ErrUsernameExists
	case "users_email_lower_idx":
		return ErrEmailExists
	default:
		return fmt.Errorf("create user: %w", err)
	}
}
