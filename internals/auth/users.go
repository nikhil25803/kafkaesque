package auth

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

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
	Active    bool      `json:"active"`
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
		SELECT u.id, u.name, u.username, u.email, r.name, u.active, u.created_at, u.updated_at
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
			&user.Active,
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

// CreateUser validates and creates one active Admin or User account.
func (s *Service) CreateUser(ctx context.Context, input CreateUserInput) (ManagedUser, error) {
	input, err := validateCreateUser(input)
	if err != nil {
		return ManagedUser{}, err
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
	if err := tx.QueryRow(ctx, "SELECT id FROM roles WHERE name=$1", input.Role).Scan(&roleID); err != nil {
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
		RETURNING id, active, created_at, updated_at`
	err = tx.QueryRow(
		ctx,
		insertUser,
		input.Name,
		input.Username,
		input.Email,
		passwordHash,
	).Scan(&user.ID, &user.Active, &user.CreatedAt, &user.UpdatedAt)
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

func validateCreateUser(input CreateUserInput) (CreateUserInput, error) {
	var valid bool
	input.Name, valid = credentials.NormalizeName(input.Name)
	if !valid {
		return CreateUserInput{}, &ValidationError{Field: "name", Message: "must contain 1-100 characters without line breaks"}
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
		return CreateUserInput{}, &ValidationError{Field: "username", Message: "must match email when it is an email address"}
	}

	if err := credentials.ValidatePassword(input.Password); err != nil {
		return CreateUserInput{}, &ValidationError{Field: "password", Message: err.Error()}
	}

	input.Role = strings.ToLower(strings.TrimSpace(input.Role))
	if input.Role == "" {
		input.Role = "user"
	}
	if input.Role != "admin" && input.Role != "user" {
		return CreateUserInput{}, &ValidationError{Field: "role", Message: "must be admin or user"}
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
