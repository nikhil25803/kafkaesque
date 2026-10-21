package auth

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrForbidden    = errors.New("operation is not permitted")
	ErrNotFound     = errors.New("record not found")
	ErrRoleExists   = errors.New("role already exists")
	ErrRoleAssigned = errors.New("role is assigned to users")
)

var roleNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{2,63}$`)

// ManagedRole is a role returned by role-management operations.
type ManagedRole struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Permissions []string  `json:"permissions"`
	System      bool      `json:"system"`
	UserCount   int       `json:"user_count"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// RoleInput contains fields accepted when creating or updating a custom role.
type RoleInput struct {
	Name        string
	Permissions []string
}

// ListRoles returns all roles in deterministic name order.
func (s *Service) ListRoles(ctx context.Context) ([]ManagedRole, error) {
	const query = `
		SELECT r.id, r.name, r.system, r.created_at, r.updated_at,
			count(DISTINCT ur.user_id),
			COALESCE(array_agg(DISTINCT p.module || ':' || p.resource || ':' || p.action)
				FILTER (WHERE p.id IS NOT NULL), ARRAY[]::text[])
		FROM roles r
		LEFT JOIN user_roles ur ON ur.role_id = r.id
		LEFT JOIN role_permissions rp ON rp.role_id = r.id
		LEFT JOIN permissions p ON p.id = rp.permission_id
		GROUP BY r.id
		ORDER BY r.name`
	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list roles: %w", err)
	}
	defer rows.Close()

	roles := make([]ManagedRole, 0)
	for rows.Next() {
		var role ManagedRole
		if err := rows.Scan(
			&role.ID,
			&role.Name,
			&role.System,
			&role.CreatedAt,
			&role.UpdatedAt,
			&role.UserCount,
			&role.Permissions,
		); err != nil {
			return nil, fmt.Errorf("scan role: %w", err)
		}
		sort.Strings(role.Permissions)
		roles = append(roles, role)
	}
	return roles, rows.Err()
}

// GetRole returns one role by ID.
func (s *Service) GetRole(ctx context.Context, id string) (ManagedRole, error) {
	const query = `
		SELECT r.id, r.name, r.system, r.created_at, r.updated_at,
			count(DISTINCT ur.user_id),
			COALESCE(array_agg(DISTINCT p.module || ':' || p.resource || ':' || p.action)
				FILTER (WHERE p.id IS NOT NULL), ARRAY[]::text[])
		FROM roles r
		LEFT JOIN user_roles ur ON ur.role_id = r.id
		LEFT JOIN role_permissions rp ON rp.role_id = r.id
		LEFT JOIN permissions p ON p.id = rp.permission_id
		WHERE r.id::text = $1
		GROUP BY r.id`
	var role ManagedRole
	err := s.pool.QueryRow(ctx, query, id).Scan(
		&role.ID, &role.Name, &role.System, &role.CreatedAt, &role.UpdatedAt,
		&role.UserCount, &role.Permissions,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return ManagedRole{}, ErrNotFound
	}
	if err != nil {
		return ManagedRole{}, fmt.Errorf("get role: %w", err)
	}
	sort.Strings(role.Permissions)
	return role, nil
}

// CreateRole creates a custom role and its grants atomically.
func (s *Service) CreateRole(ctx context.Context, actor User, input RoleInput) (ManagedRole, error) {
	if !isSuperAdmin(actor) {
		return ManagedRole{}, ErrForbidden
	}
	input, err := validateRoleInput(input)
	if err != nil {
		return ManagedRole{}, err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ManagedRole{}, fmt.Errorf("begin role creation: %w", err)
	}
	defer tx.Rollback(ctx)

	var role ManagedRole
	const insert = `
		INSERT INTO roles(name)
		VALUES($1)
		RETURNING id, name, system, created_at, updated_at`
	err = tx.QueryRow(ctx, insert, input.Name).Scan(
		&role.ID, &role.Name, &role.System, &role.CreatedAt, &role.UpdatedAt,
	)
	if err != nil {
		return ManagedRole{}, classifyRoleConflict(err)
	}
	if err := assignPermissions(ctx, tx, role.ID, input.Permissions); err != nil {
		return ManagedRole{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ManagedRole{}, fmt.Errorf("commit role creation: %w", err)
	}
	role.Permissions = input.Permissions
	return role, nil
}

// UpdateRole changes a custom role and replaces its grants atomically.
func (s *Service) UpdateRole(ctx context.Context, actor User, id string, input RoleInput) (ManagedRole, error) {
	if !isSuperAdmin(actor) {
		return ManagedRole{}, ErrForbidden
	}
	input, err := validateRoleInput(input)
	if err != nil {
		return ManagedRole{}, err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ManagedRole{}, fmt.Errorf("begin role update: %w", err)
	}
	defer tx.Rollback(ctx)

	var system bool
	err = tx.QueryRow(ctx, "SELECT system FROM roles WHERE id::text=$1 FOR UPDATE", id).Scan(&system)
	if errors.Is(err, pgx.ErrNoRows) {
		return ManagedRole{}, ErrNotFound
	} else if err != nil {
		return ManagedRole{}, fmt.Errorf("find role: %w", err)
	}
	if system {
		return ManagedRole{}, ErrForbidden
	}
	if _, err := tx.Exec(ctx, "UPDATE roles SET name=$1, updated_at=now() WHERE id=$2", input.Name, id); err != nil {
		return ManagedRole{}, classifyRoleConflict(err)
	}
	if _, err := tx.Exec(ctx, "DELETE FROM role_permissions WHERE role_id=$1", id); err != nil {
		return ManagedRole{}, fmt.Errorf("clear role permissions: %w", err)
	}
	if err := assignPermissions(ctx, tx, id, input.Permissions); err != nil {
		return ManagedRole{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ManagedRole{}, fmt.Errorf("commit role update: %w", err)
	}
	return s.GetRole(ctx, id)
}

// DeleteRole deletes an unused custom role.
func (s *Service) DeleteRole(ctx context.Context, actor User, id string) error {
	if !isSuperAdmin(actor) {
		return ErrForbidden
	}
	result, err := s.pool.Exec(ctx, "DELETE FROM roles WHERE id::text=$1 AND NOT system", id)
	if err != nil {
		var postgresError *pgconn.PgError
		if errors.As(err, &postgresError) && (postgresError.Code == "23503" || postgresError.Code == "23001") {
			return ErrRoleAssigned
		}
		return fmt.Errorf("delete role: %w", err)
	}
	if result.RowsAffected() == 0 {
		role, findErr := s.GetRole(ctx, id)
		if errors.Is(findErr, ErrNotFound) {
			return ErrNotFound
		}
		if findErr != nil {
			return findErr
		}
		if role.System {
			return ErrForbidden
		}
	}
	return nil
}

func validateRoleInput(input RoleInput) (RoleInput, error) {
	input.Name = strings.ToLower(strings.TrimSpace(input.Name))
	reserved := input.Name == "super_admin" || input.Name == "admin" || input.Name == "user"
	if !roleNamePattern.MatchString(input.Name) || reserved {
		return RoleInput{}, &ValidationError{Field: "name", Message: "must be a unique lowercase role name"}
	}

	seen := make(map[string]bool, len(input.Permissions))
	permissions := make([]string, 0, len(input.Permissions))
	for _, permission := range input.Permissions {
		permission = strings.TrimSpace(permission)
		if permission == PermissionUserDelete {
			return RoleInput{}, &ValidationError{
				Field:   "permissions",
				Message: "cannot include the Super Admin delete permission",
			}
		}
		if permission != PermissionUserView && permission != PermissionUserCreate && permission != PermissionUserEdit {
			return RoleInput{}, &ValidationError{Field: "permissions", Message: "contains an unsupported permission"}
		}
		if !seen[permission] {
			seen[permission] = true
			permissions = append(permissions, permission)
		}
	}
	if (seen[PermissionUserCreate] || seen[PermissionUserEdit]) && !seen[PermissionUserView] {
		return RoleInput{}, &ValidationError{
			Field:   "permissions",
			Message: "must include user:account:view with mutation permissions",
		}
	}
	sort.Strings(permissions)
	input.Permissions = permissions
	return input, nil
}

func assignPermissions(ctx context.Context, tx pgx.Tx, roleID string, permissions []string) error {
	for _, permission := range permissions {
		parts := strings.Split(permission, ":")
		const insert = `
			INSERT INTO role_permissions(role_id, permission_id)
			SELECT $1, id FROM permissions
			WHERE module=$2 AND resource=$3 AND action=$4`
		result, err := tx.Exec(ctx, insert, roleID, parts[0], parts[1], parts[2])
		if err != nil {
			return fmt.Errorf("assign role permission: %w", err)
		}
		if result.RowsAffected() != 1 {
			return fmt.Errorf("assign role permission: permission is missing")
		}
	}
	return nil
}

func classifyRoleConflict(err error) error {
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) && postgresError.Code == "23505" {
		return ErrRoleExists
	}
	return fmt.Errorf("save role: %w", err)
}
