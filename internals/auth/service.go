package auth

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	kafkaesque_config "github.com/nikhil25803/kafkaesque/internals/config"
	"github.com/nikhil25803/kafkaesque/internals/credentials"
)

var (
	ErrInvalidCredentials = errors.New("invalid username or password")
	ErrInvalidToken       = errors.New("invalid or expired token")
)

const authenticationBootstrapLockID = int64(0x4b41464b455351)

const pendingBootstrapEmail = "bootstrap-pending@invalid.local"

//go:embed migrations/*.sql
var migrations embed.FS

type User struct {
	ID          string   `json:"id"`
	Username    string   `json:"username"`
	Role        string   `json:"role"`
	Permissions []string `json:"permissions"`
}

type Tokens struct {
	AccessToken      string    `json:"access_token"`
	RefreshToken     string    `json:"refresh_token"`
	CSRFToken        string    `json:"csrf_token"`
	AccessExpiresAt  time.Time `json:"access_expires_at"`
	RefreshExpiresAt time.Time `json:"refresh_expires_at"`
	User             User      `json:"user"`
}

// Manager provides the authentication operations used by HTTP handlers.
type Manager interface {
	Login(context.Context, string, string) (Tokens, error)
	AuthenticateAccess(context.Context, string) (User, error)
	Refresh(context.Context, string) (Tokens, error)
	Logout(context.Context, string) error
	ValidateCSRF(context.Context, string, string) (bool, error)
	ValidateRefreshCSRF(context.Context, string, string) (bool, error)
}

// UserManager provides user administration operations.
type UserManager interface {
	ListUsers(context.Context, int, int) ([]ManagedUser, int, error)
	GetUser(context.Context, string) (ManagedUser, error)
	CreateUser(context.Context, User, CreateUserInput) (ManagedUser, error)
	UpdateUser(context.Context, User, string, UpdateUserInput) (ManagedUser, error)
	DeleteUser(context.Context, User, string) (ManagedUser, error)
}

// RoleManager provides custom-role administration operations.
type RoleManager interface {
	ListRoles(context.Context) ([]ManagedRole, error)
	GetRole(context.Context, string) (ManagedRole, error)
	CreateRole(context.Context, User, RoleInput) (ManagedRole, error)
	UpdateRole(context.Context, User, string, RoleInput) (ManagedRole, error)
	DeleteRole(context.Context, User, string) error
}

// RuntimeManager provides every operation required by the API server.
type RuntimeManager interface {
	Manager
	UserManager
	RoleManager
}

type Service struct {
	pool              *pgxpool.Pool
	accessTTL         time.Duration
	refreshTTL        time.Duration
	dummyPasswordHash string
}

type bootstrapRecoveryError struct {
	reason     string
	mismatches []string
	message    string
}

func (e *bootstrapRecoveryError) Error() string { return e.message }

type sqlExecutor interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// NormalizeUsername normalizes and validates an account username.
func NormalizeUsername(value string) (string, bool) {
	return credentials.NormalizeUsername(value)
}

// Open connects to PostgreSQL and prepares the authentication service.
func Open(ctx context.Context, cfg *kafkaesque_config.Config, logger *slog.Logger) (*Service, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.Database.URL)
	if err != nil {
		logger.Error("database_connect_failed", "stage", "configuration")
		return nil, fmt.Errorf("parse database configuration")
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		logger.Error("database_connect_failed", "stage", "pool")
		return nil, fmt.Errorf("connect database")
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		logger.Error("database_connect_failed", "stage", "ping")
		return nil, fmt.Errorf("connect database: %w", err)
	}
	var versionText string
	if err := pool.QueryRow(ctx, "SHOW server_version_num").Scan(&versionText); err != nil {
		pool.Close()
		logger.Error("database_version_failed", "stage", "read")
		return nil, fmt.Errorf("read database version: %w", err)
	}
	version, err := strconv.Atoi(versionText)
	if err != nil {
		pool.Close()
		logger.Error("database_version_failed", "stage", "parse")
		return nil, fmt.Errorf("read database version")
	}
	if version < 180000 {
		pool.Close()
		logger.Error("database_version_failed", "reason", "unsupported_version")
		return nil, fmt.Errorf("PostgreSQL 18 or newer is required")
	}
	logger.Info("database_connected")

	dummyHash, err := HashPassword("dummy-password-never-accepted")
	if err != nil {
		pool.Close()
		logger.Error("super_admin_reconciliation_failed", "error_type", fmt.Sprintf("%T", err))
		return nil, err
	}
	service := &Service{
		pool:              pool,
		accessTTL:         cfg.Authentication.AccessTTL,
		refreshTTL:        cfg.Authentication.RefreshTTL,
		dummyPasswordHash: dummyHash,
	}
	event, mismatches, recoveryReason, err := service.bootstrap(ctx, cfg.Authentication)
	if err != nil {
		pool.Close()
		attributes := []any{"error_type", fmt.Sprintf("%T", err)}
		if recoveryReason != "" {
			attributes = append(attributes, "reason", recoveryReason)
		}
		if len(mismatches) > 0 {
			attributes = append(
				attributes,
				"recovery", cfg.Authentication.SuperAdminRecovery,
				"mismatched_fields", mismatches,
			)
		}
		logger.Error("authentication_bootstrap_failed", attributes...)
		return nil, err
	}
	logger.Info("migrations_applied")
	if recoveryReason != "" {
		logger.Warn(
			"authentication_recovery_applied",
			"recovery", cfg.Authentication.SuperAdminRecovery,
			"reason", recoveryReason,
			"mismatched_fields", mismatches,
		)
	}
	logger.Info(event)
	if err := service.Cleanup(ctx); err != nil {
		pool.Close()
		logger.Error("session_cleanup_failed", "error_type", fmt.Sprintf("%T", err))
		return nil, err
	}
	logger.Info("sessions_cleaned")
	return service, nil
}

// Close releases the PostgreSQL connection pool.
func (s *Service) Close() { s.pool.Close() }

// bootstrap serializes migrations and Super Admin reconciliation.
func (s *Service) bootstrap(
	ctx context.Context,
	cfg kafkaesque_config.AuthenticationConfig,
) (string, []string, string, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return "", nil, "", fmt.Errorf("acquire authentication bootstrap connection: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", authenticationBootstrapLockID); err != nil {
		return "", nil, "", fmt.Errorf("lock authentication bootstrap: %w", err)
	}
	defer conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", authenticationBootstrapLockID)

	if err := migrate(ctx, conn); err != nil {
		return s.recoverBootstrap(ctx, conn, cfg, err)
	}

	event, mismatches, err := s.reconcileSuperAdmin(ctx, conn, cfg)
	if err != nil {
		return s.recoverBootstrap(ctx, conn, cfg, err)
	}
	if err := verifySchema(ctx, conn); err != nil {
		return s.recoverBootstrap(ctx, conn, cfg, err)
	}

	return event, mismatches, "", nil
}

func (s *Service) recoverBootstrap(
	ctx context.Context,
	conn *pgxpool.Conn,
	cfg kafkaesque_config.AuthenticationConfig,
	err error,
) (string, []string, string, error) {
	var recoveryError *bootstrapRecoveryError
	if !errors.As(err, &recoveryError) {
		return "", nil, "", err
	}
	if cfg.SuperAdminRecovery != "reinitialize" {
		return "", recoveryError.mismatches, recoveryError.reason, err
	}
	if err := reinitializeDatabase(ctx, conn, cfg); err != nil {
		return "", recoveryError.mismatches, recoveryError.reason, err
	}
	return "authentication_reinitialized", recoveryError.mismatches, recoveryError.reason, nil
}

// migrate applies every pending checksum-verified migration.
func migrate(ctx context.Context, conn *pgxpool.Conn) error {
	if err := createMigrationTable(ctx, conn); err != nil {
		return fmt.Errorf("create migration table: %w", err)
	}
	entries, err := migrationEntries(migrations)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		body, checksum, err := readMigration(migrations, entry.Name())
		if err != nil {
			return err
		}
		var existing string
		err = conn.QueryRow(ctx, "SELECT checksum FROM schema_migrations WHERE version=$1", entry.Name()).Scan(&existing)
		if err == nil {
			if existing != checksum {
				return &bootstrapRecoveryError{
					reason:  "migration_checksum",
					message: fmt.Sprintf("migration checksum mismatch for %s", entry.Name()),
				}
			}
			continue
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("read migration state: %w", err)
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		if err := applyMigration(ctx, tx, entry.Name(), body, checksum); err != nil {
			_ = tx.Rollback(ctx)
			return &bootstrapRecoveryError{
				reason:  "schema_integrity",
				message: fmt.Sprintf("apply migration %s: %v", entry.Name(), err),
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

func createMigrationTable(ctx context.Context, executor sqlExecutor) error {
	const query = `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version text PRIMARY KEY,
			checksum text NOT NULL,
			applied_at timestamptz NOT NULL DEFAULT now()
		)`
	_, err := executor.Exec(ctx, query)
	return err
}

func migrationEntries(source fs.FS) ([]fs.DirEntry, error) {
	entries, err := fs.ReadDir(source, "migrations")
	if err != nil {
		return nil, err
	}

	files := make([]fs.DirEntry, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sql") {
			files = append(files, entry)
		}
	}
	return files, nil
}

func readMigration(source fs.FS, name string) ([]byte, string, error) {
	body, err := fs.ReadFile(source, "migrations/"+name)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(body)
	return body, hex.EncodeToString(sum[:]), nil
}

func applyMigration(
	ctx context.Context,
	executor sqlExecutor,
	name string,
	body []byte,
	checksum string,
) error {
	if _, err := executor.Exec(ctx, string(body)); err != nil {
		return err
	}
	_, err := executor.Exec(
		ctx,
		"INSERT INTO schema_migrations(version, checksum) VALUES($1, $2)",
		name,
		checksum,
	)
	return err
}

// verifySchema confirms every required authentication column exists.
func verifySchema(ctx context.Context, executor sqlExecutor) error {
	var count int
	const query = `
		WITH required(table_name, column_name) AS (VALUES
			('users', 'id'),
			('users', 'username'),
			('users', 'name'),
			('users', 'email'),
			('users', 'password_hash'),
			('users', 'active'),
			('users', 'created_at'),
			('users', 'updated_at'),
			('users', 'password_changed_at'),
			('roles', 'id'),
			('roles', 'name'),
			('roles', 'created_at'),
			('roles', 'updated_at'),
			('roles', 'system'),
			('user_roles', 'user_id'),
			('user_roles', 'role_id'),
			('user_roles', 'assigned_at'),
			('permissions', 'id'),
			('permissions', 'module'),
			('permissions', 'resource'),
			('permissions', 'action'),
			('permissions', 'created_at'),
			('permissions', 'updated_at'),
			('role_permissions', 'role_id'),
			('role_permissions', 'permission_id'),
			('role_permissions', 'assigned_at'),
			('auth_sessions', 'id'),
			('auth_sessions', 'user_id'),
			('auth_sessions', 'access_token_hash'),
			('auth_sessions', 'refresh_token_hash'),
			('auth_sessions', 'csrf_token_hash'),
			('auth_sessions', 'access_expires_at'),
			('auth_sessions', 'refresh_expires_at'),
			('auth_sessions', 'revoked_at'),
			('auth_sessions', 'created_at'),
			('auth_sessions', 'last_activity_at')
		)
		SELECT count(*)
		FROM required r
		JOIN information_schema.columns c
			ON c.table_schema = 'public'
			AND c.table_name = r.table_name
			AND c.column_name = r.column_name`
	if err := executor.QueryRow(ctx, query).Scan(&count); err != nil || count != 36 {
		return schemaIntegrityError()
	}
	var obsoleteRoleColumn int
	const obsoleteQuery = `
		SELECT count(*)
		FROM information_schema.columns
		WHERE table_schema = 'public'
			AND table_name = 'users'
			AND column_name = 'role'`
	if err := executor.QueryRow(ctx, obsoleteQuery).Scan(&obsoleteRoleColumn); err != nil || obsoleteRoleColumn != 0 {
		return schemaIntegrityError()
	}

	var roles int
	const systemRolesQuery = `
		SELECT count(*)
		FROM roles
		WHERE system AND name IN ('super_admin', 'admin', 'user')`
	if err := executor.QueryRow(ctx, systemRolesQuery).Scan(&roles); err != nil || roles != 3 {
		return schemaIntegrityError()
	}
	if err := executor.QueryRow(ctx, "SELECT count(*) FROM roles WHERE system").Scan(&roles); err != nil || roles != 3 {
		return schemaIntegrityError()
	}
	var grants int
	const grantsQuery = `
		SELECT count(*)
		FROM role_permissions rp
		JOIN roles r ON r.id = rp.role_id
		JOIN permissions p ON p.id = rp.permission_id
		WHERE (r.name = 'super_admin' AND p.module = 'user' AND p.resource = 'account' AND p.action IN ('view', 'create', 'edit', 'delete'))
		   OR (r.name = 'admin' AND p.module = 'user' AND p.resource = 'account' AND p.action IN ('view', 'create', 'edit'))
		   OR (r.name = 'user' AND p.module = 'user' AND p.resource = 'account' AND p.action = 'view')`
	if err := executor.QueryRow(ctx, grantsQuery).Scan(&grants); err != nil || grants != 8 {
		return schemaIntegrityError()
	}
	var catalogPermissions, systemGrants int
	if err := executor.QueryRow(ctx, "SELECT count(*) FROM permissions").Scan(&catalogPermissions); err != nil ||
		catalogPermissions != 4 {
		return schemaIntegrityError()
	}
	const systemGrantsQuery = `
		SELECT count(*)
		FROM role_permissions rp
		JOIN roles r ON r.id = rp.role_id
		WHERE r.system`
	if err := executor.QueryRow(ctx, systemGrantsQuery).Scan(&systemGrants); err != nil || systemGrants != 8 {
		return schemaIntegrityError()
	}
	var guards int
	const guardsQuery = `
		SELECT count(*)
		FROM pg_trigger
		WHERE NOT tgisinternal
			AND tgenabled <> 'D'
			AND tgname IN (
				'protect_system_role_update',
				'protect_system_role_permission_changes',
				'protect_permission_catalog_changes'
			)`
	if err := executor.QueryRow(ctx, guardsQuery).Scan(&guards); err != nil || guards != 3 {
		return schemaIntegrityError()
	}

	var superAdmins int
	const superAdminQuery = `
		SELECT count(*)
		FROM users u
		JOIN user_roles ur ON ur.user_id = u.id
		JOIN roles r ON r.id = ur.role_id
		WHERE r.name = 'super_admin' AND u.active`
	if err := executor.QueryRow(ctx, superAdminQuery).Scan(&superAdmins); err != nil || superAdmins != 1 {
		return schemaIntegrityError()
	}

	var invalidAssignments int
	const assignmentQuery = `
		SELECT count(*)
		FROM (
			SELECT u.id
			FROM users u
			LEFT JOIN user_roles ur ON ur.user_id = u.id
			GROUP BY u.id
			HAVING count(ur.role_id) <> 1
		) invalid`
	if err := executor.QueryRow(ctx, assignmentQuery).Scan(&invalidAssignments); err != nil || invalidAssignments != 0 {
		return schemaIntegrityError()
	}

	return nil
}

func schemaIntegrityError() error {
	return &bootstrapRecoveryError{
		reason:  "schema_integrity",
		message: "authentication schema verification failed",
	}
}

// reconcileSuperAdmin applies the configured recovery policy to bootstrap mismatches.
func (s *Service) reconcileSuperAdmin(
	ctx context.Context,
	conn *pgxpool.Conn,
	cfg kafkaesque_config.AuthenticationConfig,
) (string, []string, error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return "", nil, err
	}
	defer tx.Rollback(ctx)

	var roleID string
	if err := tx.QueryRow(ctx, "SELECT id FROM roles WHERE name = 'super_admin'").Scan(&roleID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil, &bootstrapRecoveryError{
				reason:  "super_admin_role",
				message: "Super Admin role is missing; reinitialize is required",
			}
		}
		return "", nil, classifyBootstrapQueryError(err, "find Super Admin role")
	}

	var userCount, superAdminCount int
	if err := tx.QueryRow(ctx, "SELECT count(*) FROM users").Scan(&userCount); err != nil {
		return "", nil, classifyBootstrapQueryError(err, "count authentication users")
	}
	const countSuperAdmins = `
		SELECT count(*)
		FROM user_roles ur
		JOIN roles r ON r.id = ur.role_id
		WHERE r.name = 'super_admin'`
	if err := tx.QueryRow(ctx, countSuperAdmins).Scan(&superAdminCount); err != nil {
		return "", nil, classifyBootstrapQueryError(err, "count Super Admin assignments")
	}

	if userCount == 0 && superAdminCount == 0 {
		if err := createSuperAdmin(ctx, tx, roleID, cfg); err != nil {
			return "", nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return "", nil, err
		}
		return "super_admin_created", nil, nil
	}
	if superAdminCount != 1 {
		return "", nil, &bootstrapRecoveryError{
			reason:  "super_admin_assignment",
			message: "authentication database must contain exactly one assigned Super Admin; reinitialize is required",
		}
	}

	var id, currentUsername, currentEmail, name, hash string
	var active bool
	const findSuperAdmin = `
		SELECT u.id, u.username, u.email, u.name, u.password_hash, u.active
		FROM users u
		JOIN user_roles ur ON ur.user_id = u.id
		JOIN roles r ON r.id = ur.role_id
		WHERE r.name = 'super_admin'`
	if err := tx.QueryRow(ctx, findSuperAdmin).Scan(
		&id,
		&currentUsername,
		&currentEmail,
		&name,
		&hash,
		&active,
	); err != nil {
		return "", nil, classifyBootstrapQueryError(err, "read Super Admin")
	}
	if !active {
		return "", nil, &bootstrapRecoveryError{
			reason:  "super_admin_record",
			message: "Super Admin record is inactive; reinitialize is required",
		}
	}

	if currentEmail == pendingBootstrapEmail {
		currentEmail = cfg.SuperAdminEmail
		name = "Super Admin"
		if _, err := tx.Exec(
			ctx,
			"UPDATE users SET name=$1, email=$2, updated_at=now() WHERE id=$3",
			name,
			currentEmail,
			id,
		); err != nil {
			return "", nil, fmt.Errorf("backfill Super Admin profile: %w", err)
		}
	}
	if name != "Super Admin" {
		return "", nil, &bootstrapRecoveryError{
			reason:  "super_admin_record",
			message: "Super Admin record is invalid; reinitialize is required",
		}
	}

	passwordChanged := !VerifyPassword(cfg.SuperAdminPassword, hash)
	mismatches := bootstrapMismatches(currentUsername, currentEmail, cfg, passwordChanged)
	if len(mismatches) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return "", nil, err
		}
		return "super_admin_unchanged", nil, nil
	}

	switch cfg.SuperAdminRecovery {
	case "verify":
		return "", mismatches, &bootstrapRecoveryError{
			reason:     "bootstrap_mismatch",
			mismatches: mismatches,
			message:    fmt.Sprintf("Super Admin bootstrap mismatch in fields: %s", strings.Join(mismatches, ", ")),
		}
	case "reinitialize":
		return "", mismatches, &bootstrapRecoveryError{
			reason:     "bootstrap_mismatch",
			mismatches: mismatches,
			message:    "Super Admin bootstrap mismatch requires reinitialization",
		}
	case "update":
		// Apply the update below.
	default:
		return "", mismatches, fmt.Errorf("unsupported Super Admin recovery policy")
	}

	if passwordChanged {
		hash, err = HashPassword(cfg.SuperAdminPassword)
		if err != nil {
			return "", mismatches, err
		}
	}
	const updateAdmin = `
		UPDATE users
		SET username = $1,
			email = $2,
			name = 'Super Admin',
			password_hash = $3,
			updated_at = now(),
			password_changed_at = CASE WHEN $4 THEN now() ELSE password_changed_at END
		WHERE id = $5`
	if _, err = tx.Exec(
		ctx,
		updateAdmin,
		cfg.SuperAdminUsername,
		cfg.SuperAdminEmail,
		hash,
		passwordChanged,
		id,
	); err != nil {
		return "", mismatches, err
	}
	if _, err = tx.Exec(
		ctx,
		"UPDATE auth_sessions SET revoked_at=now() WHERE user_id=$1 AND revoked_at IS NULL",
		id,
	); err != nil {
		return "", mismatches, err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", mismatches, err
	}
	return "super_admin_updated", mismatches, nil
}

func classifyBootstrapQueryError(err error, action string) error {
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) &&
		(postgresError.Code == "42P01" || postgresError.Code == "42703") {
		return schemaIntegrityError()
	}
	return fmt.Errorf("%s: %w", action, err)
}

func bootstrapMismatches(
	currentUsername string,
	currentEmail string,
	cfg kafkaesque_config.AuthenticationConfig,
	passwordChanged bool,
) []string {
	var mismatches []string
	if currentUsername != cfg.SuperAdminUsername {
		mismatches = append(mismatches, "username")
	}
	if currentEmail != cfg.SuperAdminEmail {
		mismatches = append(mismatches, "email")
	}
	if passwordChanged {
		mismatches = append(mismatches, "password")
	}
	return mismatches
}

func createSuperAdmin(
	ctx context.Context,
	tx pgx.Tx,
	roleID string,
	cfg kafkaesque_config.AuthenticationConfig,
) error {
	hash, err := HashPassword(cfg.SuperAdminPassword)
	if err != nil {
		return err
	}

	var userID string
	const insertUser = `
		INSERT INTO users(username, name, email, password_hash)
		VALUES($1, 'Super Admin', $2, $3)
		RETURNING id`
	if err := tx.QueryRow(ctx, insertUser, cfg.SuperAdminUsername, cfg.SuperAdminEmail, hash).Scan(&userID); err != nil {
		return fmt.Errorf("create Super Admin: %w", err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO user_roles(user_id, role_id) VALUES($1, $2)", userID, roleID); err != nil {
		return fmt.Errorf("assign Super Admin role: %w", err)
	}
	return nil
}

// reinitializeDatabase replaces every object in the public schema atomically.
func reinitializeDatabase(
	ctx context.Context,
	conn *pgxpool.Conn,
	cfg kafkaesque_config.AuthenticationConfig,
) error {
	return reinitializeDatabaseWithMigrations(ctx, conn, cfg, migrations)
}

func reinitializeDatabaseWithMigrations(
	ctx context.Context,
	conn *pgxpool.Conn,
	cfg kafkaesque_config.AuthenticationConfig,
	source fs.FS,
) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, "DROP SCHEMA public CASCADE"); err != nil {
		return fmt.Errorf("drop public schema: %w", err)
	}
	if _, err := tx.Exec(ctx, "CREATE SCHEMA public AUTHORIZATION CURRENT_USER"); err != nil {
		return fmt.Errorf("create public schema: %w", err)
	}
	if err := createMigrationTable(ctx, tx); err != nil {
		return fmt.Errorf("create migration table: %w", err)
	}

	entries, err := migrationEntries(source)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		body, checksum, err := readMigration(source, entry.Name())
		if err != nil {
			return err
		}
		if err := applyMigration(ctx, tx, entry.Name(), body, checksum); err != nil {
			return fmt.Errorf("apply migration %s: %w", entry.Name(), err)
		}
	}

	var roleID string
	if err := tx.QueryRow(ctx, "SELECT id FROM roles WHERE name='super_admin'").Scan(&roleID); err != nil {
		return fmt.Errorf("find fresh Super Admin role: %w", err)
	}
	if err := createSuperAdmin(ctx, tx, roleID, cfg); err != nil {
		return err
	}
	if err := verifySchema(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit database reinitialization: %w", err)
	}
	return nil
}

// Cleanup deletes expired sessions and old revocation records.
func (s *Service) Cleanup(ctx context.Context) error {
	const query = `
		DELETE FROM auth_sessions
		WHERE refresh_expires_at <= now()
			OR revoked_at < now() - interval '7 days'`
	_, err := s.pool.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("clean authentication sessions: %w", err)
	}
	return nil
}

func tokenHash(token string) []byte {
	sum := sha256.Sum256([]byte(token))
	return sum[:]
}
