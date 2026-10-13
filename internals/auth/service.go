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
	"github.com/jackc/pgx/v5/pgxpool"
	kafkaesque_config "github.com/nikhil25803/kafkaesque/internals/config"
)

var (
	ErrInvalidCredentials = errors.New("invalid username or password")
	ErrInvalidToken       = errors.New("invalid or expired token")
)

const (
	migrationLockID  = int64(0x4b41464b455351)
	superAdminLockID = migrationLockID + 1
)

//go:embed migrations/*.sql
var migrations embed.FS

type User struct {
	ID       string `json:"id"`
	Username string `json:"username"`
	Role     string `json:"role"`
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

type Service struct {
	pool              *pgxpool.Pool
	accessTTL         time.Duration
	refreshTTL        time.Duration
	dummyPasswordHash string
}

// NormalizeUsername normalizes and validates a Super Admin username.
func NormalizeUsername(value string) (string, bool) {
	value = strings.ToLower(strings.TrimSpace(value))
	if len(value) < 3 || len(value) > 64 {
		return "", false
	}
	for _, r := range value {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '.' || r == '_' || r == '-' {
			continue
		}
		return "", false
	}
	return value, true
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
	if err := migrate(ctx, pool); err != nil {
		pool.Close()
		logger.Error("migration_failed", "error_type", fmt.Sprintf("%T", err))
		return nil, err
	}
	logger.Info("migrations_applied")
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
	event, err := service.reconcileSuperAdmin(
		ctx,
		cfg.Authentication.SuperAdminUsername,
		cfg.Authentication.SuperAdminPassword,
	)
	if err != nil {
		pool.Close()
		logger.Error("super_admin_reconciliation_failed", "error_type", fmt.Sprintf("%T", err))
		return nil, err
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

// migrate applies checksum-verified migrations while holding a database lock.
func migrate(ctx context.Context, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration connection: %w", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", migrationLockID); err != nil {
		return fmt.Errorf("lock migrations: %w", err)
	}
	defer conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", migrationLockID)
	const createMigrations = `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version text PRIMARY KEY,
			checksum text NOT NULL,
			applied_at timestamptz NOT NULL DEFAULT now()
		)`
	if _, err := conn.Exec(ctx, createMigrations); err != nil {
		return fmt.Errorf("create migration table: %w", err)
	}
	entries, err := fs.ReadDir(migrations, "migrations")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		body, err := migrations.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return err
		}
		sum := sha256.Sum256(body)
		checksum := hex.EncodeToString(sum[:])
		var existing string
		err = conn.QueryRow(ctx, "SELECT checksum FROM schema_migrations WHERE version=$1", entry.Name()).Scan(&existing)
		if err == nil {
			if existing != checksum {
				return fmt.Errorf("migration checksum mismatch for %s", entry.Name())
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
		if _, err = tx.Exec(ctx, string(body)); err == nil {
			_, err = tx.Exec(ctx, "INSERT INTO schema_migrations(version, checksum) VALUES($1,$2)", entry.Name(), checksum)
		}
		if err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply migration %s: %w", entry.Name(), err)
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	return verifySchema(ctx, conn)
}

// verifySchema confirms every required authentication column exists.
func verifySchema(ctx context.Context, conn *pgxpool.Conn) error {
	var count int
	const query = `
		WITH required(table_name, column_name) AS (VALUES
			('users', 'id'),
			('users', 'username'),
			('users', 'password_hash'),
			('users', 'active'),
			('users', 'created_at'),
			('users', 'updated_at'),
			('users', 'password_changed_at'),
			('roles', 'id'),
			('roles', 'name'),
			('roles', 'created_at'),
			('roles', 'updated_at'),
			('user_roles', 'user_id'),
			('user_roles', 'role_id'),
			('user_roles', 'assigned_at'),
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
	if err := conn.QueryRow(ctx, query).Scan(&count); err != nil || count != 24 {
		return fmt.Errorf("authentication schema verification failed")
	}
	var obsoleteRoleColumn int
	const obsoleteQuery = `
		SELECT count(*)
		FROM information_schema.columns
		WHERE table_schema = 'public'
			AND table_name = 'users'
			AND column_name = 'role'`
	if err := conn.QueryRow(ctx, obsoleteQuery).Scan(&obsoleteRoleColumn); err != nil || obsoleteRoleColumn != 0 {
		return fmt.Errorf("authentication schema verification failed")
	}
	return nil
}

// reconcileSuperAdmin serializes creation and environment-driven credential changes.
func (s *Service) reconcileSuperAdmin(ctx context.Context, username, password string) (string, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", superAdminLockID); err != nil {
		return "", fmt.Errorf("lock Super Admin reconciliation: %w", err)
	}
	var roleID string
	if err := tx.QueryRow(ctx, "SELECT id FROM roles WHERE name = 'super_admin'").Scan(&roleID); err != nil {
		return "", fmt.Errorf("find Super Admin role: %w", err)
	}
	var count int
	if err := tx.QueryRow(ctx, "SELECT count(*) FROM users").Scan(&count); err != nil {
		return "", err
	}
	if count == 0 {
		hash, err := HashPassword(password)
		if err != nil {
			return "", err
		}
		var userID string
		if err = tx.QueryRow(
			ctx,
			"INSERT INTO users(username,password_hash) VALUES($1,$2) RETURNING id",
			username,
			hash,
		).Scan(&userID); err != nil {
			return "", fmt.Errorf("create Super Admin: %w", err)
		}
		if _, err = tx.Exec(ctx, "INSERT INTO user_roles(user_id,role_id) VALUES($1,$2)", userID, roleID); err != nil {
			return "", fmt.Errorf("assign Super Admin role: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return "", err
		}
		return "super_admin_created", nil
	}
	if count != 1 {
		return "", fmt.Errorf("authentication database must contain exactly one Super Admin")
	}
	var id, currentUsername, hash, role string
	var active bool
	const findSuperAdmin = `
		SELECT u.id, u.username, u.password_hash, r.name, u.active
		FROM users u
		JOIN user_roles ur ON ur.user_id = u.id
		JOIN roles r ON r.id = ur.role_id`
	if err := tx.QueryRow(ctx, findSuperAdmin).Scan(
		&id,
		&currentUsername,
		&hash,
		&role,
		&active,
	); err != nil {
		return "", err
	}
	if role != "super_admin" || !active {
		return "", fmt.Errorf("Super Admin record is invalid")
	}
	usernameChanged, passwordChanged := currentUsername != username, !VerifyPassword(password, hash)
	if !usernameChanged && !passwordChanged {
		if err := tx.Commit(ctx); err != nil {
			return "", err
		}
		return "super_admin_unchanged", nil
	}
	if passwordChanged {
		hash, err = HashPassword(password)
		if err != nil {
			return "", err
		}
	}
	const updateAdmin = `
		UPDATE users
		SET username = $1,
			password_hash = $2,
			updated_at = now(),
			password_changed_at = CASE WHEN $3 THEN now() ELSE password_changed_at END
		WHERE id = $4`
	if _, err = tx.Exec(ctx, updateAdmin, username, hash, passwordChanged, id); err != nil {
		return "", err
	}
	if _, err = tx.Exec(ctx, "UPDATE auth_sessions SET revoked_at=now() WHERE user_id=$1 AND revoked_at IS NULL", id); err != nil {
		return "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return "super_admin_updated", nil
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
