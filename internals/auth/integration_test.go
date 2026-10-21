package auth

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	kafkaesque_config "github.com/nikhil25803/kafkaesque/internals/config"
)

func TestPostgreSQLAuthenticationLifecycle(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	resetAuthenticationDatabase(t, databaseURL)
	ctx := context.Background()
	cfg := testAuthenticationConfig(databaseURL)
	service := openTestService(t, ctx, cfg)

	tokens, err := service.Login(ctx, "SUPER_ADMIN", cfg.Authentication.SuperAdminPassword)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.AuthenticateAccess(ctx, tokens.AccessToken); err != nil {
		t.Fatal(err)
	}

	rotated, err := service.Refresh(ctx, tokens.RefreshToken)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Refresh(ctx, tokens.RefreshToken); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("old refresh token was accepted: %v", err)
	}
	if err := service.Logout(ctx, rotated.AccessToken); err != nil {
		t.Fatal(err)
	}
	if _, err := service.AuthenticateAccess(ctx, rotated.AccessToken); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("logged-out token was accepted: %v", err)
	}
	service.Close()

	service = openTestService(t, ctx, cfg)
	tokens, err = service.Login(ctx, "super_admin", cfg.Authentication.SuperAdminPassword)
	if err != nil {
		t.Fatal(err)
	}
	service.Close()

	originalConfig := *cfg
	cfg.Authentication.SuperAdminUsername = "root.admin"
	cfg.Authentication.SuperAdminEmail = "root.admin@example.com"
	cfg.Authentication.SuperAdminPassword = integrationPassword("second")
	var bootstrapLogs bytes.Buffer
	failedService, openErr := Open(
		ctx,
		cfg,
		slog.New(slog.NewJSONHandler(&bootstrapLogs, nil)),
	)
	if openErr == nil {
		failedService.Close()
		t.Fatal("verify recovery accepted changed bootstrap credentials")
	}
	if strings.Contains(openErr.Error(), cfg.Authentication.SuperAdminUsername) ||
		strings.Contains(openErr.Error(), cfg.Authentication.SuperAdminEmail) ||
		strings.Contains(openErr.Error(), cfg.Authentication.SuperAdminPassword) {
		t.Fatalf("bootstrap error exposed configured values: %v", openErr)
	}
	logOutput := bootstrapLogs.String()
	if !strings.Contains(logOutput, `"mismatched_fields":["username","email","password"]`) {
		t.Fatalf("bootstrap log omitted mismatch fields: %s", logOutput)
	}
	if strings.Contains(logOutput, cfg.Authentication.SuperAdminUsername) ||
		strings.Contains(logOutput, cfg.Authentication.SuperAdminEmail) ||
		strings.Contains(logOutput, cfg.Authentication.SuperAdminPassword) {
		t.Fatal("bootstrap log exposed configured values")
	}
	service = openTestService(t, ctx, &originalConfig)
	if _, err := service.AuthenticateAccess(ctx, tokens.AccessToken); err != nil {
		service.Close()
		t.Fatalf("verify recovery mutated the existing session: %v", err)
	}
	service.Close()

	cfg.Authentication.SuperAdminRecovery = "update"
	service = openTestService(t, ctx, cfg)
	defer service.Close()
	if _, err := service.AuthenticateAccess(ctx, tokens.AccessToken); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("credential rotation did not revoke sessions: %v", err)
	}
	if _, err := service.Login(
		ctx,
		cfg.Authentication.SuperAdminUsername,
		cfg.Authentication.SuperAdminPassword,
	); err != nil {
		t.Fatal(err)
	}
}

func TestPostgreSQLUpdatePreservesOtherUsers(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	resetAuthenticationDatabase(t, databaseURL)
	ctx := context.Background()
	cfg := testAuthenticationConfig(databaseURL)
	service := openTestService(t, ctx, cfg)
	tokens, err := service.Login(ctx, cfg.Authentication.SuperAdminUsername, cfg.Authentication.SuperAdminPassword)
	if err != nil {
		service.Close()
		t.Fatal(err)
	}
	service.Close()

	pool := openTestPool(t, databaseURL)
	insertOrdinaryUser(t, pool, "member", "member@example.com", "user")
	pool.Close()

	cfg.Authentication.SuperAdminEmail = "updated@example.com"
	cfg.Authentication.SuperAdminRecovery = "update"
	service = openTestService(t, ctx, cfg)
	defer service.Close()

	if _, err := service.AuthenticateAccess(ctx, tokens.AccessToken); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("updated Super Admin session was not revoked: %v", err)
	}
	var users int
	if err := service.pool.QueryRow(ctx, "SELECT count(*) FROM users").Scan(&users); err != nil {
		t.Fatal(err)
	}
	if users != 2 {
		t.Fatalf("users = %d, want 2", users)
	}
}

func TestPostgreSQLReinitializeRebuildsEntireDatabase(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	resetAuthenticationDatabase(t, databaseURL)
	ctx := context.Background()
	cfg := testAuthenticationConfig(databaseURL)
	service := openTestService(t, ctx, cfg)
	service.Close()

	pool := openTestPool(t, databaseURL)
	insertOrdinaryUser(t, pool, "member", "member@example.com", "admin")
	if _, err := pool.Exec(ctx, "CREATE TABLE unrelated_data(value text NOT NULL)"); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO unrelated_data(value) VALUES('preserved')"); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO schema_migrations(version, checksum) VALUES('unrelated.sql', 'keep')"); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	pool.Close()

	cfg.Authentication.SuperAdminRecovery = "reinitialize"
	service = openTestService(t, ctx, cfg)
	var usersBeforeMismatch int
	if err := service.pool.QueryRow(ctx, "SELECT count(*) FROM users").Scan(&usersBeforeMismatch); err != nil {
		service.Close()
		t.Fatal(err)
	}
	service.Close()
	if usersBeforeMismatch != 2 {
		t.Fatalf("reinitialize without mismatch changed users: %d", usersBeforeMismatch)
	}

	cfg.Authentication.SuperAdminPassword = integrationPassword("replacement")
	service = openTestService(t, ctx, cfg)
	defer service.Close()

	var users, roles, unrelatedMigrations int
	if err := service.pool.QueryRow(ctx, "SELECT count(*) FROM users").Scan(&users); err != nil {
		t.Fatal(err)
	}
	if err := service.pool.QueryRow(ctx, "SELECT count(*) FROM roles").Scan(&roles); err != nil {
		t.Fatal(err)
	}
	if err := service.pool.QueryRow(ctx, "SELECT count(*) FROM schema_migrations WHERE version='unrelated.sql'").Scan(&unrelatedMigrations); err != nil {
		t.Fatal(err)
	}
	var unrelatedTable *string
	if err := service.pool.QueryRow(ctx, "SELECT to_regclass('public.unrelated_data')::text").Scan(&unrelatedTable); err != nil {
		t.Fatal(err)
	}
	if users != 1 || roles != 3 || unrelatedTable != nil || unrelatedMigrations != 0 {
		t.Fatalf(
			"users=%d roles=%d unrelated_table=%v unrelated_migrations=%d",
			users,
			roles,
			unrelatedTable,
			unrelatedMigrations,
		)
	}
}

func TestPostgreSQLReinitializeRollsBackFailedRebuild(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	resetAuthenticationDatabase(t, databaseURL)
	ctx := context.Background()
	cfg := testAuthenticationConfig(databaseURL)
	service := openTestService(t, ctx, cfg)
	service.Close()

	pool := openTestPool(t, databaseURL)
	if _, err := pool.Exec(ctx, "CREATE TABLE rollback_marker(value text NOT NULL)"); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		pool.Close()
		t.Fatal(err)
	}
	brokenMigrations := fstest.MapFS{
		"migrations/001_broken.sql": {Data: []byte("this is not valid SQL")},
	}
	err = reinitializeDatabaseWithMigrations(ctx, conn, cfg.Authentication, brokenMigrations)
	conn.Release()
	if err == nil {
		pool.Close()
		t.Fatal("expected database reinitialization to fail")
	}

	var users int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM users").Scan(&users); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	var marker *string
	if err := pool.QueryRow(ctx, "SELECT to_regclass('public.rollback_marker')::text").Scan(&marker); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	pool.Close()
	if users != 1 || marker == nil {
		t.Fatalf("rollback did not preserve database: users=%d marker=%v", users, marker)
	}
}

func TestPostgreSQLUpgradesExistingAuthenticationSchema(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	resetAuthenticationDatabase(t, databaseURL)
	ctx := context.Background()
	cfg := testAuthenticationConfig(databaseURL)

	body, err := migrations.ReadFile("migrations/001_auth.sql")
	if err != nil {
		t.Fatal(err)
	}
	checksumBytes := sha256.Sum256(body)
	checksum := hex.EncodeToString(checksumBytes[:])
	hash, err := HashPassword(cfg.Authentication.SuperAdminPassword)
	if err != nil {
		t.Fatal(err)
	}

	pool := openTestPool(t, databaseURL)
	if _, err := pool.Exec(ctx, string(body)); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO schema_migrations(version, checksum) VALUES('001_auth.sql', $1)", checksum); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	const createExistingAdmin = `
		WITH new_user AS (
			INSERT INTO users(username, password_hash) VALUES($1, $2) RETURNING id
		)
		INSERT INTO user_roles(user_id, role_id)
		SELECT new_user.id, roles.id FROM new_user, roles WHERE roles.name='super_admin'`
	if _, err := pool.Exec(ctx, createExistingAdmin, cfg.Authentication.SuperAdminUsername, hash); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	pool.Close()

	service := openTestService(t, ctx, cfg)
	defer service.Close()
	var name, email string
	if err := service.pool.QueryRow(ctx, "SELECT name, email FROM users").Scan(&name, &email); err != nil {
		t.Fatal(err)
	}
	if name != "Super Admin" || email != cfg.Authentication.SuperAdminEmail {
		t.Fatalf("upgraded profile = %q <%s>", name, email)
	}
}

func TestPostgreSQLConcurrentStartupCreatesOneSuperAdmin(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	resetAuthenticationDatabase(t, databaseURL)
	ctx := context.Background()
	cfg := testAuthenticationConfig(databaseURL)

	services := make(chan *Service, 2)
	errorsFound := make(chan error, 2)
	var wait sync.WaitGroup
	for attempt := 0; attempt < 2; attempt++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			service, err := Open(ctx, cfg, testLogger())
			if err != nil {
				errorsFound <- err
				return
			}
			services <- service
		}()
	}
	wait.Wait()
	close(services)
	close(errorsFound)

	for err := range errorsFound {
		t.Errorf("concurrent startup failed: %v", err)
	}
	opened := 0
	for service := range services {
		opened++
		service.Close()
	}
	if opened != 2 {
		t.Fatalf("opened services = %d, want 2", opened)
	}

	pool := openTestPool(t, databaseURL)
	defer pool.Close()
	var superAdmins int
	const countSuperAdmins = `
		SELECT count(*)
		FROM users u
		JOIN user_roles ur ON ur.user_id = u.id
		JOIN roles r ON r.id = ur.role_id
		WHERE r.name = 'super_admin'`
	if err := pool.QueryRow(ctx, countSuperAdmins).Scan(&superAdmins); err != nil {
		t.Fatal(err)
	}
	if superAdmins != 1 {
		t.Fatalf("Super Admins = %d, want 1", superAdmins)
	}
	var embeddedRoleColumns int
	const countEmbeddedRoles = `
		SELECT count(*)
		FROM information_schema.columns
		WHERE table_schema = 'public'
			AND table_name = 'users'
			AND column_name = 'role'`
	if err := pool.QueryRow(ctx, countEmbeddedRoles).Scan(&embeddedRoleColumns); err != nil {
		t.Fatal(err)
	}
	if embeddedRoleColumns != 0 {
		t.Fatal("users.role still exists")
	}
	var roles int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM roles").Scan(&roles); err != nil {
		t.Fatal(err)
	}
	if roles != 3 {
		t.Fatalf("roles = %d, want 3", roles)
	}
	const assignSecondRole = `
		INSERT INTO user_roles(user_id, role_id)
		SELECT u.id, r.id FROM users u, roles r WHERE r.name='admin'`
	if _, err := pool.Exec(ctx, assignSecondRole); err == nil {
		t.Fatal("database accepted a second role for one user")
	}
}

func TestPostgreSQLConcurrentRefreshAllowsOneRotation(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	resetAuthenticationDatabase(t, databaseURL)
	ctx := context.Background()
	service := openTestService(t, ctx, testAuthenticationConfig(databaseURL))
	defer service.Close()

	tokens, err := service.Login(ctx, "super_admin", integrationPassword("first"))
	if err != nil {
		t.Fatal(err)
	}

	results := make(chan error, 2)
	var wait sync.WaitGroup
	for attempt := 0; attempt < 2; attempt++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, refreshErr := service.Refresh(ctx, tokens.RefreshToken)
			results <- refreshErr
		}()
	}
	wait.Wait()
	close(results)

	successes := 0
	rejections := 0
	for refreshErr := range results {
		switch {
		case refreshErr == nil:
			successes++
		case errors.Is(refreshErr, ErrInvalidToken):
			rejections++
		default:
			t.Fatalf("unexpected refresh error: %v", refreshErr)
		}
	}
	if successes != 1 || rejections != 1 {
		t.Fatalf("refresh results = %d success, %d rejected", successes, rejections)
	}
}

func TestPostgreSQLSessionCleanup(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	resetAuthenticationDatabase(t, databaseURL)
	ctx := context.Background()
	service := openTestService(t, ctx, testAuthenticationConfig(databaseURL))
	defer service.Close()

	tokens, err := service.Login(ctx, "super_admin", integrationPassword("first"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.pool.Exec(ctx, "UPDATE auth_sessions SET refresh_expires_at=now() - interval '1 minute'"); err != nil {
		t.Fatal(err)
	}
	if err := service.Cleanup(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Refresh(ctx, tokens.RefreshToken); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expired refresh token was accepted: %v", err)
	}
}

func TestPostgreSQLMigrationChecksumMismatch(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	resetAuthenticationDatabase(t, databaseURL)
	ctx := context.Background()
	cfg := testAuthenticationConfig(databaseURL)
	service := openTestService(t, ctx, cfg)
	service.Close()

	pool := openTestPool(t, databaseURL)
	if _, err := pool.Exec(ctx, "UPDATE schema_migrations SET checksum='invalid'"); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	pool.Close()

	service, err := Open(ctx, cfg, testLogger())
	if err == nil {
		service.Close()
		t.Fatal("expected migration checksum mismatch")
	}

	cfg.Authentication.SuperAdminRecovery = "reinitialize"
	service = openTestService(t, ctx, cfg)
	defer service.Close()
	var migrationsApplied int
	if err := service.pool.QueryRow(ctx, "SELECT count(*) FROM schema_migrations").Scan(&migrationsApplied); err != nil {
		t.Fatal(err)
	}
	if migrationsApplied != 3 {
		t.Fatalf("migrations = %d, want 3", migrationsApplied)
	}
}

func TestPostgreSQLSchemaVerification(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	resetAuthenticationDatabase(t, databaseURL)
	ctx := context.Background()
	cfg := testAuthenticationConfig(databaseURL)
	service := openTestService(t, ctx, cfg)
	service.Close()

	pool := openTestPool(t, databaseURL)
	if _, err := pool.Exec(ctx, "ALTER TABLE auth_sessions DROP COLUMN csrf_token_hash"); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	pool.Close()

	service, err := Open(ctx, cfg, testLogger())
	if err == nil {
		service.Close()
		t.Fatal("expected schema verification failure")
	}

	cfg.Authentication.SuperAdminRecovery = "reinitialize"
	service = openTestService(t, ctx, cfg)
	defer service.Close()
	var csrfColumns int
	const countCSRFColumns = `
		SELECT count(*) FROM information_schema.columns
		WHERE table_schema='public' AND table_name='auth_sessions' AND column_name='csrf_token_hash'`
	if err := service.pool.QueryRow(ctx, countCSRFColumns).Scan(&csrfColumns); err != nil {
		t.Fatal(err)
	}
	if csrfColumns != 1 {
		t.Fatal("reinitialize did not restore authentication schema")
	}
}

func TestPostgreSQLRejectsMissingSuperAdminAssignment(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	resetAuthenticationDatabase(t, databaseURL)
	ctx := context.Background()
	cfg := testAuthenticationConfig(databaseURL)
	service := openTestService(t, ctx, cfg)
	service.Close()

	pool := openTestPool(t, databaseURL)
	if _, err := pool.Exec(ctx, "DELETE FROM user_roles"); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	pool.Close()

	service, err := Open(ctx, cfg, testLogger())
	if err == nil {
		service.Close()
		t.Fatal("expected missing Super Admin assignment to fail startup")
	}
	var users int
	pool = openTestPool(t, databaseURL)
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM users").Scan(&users); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	pool.Close()
	if users != 1 {
		t.Fatalf("verify changed users: %d", users)
	}

	cfg.Authentication.SuperAdminRecovery = "update"
	service, err = Open(ctx, cfg, testLogger())
	if err == nil {
		service.Close()
		t.Fatal("update accepted missing Super Admin assignment")
	}

	cfg.Authentication.SuperAdminRecovery = "reinitialize"
	service = openTestService(t, ctx, cfg)
	defer service.Close()
	var assignments int
	const countAssignments = `
		SELECT count(*)
		FROM user_roles ur JOIN roles r ON r.id=ur.role_id
		WHERE r.name='super_admin'`
	if err := service.pool.QueryRow(ctx, countAssignments).Scan(&assignments); err != nil {
		t.Fatal(err)
	}
	if assignments != 1 {
		t.Fatalf("Super Admin assignments = %d, want 1", assignments)
	}
}

func TestPostgreSQLReinitializeRepairsSuperAdminIntegrity(t *testing.T) {
	tests := map[string]func(context.Context, *testing.T, *pgxpool.Pool){
		"duplicate assignment": func(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
			insertOrdinaryUser(t, pool, "duplicate", "duplicate@example.com", "super_admin")
		},
		"missing role": func(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
			if _, err := pool.Exec(ctx, "DELETE FROM user_roles; DELETE FROM roles WHERE name='super_admin'"); err != nil {
				t.Fatal(err)
			}
		},
		"inactive record": func(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
			if _, err := pool.Exec(ctx, "UPDATE users SET active=false"); err != nil {
				t.Fatal(err)
			}
		},
		"invalid profile": func(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
			if _, err := pool.Exec(ctx, "UPDATE users SET name='Changed'"); err != nil {
				t.Fatal(err)
			}
		},
	}

	for name, corrupt := range tests {
		t.Run(name, func(t *testing.T) {
			databaseURL := testDatabaseURL(t)
			resetAuthenticationDatabase(t, databaseURL)
			ctx := context.Background()
			cfg := testAuthenticationConfig(databaseURL)
			service := openTestService(t, ctx, cfg)
			service.Close()

			pool := openTestPool(t, databaseURL)
			corrupt(ctx, t, pool)
			pool.Close()

			cfg.Authentication.SuperAdminRecovery = "reinitialize"
			service = openTestService(t, ctx, cfg)
			defer service.Close()
			var users, roles, assignments int
			if err := service.pool.QueryRow(ctx, "SELECT count(*) FROM users").Scan(&users); err != nil {
				t.Fatal(err)
			}
			if err := service.pool.QueryRow(ctx, "SELECT count(*) FROM roles").Scan(&roles); err != nil {
				t.Fatal(err)
			}
			const countAssignments = `
				SELECT count(*) FROM user_roles ur
				JOIN roles r ON r.id=ur.role_id WHERE r.name='super_admin'`
			if err := service.pool.QueryRow(ctx, countAssignments).Scan(&assignments); err != nil {
				t.Fatal(err)
			}
			if users != 1 || roles != 3 || assignments != 1 {
				t.Fatalf("users=%d roles=%d assignments=%d", users, roles, assignments)
			}
		})
	}
}

func testDatabaseURL(t *testing.T) string {
	t.Helper()
	databaseURL := os.Getenv("KAFKAESQUE_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("KAFKAESQUE_TEST_DATABASE_URL is not set")
	}
	if err := validateAuthenticationTestDatabaseURL(databaseURL); err != nil {
		t.Fatal(err)
	}
	return databaseURL
}

func TestAuthenticationTestDatabaseGuard(t *testing.T) {
	if err := validateAuthenticationTestDatabaseURL("postgres://localhost/kafkaesque"); err == nil {
		t.Fatal("development database was accepted for destructive tests")
	}
	if err := validateAuthenticationTestDatabaseURL("postgres://localhost/kafkaesque_auth_test"); err != nil {
		t.Fatalf("test database was rejected: %v", err)
	}
}

func validateAuthenticationTestDatabaseURL(databaseURL string) error {
	configuration, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return fmt.Errorf("parse authentication test database URL: %w", err)
	}
	if !strings.HasSuffix(configuration.ConnConfig.Database, "_test") {
		return fmt.Errorf("authentication integration database name must end with _test")
	}
	return nil
}

func testAuthenticationConfig(databaseURL string) *kafkaesque_config.Config {
	return &kafkaesque_config.Config{
		Database: kafkaesque_config.DatabaseConfig{URL: databaseURL},
		Authentication: kafkaesque_config.AuthenticationConfig{
			SuperAdminUsername: "super_admin",
			SuperAdminEmail:    "super-admin@example.com",
			SuperAdminPassword: integrationPassword("first"),
			SuperAdminRecovery: "verify",
			AccessTTL:          time.Minute,
			RefreshTTL:         time.Hour,
		},
	}
}

func integrationPassword(seed string) string {
	return string([]rune{'A', 'a', '1', '_'}) + seed + "_test_value"
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

func openTestService(t *testing.T, ctx context.Context, cfg *kafkaesque_config.Config) *Service {
	t.Helper()
	service, err := Open(ctx, cfg, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func openTestPool(t *testing.T, databaseURL string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	return pool
}

func resetAuthenticationDatabase(t *testing.T, databaseURL string) {
	t.Helper()
	pool := openTestPool(t, databaseURL)
	defer pool.Close()
	_, err := pool.Exec(
		context.Background(),
		"DROP TABLE IF EXISTS auth_sessions, user_roles, roles, users, schema_migrations, unrelated_data, rollback_marker CASCADE",
	)
	if err != nil {
		t.Fatal(err)
	}
}

func insertOrdinaryUser(t *testing.T, pool *pgxpool.Pool, username, email, role string) {
	t.Helper()
	hash, err := HashPassword(integrationPassword(username))
	if err != nil {
		t.Fatal(err)
	}
	const query = `
		WITH new_user AS (
			INSERT INTO users(username, name, email, password_hash)
			VALUES($1, 'Test User', $2, $3)
			RETURNING id
		)
		INSERT INTO user_roles(user_id, role_id)
		SELECT new_user.id, roles.id
		FROM new_user, roles
		WHERE roles.name=$4`
	if _, err := pool.Exec(context.Background(), query, username, email, hash, role); err != nil {
		t.Fatal(err)
	}
}
