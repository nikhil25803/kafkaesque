package auth

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
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

	cfg.Authentication.SuperAdminUsername = "root.admin"
	cfg.Authentication.SuperAdminPassword = "second-super-admin-password"
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
}

func TestPostgreSQLConcurrentRefreshAllowsOneRotation(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	resetAuthenticationDatabase(t, databaseURL)
	ctx := context.Background()
	service := openTestService(t, ctx, testAuthenticationConfig(databaseURL))
	defer service.Close()

	tokens, err := service.Login(ctx, "super_admin", "first-super-admin-password")
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

	tokens, err := service.Login(ctx, "super_admin", "first-super-admin-password")
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
}

func testDatabaseURL(t *testing.T) string {
	t.Helper()
	databaseURL := os.Getenv("KAFKAESQUE_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("KAFKAESQUE_TEST_DATABASE_URL is not set")
	}
	return databaseURL
}

func testAuthenticationConfig(databaseURL string) *kafkaesque_config.Config {
	return &kafkaesque_config.Config{
		Database: kafkaesque_config.DatabaseConfig{URL: databaseURL},
		Authentication: kafkaesque_config.AuthenticationConfig{
			SuperAdminUsername: "super_admin",
			SuperAdminPassword: "first-super-admin-password",
			AccessTTL:          time.Minute,
			RefreshTTL:         time.Hour,
		},
	}
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
		"DROP TABLE IF EXISTS auth_sessions, user_roles, roles, users, schema_migrations CASCADE",
	)
	if err != nil {
		t.Fatal(err)
	}
}
