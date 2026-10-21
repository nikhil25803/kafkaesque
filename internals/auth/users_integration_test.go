package auth

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestPostgreSQLUserManagement(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	resetAuthenticationDatabase(t, databaseURL)
	ctx := context.Background()
	cfg := testAuthenticationConfig(databaseURL)
	service := openTestService(t, ctx, cfg)
	defer service.Close()

	created, err := service.CreateUser(ctx, CreateUserInput{
		Name: "Jane Doe", Username: "JANE", Email: "JANE@EXAMPLE.COM",
		Password: integrationPassword("jane"), Role: "admin",
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.Username != "jane" || created.Email != "jane@example.com" || created.Role != "admin" || !created.Active {
		t.Fatalf("created user = %+v", created)
	}

	var storedHash string
	if err := service.pool.QueryRow(ctx, "SELECT password_hash FROM users WHERE id=$1", created.ID).Scan(&storedHash); err != nil {
		t.Fatal(err)
	}
	if storedHash == integrationPassword("jane") || !VerifyPassword(integrationPassword("jane"), storedHash) {
		t.Fatal("password was not stored as a valid hash")
	}
	assertCanAuthenticate := func(username, password, role string) {
		t.Helper()
		tokens, err := service.Login(ctx, username, password)
		if err != nil || tokens.User.Role != role {
			t.Fatalf("%s login tokens=%+v err=%v", role, tokens.User, err)
		}
		if user, err := service.AuthenticateAccess(ctx, tokens.AccessToken); err != nil || user.Role != role {
			t.Fatalf("%s access user=%+v err=%v", role, user, err)
		}
		refreshed, err := service.Refresh(ctx, tokens.RefreshToken)
		if err != nil || refreshed.User.Role != role {
			t.Fatalf("%s refresh user=%+v err=%v", role, refreshed.User, err)
		}
		if user, err := service.AuthenticateAccess(ctx, refreshed.AccessToken); err != nil || user.Role != role {
			t.Fatalf("%s refreshed access user=%+v err=%v", role, user, err)
		}
		if err := service.Logout(ctx, refreshed.AccessToken); err != nil {
			t.Fatalf("%s logout: %v", role, err)
		}
	}
	assertCanAuthenticate(created.Username, integrationPassword("jane"), "admin")

	users, total, err := service.ListUsers(ctx, 1, 25)
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 || len(users) != 2 || users[0].ID != created.ID || users[1].Role != "super_admin" {
		t.Fatalf("users=%+v total=%d", users, total)
	}

	if _, err := service.CreateUser(ctx, CreateUserInput{
		Name: "Duplicate Username", Username: "JANE", Email: "other@example.com",
		Password: integrationPassword("duplicate"),
	}); !errors.Is(err, ErrUsernameExists) {
		t.Fatalf("duplicate username error = %v", err)
	}
	if _, err := service.CreateUser(ctx, CreateUserInput{
		Name: "Duplicate Email", Username: "other", Email: "JANE@EXAMPLE.COM",
		Password: integrationPassword("duplicate"),
	}); !errors.Is(err, ErrEmailExists) {
		t.Fatalf("duplicate email error = %v", err)
	}

	users, total, err = service.ListUsers(ctx, 2, 1)
	if err != nil || total != 2 || len(users) != 1 || users[0].Role != "super_admin" {
		t.Fatalf("second page users=%+v total=%d err=%v", users, total, err)
	}

	createdUser, err := service.CreateUser(ctx, CreateUserInput{
		Name: "John Doe", Username: "john", Email: "john@example.com",
		Password: integrationPassword("john"), Role: "user",
	})
	if err != nil {
		t.Fatal(err)
	}
	assertCanAuthenticate(createdUser.Username, integrationPassword("john"), "user")
	if _, err := service.pool.Exec(ctx, "UPDATE users SET active=false WHERE id=$1", createdUser.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Login(ctx, createdUser.Username, integrationPassword("john")); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("inactive account login error = %v", err)
	}
}

func TestPostgreSQLConcurrentUserConflict(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	resetAuthenticationDatabase(t, databaseURL)
	ctx := context.Background()
	service := openTestService(t, ctx, testAuthenticationConfig(databaseURL))
	defer service.Close()

	start := make(chan struct{})
	errorsFound := make(chan error, 2)
	var wait sync.WaitGroup
	for _, email := range []string{"one@example.com", "two@example.com"} {
		wait.Add(1)
		go func(email string) {
			defer wait.Done()
			<-start
			_, err := service.CreateUser(ctx, CreateUserInput{
				Name: "Concurrent User", Username: "concurrent", Email: email,
				Password: integrationPassword("concurrent"),
			})
			errorsFound <- err
		}(email)
	}
	close(start)
	wait.Wait()
	close(errorsFound)

	successes, conflicts := 0, 0
	for err := range errorsFound {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrUsernameExists):
			conflicts++
		default:
			t.Fatalf("unexpected creation error: %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes=%d conflicts=%d", successes, conflicts)
	}
}

func TestPostgreSQLUserCreationRollsBackWithoutRole(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	resetAuthenticationDatabase(t, databaseURL)
	ctx := context.Background()
	service := openTestService(t, ctx, testAuthenticationConfig(databaseURL))
	defer service.Close()

	if _, err := service.pool.Exec(ctx, "DELETE FROM roles WHERE name='admin'"); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateUser(ctx, CreateUserInput{
		Name: "Missing Role", Username: "missing-role", Email: "missing-role@example.com",
		Password: integrationPassword("missing-role"), Role: "admin",
	}); err == nil {
		t.Fatal("creation succeeded without the requested role")
	}

	var count int
	if err := service.pool.QueryRow(ctx, "SELECT count(*) FROM users WHERE username='missing-role'").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("partial users = %d", count)
	}
}
