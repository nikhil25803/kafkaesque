package auth

import (
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestValidateCreateUser(t *testing.T) {
	validPassword := userTestPassword()
	tests := []struct {
		name      string
		input     CreateUserInput
		wantField string
		wantRole  string
	}{
		{
			name: "defaults and normalizes",
			input: CreateUserInput{
				Name: "  Jane Doe  ", Username: " JANE ", Email: " JANE@EXAMPLE.COM ", Password: validPassword,
			},
			wantRole: "user",
		},
		{
			name: "admin role",
			input: CreateUserInput{
				Name: "Jane Doe", Username: "jane", Email: "jane@example.com", Password: validPassword, Role: " ADMIN ",
			},
			wantRole: "admin",
		},
		{
			name: "matching email username",
			input: CreateUserInput{
				Name: "Jane Doe", Username: " JANE@EXAMPLE.COM ", Email: "jane@example.com", Password: validPassword,
			},
			wantRole: "user",
		},
		{
			name: "invalid name",
			input: CreateUserInput{
				Name: "", Username: "jane", Email: "jane@example.com", Password: validPassword,
			},
			wantField: "name",
		},
		{
			name: "invalid username",
			input: CreateUserInput{
				Name: "Jane", Username: "x", Email: "jane@example.com", Password: validPassword,
			},
			wantField: "username",
		},
		{
			name: "invalid email",
			input: CreateUserInput{
				Name: "Jane", Username: "jane", Email: "invalid", Password: validPassword,
			},
			wantField: "email",
		},
		{
			name: "different email username",
			input: CreateUserInput{
				Name: "Jane", Username: "other@example.com", Email: "jane@example.com", Password: validPassword,
			},
			wantField: "username",
		},
		{
			name: "invalid password",
			input: CreateUserInput{
				Name: "Jane", Username: "jane", Email: "jane@example.com", Password: strings.Repeat("x", 7),
			},
			wantField: "password",
		},
		{
			name: "Super Admin role",
			input: CreateUserInput{
				Name: "Jane", Username: "jane", Email: "jane@example.com", Password: validPassword, Role: "super_admin",
			},
			wantField: "role",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := validateCreateUser(test.input)
			if test.wantField == "" {
				if err != nil {
					t.Fatal(err)
				}
				if got.Role != test.wantRole {
					t.Fatalf("normalized input = %+v", got)
				}
				if test.name == "defaults and normalizes" && got.Name != "Jane Doe" {
					t.Fatalf("normalized name = %q", got.Name)
				}
				return
			}

			var validationError *ValidationError
			if !errors.As(err, &validationError) || validationError.Field != test.wantField {
				t.Fatalf("error = %v, want field %q", err, test.wantField)
			}
		})
	}
}

func userTestPassword() string {
	return string([]rune{'A', 'a', '1', '_'}) + strings.Repeat("x", 8)
}

func TestClassifyUserConflict(t *testing.T) {
	for _, test := range []struct {
		constraint string
		want       error
	}{
		{constraint: "users_username_key", want: ErrUsernameExists},
		{constraint: "users_email_lower_idx", want: ErrEmailExists},
	} {
		err := classifyUserConflict(&pgconn.PgError{Code: "23505", ConstraintName: test.constraint})
		if !errors.Is(err, test.want) {
			t.Fatalf("constraint %q error = %v", test.constraint, err)
		}
	}
}
