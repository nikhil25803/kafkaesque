package auth

import (
	"errors"
	"reflect"
	"testing"
)

func TestValidateRoleInput(t *testing.T) {
	tests := []struct {
		name        string
		input       RoleInput
		want        RoleInput
		wantInvalid bool
	}{
		{
			name: "normalizes and sorts",
			input: RoleInput{Name: " Support_Team ", Permissions: []string{
				PermissionUserEdit, PermissionUserView, PermissionUserView,
			}},
			want: RoleInput{Name: "support_team", Permissions: []string{PermissionUserEdit, PermissionUserView}},
		},
		{name: "reserved name", input: RoleInput{Name: "admin"}, wantInvalid: true},
		{name: "invalid name", input: RoleInput{Name: "Support Team"}, wantInvalid: true},
		{name: "delete reserved", input: RoleInput{Name: "support", Permissions: []string{PermissionUserView, PermissionUserDelete}}, wantInvalid: true},
		{name: "mutation needs view", input: RoleInput{Name: "support", Permissions: []string{PermissionUserCreate}}, wantInvalid: true},
		{name: "unknown permission", input: RoleInput{Name: "support", Permissions: []string{"kafka:topic:view"}}, wantInvalid: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := validateRoleInput(test.input)
			var validationError *ValidationError
			if test.wantInvalid {
				if !errors.As(err, &validationError) {
					t.Fatalf("error = %v", err)
				}
				return
			}
			if err != nil || !reflect.DeepEqual(got, test.want) {
				t.Fatalf("input = %+v, error = %v", got, err)
			}
		})
	}
}

func TestHasPermission(t *testing.T) {
	user := User{Permissions: []string{PermissionUserView}}
	if !HasPermission(user, PermissionUserView) || HasPermission(user, PermissionUserDelete) {
		t.Fatalf("unexpected permission result for %+v", user)
	}
}
