package credentials

import (
	"strings"
	"testing"
)

func TestNormalizeName(t *testing.T) {
	name, valid := NormalizeName("  Jane Doe  ")
	if !valid || name != "Jane Doe" {
		t.Fatalf("name = %q, valid = %v", name, valid)
	}

	for _, value := range []string{"", "Jane\nDoe", "Jane\x00Doe", strings.Repeat("界", 101)} {
		if _, valid := NormalizeName(value); valid {
			t.Errorf("accepted invalid name %q", value)
		}
	}
}

func TestNormalizeEmail(t *testing.T) {
	email, valid := NormalizeEmail(" Super.Admin@Example.COM ")
	if !valid || email != "super.admin@example.com" {
		t.Fatalf("email = %q, valid = %v", email, valid)
	}

	for _, value := range []string{"", "Display Name <admin@example.com>", "invalid", strings.Repeat("a", 255)} {
		if _, valid := NormalizeEmail(value); valid {
			t.Errorf("accepted invalid email %q", value)
		}
	}
}

func TestNormalizeUsername(t *testing.T) {
	for _, value := range []string{"admin", "admin.user", "admin@example.com"} {
		if _, valid := NormalizeUsername(value); !valid {
			t.Errorf("rejected username %q", value)
		}
	}

	for _, value := range []string{"ab", "Admin Name", "admin+tag"} {
		if _, valid := NormalizeUsername(value); valid {
			t.Errorf("accepted username %q", value)
		}
	}
}

func TestValidatePassword(t *testing.T) {
	password := func(characters ...rune) string {
		return string(characters)
	}

	validWithLength := func(length int) string {
		return password('A', 'a', '1', '_') + strings.Repeat("x", length-4)
	}

	tests := map[string]struct {
		password string
		valid    bool
	}{
		"minimum":          {password: validWithLength(8), valid: true},
		"maximum":          {password: validWithLength(99), valid: true},
		"too short":        {password: validWithLength(7)},
		"too long":         {password: validWithLength(100)},
		"unicode length":   {password: password('A', 'a', '1', '_', '界', '界', '界', '界'), valid: true},
		"uppercase":        {password: password('a', 'a', '1', '_') + "value"},
		"lowercase":        {password: password('A', 'A', '1', '_') + "VALUE"},
		"number":           {password: password('A', 'a', '_', '_') + "value"},
		"special":          {password: password('A', 'a', '1', '2') + "value"},
		"unsupported mark": {password: password('A', 'a', '1', '-') + "value"},
		"line break":       {password: password('A', 'a', '1', '_') + "value\n"},
		"null":             {password: password('A', 'a', '1', '_') + "value\x00"},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := ValidatePassword(test.password)
			if (err == nil) != test.valid {
				t.Fatalf("error = %v, valid = %v", err, test.valid)
			}
		})
	}

	for _, special := range "!@#$%^&*()_" {
		if err := ValidatePassword(password('A', 'a', '1') + "value" + string(special)); err != nil {
			t.Errorf("special character %q was rejected: %v", special, err)
		}
	}
}
