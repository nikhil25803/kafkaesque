// Package credentials validates account identifiers and passwords.
package credentials

import (
	"fmt"
	"net/mail"
	"strings"
	"unicode/utf8"
)

const passwordSpecialCharacters = "!@#$%^&*()_"

// NormalizeName returns a trimmed display name when it is valid.
func NormalizeName(value string) (string, bool) {
	value = strings.TrimSpace(value)
	length := utf8.RuneCountInString(value)
	if length < 1 || length > 100 || strings.ContainsAny(value, "\r\n\x00") {
		return "", false
	}

	return value, true
}

// NormalizeEmail returns a normalized email address when it is valid.
func NormalizeEmail(value string) (string, bool) {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" || len(value) > 254 {
		return "", false
	}

	address, err := mail.ParseAddress(value)
	if err != nil || address.Address != value {
		return "", false
	}

	return value, true
}

// NormalizeUsername returns a supported handle or email username.
func NormalizeUsername(value string) (string, bool) {
	value = strings.ToLower(strings.TrimSpace(value))
	if _, valid := NormalizeEmail(value); valid {
		return value, true
	}
	if len(value) < 3 || len(value) > 64 {
		return "", false
	}

	for _, character := range value {
		if character >= 'a' && character <= 'z' ||
			character >= '0' && character <= '9' ||
			character == '.' || character == '_' || character == '-' {
			continue
		}
		return "", false
	}

	return value, true
}

// ValidatePassword applies the password policy shared by every account.
func ValidatePassword(password string) error {
	length := len([]rune(password))
	if length < 8 || length > 99 {
		return fmt.Errorf("must contain 8-99 characters")
	}
	if strings.ContainsAny(password, "\r\n\x00") {
		return fmt.Errorf("must not contain line breaks or null characters")
	}

	var uppercase, lowercase, number, special bool
	for _, character := range password {
		switch {
		case character >= 'A' && character <= 'Z':
			uppercase = true
		case character >= 'a' && character <= 'z':
			lowercase = true
		case character >= '0' && character <= '9':
			number = true
		case strings.ContainsRune(passwordSpecialCharacters, character):
			special = true
		}
	}

	if !uppercase {
		return fmt.Errorf("must contain an ASCII uppercase letter")
	}
	if !lowercase {
		return fmt.Errorf("must contain an ASCII lowercase letter")
	}
	if !number {
		return fmt.Errorf("must contain a number")
	}
	if !special {
		return fmt.Errorf("must contain one of %s", passwordSpecialCharacters)
	}

	return nil
}
