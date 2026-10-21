package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestLimiterWindowAndReset(t *testing.T) {
	now := time.Date(2026, time.October, 13, 12, 0, 0, 0, time.UTC)
	limiter := newLimiter(func() time.Time { return now })
	for attempt := 0; attempt < loginFailureLimit; attempt++ {
		if !limiter.Allow("admin", "127.0.0.1:8080") {
			t.Fatalf("attempt %d was blocked early", attempt+1)
		}
		limiter.Failure("admin", "127.0.0.1:8080")
	}
	if limiter.Allow("admin", "127.0.0.1:8080") {
		t.Fatal("limit did not block the next attempt")
	}
	now = now.Add(loginWindow)
	if !limiter.Allow("admin", "127.0.0.1:8080") {
		t.Fatal("attempt remained blocked after the window")
	}
	limiter.Failure("admin", "127.0.0.1:8080")
	limiter.Success("admin", "127.0.0.1:8080")
	if !limiter.Allow("admin", "127.0.0.1:8080") {
		t.Fatal("success did not clear failed attempts")
	}
}

func TestNormalizeUsername(t *testing.T) {
	tests := []struct {
		input string
		want  string
		valid bool
	}{
		{input: " Admin.User ", want: "admin.user", valid: true},
		{input: "ab", valid: false},
		{input: "invalid name", valid: false},
		{input: "admin@example.com", want: "admin@example.com", valid: true},
	}
	for _, test := range tests {
		got, valid := NormalizeUsername(test.input)
		if got != test.want || valid != test.valid {
			t.Errorf("NormalizeUsername(%q) = %q, %v", test.input, got, valid)
		}
	}
}

func TestSessionCookieSecurityModes(t *testing.T) {
	tokens := Tokens{
		AccessToken:      "access",
		RefreshToken:     "refresh",
		CSRFToken:        "csrf",
		AccessExpiresAt:  time.Now().Add(time.Minute),
		RefreshExpiresAt: time.Now().Add(time.Hour),
	}

	for _, secure := range []bool{false, true} {
		response := httptest.NewRecorder()
		SetCookies(response, tokens, secure)
		cookies := response.Result().Cookies()
		if len(cookies) != 3 {
			t.Fatalf("cookies = %d, want 3", len(cookies))
		}
		for _, cookie := range cookies {
			if cookie.Secure != secure || cookie.Path != "/" || cookie.Domain != "" {
				t.Fatalf("unexpected cookie policy: %+v", cookie)
			}
			if cookie.SameSite != http.SameSiteStrictMode {
				t.Fatalf("cookie %s is not SameSite=Strict", cookie.Name)
			}
		}
	}
}
