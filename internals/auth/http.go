package auth

import (
	"context"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	AccessCookie  = "kafkaesque_access"
	RefreshCookie = "kafkaesque_refresh"
	CSRFCookie    = "kafkaesque_csrf"

	loginFailureLimit = 5
	loginWindow       = 5 * time.Minute
	maxLoginKeys      = 10000
)

type userKey struct{}

// WithUser adds the authenticated user to a request context.
func WithUser(ctx context.Context, user User) context.Context {
	return context.WithValue(ctx, userKey{}, user)
}

// UserFromContext returns the authenticated user from a request context.
func UserFromContext(ctx context.Context) (User, bool) {
	user, ok := ctx.Value(userKey{}).(User)
	return user, ok
}

// RecordAuthenticatedUser lets the outer request logger include identity without emitting a header.
func RecordAuthenticatedUser(w http.ResponseWriter, userID string) {
	if recorder, ok := w.(interface{ SetAuthenticatedUser(string) }); ok {
		recorder.SetAuthenticatedUser(userID)
	}
}

// BearerToken returns the bearer token from an HTTP request.
func BearerToken(r *http.Request) string {
	header := strings.TrimSpace(r.Header.Get("Authorization"))
	if len(header) > 7 && strings.EqualFold(header[:7], "Bearer ") {
		return strings.TrimSpace(header[7:])
	}
	return ""
}

// CookieValue returns a named cookie value or an empty string.
func CookieValue(r *http.Request, name string) string {
	cookie, err := r.Cookie(name)
	if err != nil {
		return ""
	}
	return cookie.Value
}

// ClientAddress removes the port from a direct client address.
func ClientAddress(remote string) string {
	host, _, err := net.SplitHostPort(remote)
	if err == nil {
		return host
	}
	return remote
}

// SetCookies writes the browser session cookies.
func SetCookies(w http.ResponseWriter, tokens Tokens, secure bool) {
	http.SetCookie(w, sessionCookie(AccessCookie, tokens.AccessToken, tokens.AccessExpiresAt, true, secure))
	http.SetCookie(w, sessionCookie(RefreshCookie, tokens.RefreshToken, tokens.RefreshExpiresAt, true, secure))
	http.SetCookie(w, sessionCookie(CSRFCookie, tokens.CSRFToken, tokens.RefreshExpiresAt, false, secure))
}

// ClearCookies removes every browser session cookie.
func ClearCookies(w http.ResponseWriter, secure bool) {
	for _, cookie := range []struct {
		name     string
		httpOnly bool
	}{{AccessCookie, true}, {RefreshCookie, true}, {CSRFCookie, false}} {
		cleared := sessionCookie(cookie.name, "", time.Time{}, cookie.httpOnly, secure)
		cleared.MaxAge = -1
		http.SetCookie(w, cleared)
	}
}

// sessionCookie applies the same host-only policy to every auth cookie.
func sessionCookie(name, value string, expires time.Time, httpOnly, secure bool) *http.Cookie {
	return &http.Cookie{
		Name:     name,
		Value:    value,
		Path:     "/",
		Expires:  expires,
		HttpOnly: httpOnly,
		Secure:   secure,
		SameSite: http.SameSiteStrictMode,
	}
}

type attempt struct {
	failures int
	started  time.Time
}

type Limiter struct {
	mu       sync.Mutex
	attempts map[string]attempt
	now      func() time.Time
}

// NewLimiter creates an in-memory login attempt limiter.
func NewLimiter() *Limiter {
	return newLimiter(time.Now)
}

func newLimiter(now func() time.Time) *Limiter {
	return &Limiter{
		attempts: make(map[string]attempt),
		now:      now,
	}
}

// Allow reports whether another login attempt is allowed.
func (l *Limiter) Allow(username, remote string) bool {
	key := limiterKey(username, remote)
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	for candidate, value := range l.attempts {
		if now.Sub(value.started) >= loginWindow {
			delete(l.attempts, candidate)
		}
	}
	if _, exists := l.attempts[key]; !exists && len(l.attempts) >= maxLoginKeys {
		return false
	}
	current := l.attempts[key]
	if now.Sub(current.started) >= loginWindow {
		delete(l.attempts, key)
		return true
	}
	return current.failures < loginFailureLimit
}

// Failure records a failed login attempt.
func (l *Limiter) Failure(username, remote string) {
	key := limiterKey(username, remote)
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	current := l.attempts[key]
	if current.started.IsZero() || now.Sub(current.started) >= loginWindow {
		current = attempt{started: now}
	}
	current.failures++
	l.attempts[key] = current
}

// Success clears failed attempts after a successful login.
func (l *Limiter) Success(username, remote string) {
	l.mu.Lock()
	delete(l.attempts, limiterKey(username, remote))
	l.mu.Unlock()
}

// limiterKey isolates attempts by normalized username and direct client address.
func limiterKey(username, remote string) string {
	return strings.ToLower(strings.TrimSpace(username)) + "|" + ClientAddress(remote)
}
