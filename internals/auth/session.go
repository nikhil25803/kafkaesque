package auth

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Login validates credentials and creates a session.
func (s *Service) Login(ctx context.Context, username, password string) (Tokens, error) {
	if err := s.Cleanup(ctx); err != nil {
		return Tokens{}, err
	}
	var user User
	var hash string
	var active bool
	username, valid := NormalizeUsername(username)
	if !valid {
		VerifyPassword(password, s.dummyPasswordHash)
		return Tokens{}, ErrInvalidCredentials
	}
	const query = `
		SELECT u.id, u.username, r.name, u.password_hash, u.active
		FROM users u
		JOIN user_roles ur ON ur.user_id = u.id
		JOIN roles r ON r.id = ur.role_id
		WHERE u.username = $1`
	err := s.pool.QueryRow(ctx, query, username).Scan(
		&user.ID,
		&user.Username,
		&user.Role,
		&hash,
		&active,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		VerifyPassword(password, s.dummyPasswordHash)
		return Tokens{}, ErrInvalidCredentials
	}
	if err != nil {
		return Tokens{}, fmt.Errorf("authenticate credentials: %w", err)
	}
	if !active || !VerifyPassword(password, hash) {
		return Tokens{}, ErrInvalidCredentials
	}
	return s.createSession(ctx, user)
}

func (s *Service) createSession(ctx context.Context, user User) (Tokens, error) {
	access, accessHash, err := newToken()
	if err != nil {
		return Tokens{}, err
	}
	refresh, refreshHash, err := newToken()
	if err != nil {
		return Tokens{}, err
	}
	csrf, csrfHash, err := newToken()
	if err != nil {
		return Tokens{}, err
	}
	now := time.Now().UTC()
	tokens := Tokens{
		AccessToken:      access,
		RefreshToken:     refresh,
		CSRFToken:        csrf,
		AccessExpiresAt:  now.Add(s.accessTTL),
		RefreshExpiresAt: now.Add(s.refreshTTL),
		User:             user,
	}
	const query = `
		INSERT INTO auth_sessions (
			user_id,
			access_token_hash,
			refresh_token_hash,
			csrf_token_hash,
			access_expires_at,
			refresh_expires_at
		) VALUES ($1, $2, $3, $4, $5, $6)`
	_, err = s.pool.Exec(
		ctx,
		query,
		user.ID,
		accessHash,
		refreshHash,
		csrfHash,
		tokens.AccessExpiresAt,
		tokens.RefreshExpiresAt,
	)
	if err != nil {
		return Tokens{}, fmt.Errorf("create authentication session: %w", err)
	}
	return tokens, nil
}

// AuthenticateAccess validates an access token.
func (s *Service) AuthenticateAccess(ctx context.Context, token string) (User, error) {
	var user User
	const query = `
		SELECT u.id, u.username, r.name
		FROM auth_sessions s
		JOIN users u ON u.id = s.user_id
		JOIN user_roles ur ON ur.user_id = u.id
		JOIN roles r ON r.id = ur.role_id
		WHERE s.access_token_hash = $1
			AND s.revoked_at IS NULL
			AND s.access_expires_at > now()
			AND u.active`
	err := s.pool.QueryRow(ctx, query, tokenHash(token)).Scan(
		&user.ID,
		&user.Username,
		&user.Role,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return User{}, ErrInvalidToken
	}
	if err != nil {
		return User{}, fmt.Errorf("authenticate session: %w", err)
	}
	_, _ = s.pool.Exec(ctx, "UPDATE auth_sessions SET last_activity_at=now() WHERE access_token_hash=$1", tokenHash(token))
	return user, nil
}

// Refresh replaces a valid refresh token with a new session.
func (s *Service) Refresh(ctx context.Context, token string) (Tokens, error) {
	if err := s.Cleanup(ctx); err != nil {
		return Tokens{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Tokens{}, err
	}
	defer tx.Rollback(ctx)
	var user User
	var sessionID string
	const findSession = `
		SELECT s.id, u.id, u.username, r.name
		FROM auth_sessions s
		JOIN users u ON u.id = s.user_id
		JOIN user_roles ur ON ur.user_id = u.id
		JOIN roles r ON r.id = ur.role_id
		WHERE s.refresh_token_hash = $1
			AND s.revoked_at IS NULL
			AND s.refresh_expires_at > now()
			AND u.active
		FOR UPDATE OF s`
	err = tx.QueryRow(ctx, findSession, tokenHash(token)).Scan(
		&sessionID,
		&user.ID,
		&user.Username,
		&user.Role,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Tokens{}, ErrInvalidToken
	}
	if err != nil {
		return Tokens{}, fmt.Errorf("refresh session: %w", err)
	}
	if _, err = tx.Exec(ctx, "UPDATE auth_sessions SET revoked_at=now() WHERE id=$1", sessionID); err != nil {
		return Tokens{}, err
	}
	access, accessHash, err := newToken()
	if err != nil {
		return Tokens{}, err
	}
	refresh, refreshHash, err := newToken()
	if err != nil {
		return Tokens{}, err
	}
	csrf, csrfHash, err := newToken()
	if err != nil {
		return Tokens{}, err
	}
	now := time.Now().UTC()
	out := Tokens{
		AccessToken:      access,
		RefreshToken:     refresh,
		CSRFToken:        csrf,
		AccessExpiresAt:  now.Add(s.accessTTL),
		RefreshExpiresAt: now.Add(s.refreshTTL),
		User:             user,
	}
	const insertSession = `
		INSERT INTO auth_sessions (
			user_id,
			access_token_hash,
			refresh_token_hash,
			csrf_token_hash,
			access_expires_at,
			refresh_expires_at
		) VALUES ($1, $2, $3, $4, $5, $6)`
	_, err = tx.Exec(
		ctx,
		insertSession,
		user.ID,
		accessHash,
		refreshHash,
		csrfHash,
		out.AccessExpiresAt,
		out.RefreshExpiresAt,
	)
	if err != nil {
		return Tokens{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Tokens{}, err
	}
	return out, nil
}

// Logout revokes the session that owns a token.
func (s *Service) Logout(ctx context.Context, token string) error {
	if token == "" {
		return ErrInvalidToken
	}
	const query = `
		UPDATE auth_sessions
		SET revoked_at = now()
		WHERE revoked_at IS NULL
			AND (access_token_hash = $1 OR refresh_token_hash = $1)`
	result, err := s.pool.Exec(ctx, query, tokenHash(token))
	if err != nil {
		return fmt.Errorf("revoke session: %w", err)
	}
	if result.RowsAffected() == 0 {
		return ErrInvalidToken
	}
	return nil
}

// ValidateCSRF checks the CSRF token for an access token.
func (s *Service) ValidateCSRF(ctx context.Context, accessToken, csrfToken string) (bool, error) {
	var ok bool
	const query = `
		SELECT csrf_token_hash = $2
		FROM auth_sessions
		WHERE access_token_hash = $1
			AND revoked_at IS NULL
			AND access_expires_at > now()`
	err := s.pool.QueryRow(ctx, query, tokenHash(accessToken), tokenHash(csrfToken)).Scan(&ok)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("validate CSRF token: %w", err)
	}
	return ok, nil
}

// ValidateRefreshCSRF checks the CSRF token for a refresh token.
func (s *Service) ValidateRefreshCSRF(ctx context.Context, refreshToken, csrfToken string) (bool, error) {
	var ok bool
	const query = `
		SELECT csrf_token_hash = $2
		FROM auth_sessions
		WHERE refresh_token_hash = $1
			AND revoked_at IS NULL
			AND refresh_expires_at > now()`
	err := s.pool.QueryRow(ctx, query, tokenHash(refreshToken), tokenHash(csrfToken)).Scan(&ok)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("validate CSRF token: %w", err)
	}
	return ok, nil
}
