# API Authentication and Authorization

Kafkaesque uses opaque access and refresh tokens backed by PostgreSQL. Tokens
are generated with cryptographically secure randomness; only SHA-256 token
hashes are stored. Passwords are stored as Argon2id hashes.

## Authentication endpoints

| Method | Path | Query Params | Description |
| --- | --- | --- | --- |
| `POST` | `/api/v1/auth/login` | — | Validate credentials and create a session. |
| `POST` | `/api/v1/auth/refresh` | — | Rotate a valid refresh token and complete token pair. |
| `POST` | `/api/v1/auth/logout` | — | Revoke the access or refresh session. |
| `GET` | `/api/v1/auth/me` | — | Return the current identity, role, and permissions. |

Login and refresh are public. Logout accepts access or refresh authentication.
`/auth/me` requires a valid access token.

## Login

```http
POST /api/v1/auth/login
Content-Type: application/json

{"username":"account-name","password":"<password>"}
```

The response data contains:

- `access_token` and `access_expires_at`.
- `refresh_token` and `refresh_expires_at`.
- `csrf_token`.
- Current account identity, role, and sorted permission keys.

Invalid username and password cases share one generic `401` response. Five
failed attempts for the same normalized username and direct client address
within five minutes return `429`. Throttling is per Kafkaesque replica.

## Bearer clients

Send the access token on protected requests:

```http
Authorization: Bearer <access-token>
```

When it expires, explicitly call refresh:

```http
POST /api/v1/auth/refresh
Content-Type: application/json

{"refresh_token":"<refresh-token>"}
```

Refresh tokens rotate atomically and are single-use. Concurrent or repeated use
of an old token returns `401`.

## Browser sessions

Login also sets host-only `HttpOnly`, `SameSite=Strict` access and refresh
cookies. `Secure` follows `authentication.cookie_secure` and should remain true
behind HTTPS.

Browser state-changing requests require the session-bound CSRF value in the
form or `X-CSRF-Token` header. When an access cookie expires, middleware rotates
a valid refresh cookie automatically. Failed HTMX authentication redirects to
the login page rather than replacing valid content with an error fragment.

Tokens are never stored in local storage or session storage.

## Logout

Bearer clients send an access token. Browser clients send their cookie and CSRF
value. Logout revokes the session and clears browser cookies:

```http
POST /api/v1/auth/logout
Authorization: Bearer <access-token>
```

Password or bootstrap identity reconciliation also revokes affected sessions.

## Account permissions

Kafka observability is available to every authenticated account. Permissions
currently control account management only.

| Role | List users | Create users | Edit users | Delete users |
| --- | --- | --- | --- | --- |
| Super Admin | Yes | Yes | Yes | Yes |
| Admin | Yes | Yes | Yes | No |
| User | Yes | No | No | No |

Canonical permission keys:

- `user:account:view`
- `user:account:create`
- `user:account:edit`
- `user:account:delete`

Permissions are resolved from PostgreSQL on every authenticated request, so
role changes apply immediately.

## Role rules

- `super_admin`, `admin`, and `user` are immutable system roles.
- Every account has exactly one role.
- Only Super Admin can administer roles or assign Admin/custom roles.
- Custom roles may use View, Create, and Edit; mutation permissions require View.
- Delete is reserved for Super Admin.
- Assigned custom roles cannot be deleted.
- The environment-managed Super Admin cannot be edited, reassigned, or deleted.

Account deletion is permanent and cascades to sessions and the role assignment.
The custom role itself remains available after its last user is deleted.

## Public routes

Only these routes are public:

- `/`
- `/api/v1/health`
- Login and refresh endpoints.
- `/ui/login`
- `/ui/assets/*`

All Kafka API and UI routes require authentication.
