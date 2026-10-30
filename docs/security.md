# Runtime Security

This guide covers secure operation. To report a vulnerability privately, use
the repository [security policy](../SECURITY.md).

## Security boundary

Kafkaesque does not produce records or mutate Kafka topics, partitions, groups,
or broker configuration. It reads metadata, group descriptions, committed
offsets, and log-end offsets.

Server mode does write application data to PostgreSQL:

- Users and Argon2id password hashes.
- Roles and account-management permissions.
- SHA-256 hashes of access, refresh, and CSRF tokens.
- Session expiry, revocation, and activity timestamps.
- Migration history and checksums.
- Alert subscription state and AES-256-GCM-encrypted Slack webhook URLs.

Use a PostgreSQL database dedicated to Kafkaesque because destructive bootstrap
recovery can rebuild the complete `public` schema.

## Authentication

`serve` requires one environment/YAML-managed Super Admin at startup. Other
accounts can be created through the UI or API. Active Super Admin, Admin, User,
and custom-role accounts can sign in.

Default token lifetimes are 15 minutes for access and seven days for refresh.
Refresh tokens rotate atomically and cannot be reused. Logout and bootstrap
credential changes revoke sessions.

Login failures use generic messages. In-process throttling limits repeated
attempts by normalized username and direct client address. It is per replica,
so public deployments should rate-limit login at the ingress as well.

## Browser security

Browser tokens are stored only in host-only `HttpOnly`, `SameSite=Strict`
cookies. State-changing cookie requests require a session-bound CSRF token.
Tokens are not written to local storage or session storage.

Keep `authentication.cookie_secure: true` behind HTTPS. Setting it false is
appropriate only for trusted local HTTP development.

## Authorization

Every authenticated account can inspect Kafka. RBAC protects account and alert management:

| Role | List users | Create users | Edit users | Delete users |
| --- | --- | --- | --- | --- |
| Super Admin | Yes | Yes | Yes | Yes |
| Admin | Yes | Yes | Yes | No |
| User | Yes | No | No | No |

| Role | View alerts | Create alerts | Edit alerts | Delete alerts |
| --- | --- | --- | --- | --- |
| Super Admin | Yes | Yes | Yes | Yes |
| Admin | Yes | Yes | Yes | No |
| User | Yes | No | No | No |

System roles and grants are protected by PostgreSQL guards and verified at
startup. Custom roles may receive alert delete permission, while deleting users
remains reserved for Super Admin. Only Super Admin can manage roles and assign
Admin/custom roles. See [API authentication](api/authentication.md#account-permissions).

## Secrets

- Inject database, bootstrap, and SASL credentials from a secret manager or
  Kubernetes Secret.
- Mount CA certificates, client certificates, and private keys read-only.
- Do not place secrets in container images, Git, command examples, or build arguments.
- Use absolute certificate paths in container environment variables.
- Review captured logs before sharing internal infrastructure names.
- Keep `alerts.encryption_key` stable and separate from the database. Slack
  webhook URLs are never rendered or logged after submission.

Kafkaesque structured logs exclude request bodies, authorization headers,
cookies, tokens, password hashes, database URLs, and configured credentials.

## Kafka transport

Prefer TLS for broker traffic and always combine SASL PLAIN with TLS outside a
trusted network. Kafkaesque verifies certificates and intentionally has no
`insecure_skip_verify` setting. Use mTLS where required.

Kafka authorization should grant only the metadata, group, and offset access
needed for inspection. Kafkaesque does not need produce or administration
permissions.

## PostgreSQL

- Use PostgreSQL 18 or newer.
- Enforce TLS for production database connections.
- Restrict the database role and network path to Kafkaesque deployments.
- Back up the database before upgrades or recovery changes.
- Keep `super_admin_recovery: verify` during normal operation.

`reinitialize` drops and recreates the complete `public` schema when a mismatch
or integrity problem exists. Use it only for explicit recovery on a dedicated,
backed-up database.

## Network exposure

The server defaults to `0.0.0.0:8085`. Keep it private or expose it through an
HTTPS ingress. `/api/v1/health`, login, refresh, the login page, UI assets, and
the root discovery document are public. Other API/UI routes require a session.

Set ingress limits for request body size, login rate, connection count, and
timeouts appropriate to the deployment.

## Known limitations

- No MFA, OAuth/OIDC, SSO, or password-reset workflow.
- No distributed login limiter; limits are per replica.
- No observability-specific permissions; all authenticated accounts can inspect Kafka.
- No audit-history database beyond structured runtime logs.
- No Kafka OAuth/OIDC, Kerberos/GSSAPI, or provider IAM mechanisms.
- No generic webhooks, Slack OAuth installation, alert history, or distributed
  monitor coordination. Multiple replicas independently evaluate subscriptions.
