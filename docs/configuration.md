# Configuration

Kafkaesque reads five top-level YAML sections:

```yaml
kafka:
lag:
authentication:
database:
api:
```

The complete commented reference is
[`kafkaesque.example.yaml`](../kafkaesque.example.yaml).

## Precedence

Values are applied in this order, with later sources winning:

1. Built-in defaults.
2. YAML configuration.
3. Environment variables.
4. Explicit `serve --host` and `serve --port` flags.

Environment variables always override their YAML equivalent.

## Default YAML paths

| Platform | Path |
| --- | --- |
| Linux | `$XDG_CONFIG_HOME/kafkaesque/config.yaml` or `~/.config/kafkaesque/config.yaml` |
| macOS | `~/Library/Application Support/kafkaesque/config.yaml` |
| Windows | `%AppData%\kafkaesque\config.yaml` |

Use `--config <path>` to bypass the default location. Kafkaesque does not
create a configuration file automatically.

## Kafka

| YAML | Environment | Default and rules |
| --- | --- | --- |
| `kafka.bootstrap_server` | `KAFKAESQUE_KAFKA_BOOTSTRAP_SERVER` | One `host:port`; default `localhost:9092`. |
| `kafka.bootstrap_servers` | `KAFKAESQUE_KAFKA_BOOTSTRAP_SERVERS` | YAML list or comma-separated environment value. Replaces the configured list. |
| `kafka.connection_timeout` | `KAFKAESQUE_KAFKA_CONNECTION_TIMEOUT` | Positive Go duration; default `10s`. |
| `kafka.broker_address_overrides` | `KAFKAESQUE_KAFKA_BROKER_ADDRESS_OVERRIDES` | YAML map or environment JSON object. |

Do not configure both singular and plural bootstrap forms. Addresses must be
valid `host:port` values and duplicates are rejected.

Broker overrides map the exact address advertised by Kafka to the address
Kafkaesque can reach:

```yaml
kafka:
  bootstrap_server: 10.100.0.72:9094
  broker_address_overrides:
    "broker-0.kafka.svc.cluster.local:9092": "10.100.0.72:9094"
    "broker-1.kafka.svc.cluster.local:9092": "10.100.0.74:9094"
```

Each destination must route to the corresponding broker. Overrides also apply
to consumer coordinators and partition leaders.

## Kafka security

| YAML | Environment | Rules |
| --- | --- | --- |
| `kafka.security.tls.enabled` | `KAFKAESQUE_KAFKA_SECURITY_TLS_ENABLED` | Boolean; enables TLS. |
| `kafka.security.tls.ca_file` | `KAFKAESQUE_KAFKA_SECURITY_TLS_CA_FILE` | Optional custom CA; system roots are used when omitted. |
| `kafka.security.tls.client_cert_file` | `KAFKAESQUE_KAFKA_SECURITY_TLS_CLIENT_CERT_FILE` | Must be paired with the client key. |
| `kafka.security.tls.client_key_file` | `KAFKAESQUE_KAFKA_SECURITY_TLS_CLIENT_KEY_FILE` | Must be paired with the client certificate. |
| `kafka.security.tls.server_name` | `KAFKAESQUE_KAFKA_SECURITY_TLS_SERVER_NAME` | Optional certificate hostname override. |
| `kafka.security.sasl.mechanism` | `KAFKAESQUE_KAFKA_SECURITY_SASL_MECHANISM` | `PLAIN`, `SCRAM-SHA-256`, or `SCRAM-SHA-512`. |
| `kafka.security.sasl.username` | `KAFKAESQUE_KAFKA_SECURITY_SASL_USERNAME` | Required with a mechanism. |
| `kafka.security.sasl.password` | `KAFKAESQUE_KAFKA_SECURITY_SASL_PASSWORD` | Required with a mechanism. |

Relative certificate paths in YAML are resolved from the YAML file directory.
Environment certificate paths are used as supplied; containers should use
absolute mounted paths. See [TLS and SASL](guides/tls-sasl.md).

## Lag thresholds

| YAML | Environment | Default and rules |
| --- | --- | --- |
| `lag.warning_threshold` | `KAFKAESQUE_LAG_WARNING_THRESHOLD` | Non-negative integer; default `100`. |
| `lag.unhealthy_threshold` | `KAFKAESQUE_LAG_UNHEALTHY_THRESHOLD` | Greater than warning; default `500`. |

- Lag at or below warning is `HEALTHY`.
- Lag above warning and at or below unhealthy is `WARNING`.
- Lag above unhealthy is `UNHEALTHY`.

## Authentication

These settings are required only by `serve` unless a default is stated:

| YAML | Environment | Default and rules |
| --- | --- | --- |
| `authentication.super_admin_username` | `KAFKAESQUE_AUTHENTICATION_SUPER_ADMIN_USERNAME` | Required 3–64 character handle or the configured email. |
| `authentication.super_admin_email` | `KAFKAESQUE_AUTHENTICATION_SUPER_ADMIN_EMAIL` | Required normalized email. |
| `authentication.super_admin_password` | `KAFKAESQUE_AUTHENTICATION_SUPER_ADMIN_PASSWORD` | Required 8–99 character policy-compliant password. |
| `authentication.super_admin_recovery` | `KAFKAESQUE_AUTHENTICATION_SUPER_ADMIN_RECOVERY` | `verify` by default; also `update` or `reinitialize`. |
| `authentication.access_ttl` | `KAFKAESQUE_AUTHENTICATION_ACCESS_TTL` | Positive duration; default `15m`. |
| `authentication.refresh_ttl` | `KAFKAESQUE_AUTHENTICATION_REFRESH_TTL` | Must exceed access TTL; default `168h`. |
| `authentication.cookie_secure` | `KAFKAESQUE_AUTHENTICATION_COOKIE_SECURE` | Default `true`; set false only for trusted local HTTP. |

Passwords require ASCII uppercase, lowercase, a number, and one of
`!@#$%^&*()_`. They are stored as Argon2id hashes.

Recovery behavior:

- `verify` stops when the configured identity or password differs.
- `update` updates one structurally valid Super Admin and revokes its sessions.
- `reinitialize` atomically rebuilds the complete PostgreSQL `public` schema
  when a mismatch or integrity problem exists.

`reinitialize` permanently removes all objects and data in `public`. Use it
only with a PostgreSQL database dedicated to Kafkaesque, then return the value
to `verify`.

## Database and API

| YAML | Environment | Default and rules |
| --- | --- | --- |
| `database.url` | `KAFKAESQUE_DATABASE_URL` | Complete PostgreSQL 18+ URL; required by `serve`. |
| `api.host` | `KAFKAESQUE_API_HOST` | Non-empty listener host; default `0.0.0.0`. |
| `api.port` | `KAFKAESQUE_API_PORT` | `1`–`65535`; default `8085`. |

Production PostgreSQL URLs should enforce TLS. `--host` and `--port` override
only the API listener values.

## Environment-only example

```sh
export KAFKAESQUE_KAFKA_BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092
export KAFKAESQUE_DATABASE_URL='postgres://kafkaesque:<password>@postgres:5432/kafkaesque?sslmode=require'
export KAFKAESQUE_AUTHENTICATION_SUPER_ADMIN_USERNAME=super_admin
export KAFKAESQUE_AUTHENTICATION_SUPER_ADMIN_EMAIL=super-admin@example.com
export KAFKAESQUE_AUTHENTICATION_SUPER_ADMIN_PASSWORD='<policy-compliant-password>'
kafkaesque serve
```

Do not commit populated credential values. Use environment-backed secrets in
production.

## Removed environment names

Legacy flat Kafka and Admin environment names are rejected with migration
guidance instead of being silently ignored. Use only the hierarchical names in
the tables above.
