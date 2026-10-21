<h1 align="center">Kafkaesque</h1>

<p align="center">
  A lightweight, read-only Kafka cluster inspector.<br>
  Understand your cluster without changing it.
</p>

<p align="center">
  <a href="https://github.com/nikhil25803/kafkaesque/releases/latest"><img src="https://img.shields.io/github/v/release/nikhil25803/kafkaesque.svg" alt="Latest release"></a>
  <a href="https://github.com/nikhil25803/kafkaesque/releases"><img src="https://img.shields.io/github/downloads/nikhil25803/kafkaesque/total.svg" alt="Release downloads"></a>
  <a href="go.mod"><img src="https://img.shields.io/github/go-mod/go-version/nikhil25803/kafkaesque.svg" alt="Go version"></a>
  <a href="https://github.com/nikhil25803/kafkaesque/actions/workflows/ci.yml"><img src="https://github.com/nikhil25803/kafkaesque/actions/workflows/ci.yml/badge.svg" alt="Build status"></a>
  <a href="https://github.com/nikhil25803/kafkaesque/actions/workflows/tests.yml"><img src="https://github.com/nikhil25803/kafkaesque/actions/workflows/tests.yml/badge.svg" alt="Test status"></a>
  <a href="https://github.com/nikhil25803/kafkaesque/actions/workflows/kafka-integration.yml"><img src="https://github.com/nikhil25803/kafkaesque/actions/workflows/kafka-integration.yml/badge.svg" alt="Kafka integration status"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-white.svg" alt="MIT license"></a>
</p>

![Kafkaesque product vision](asset/image/KafkaesqueBanner.png)

## Table of contents

- [Install](#install)
- [Container image](#container-image)
- [Flags](#flags)
- [UI sneak peek](#ui-sneak-peek)
- [Combining flags](#combining-flags)
- [Build from source](#build-from-source)
- [Configuration](#configuration)
- [Read-only HTTP API](#read-only-http-api)
- [Local Kafka environment](#local-kafka-environment)
- [Command documentation](DOCS.md)
- [API documentation](API.md)
- [Contributing](CONTRIBUTING.md)
- [License](#license)

## Install

### Linux and macOS

The installer detects your platform, verifies the release checksum, and installs
Kafkaesque into `~/.local/bin` without `sudo`.

```sh
curl -fsSL https://raw.githubusercontent.com/nikhil25803/kafkaesque/main/install.sh | sh
```

```sh
export PATH="$HOME/.local/bin:$PATH"
```

```sh
kafkaesque --help
```

<details>
<summary><code>kafkaesque: command not found</code></summary>

The installer places Kafkaesque in `~/.local/bin`. Add it to the current shell:

```sh
export PATH="$HOME/.local/bin:$PATH"
```

Persist the change for Bash:

```sh
echo 'export PATH="$HOME/.local/bin:$PATH"' >> "$HOME/.bashrc"
source "$HOME/.bashrc"
```

Or for Zsh:

```sh
echo 'export PATH="$HOME/.local/bin:$PATH"' >> "$HOME/.zshrc"
source "$HOME/.zshrc"
```

</details>

<details>
<summary>Windows PowerShell installation</summary>

Download and run the checksum-verifying installer:

```powershell
curl.exe -fsSLo install.ps1 https://raw.githubusercontent.com/nikhil25803/kafkaesque/main/install.ps1
```

```powershell
powershell -ExecutionPolicy Bypass -File .\install.ps1
```

Open a new terminal, then verify the installation:

```powershell
kafkaesque --help
```

</details>

<details>
<summary>Install a specific version</summary>

Both installers use the latest release by default. Pin a release when needed:

```sh
curl -fsSL https://raw.githubusercontent.com/nikhil25803/kafkaesque/main/install.sh | VERSION=v0.1.1 sh
```

```powershell
powershell -ExecutionPolicy Bypass -File .\install.ps1 -Version v0.1.1
```

</details>

## Container image

Each version tag publishes a public multi-platform image for Linux on AMD64 and
ARM64:

```sh
docker pull ghcr.io/nikhil25803/kafkaesque:v0.6.0
```

```sh
docker run --rm -p 8085:8085 \
  -e KAFKAESQUE_KAFKA_BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092 \
  -e KAFKAESQUE_DATABASE_URL='postgres://kafkaesque:password@postgres:5432/kafkaesque?sslmode=require' \
  -e KAFKAESQUE_AUTHENTICATION_SUPER_ADMIN_USERNAME=super_admin \
  -e KAFKAESQUE_AUTHENTICATION_SUPER_ADMIN_EMAIL=super-admin@example.com \
  -e KAFKAESQUE_AUTHENTICATION_SUPER_ADMIN_PASSWORD='Replace1_with_a_long_secret' \
  -e KAFKAESQUE_AUTHENTICATION_COOKIE_SECURE=false \
  ghcr.io/nikhil25803/kafkaesque:v0.6.0
```

The container runs `kafkaesque serve` as a non-root user and includes the API
and embedded web UI. Use an exact release tag or image digest in deployments;
`latest` follows the newest successful stable release.

The example disables secure cookies only for local HTTP. Keep the default
`KAFKAESQUE_AUTHENTICATION_COOKIE_SECURE=true` when Kafkaesque is served
through HTTPS.

<details>
<summary>Build, publish, and deploy the container</summary>

Build and publish the same image to another registry when required:

```sh
docker build --build-arg VERSION=dev -t kafkaesque:dev .
docker tag kafkaesque:dev registry.example.com/kafkaesque:v0.6.0
docker push registry.example.com/kafkaesque:v0.6.0
```

Pushing a `v*.*.*` Git tag runs the existing release checks, creates the GitHub
Release, and publishes both the versioned image and `latest` to GHCR.
After the first publication, set the package visibility to **Public** in its
GitHub package settings if GHCR initially creates it as private.

For TLS or mutual TLS, mount certificates read-only and configure their absolute
container paths:

```sh
docker run --rm -p 8085:8085 \
  --mount type=bind,src=/path/to/certificates,dst=/etc/kafka/certificates,readonly \
  -e KAFKAESQUE_KAFKA_BOOTSTRAP_SERVER=kafka.example.com:9093 \
  -e KAFKAESQUE_DATABASE_URL='postgres://kafkaesque:password@postgres:5432/kafkaesque?sslmode=require' \
  -e KAFKAESQUE_AUTHENTICATION_SUPER_ADMIN_USERNAME=super_admin \
  -e KAFKAESQUE_AUTHENTICATION_SUPER_ADMIN_EMAIL=super-admin@example.com \
  -e KAFKAESQUE_AUTHENTICATION_SUPER_ADMIN_PASSWORD='Replace1_with_a_long_secret' \
  -e KAFKAESQUE_KAFKA_SECURITY_TLS_ENABLED=true \
  -e KAFKAESQUE_KAFKA_SECURITY_TLS_CA_FILE=/etc/kafka/certificates/ca.pem \
  ghcr.io/nikhil25803/kafkaesque:v0.6.0
```

In Kubernetes, provide the database URL, Super Admin password, and SASL
credentials through Secrets or an external secret provider and mount TLS
material as files. Production PostgreSQL connections should enforce TLS. Use
`GET /api/v1/health` for process uptime probes and the authenticated
`GET /api/v1/metadata/connection` when Kafka connectivity must also be checked.

</details>

## Flags

### General

| Short | Long        | Value            | Description                            | Example                              |
| ----- | ----------- | ---------------- | -------------------------------------- | ------------------------------------ |
| `-v`  | `--version` | —                | Show the installed Kafkaesque version. | `kafkaesque --version`               |
| `-h`  | `--help`    | —                | Show command help.                     | `kafkaesque --help`                  |
| —     | `--output`  | `text` or `json` | Select information output format.      | `kafkaesque --brokers --output json` |
| —     | `--watch`   | seconds          | Refresh information in place.          | `kafkaesque --health --watch 5`      |

### Checks and configuration

| Short | Long       | Value                       | Description                                          | Example                                               |
| ----- | ---------- | --------------------------- | ---------------------------------------------------- | ----------------------------------------------------- |
| —     | `--check`  | optional `config` or `conn` | Validate configuration or test the Kafka connection. | `kafkaesque --check`                                  |
| —     | `--config` | path                        | Load an explicit YAML configuration file.            | `kafkaesque --config /path/to/config.yaml --metadata` |

### Information

| Short | Long           | Value | Description                                               | Example                                                        |
| ----- | -------------- | ----- | --------------------------------------------------------- | -------------------------------------------------------------- |
| `-m`  | `--metadata`   | —     | Show cluster metadata and connection status.              | `kafkaesque --metadata`                                        |
| —     | `--health`     | —     | Show cluster, partition, and consumer health.             | `kafkaesque --health`                                          |
| `-b`  | `--brokers`    | —     | List Kafka brokers.                                       | `kafkaesque --brokers`                                         |
| `-t`  | `--topics`     | —     | List Kafka topics.                                        | `kafkaesque --topics`                                          |
| `-p`  | `--partitions` | —     | List partitions for the topic selected by `--topic`.      | `kafkaesque --partitions --topic orders`                       |
| `-c`  | `--consumers`  | —     | List consumer groups.                                     | `kafkaesque --consumers`                                       |
| —     | `--consumer`   | —     | Inspect the consumer group selected by `--group`.         | `kafkaesque --consumer --group order-processor`                |
| —     | `--member`     | —     | Include active member assignments in consumer inspection. | `kafkaesque --consumer --member --group order-processor`       |
| —     | `--group`      | name  | Select a consumer group.                                  | `kafkaesque --consumer --group order-processor`                |
| —     | `--topic`      | name  | Select a topic for partition or consumer lag inspection.  | `kafkaesque --consumer --group order-processor --topic orders` |

See [Command documentation](DOCS.md) for complete examples and representative
output.

## UI sneak peek

<table>
  <tr>
    <td colspan="2" align="center">
      <strong>Dashboard</strong><br>
      <img src="asset/ui/dashboard.png" alt="Kafkaesque dashboard showing connection status, cluster totals, consumer groups, and cluster health" width="100%">
    </td>
  </tr>
  <tr>
    <td align="center" width="50%">
      <strong>Consumer Groups</strong><br>
      <img src="asset/ui/consumer_groups.png" alt="Kafkaesque consumer groups list with search, state filtering, and lag status" width="100%">
    </td>
    <td align="center" width="50%">
      <strong>Consumer Group Summary</strong><br>
      <img src="asset/ui/consumer_group_summary.png" alt="Kafkaesque consumer group summary showing lag totals and member information" width="100%">
    </td>
  </tr>
  <tr>
    <td align="center" width="50%">
      <strong>Partition Lag and Topic Summary</strong><br>
      <img src="asset/ui/partition_lag_and_topic_summary.png" alt="Kafkaesque partition lag and topic summary with offsets and assignments" width="100%">
    </td>
    <td align="center" width="50%">
      <strong>Topics</strong><br>
      <img src="asset/ui/topics.png" alt="Kafkaesque topics list with search and level filtering" width="100%">
    </td>
  </tr>
  <tr>
    <td align="center" width="50%">
      <strong>Brokers</strong><br>
      <img src="asset/ui/brokers.png" alt="Kafkaesque brokers list showing advertised addresses, connect addresses, and racks" width="100%">
    </td>
    <td align="center" width="50%">
      <strong>Cluster Health</strong><br>
      <img src="asset/ui/cluster_health.png" alt="Kafkaesque cluster health page showing cluster, partition, and consumer diagnostics" width="100%">
    </td>
  </tr>
</table>

## Combining flags

<details>
<summary>Inspect several parts of a cluster in one command</summary>

Information flags can be combined. Kafkaesque shares the Kafka metadata request
between them and prints each requested section in a stable order.

```sh
kafkaesque --metadata --brokers --topics
```

```sh
kafkaesque --metadata --partitions --topic orders
```

Consumer list and consumer detail modes are intentionally separate, so
`--consumers` cannot be combined with `--consumer`.

Cluster health is a standalone inspection. `--health` can be combined with
`--config`, but not with other information flags or selectors.

Information commands default to text output. Use `--output json` for a stable
snake_case JSON document containing only the requested sections. Use
`--watch N` to refresh either output format in place every `N` seconds.

</details>

## Build from source

Kafkaesque requires the Go version declared in `go.mod`.

```sh
git clone https://github.com/nikhil25803/kafkaesque.git
cd kafkaesque
make build
./bin/kafkaesque --help
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for tests, architecture, and the complete
development workflow.

## Configuration

<details>
<summary>Bootstrap server, YAML paths, environment override, and lag thresholds</summary>

Configuration precedence is:

1. Built-in defaults.
2. YAML from the platform default path or the file selected by `--config`.
3. Environment variables.
4. Explicit `serve --host` and `serve --port` flags.

```yaml
kafka:
  bootstrap_servers:
    - localhost:9092
  connection_timeout: 10s

lag:
  warning_threshold: 100
  unhealthy_threshold: 500

api:
  host: 0.0.0.0
  port: 8085
```

| Platform | Default path                                                                    |
| -------- | ------------------------------------------------------------------------------- |
| Linux    | `$XDG_CONFIG_HOME/kafkaesque/config.yaml` or `~/.config/kafkaesque/config.yaml` |
| macOS    | `~/Library/Application Support/kafkaesque/config.yaml`                          |
| Windows  | `%AppData%\kafkaesque\config.yaml`                                              |

Lag up to the warning threshold is `HEALTHY`, lag above it is `WARNING`, and
lag above the unhealthy threshold is `UNHEALTHY`. The warning threshold must
be lower than the unhealthy threshold.

Configuration follows five top-level sections:

```yaml
kafka:
lag:
authentication:
database:
api:
```

See [`kafkaesque.example.yaml`](kafkaesque.example.yaml) for a ready-to-copy
configuration. Kafkaesque never creates configuration files automatically.

For local fixture testing, copy `test-config.example.yaml` to
`test-config.yaml`, then add the local Super Admin credentials and PostgreSQL
URL. The populated file is ignored by Git and GitGuardian.

All runtime configuration can be supplied through YAML or its equivalent
environment variable. Environment variables always override YAML values.

| YAML path | Environment variable | Value |
| --- | --- | --- |
| `kafka.bootstrap_server` | `KAFKAESQUE_KAFKA_BOOTSTRAP_SERVER` | One `host:port` bootstrap server. |
| `kafka.bootstrap_servers` | `KAFKAESQUE_KAFKA_BOOTSTRAP_SERVERS` | Comma-separated `host:port` bootstrap servers. |
| `kafka.connection_timeout` | `KAFKAESQUE_KAFKA_CONNECTION_TIMEOUT` | Go duration such as `10s`. |
| `kafka.broker_address_overrides` | `KAFKAESQUE_KAFKA_BROKER_ADDRESS_OVERRIDES` | JSON object mapping advertised addresses to connect addresses. |
| `kafka.security.tls.enabled` | `KAFKAESQUE_KAFKA_SECURITY_TLS_ENABLED` | `true` or `false`. |
| `kafka.security.tls.ca_file` | `KAFKAESQUE_KAFKA_SECURITY_TLS_CA_FILE` | CA certificate path. |
| `kafka.security.tls.client_cert_file` | `KAFKAESQUE_KAFKA_SECURITY_TLS_CLIENT_CERT_FILE` | Client certificate path. |
| `kafka.security.tls.client_key_file` | `KAFKAESQUE_KAFKA_SECURITY_TLS_CLIENT_KEY_FILE` | Client private-key path. |
| `kafka.security.tls.server_name` | `KAFKAESQUE_KAFKA_SECURITY_TLS_SERVER_NAME` | TLS server name. |
| `kafka.security.sasl.mechanism` | `KAFKAESQUE_KAFKA_SECURITY_SASL_MECHANISM` | `PLAIN`, `SCRAM-SHA-256`, or `SCRAM-SHA-512`. |
| `kafka.security.sasl.username` | `KAFKAESQUE_KAFKA_SECURITY_SASL_USERNAME` | SASL username. |
| `kafka.security.sasl.password` | `KAFKAESQUE_KAFKA_SECURITY_SASL_PASSWORD` | SASL password. |
| `lag.warning_threshold` | `KAFKAESQUE_LAG_WARNING_THRESHOLD` | Non-negative integer. |
| `lag.unhealthy_threshold` | `KAFKAESQUE_LAG_UNHEALTHY_THRESHOLD` | Non-negative integer greater than the warning threshold. |
| `authentication.super_admin_username` | `KAFKAESQUE_AUTHENTICATION_SUPER_ADMIN_USERNAME` | Super Admin name required by `serve`. |
| `authentication.super_admin_email` | `KAFKAESQUE_AUTHENTICATION_SUPER_ADMIN_EMAIL` | Normalized Super Admin email required by `serve`. |
| `authentication.super_admin_password` | `KAFKAESQUE_AUTHENTICATION_SUPER_ADMIN_PASSWORD` | Password containing 8–99 characters, uppercase, lowercase, a number, and an allowed special character. |
| `authentication.super_admin_recovery` | `KAFKAESQUE_AUTHENTICATION_SUPER_ADMIN_RECOVERY` | Recovery policy: `verify` (default), `update`, or destructive `reinitialize`. |
| `authentication.access_ttl` | `KAFKAESQUE_AUTHENTICATION_ACCESS_TTL` | Access-token lifetime; defaults to `15m`. |
| `authentication.refresh_ttl` | `KAFKAESQUE_AUTHENTICATION_REFRESH_TTL` | Refresh-token lifetime; defaults to `168h`. |
| `authentication.cookie_secure` | `KAFKAESQUE_AUTHENTICATION_COOKIE_SECURE` | Require HTTPS for browser cookies; defaults to `true`. |
| `database.url` | `KAFKAESQUE_DATABASE_URL` | Complete PostgreSQL 18+ connection URL required by `serve`. |
| `api.host` | `KAFKAESQUE_API_HOST` | API listener host. |
| `api.port` | `KAFKAESQUE_API_PORT` | Port from `1` to `65535`. |

The hierarchical Kafka and authentication environment names are a breaking
change. Removed names such as `KAFKAESQUE_BOOTSTRAP_SERVERS`,
`KAFKAESQUE_ADMIN_USERNAME`, and
`KAFKAESQUE_AUTHENTICATION_ADMIN_USERNAME` now produce an error that names
their replacement.

The singular and plural bootstrap environment variables cannot be set together.
Environment certificate paths are used as supplied; use absolute paths inside
containers. Environment values pass through the same validation as YAML and
always take precedence.

On startup, the default `verify` recovery policy compares the configured Super
Admin username, email, and password with PostgreSQL and stops on a mismatch.
Use `update` to reconcile that account and revoke its sessions while preserving
other data, but only when one valid Super Admin assignment exists. Use
`reinitialize` only for deliberate recovery: when credentials, migration
checksums, schema integrity, or the Super Admin assignment differ, it atomically
drops the complete `public` schema, reapplies embedded migrations, and creates
one fresh Super Admin. Every table and record in that schema is permanently
deleted, so the PostgreSQL database must be dedicated to Kafkaesque. After a
successful recovery, change the policy back to `verify`.

`bootstrap_server` remains supported for existing configurations. New
configurations can use `bootstrap_servers` for multiple seed brokers; setting
both forms is invalid. `connection_timeout` controls TCP, TLS, and SASL setup
and defaults to `10s`.

### TLS and SASL authentication

Kafkaesque supports TLS server verification, mutual TLS, SASL PLAIN,
SCRAM-SHA-256, and SCRAM-SHA-512. TLS uses the system certificate pool when
`ca_file` is omitted. Relative certificate paths are resolved from the
configuration file directory.

```yaml
kafka:
  bootstrap_servers:
    - kafka-1.example.com:9093
    - kafka-2.example.com:9093
  security:
    tls:
      enabled: true
      ca_file: certificates/ca.pem
      # Include both fields when the broker requires mutual TLS.
      client_cert_file: certificates/client.pem
      client_key_file: certificates/client-key.pem
      server_name: kafka.example.com
    sasl:
      mechanism: SCRAM-SHA-256
      username: kafkaesque
      password: secret
```

A SASL mechanism enables authentication automatically. Combining it with
`tls.enabled: true` selects `SASL_SSL`; without TLS it selects
`SASL_PLAINTEXT`. Do not use PLAIN without TLS on an untrusted network because
PLAIN credentials are not encrypted.

OAuth/OIDC, Kerberos/GSSAPI, AWS MSK IAM, and other provider-specific
mechanisms are not currently supported.

### Broker address overrides

Some Kafka clusters advertise internal broker addresses even though clients
connect through one external load balancer per broker. Add exact address
overrides when Kafkaesque can reach the external listeners but cannot resolve
or connect to the advertised addresses:

```yaml
kafka:
  bootstrap_server: 10.100.0.72:9094
  broker_address_overrides:
    "broker-0.broker-headless.kafka-connect.svc.cluster.local:9092": "10.100.0.72:9094"
    "broker-1.broker-headless.kafka-connect.svc.cluster.local:9092": "10.100.0.74:9094"
    "broker-2.broker-headless.kafka-connect.svc.cluster.local:9092": "10.100.0.73:9094"
```

Overrides apply to consumer coordinators and partition leaders as well as
general broker requests. Each destination must route to the corresponding
broker; do not map multiple brokers to a non-sticky shared load balancer.
Kafkaesque shows both advertised and connect addresses in `--brokers` output
when an override is active.

Correct Kafka `advertised.listeners` configuration is preferred when you
control the cluster. Overrides are intended for environments where changing
the broker configuration is not practical.

</details>

## Read-only HTTP API

Start the API with the same Kafka configuration used by the CLI. PostgreSQL and
Super Admin values can come from YAML or environment variables:

```sh
KAFKAESQUE_DATABASE_URL='postgres://kafkaesque:password@db:5432/kafkaesque?sslmode=require' \
KAFKAESQUE_AUTHENTICATION_SUPER_ADMIN_USERNAME=super_admin \
KAFKAESQUE_AUTHENTICATION_SUPER_ADMIN_EMAIL=super-admin@example.com \
KAFKAESQUE_AUTHENTICATION_SUPER_ADMIN_PASSWORD='Replace1_with_a_long_secret' \
kafkaesque serve --config kafkaesque.yaml --port 8085
```

The API defaults to `0.0.0.0:8085`. Explicit `--host` and `--port` values
override the YAML `api` section. It serves the read-only API and web UI, and
keeps one authenticated Kafka connection for the server lifetime. PostgreSQL
18 or newer and the required effective database/Super Admin values must be
available at startup. Browser users sign in at `/ui/login`; API clients obtain
opaque access and refresh tokens from `/api/v1/auth/login`.

```http
GET /api/v1/health
```

See [API documentation](API.md) for all endpoints, filters, envelopes, and
status codes.

The web UI is available from the same process:

```http
GET /ui
```

It contains responsive Dashboard, Consumer Groups, Consumer Lag, Topics,
Brokers, Cluster Health, and User Management pages. Kafka
pages use live request-time data; User Management uses PostgreSQL directly.
Filtering and pagination use a locally vendored HTMX runtime and continue to
work as ordinary links and forms without JavaScript. Consumer Lag includes
topic and partition offsets, lag sorting, member grouping, and assignments.
New accounts use the shared password policy and can sign in immediately. Every
authenticated role can inspect Kafka. Account management uses the built-in
Super Admin, Admin, and User permission matrix plus optional custom roles; see
[Account RBAC](RBAC.md).

For local authentication development, start the PostgreSQL 18 fixture with
`make postgres-up`. `make postgres-down` preserves its data;
`make postgres-reset` removes and recreates the development volume.
Run destructive authentication integration tests with `make postgres-test`.
That target creates and uses the separate `kafkaesque_auth_test` database; the
test suite rejects the normal development database.

## Local Kafka environment

<details>
<summary>Run the disposable five-broker development cluster</summary>

The repository includes a plaintext Kafka fixture on `localhost:9090` with
seeded topics, partitions, records, and consumer groups.

```sh
make kafka-up
```

```sh
KAFKAESQUE_KAFKA_BOOTSTRAP_SERVER=localhost:9090 kafkaesque --metadata
```

Start the optional activity simulator to keep consumer groups active:

```sh
make kafka-simulator-start
KAFKAESQUE_KAFKA_BOOTSTRAP_SERVER=localhost:9090 kafkaesque --consumers
make kafka-simulator-stop
```

```sh
make kafka-down
```

See [`kafka-env/README.md`](kafka-env/README.md) for fixture contents, reset,
logs, and direct Docker Compose commands.

</details>

## Documentation

- [Command reference and sample output](DOCS.md)
- [HTTP API reference](API.md)
- [Contributor guide and architecture](CONTRIBUTING.md)

## License

Kafkaesque is available under the [MIT License](LICENSE).
