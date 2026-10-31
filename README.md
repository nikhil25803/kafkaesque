<h1 align="center">Kafkaesque</h1>

<p align="center">
  <strong>A focused Kafka monitoring dashboard, API, and CLI.</strong><br>
  Monitor cluster health, topic activity, consumer lag, and Slack lag alerts from one read-only interface.
</p>

<p align="center">
  <a href="https://github.com/nikhil25803/kafkaesque/releases/latest"><img src="https://img.shields.io/github/v/release/nikhil25803/kafkaesque.svg" alt="Latest release"></a>
  <a href="https://github.com/nikhil25803/kafkaesque/releases"><img src="https://img.shields.io/github/downloads/nikhil25803/kafkaesque/total.svg" alt="Release downloads"></a>
  <a href="go.mod"><img src="https://img.shields.io/github/go-mod/go-version/nikhil25803/kafkaesque.svg" alt="Go version"></a>
  <a href="https://github.com/nikhil25803/kafkaesque/actions/workflows/ci.yml"><img src="https://github.com/nikhil25803/kafkaesque/actions/workflows/ci.yml/badge.svg" alt="Build status"></a>
  <a href="https://github.com/nikhil25803/kafkaesque/actions/workflows/tests.yml"><img src="https://github.com/nikhil25803/kafkaesque/actions/workflows/tests.yml/badge.svg" alt="Test status"></a>
  <a href="https://github.com/nikhil25803/kafkaesque/actions/workflows/kafka-integration.yml"><img src="https://github.com/nikhil25803/kafkaesque/actions/workflows/kafka-integration.yml/badge.svg" alt="Kafka integration status"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-white.svg" alt="MIT license"></a>
  <img src="https://img.shields.io/badge/Apache_Kafka-231F20?logo=apachekafka&amp;logoColor=white" alt="Apache Kafka">
  <img src="https://img.shields.io/badge/PostgreSQL_18%2B-4169E1?logo=postgresql&amp;logoColor=white" alt="PostgreSQL 18 or newer">
  <img src="https://img.shields.io/badge/Docker-2496ED?logo=docker&amp;logoColor=white" alt="Docker">
  <img src="https://img.shields.io/badge/HTMX_4.0-3366CC?logo=htmx&amp;logoColor=white" alt="HTMX 4.0">
</p>

![Kafkaesque product banner](docs/assets/branding/KafkaesqueBanner.png)

## Why Kafkaesque?

Monitoring Kafka is important, but it can become tedious. The standard CLI is
useful for one-off checks, while broader observability platforms often require
more setup than a team needs just to understand its Kafka cluster.

Kafkaesque is a dedicated, read-only Kafka monitoring tool. Its web UI gives
developers and operators one place to inspect cluster health, brokers, topics,
hourly message rates, consumer groups, and partition lag. You can also create
Slack alerts with configurable lag thresholds and receive a follow-up when the
lag is resolved.

Kafkaesque does not produce messages or modify Kafka resources. PostgreSQL is
used only for application accounts, sessions, roles, permissions, and alert
subscriptions—not Kafka messages or historical metrics.

## How it works

```mermaid
flowchart LR
    User[Browser user]
    APIClient[API client]
    CLI[CLI user] --> CLIClient[Kafkaesque CLI]

    subgraph Process[Kafkaesque server]
        Web[Web UI]
        API[HTTP API]
        Web --> Auth[Authentication and RBAC]
        API --> Auth
        Auth --> Collectors[Live Kafka collectors]
        Monitors[Message-rate and lag-alert monitors] --> Collectors
    end

    User -->|Sign in| Web
    APIClient -->|Login and bearer token| API
    Auth <-->|Users, sessions, roles| PostgreSQL[(PostgreSQL 18+)]
    Monitors <-->|Alert definitions and state| PostgreSQL
    Collectors -->|Metadata, groups, offsets| Kafka[Kafka cluster]
    CLIClient -->|Direct inspection| Kafka
    Monitors -->|Firing and resolved alerts| Slack[Slack]
```

The UI and API share the same authenticated Kafka connection and collectors.
Browser users authenticate with secure session cookies, API clients use opaque
access and refresh tokens, and the standalone CLI connects directly to Kafka.
See [Architecture](docs/architecture.md) for detailed request and startup flows.

## Features

| Concern                       | Details                                                                                                                                                                                                                                                                                         |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Web monitoring**            | <ul><li>Dashboard for connection status, cluster totals, and health.</li><li>Broker, topic, partition, and consumer-group inspection.</li><li>Hourly topic message rates and partition-level consumer lag.</li><li>Responsive server-rendered UI with configurable refresh intervals.</li></ul> |
| **Kafka connectivity**        | <ul><li>Plaintext, TLS, and mutual TLS connections.</li><li>SASL PLAIN, SCRAM-SHA-256, and SCRAM-SHA-512 authentication.</li><li>Broker address overrides for containers, Kubernetes, and split networks.</li></ul>                                                                             |
| **Authentication and access** | <ul><li>PostgreSQL-backed users, sessions, built-in roles, and custom RBAC.</li><li>Secure browser cookies with CSRF protection.</li><li>Opaque API access and refresh tokens.</li></ul>                                                                                                        |
| **Slack lag alerts**          | <ul><li>Average or maximum partition-lag thresholds.</li><li>Slack Incoming Webhook and Workflow Builder trigger support.</li><li>One firing notification per breach and optional resolution notification.</li></ul>                                                                            |
| **API and CLI**               | <ul><li>Authenticated JSON API for Kafka inspection.</li><li>Text or JSON CLI output with in-place watch mode.</li><li>CLI inspection requires neither PostgreSQL nor an application account.</li></ul>                                                                                         |
| **Deployment**                | <ul><li>One Go binary with embedded UI assets and no Node.js runtime.</li><li>Non-root Linux containers for AMD64 and ARM64.</li><li>Versioned PostgreSQL migrations applied during server startup.</li></ul>                                                                                   |

See [Features and limitations](docs/features.md) for the complete capability
map and intentionally unsupported functionality.

## Quick start

### Try the complete local demo

The all-in-one demo builds Kafkaesque and starts three Kafka brokers, seeded
topics and consumer groups, continuous activity, and PostgreSQL:

```sh
git clone https://github.com/nikhil25803/kafkaesque.git
cd kafkaesque
cp dev/demo/.env.example dev/demo/.env
# Fill both required values in dev/demo/.env.
make demo-up
```

Open <http://127.0.0.1:8085/ui> and sign in as `demo_captain` with the
password configured in `dev/demo/.env`. The services bind only to localhost.
See the [demo guide](dev/demo/README.md) for secret generation, data,
permissions, ports, logs, and reset commands.

### Run the web UI with Docker

Before starting Kafkaesque, provide:

1. Kafka bootstrap addresses that the container can reach. Kafkaesque must also
   reach the broker, coordinator, and partition-leader addresses returned by
   Kafka.
2. A PostgreSQL 18+ database and role that can create and modify objects in its
   `public` schema. Use a database dedicated to Kafkaesque.
3. A Super Admin username, email, and policy-compliant password.
4. A stable encryption key for Slack webhook URLs. Generate one with:

   ```sh
   openssl rand -base64 32
   ```

Pull the latest image:

```sh
docker pull ghcr.io/nikhil25803/kafkaesque:latest
```

Addresses such as `localhost` inside the container refer to the container
itself. Use a Docker network hostname, `host.docker.internal` where supported,
or another address reachable from the container.

#### Option A: mounted YAML configuration

Create `config.yaml` and replace every `CHANGE_ME` value:

```yaml
kafka:
  bootstrap_server: kafka.example.com:9092

authentication:
  super_admin_username: super_admin
  super_admin_email: super-admin@example.com
  super_admin_password: CHANGE_ME_A1_
  super_admin_recovery: verify
  cookie_secure: false

database:
  url: postgres://kafkaesque:CHANGE_ME@postgres.example.com:5432/kafkaesque?sslmode=require

alerts:
  encryption_key: CHANGE_ME_WITH_OPENSSL_OUTPUT

api:
  host: 0.0.0.0
  port: 8085
```

Start Kafkaesque with the file mounted read-only:

```sh
docker run --rm --name kafkaesque \
  --publish 8085:8085 \
  --volume "$PWD/config.yaml:/config.yaml:ro" \
  ghcr.io/nikhil25803/kafkaesque:latest \
  serve --config /config.yaml
```

#### Option B: environment variables

The same configuration can be supplied without a YAML file:

```sh
docker run --rm --name kafkaesque \
  --publish 8085:8085 \
  --env KAFKAESQUE_KAFKA_BOOTSTRAP_SERVER=kafka.example.com:9092 \
  --env KAFKAESQUE_AUTHENTICATION_SUPER_ADMIN_USERNAME=super_admin \
  --env KAFKAESQUE_AUTHENTICATION_SUPER_ADMIN_EMAIL=super-admin@example.com \
  --env KAFKAESQUE_AUTHENTICATION_SUPER_ADMIN_PASSWORD=CHANGE_ME_A1_ \
  --env KAFKAESQUE_AUTHENTICATION_SUPER_ADMIN_RECOVERY=verify \
  --env KAFKAESQUE_AUTHENTICATION_COOKIE_SECURE=false \
  --env 'KAFKAESQUE_DATABASE_URL=postgres://kafkaesque:CHANGE_ME@postgres.example.com:5432/kafkaesque?sslmode=require' \
  --env KAFKAESQUE_ALERTS_ENCRYPTION_KEY=CHANGE_ME_WITH_OPENSSL_OUTPUT \
  --env KAFKAESQUE_API_HOST=0.0.0.0 \
  --env KAFKAESQUE_API_PORT=8085 \
  ghcr.io/nikhil25803/kafkaesque:latest serve
```

Check the process, then open the UI and sign in with the configured Super Admin:

```sh
curl --fail http://127.0.0.1:8085/api/v1/health
```

```text
http://127.0.0.1:8085/ui
```

The examples disable secure cookies only for trusted local HTTP. Keep secure
cookies enabled behind HTTPS, inject secrets through your deployment platform,
and never commit a populated configuration file. See the complete
[configuration reference](docs/configuration.md),
[deployment guide](docs/deployment.md), and [security guide](docs/security.md).

### Install the CLI

The checksummed installer supports Linux and macOS and installs without `sudo`:

```sh
curl -fsSL https://raw.githubusercontent.com/nikhil25803/kafkaesque/main/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
```

Point the CLI at Kafka and start inspecting the cluster:

```sh
export KAFKAESQUE_KAFKA_BOOTSTRAP_SERVER=localhost:9092

kafkaesque --check
kafkaesque --health
kafkaesque --brokers
kafkaesque --topics
kafkaesque --partitions --topic orders
kafkaesque --consumers
kafkaesque --consumer --group order-processor
kafkaesque --consumer --group order-processor --topic orders
kafkaesque --consumer --member --group order-processor
kafkaesque --metadata --brokers --topics --output json
kafkaesque --health --watch 10
```

CLI inspection connects directly to Kafka and does not require PostgreSQL or a
Kafkaesque account. Read [Getting started](docs/getting-started.md) for YAML,
TLS, SASL, and consumer-lag examples.

## CLI reference

<details>
<summary><strong>Commands, flags, relationships, and examples</strong></summary>

### Commands

| Command                         | Description                                                     | Example                                   |
| ------------------------------- | --------------------------------------------------------------- | ----------------------------------------- |
| `kafkaesque [flags]`            | Inspect Kafka directly.                                         | `kafkaesque --health`                     |
| `kafkaesque serve [flags]`      | Start the authenticated web UI and HTTP API.                    | `kafkaesque serve --config ./config.yaml` |
| `kafkaesque completion <shell>` | Generate completion for `bash`, `fish`, `powershell`, or `zsh`. | `kafkaesque completion zsh`               |
| `kafkaesque help [command]`     | Show help for Kafkaesque or a command.                          | `kafkaesque help serve`                   |

### General and presentation flags

| Flag                     | Description                                                                | Example                                      |
| ------------------------ | -------------------------------------------------------------------------- | -------------------------------------------- |
| `-h`, `--help`           | Show command help.                                                         | `kafkaesque --help`                          |
| `-v`, `--version`        | Show the installed version.                                                | `kafkaesque --version`                       |
| `--config <path>`        | Load an explicit YAML configuration file.                                  | `kafkaesque --config ./config.yaml --health` |
| `--check [config\|conn]` | Validate configuration or Kafka connectivity. Bare `--check` means `conn`. | `kafkaesque --check config`                  |
| `--output text\|json`    | Select text or structured JSON output; default `text`.                     | `kafkaesque --brokers --output json`         |
| `--watch <seconds>`      | Refresh an information command in place at a positive interval.            | `kafkaesque --health --watch 10`             |

### Information flags

| Flag                 | Description                                                                   | Example                                                        |
| -------------------- | ----------------------------------------------------------------------------- | -------------------------------------------------------------- |
| `-m`, `--metadata`   | Show cluster identity and resource totals.                                    | `kafkaesque --metadata`                                        |
| `--health`           | Show cluster, partition, and consumer health.                                 | `kafkaesque --health`                                          |
| `-b`, `--brokers`    | List brokers, advertised and effective addresses, and racks.                  | `kafkaesque --brokers`                                         |
| `-t`, `--topics`     | List internal and external topics with partition and replication information. | `kafkaesque --topics`                                          |
| `-p`, `--partitions` | List leaders, replicas, ISR, and health; requires `--topic`.                  | `kafkaesque --partitions --topic orders`                       |
| `--topic <name>`     | Select a topic for partition or consumer-group inspection.                    | `kafkaesque --consumer --group order-processor --topic orders` |
| `-c`, `--consumers`  | List consumer groups.                                                         | `kafkaesque --consumers`                                       |
| `--consumer`         | Inspect one consumer group; requires `--group`.                               | `kafkaesque --consumer --group order-processor`                |
| `--member`           | Include member assignments; requires `--consumer` and cannot use `--topic`.   | `kafkaesque --consumer --member --group order-processor`       |
| `--group <name>`     | Select the consumer group used by `--consumer`.                               | `kafkaesque --consumer --group order-processor`                |

Information flags may share one command when their selectors are compatible.
`--health` is standalone except for `--config`, `--output`, and `--watch`.
`--consumers` and `--consumer` are mutually exclusive. `--topic` requires
`--partitions` or `--consumer`; `--group` requires `--consumer`; and
`--check` cannot be combined with information or presentation flags.

### Server flags

| Flag              | Description                                         | Example                                   |
| ----------------- | --------------------------------------------------- | ----------------------------------------- |
| `--config <path>` | Load an explicit server YAML configuration.         | `kafkaesque serve --config ./config.yaml` |
| `--host <host>`   | Override `api.host` for this process.               | `kafkaesque serve --host 0.0.0.0`         |
| `--port <port>`   | Override `api.port`; accepted range is `1`–`65535`. | `kafkaesque serve --port 8085`            |
| `-h`, `--help`    | Show help for `serve`.                              | `kafkaesque serve --help`                 |

Server mode requires PostgreSQL 18+, Super Admin bootstrap values, and an alert
encryption key. See the [complete CLI reference](docs/cli-reference.md) for
output formats, watch behavior, exit codes, and examples.

</details>

## Common workflows

| Goal                                       | Start in the UI                      | Detailed guide                                                 |
| ------------------------------------------ | ------------------------------------ | -------------------------------------------------------------- |
| Verify connection and cluster health       | **Dashboard** and **Cluster Health** | [Cluster health](docs/guides/cluster-health.md)                |
| Inspect topics and hourly message rates    | **Topics**                           | [Features and limitations](docs/features.md)                   |
| Investigate a lagging consumer             | **Consumer Groups**                  | [Consumer lag](docs/guides/consumer-lag.md)                    |
| Create firing and resolution notifications | **Settings → Alerts**                | [Slack alerts](docs/features.md#slack-alerts)                  |
| Manage accounts and permissions            | **Settings → Users / Roles**         | [Authentication and authorization](docs/api/authentication.md) |
| Connect with TLS, mTLS, or SASL            | Configure the server connection      | [TLS and SASL](docs/guides/tls-sasl.md)                        |
| Automate Kafka inspection                  | Use the JSON API or CLI              | [API overview](docs/api/overview.md)                           |
| Diagnose startup or Kafka failures         | Review the reported error            | [Troubleshooting](docs/troubleshooting.md)                     |

## UI walkthrough

### Dashboard and cluster monitoring

<table>
  <tr>
    <td colspan="2" align="center">
      <strong>Dashboard</strong><br>
      <img src="docs/assets/screenshots/homepage_dashboard.png" alt="Kafkaesque dashboard showing connection status, cluster totals, consumer groups, and cluster health" width="100%">
    </td>
  </tr>
  <tr>
    <td align="center" width="50%">
      <strong>Consumer Groups</strong><br>
      <img src="docs/assets/screenshots/consumer_groups.png" alt="Kafkaesque consumer groups list with search, state filtering, and lag status" width="100%">
    </td>
    <td align="center" width="50%">
      <strong>Topics</strong><br>
      <img src="docs/assets/screenshots/topics.png" alt="Kafkaesque topics list showing topic levels, partitions, replication, and message rates" width="100%">
    </td>
  </tr>
  <tr>
    <td align="center" width="50%">
      <strong>Brokers</strong><br>
      <img src="docs/assets/screenshots/brokers_page.png" alt="Kafkaesque brokers list showing advertised addresses, connect addresses, and racks" width="100%">
    </td>
    <td align="center" width="50%">
      <strong>Cluster Health</strong><br>
      <img src="docs/assets/screenshots/cluster_health.png" alt="Kafkaesque cluster health page showing cluster, partition, and consumer diagnostics" width="100%">
    </td>
  </tr>
</table>

### Access and settings

<table>
  <tr>
    <td colspan="2" align="center">
      <strong>Sign in</strong><br>
      <img src="docs/assets/screenshots/signin_page.png" alt="Kafkaesque sign-in page with account credentials and product branding" width="100%">
    </td>
  </tr>
  <tr>
    <td align="center" width="50%">
      <strong>Settings · Users</strong><br>
      <img src="docs/assets/screenshots/settings_user.png" alt="Kafkaesque user settings showing account details, roles, and pagination" width="100%">
    </td>
    <td align="center" width="50%">
      <strong>Settings · Roles</strong><br>
      <img src="docs/assets/screenshots/settings_role.png" alt="Kafkaesque role settings showing system roles, permissions, and assigned users" width="100%">
    </td>
  </tr>
  <tr>
    <td align="center" width="50%">
      <strong>Add User</strong><br>
      <img src="docs/assets/screenshots/add_user.png" alt="Kafkaesque add-user form with identity, password, and role fields" width="100%">
    </td>
    <td align="center" width="50%">
      <strong>Create Role</strong><br>
      <img src="docs/assets/screenshots/create_roles.png" alt="Kafkaesque create-role form with account and alert permissions" width="100%">
    </td>
  </tr>
</table>

### Slack alert lifecycle

<table>
  <tr>
    <td align="center" width="50%">
      <strong>Slack Alerts</strong><br>
      <img src="docs/assets/screenshots/slack_alert_page.png" alt="Kafkaesque Slack alert subscription settings" width="100%">
    </td>
    <td align="center" width="50%">
      <strong>Create Slack Alert</strong><br>
      <img src="docs/assets/screenshots/slack_alert_create.png" alt="Kafkaesque form for creating a Slack lag alert" width="100%">
    </td>
  </tr>
  <tr>
    <td align="center" width="50%">
      <strong>Lag Alert Fired</strong><br>
      <img src="docs/assets/screenshots/slack_alert_fired.png" alt="Slack notification showing a fired Kafkaesque lag alert" width="100%">
    </td>
    <td align="center" width="50%">
      <strong>Lag Alert Resolved</strong><br>
      <img src="docs/assets/screenshots/slack_alert_resolved.png" alt="Slack notification showing a resolved Kafkaesque lag alert" width="100%">
    </td>
  </tr>
</table>

## Documentation

| Topic                | Reference                                                                         |
| -------------------- | --------------------------------------------------------------------------------- |
| Install and connect  | [Installation](docs/installation.md) · [Getting started](docs/getting-started.md) |
| Configure and deploy | [Configuration](docs/configuration.md) · [Deployment](docs/deployment.md)         |
| Operate securely     | [Security](docs/security.md) · [Troubleshooting](docs/troubleshooting.md)         |
| Integrate and extend | [API endpoints](docs/api/endpoints.md) · [Architecture](docs/architecture.md)     |

Browse the [documentation index](docs/README.md) for every guide and reference.

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) before submitting a change. Local setup,
fixtures, and test commands are documented in
[Development](docs/development.md).

## License

Kafkaesque is available under the [MIT License](LICENSE).
