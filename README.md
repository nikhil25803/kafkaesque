<p align="center">
  <img src="asset/image/KafkaesqueLogo.png" alt="Kafkaesque logo" width="220">
</p>

<h1 align="center">Kafkaesque</h1>

<p align="center">
  A lightweight, read-only Kafka cluster inspector.<br>
  Understand your cluster without changing it.
</p>

<p align="center">
  <a href="https://github.com/nikhil25803/kafkaesque/actions/workflows/ci.yml"><img src="https://github.com/nikhil25803/kafkaesque/actions/workflows/ci.yml/badge.svg" alt="Build status"></a>
  <a href="https://github.com/nikhil25803/kafkaesque/actions/workflows/tests.yml"><img src="https://github.com/nikhil25803/kafkaesque/actions/workflows/tests.yml/badge.svg" alt="Test status"></a>
  <a href="https://github.com/nikhil25803/kafkaesque/actions/workflows/kafka-integration.yml"><img src="https://github.com/nikhil25803/kafkaesque/actions/workflows/kafka-integration.yml/badge.svg" alt="Kafka integration status"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-white.svg" alt="MIT license"></a>
</p>

![Kafkaesque product vision](asset/image/KafkaesqueBanner.png)

## Upcoming Releases

| Version | Release Info                                  | Status      |
| ------- | --------------------------------------------- | ----------- |
| v0.0.1  | Kafka Cluster Inspector                       | Released    |
| v0.1.0  | Consumer Groups & Basic Lag                   | Coming Soon |
| v0.2.0  | Kafka Connectivity & Authentication           | Coming Soon |
| v0.3.0  | Deep Consumer Observability                   | Coming Soon |
| v0.4.0  | Cluster Health & Diagnostics                  | Coming Soon |
| v0.5.0  | CLI UX & Structured Output                    | Coming Soon |
| v0.6.0  | Watch Mode & Lag Trends                       | Coming Soon |
| v0.7.0  | HTMX Web UI                                   | Coming Soon |
| v0.8.0  | Web Authentication                            | Coming Soon |
| v0.9.0  | Slack Alerts & Production Hardening           | Coming Soon |
| v1.0.0  | Stable Read-Only Kafka Observability Platform | Coming Soon |

## Install

### Linux and macOS

The installer detects your operating system and architecture, verifies the
downloaded archive, and installs Kafkaesque into `~/.local/bin`.

```sh
curl -fsSL https://raw.githubusercontent.com/nikhil25803/kafkaesque/main/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
kafkaesque --help
```

### Windows

Download and run the PowerShell installer. It verifies the archive, installs
Kafkaesque under `%LOCALAPPDATA%\Programs\kafkaesque`, and adds that directory
to your user PATH.

```powershell
curl.exe -fsSLo install.ps1 https://raw.githubusercontent.com/nikhil25803/kafkaesque/main/install.ps1
powershell -ExecutionPolicy Bypass -File .\install.ps1
```

Open a new terminal, then verify the installation:

```powershell
kafkaesque --help
```

Both installers use the latest release by default. To install v0.0.1
explicitly:

```sh
curl -fsSL https://raw.githubusercontent.com/nikhil25803/kafkaesque/main/install.sh | VERSION=v0.0.1 sh
```

```powershell
powershell -ExecutionPolicy Bypass -File .\install.ps1 -Version v0.0.1
```

## Build from source

Kafkaesque requires the Go version declared in `go.mod`.

```sh
git clone https://github.com/nikhil25803/kafkaesque.git
cd kafkaesque
make build
./bin/kafkaesque --help
```

Kafkaesque connects to `localhost:9092` by default, so a first inspection can
be as simple as:

```sh
./bin/kafkaesque --metadata
```

## Flags

| Short | Long           | Value              | Description                                              |
| ----- | -------------- | ------------------ | -------------------------------------------------------- |
| `-m`  | `--metadata`   | —                  | Show cluster metadata and connection status.             |
| `-b`  | `--brokers`    | —                  | List Kafka brokers.                                      |
| `-t`  | `--topics`     | —                  | List Kafka topics.                                       |
| `-p`  | `--partitions` | —                  | List partitions for the topic selected by `--topic`.     |
| `-c`  | `--consumers`  | —                  | List consumer groups.                                    |
| —     | `--consumer`   | —                  | Inspect the consumer group selected by `--group`.        |
| —     | `--group`      | name               | Select a group for detailed consumer lag inspection.     |
| —     | `--topic`      | name               | Select a topic for partition or consumer lag inspection. |
| —     | `--config`     | path               | Load an explicit YAML configuration file.                |
| —     | `--check`      | `config` or `conn` | Validate configuration or test the Kafka connection.     |
| `-h`  | `--help`       | —                  | Show command help.                                       |

Information flags can be combined; Kafkaesque shares the metadata request
between them.

```sh
# Inspect cluster metadata, brokers, and topics together
./bin/kafkaesque --metadata --brokers --topics

# Inspect one topic's partitions
./bin/kafkaesque --partitions --topic orders

# List consumer groups, then inspect one group's lag
./bin/kafkaesque --consumers
./bin/kafkaesque --consumer --group order-processor
./bin/kafkaesque --consumer --group order-processor --topic orders

# Validate configuration without connecting
./bin/kafkaesque --check config

# Validate configuration and connect to Kafka
./bin/kafkaesque --check conn
```

## Configuration

The effective bootstrap server is resolved in this order, with later values
winning:

1. The built-in `localhost:9092` default.
2. YAML from the platform configuration path, or the file selected by `--config`.
3. The `KAFKAESQUE_BOOTSTRAP_SERVER` environment variable.

```yaml
kafka:
  bootstrap_server: localhost:9092

lag:
  warning_threshold: 100
  unhealthy_threshold: 500
```

Consumer topic lag up to the warning threshold is `HEALTHY`, lag above the
warning threshold is `WARNING`, and lag above the unhealthy threshold is
`UNHEALTHY`. Both thresholds must be non-negative, and the warning threshold
must be lower than the unhealthy threshold.

The default YAML locations are:

| Platform | Path                                                                            |
| -------- | ------------------------------------------------------------------------------- |
| Linux    | `$XDG_CONFIG_HOME/kafkaesque/config.yaml` or `~/.config/kafkaesque/config.yaml` |
| macOS    | `~/Library/Application Support/kafkaesque/config.yaml`                          |
| Windows  | `%AppData%\kafkaesque\config.yaml`                                              |

See [`kafkaesque.example.yaml`](kafkaesque.example.yaml) for a ready-to-copy
example. Configuration files are not created automatically.

## Local Kafka environment

The repository includes a disposable five-broker Kafka cluster with seeded
topics and partitions. It uses `localhost:9090`, leaving the conventional
`9092` port available for another local cluster.

```sh
make kafka-up
./bin/kafkaesque --config kafka-env/kafkaesque.yaml --metadata
./bin/kafkaesque --config kafka-env/kafkaesque.yaml --topics
./bin/kafkaesque --config kafka-env/kafkaesque.yaml --partitions --topic orders
make kafka-down
```

See [`kafka-env/README.md`](kafka-env/README.md) for fixture details, logs,
reset commands, and direct Docker Compose usage.

Kafkaesque will grow in small, reviewable releases while remaining read-only
by design.

## License

Kafkaesque is available under the [MIT License](LICENSE).
