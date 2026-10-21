# CLI Reference

Kafkaesque CLI inspection reads Kafka directly and does not require PostgreSQL.
Use `--config` or environment variables to select the cluster.

## General flags

| Short | Long | Value | Description |
| --- | --- | --- | --- |
| `-h` | `--help` | — | Show command help. |
| `-v` | `--version` | — | Show the installed version. |
| — | `--output` | `text` or `json` | Select information output; default `text`. |
| — | `--watch` | positive seconds | Refresh an information command in place. |

## Checks and configuration

| Long | Value | Description |
| --- | --- | --- |
| `--config` | path | Load an explicit YAML file. |
| `--check` | optional `config` or `conn` | Validate configuration or test Kafka connectivity. Bare `--check` means `conn`. |

These connection forms are equivalent:

```sh
kafkaesque --check
kafkaesque --check conn
kafkaesque --check=conn
```

`--check config` validates the effective Kafka and lag configuration without
opening a Kafka connection. It does not require the server-only database and
authentication values.

## Information flags

| Short | Long | Value | Description |
| --- | --- | --- | --- |
| `-m` | `--metadata` | — | Show cluster identity and totals. |
| — | `--health` | — | Show cluster, partition, and consumer health. |
| `-b` | `--brokers` | — | List brokers and connection addresses. |
| `-t` | `--topics` | — | List topics and replication information. |
| `-p` | `--partitions` | — | List partitions for `--topic`. |
| `-c` | `--consumers` | — | List consumer groups. |
| — | `--consumer` | — | Inspect one group selected by `--group`. |
| — | `--member` | — | Include member assignments in group inspection. |
| — | `--group` | name | Select a consumer group. |
| — | `--topic` | name | Select a topic for partition or consumer inspection. |

## Server command

```sh
kafkaesque serve [flags]
```

| Flag | Value | Description |
| --- | --- | --- |
| `--config` | path | Load an explicit YAML file. |
| `--host` | host | Override `api.host`. |
| `--port` | `1`–`65535` | Override `api.port`. |

`serve` requires PostgreSQL 18+, Super Admin bootstrap values, and Kafka
configuration. See [Deployment](deployment.md).

## Valid combinations

Information flags may share one command:

```sh
kafkaesque --metadata --brokers --topics
kafkaesque --metadata --partitions --topic orders
```

The following rules apply:

- `--partitions` requires `--topic`.
- `--topic` requires `--partitions` or `--consumer`.
- `--consumer` requires `--group`.
- `--group` requires `--consumer`.
- `--member` requires `--consumer` and cannot use `--topic`.
- `--consumers` and `--consumer` are mutually exclusive.
- `--health` is standalone except for `--config`, `--output`, and `--watch`.
- `--check` cannot use information or presentation flags.
- Explicit `--output` or `--watch` requires an information command.

## Common commands

```sh
kafkaesque --metadata
kafkaesque --brokers
kafkaesque --topics
kafkaesque --partitions --topic orders
kafkaesque --consumers
kafkaesque --consumer --group order-processor
kafkaesque --consumer --member --group order-processor
kafkaesque --consumer --group order-processor --topic orders
kafkaesque --health
```

## Connection check output

A successful connection check includes the effective protocol and latency:

```text
Kafka Connection
================================================================================
Bootstrap Servers
  kafka-1.example.com:9093
Security
  Protocol: SASL_SSL
  Mechanism: SCRAM-SHA-256
Status: CONNECTED
Cluster
  Brokers: 3
  Controller: broker-2
  Topics: 24
  Partitions: 186
Connection latency: 42ms
```

Failure output includes a stable reason, sanitized error, and exit code. It
does not print configured credentials.

## Consumer lag output

```text
Consumer Group: order-processor
State: STABLE

Lag Summary
================================================================================
Total Lag: 294
Maximum Partition Lag: 161
Average Partition Lag: 49.0
Healthy Partitions: 4
Warning Partitions: 2
Unhealthy Partitions: 0

TOPICS
================================================================================
TOPIC                            PARTITIONS   LAG        STATUS
orders                           6            294        WARNING
```

The configured lag thresholds determine `HEALTHY`, `WARNING`, and `UNHEALTHY`.
See [Consumer lag](guides/consumer-lag.md).

## Cluster health output

```text
Kafka Cluster Health
================================================================================
Status: DEGRADED
CLUSTER
  Brokers: 3
  Topics: 24
  Partitions: 186
  Consumer Groups: 17
PARTITIONS
  Offline: 0
  Under-replicated: 3
CONSUMERS
  Rebalancing: 1
  Dead: 0
  Lagging: 2
```

`HEALTHY` and `DEGRADED` are both successful snapshots and exit with `0`.

## JSON output

Use `--output json` with any information command:

```sh
kafkaesque --brokers --topics --output json
```

The newline-terminated object contains only requested sections and uses stable
snake_case fields. Requested empty collections are encoded as `[]`. CLI errors
remain human-readable on standard error rather than using an error JSON schema.

## Watch mode

```sh
kafkaesque --health --watch 5
kafkaesque --consumer --group order-processor --output json --watch 10
```

Watch mode renders immediately, waits the selected interval, fetches a fresh
snapshot over the same Kafka connection, and replaces the terminal contents.
Refreshes are sequential. Press Ctrl+C to stop. A failed refresh exits nonzero
and leaves the last successful snapshot visible.

## Exit codes

| Code | Meaning |
| --- | --- |
| `0` | Command succeeded, including degraded cluster health. |
| `1` | Configuration, DNS, refused connection, or another general failure. |
| `2` | SASL, TLS, or Kafka authorization failure. |
| `3` | Connection or Kafka request timeout. |

## Output variability

Examples are representative. IDs, addresses, topics, offsets, members, lag,
and group states depend on the connected cluster and can change between calls.
