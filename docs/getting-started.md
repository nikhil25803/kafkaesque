# Getting Started

This guide connects Kafkaesque to Kafka, verifies the connection, and performs
a first useful inspection. The CLI path does not require PostgreSQL.

## Prerequisites

- A reachable Kafka bootstrap broker.
- Any TLS or SASL credentials required by the cluster.
- A Kafkaesque binary from the [installation guide](installation.md).

## Connect with an environment variable

For a plaintext local broker:

```sh
export KAFKAESQUE_KAFKA_BOOTSTRAP_SERVER=localhost:9092
```

Check configuration and connectivity:

```sh
kafkaesque --check config
kafkaesque --check
```

The connection check reports bootstrap servers, the effective security
protocol, cluster totals, and latency. A successful check exits with `0`.

## Connect with YAML

Create `kafkaesque.yaml`:

```yaml
kafka:
  bootstrap_servers:
    - kafka-1.example.com:9092
    - kafka-2.example.com:9092
  connection_timeout: 10s

lag:
  warning_threshold: 100
  unhealthy_threshold: 500
```

Use it explicitly:

```sh
kafkaesque --config kafkaesque.yaml --check
```

See [Configuration](configuration.md) for default paths, environment
precedence, authentication, and every supported field.

## Inspect the cluster

```sh
kafkaesque --metadata
kafkaesque --brokers
kafkaesque --topics
kafkaesque --health
```

Inspect partitions for one topic:

```sh
kafkaesque --partitions --topic orders
```

Inspect consumers and lag:

```sh
kafkaesque --consumers
kafkaesque --consumer --group order-processor
kafkaesque --consumer --member --group order-processor
kafkaesque --consumer --group order-processor --topic orders
```

Use `--output json` for structured output or `--watch 10` for an in-place
refresh. See the [CLI reference](cli-reference.md) for valid combinations and
exit codes.

## Start the API and web UI

Server mode additionally requires PostgreSQL 18 or newer and Super Admin
bootstrap configuration. The shortest complete local setup is the Docker
Compose example in [Deployment](deployment.md).

After the server starts:

- Open `/ui` in a browser.
- Use `GET /api/v1/health` for the public process check.
- Use the [authentication API](api/authentication.md) to obtain bearer tokens.

## Next steps

- Secure Kafka connections with [TLS and SASL](guides/tls-sasl.md).
- Understand [consumer lag](guides/consumer-lag.md).
- Diagnose [cluster health](guides/cluster-health.md).
- Resolve connection problems with [Troubleshooting](troubleshooting.md).
