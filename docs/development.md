# Development

## Prerequisites

- Git and Make.
- The Go version declared in [`go.mod`](../go.mod).
- Docker with Docker Compose for Kafka and PostgreSQL integration.
- GoReleaser v2 only for release artifact testing.

## Build and check

```sh
git clone https://github.com/nikhil25803/kafkaesque.git
cd kafkaesque
go mod download
make check
```

`make check` runs formatting validation, `go vet`, unit tests, and the binary
build. It writes `bin/kafkaesque`.

Additional checks:

```sh
go test -race ./...
make release-test
git diff --check
```

## Local Kafka fixture

Start and seed the five-broker plaintext cluster:

```sh
make kafka-up
make build
./bin/kafkaesque --config dev/kafka/kafkaesque.yaml --metadata
```

Start active consumer groups when working on members or lag:

```sh
make kafka-simulator-start
./bin/kafkaesque --config dev/kafka/kafkaesque.yaml --consumers
make kafka-simulator-logs
make kafka-simulator-stop
```

Create ten unconsumed records for a Slack alert test, then drain them without
restarting producers to exercise resolution:

```sh
make kafka-lag-simulate GROUP=order-processor TOPIC=orders COUNT=10
make kafka-lag-resolve GROUP=order-processor TOPIC=orders
```

Stop or rebuild the disposable environment:

```sh
make kafka-down
make kafka-reset
```

See [`dev/kafka/README.md`](../dev/kafka/README.md) for fixture topics,
partitions, groups, ports, and direct Compose commands.

## Local PostgreSQL fixture

```sh
make postgres-up
make postgres-logs
make postgres-down
```

`postgres-down` preserves data. `make postgres-reset` removes the development
volume and creates a fresh database.

Copy the safe configuration template and add disposable local credentials:

```sh
cp dev/kafkaesque.example.yaml dev/kafkaesque.yaml
./bin/kafkaesque serve --config dev/kafkaesque.yaml
```

`dev/kafkaesque.yaml` is ignored by Git and GitGuardian.

## Authentication integration tests

```sh
make postgres-test
```

This creates the separate `kafkaesque_auth_test` database and runs the auth
suite with the race detector. Destructive integration tests reject database
names that do not end with `_test`.

## Container development

```sh
make docker-build VERSION=dev
docker run --rm kafkaesque:dev --version
```

Override `DOCKER_IMAGE` when building a local name:

```sh
make docker-build DOCKER_IMAGE=kafkaesque VERSION=dev
```

## Workflows

| Workflow | Purpose |
| --- | --- |
| `ci.yml` | Core build and static checks. |
| `tests.yml` | Unit and race test coverage. |
| `kafka-integration.yml` | Real plaintext Kafka CLI/API/UI smoke tests. |
| `kafka-auth.yml` | TCP, TLS, mTLS, PLAIN, and SCRAM Kafka fixtures. |
| `postgres-auth.yml` | PostgreSQL authentication and migration integration. |
| `release-test.yml` | Snapshot release artifact validation. |
| `release.yml` | Tag-triggered binaries, checksums, release, and container publication. |

## Project layout

| Path | Responsibility |
| --- | --- |
| `cmd/kafkaesque` | Process entry point. |
| `internals` | CLI, configuration, collectors, Kafka transport, auth, and rendering. |
| `apis` | JSON routes, middleware, envelopes, and pagination. |
| `ui` | Embedded templates, styles, scripts, and HTML handlers. |
| `dev/kafka` | Kafka fixtures and simulator. |
| `dev/postgres` | PostgreSQL 18 development fixture. |
| `docs` | User, operator, API, and developer documentation. |

Read [Architecture](architecture.md) before changing request flow, security, or
collector ownership.
