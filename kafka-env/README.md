# Local Kafka environment

This directory runs a disposable, five-broker plaintext Kafka cluster for
developing Kafkaesque. Use `localhost:9090` as the bootstrap address; the five
brokers advertise host ports `9090` and `9095` through `9098`. Port `9092` is
left free for a conventional local Kafka installation.

From the repository root:

```sh
make kafka-up
make kafka-logs
make kafka-down
```

`make kafka-up` creates these deterministic fixtures:

| Topic | Partitions | Replicas | Records |
| --- | ---: | ---: | ---: |
| `orders` | 6 | 3 | 12 |
| `payments` | 3 | 3 | 8 |
| `inventory` | 4 | 3 | 8 |
| `notifications` | 1 | 3 | 5 |
| `audit-events` | 8 | 3 | 10 |

Re-running `make kafka-up` does not duplicate records. Use `make kafka-reset`
for a clean fixture set.

## Activity simulator

After starting the fixture, run the optional activity simulator to continuously
produce records and keep consumer groups active:

```sh
make kafka-up
make kafka-simulator-start
./bin/kafkaesque --config kafka-env/kafkaesque.yaml -c
./bin/kafkaesque --config kafka-env/kafkaesque.yaml --consumer --group order-processor
./bin/kafkaesque --config kafka-env/kafkaesque.yaml --consumer --group order-processor --topic orders
make kafka-simulator-stop
make kafka-down
```

The simulator reuses the fixture groups and adds one multi-topic group:

| Consumer group | Topics | Active members |
| --- | ---: | ---: |
| `order-processor` | 1 | 2 |
| `payment-worker` | 1 | 1 |
| `inventory-sync` | 1 | 1 |
| `notification-dispatcher` | 2 | 1 |

Use `make kafka-simulator-status` to inspect the container and
`make kafka-simulator-logs` to follow its output. The simulator requires an
existing fixture and will not start Kafka automatically. Produced records and
consumer offsets remain until `make kafka-reset`.

The equivalent direct Compose commands are:

```sh
docker compose -f kafka-env/compose.yaml up -d --wait broker-1 broker-2 broker-3 broker-4 broker-5
docker compose -f kafka-env/compose.yaml run --rm init
docker compose -f kafka-env/compose.yaml down --volumes --remove-orphans
```

Run Kafkaesque against the environment with the included configuration:

```sh
./bin/kafkaesque --config kafka-env/kafkaesque.yaml -m
./bin/kafkaesque --config kafka-env/kafkaesque.yaml -b
./bin/kafkaesque --config kafka-env/kafkaesque.yaml -t
./bin/kafkaesque --config kafka-env/kafkaesque.yaml -p --topic orders
```

The environment intentionally has no authentication, TLS, or persistent
volume. Those modes belong in separate future Compose files.
