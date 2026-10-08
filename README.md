# kafkaesque

Kafkaesque is a command-line tool for inspecting a Kafka cluster's metadata,
topics, brokers, partitions, and consumer groups.

## Usage

```text
kafkaesque [flags]

Flags:
  -b, --brokers        Retrieve broker information
  -c, --consumers      Retrieve consumer information
  --check string   Check configuration or Kafka connection (config|conn)
  --config string  Path to the YAML configuration file
  -h, --help           Help for kafkaesque
  -m, --metadata       Retrieve cluster metadata
  -p, --partitions     Retrieve partition information for a topic
      --topic string   Topic name
  -t, --topics         Retrieve topic information
```

Kafkaesque connects to the Kafka bootstrap server at `localhost:9092` by
default. Configuration is resolved in this order, with later values winning:

1. The built-in `localhost:9092` default.
2. YAML at the platform user configuration path under
   `kafkaesque/config.yaml`, or the file selected with `--config`.
3. The `KAFKAESQUE_BOOTSTRAP_SERVER` environment variable.

The default YAML locations are `$XDG_CONFIG_HOME/kafkaesque/config.yaml` (or
`~/.config/kafkaesque/config.yaml`) on Linux,
`~/Library/Application Support/kafkaesque/config.yaml` on macOS, and
`%AppData%\kafkaesque\config.yaml` on Windows.

See `kafkaesque.example.yaml` for the supported YAML shape. Keep secrets out of
the file; future authentication settings will use `KAFKAESQUE_*` environment
variables. Multiple information flags can be combined in one command.

## Examples

```sh
# Cluster metadata
kafkaesque --metadata

# Topics and brokers in one request
kafkaesque --topics --brokers

# Partitions for a topic
kafkaesque --partitions --topic orders

# Consumer groups
kafkaesque --consumers

# Use an explicit configuration file
kafkaesque --config ./config.yaml --metadata

# Validate the effective configuration without connecting
kafkaesque --check config

# Validate configuration and test the Kafka connection
kafkaesque --check conn

# Help
kafkaesque --help
```

## Local Kafka environment

A disposable five-broker plaintext Kafka environment with seeded topics,
partitions, records, and consumer groups is available under `kafka-env/`. Use
`localhost:9090` as its bootstrap address; brokers are exposed on ports
`9090` and `9095`–`9098`. It does not change Kafkaesque's default
configuration or occupy the conventional `9092` port.

```sh
make kafka-up
./bin/kafkaesque --config kafka-env/kafkaesque.yaml -m
./bin/kafkaesque --config kafka-env/kafkaesque.yaml -p --topic orders
make kafka-down
```

See `kafka-env/README.md` for the complete fixture list, direct Compose
commands, logs, and reset instructions.
