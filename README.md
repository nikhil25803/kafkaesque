# kafkaesque

Kafkaesque is a command-line tool for inspecting a Kafka cluster's metadata,
topics, brokers, partitions, and consumer groups.

## Usage

```text
kafkaesque [flags]

Flags:
  -b, --brokers        Retrieve broker information
  -c, --consumers      Retrieve consumer information
  -h, --help           Help for kafkaesque
  -m, --metadata       Retrieve cluster metadata
  -p, --partitions     Retrieve partition information for a topic
      --topic string   Topic name
  -t, --topics         Retrieve topic information
```

Kafkaesque connects to Kafka at `localhost:9092`. Multiple information flags
can be combined in one command.

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

# Help
kafkaesque --help
```
