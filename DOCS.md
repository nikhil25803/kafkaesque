# Kafkaesque command guide

This guide assumes Kafkaesque is installed and its effective configuration
points to the Kafka cluster you want to inspect.

The output below is representative. Cluster IDs, controllers, broker addresses,
offsets, lag, member counts, and consumer states vary by cluster and over time.

## Table of contents

- [Configuration checks](#configuration-checks)
- [Cluster metadata](#cluster-metadata)
- [Brokers](#brokers)
- [Topics](#topics)
- [Topic partitions](#topic-partitions)
- [Consumer groups](#consumer-groups)
- [Consumer group lag](#consumer-group-lag)
- [Consumer topic partition lag](#consumer-topic-partition-lag)
- [Combined inspection](#combined-inspection)

## Configuration checks

Validate configuration without opening a Kafka connection:

```sh
kafkaesque --check config
```

```text
Configuration is valid
```

Validate configuration and connectivity:

```sh
kafkaesque --check conn
```

```text
Kafka connection successful: broker.example.com:9092
```

## Cluster metadata

```sh
kafkaesque --metadata
```

```text
Kafka Cluster
================================================================================
Cluster ID:          4L6g3nShT-eMCtK--X86sw
Controller ID:                   broker-3
Brokers:                                5
Topics:                                 6

STATUS: CONNECTED
```

## Brokers

```sh
kafkaesque --brokers
```

```text
Kafka Brokers
================================================================================
ID   ADDRESS          RACK
1    localhost:9090
2    localhost:9095
3    localhost:9096
4    localhost:9097
5    localhost:9098

5 brokers available
```

## Topics

```sh
kafkaesque --topics
```

```text
Kafka Topics
================================================================================
ID   NAME                             LEVEL        PARTITIONS       REPLICATION FACTOR
1    __consumer_offsets               Internal     50               3
2    audit-events                     External     8                3
3    inventory                        External     4                3
4    notifications                    External     1                3
5    orders                           External     6                3
6    payments                         External     3                3

6 topics available
```

## Topic partitions

`--partitions` requires `--topic`.

```sh
kafkaesque --partitions --topic orders
```

```text
Topic: orders
================================================================================
PARTITION  LEADER                           REPLICAS     ISR
0          broker-1                         3            3
1          broker-2                         3            3
2          broker-3                         3            3
3          broker-4                         3            3
4          broker-5                         3            3
5          broker-4                         3            3
```

## Consumer groups

List consumer groups with their coordinator, state, active members, and unique
live topic subscriptions:

```sh
kafkaesque --consumers
```

```text
Kafka Consumers
=====================================================================================================================
GROUP NAME                       TYPE         COORDINATOR      STATE                MEMBERS COUNT    TOPICS
order-processor                  consumer     broker-2         Stable               2                1
payment-worker                   consumer     broker-3         Stable               1                1
inventory-sync                   consumer     broker-3         Stable               1                1
notification-dispatcher          consumer     broker-4         Stable               1                2

4 groups · 5 topics · 5 members
```

Inactive groups can report `Empty`, zero members, and zero live subscriptions.

## Consumer group lag

Inspect one group's committed topics and aggregate lag:

```sh
kafkaesque --consumer --group notification-dispatcher
```

```text
Consumer Group: notification-dispatcher
================================================================================
STATE: STABLE
MEMBERS: 1
TOPICS: 2
TOTAL LAG: 4

TOPICS
================================================================================
TOPIC                            PARTITIONS   LAG        STATUS
audit-events                     8            2          HEALTHY
notifications                    1            2          HEALTHY
```

Kafkaesque classifies topic lag using the configured warning and unhealthy
thresholds. Kafka supplies the offsets; the health labels are Kafkaesque policy.

## Consumer topic partition lag

Add `--topic` to inspect committed and log-end offsets by partition:

```sh
kafkaesque --consumer --group notification-dispatcher --topic audit-events
```

```text
Consumer Group: notification-dispatcher
Topic: audit-events
================================================================================
PARTITION    COMMITTED OFFSET    LOG END OFFSET    LAG
0            95                  95                0
1            95                  95                0
2            95                  95                0
3            190                 190               0
4            286                 286               0
5            0                   0                 0
6            200                 200               0
7            17                  19                2
TOTAL 2
```

Only partitions with committed offsets are displayed. `TOTAL` is the sum of the
displayed partition lag.

## Combined inspection

Information flags can be combined. Kafkaesque performs one shared metadata
request and prints the requested sections in a stable order.

```sh
kafkaesque --metadata --brokers
```

```text
Kafka Cluster
================================================================================
Cluster ID:          4L6g3nShT-eMCtK--X86sw
Controller ID:                   broker-3
Brokers:                                5
Topics:                                 6

STATUS: CONNECTED

Kafka Brokers
================================================================================
ID   ADDRESS          RACK
1    localhost:9090
2    localhost:9095
3    localhost:9096
4    localhost:9097
5    localhost:9098

5 brokers available
```

Consumer list mode (`--consumers`) and single-group detail mode (`--consumer`)
cannot be combined.
