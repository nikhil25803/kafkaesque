# Understand Cluster Health

Kafkaesque combines current Kafka metadata, consumer-group state, and consumer
lag into a compact health snapshot.

## Run the check

```sh
kafkaesque --health
```

Or use the authenticated API and UI:

```http
GET /api/v1/health/cluster
```

The public `GET /api/v1/health` route is different: it checks only that the
Kafkaesque HTTP process is running.

## Status rules

Cluster status is `DEGRADED` when any of these counts is nonzero:

| Signal | Meaning |
| --- | --- |
| Offline partitions | No current partition leader is available. |
| Under-replicated partitions | ISR contains fewer brokers than the replica set. |
| Rebalancing groups | A consumer group is in a transition or unknown state. |
| Dead groups | Kafka reports the group as dead. |
| Lagging groups | Total group lag is above the configured warning threshold. |

Otherwise the status is `HEALTHY`. Degraded health is still a successful
inspection and returns CLI exit code `0` and HTTP status `200`.

An offline partition can also be under-replicated, so the counts can overlap.
Internal topics and their partitions are included in cluster totals.

## Respond to a degraded signal

### Offline partitions

Check broker availability, controller logs, listener configuration, and replica
placement. Kafkaesque cannot elect a leader or repair the partition.

### Under-replicated partitions

Compare the replica and ISR broker lists with:

```sh
kafkaesque --partitions --topic <topic>
```

Transient recovery can resolve under-replication; persistent cases require
broker, network, disk, or replication investigation.

### Rebalancing groups

Inspect the group and its members:

```sh
kafkaesque --consumer --member --group <group>
```

Frequent rebalancing can indicate unstable members, timeouts, deployments, or
assignment changes.

### Dead groups

Confirm whether the group should still exist. A dead state is reported by
Kafka and does not cause Kafkaesque to mutate or delete group data.

### Lagging groups

Use the [consumer lag guide](consumer-lag.md) to inspect topic and partition
offsets. The lagging count includes warning and unhealthy groups.

## Collection failures

Kafkaesque does not return partial health. Metadata authorization failures,
missing coordinators, offset failures, and timeouts return errors instead of a
misleading healthy/degraded snapshot.
