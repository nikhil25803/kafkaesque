# Inspect Consumer Lag

Consumer lag is the difference between a partition's latest available offset
and the group's committed offset. A positive value means records are available
that the group has not committed; it does not automatically mean the group is
unhealthy.

## Configure thresholds

```yaml
lag:
  warning_threshold: 100
  unhealthy_threshold: 500
```

Kafkaesque classifies lag as:

- `HEALTHY`: lag is at or below `warning_threshold`.
- `WARNING`: lag is above warning and at or below `unhealthy_threshold`.
- `UNHEALTHY`: lag is above `unhealthy_threshold`.

Equality remains in the lower-severity category.

## Find consumer groups

```sh
kafkaesque --consumers
```

The list shows coordinator, raw Kafka state, active members, and live topic
subscriptions. Inactive groups can be `Empty` with no members.

The web UI normalizes transitional Kafka states such as
`PreparingRebalance`, `CompletingRebalance`, `Assigning`, and `Reconciling` to
`Rebalancing` for filtering. CLI detail retains raw Kafka states.

## Inspect one group

```sh
kafkaesque --consumer --group order-processor
```

The summary includes total lag, maximum partition lag, average across committed
partitions, threshold counts, and per-topic totals.

Include active member assignments:

```sh
kafkaesque --consumer --member --group order-processor
```

Member data is a point-in-time broker response. A group can rebalance again
immediately after the command completes.

## Inspect one topic by partition

```sh
kafkaesque --consumer --group order-processor --topic orders
```

For each committed partition, Kafkaesque displays:

- Committed offset from the group coordinator.
- Log-end offset from the current partition leader.
- Non-negative difference between those offsets.

Partitions with no committed offset are not included in CLI topic detail.

## Use JSON or watch mode

```sh
kafkaesque --consumer --group order-processor --output json
kafkaesque --consumer --group order-processor --watch 10
```

The Consumer Group detail page in the web UI refreshes the complete snapshot
every 10 seconds by default. Users can disable refresh or select 5, 10, 30, or
60 seconds. Failed refreshes keep the last successful data visible.

## Interpret lag safely

Investigate sustained or growing lag alongside:

- Group state and recent rebalances.
- Active member and assignment count.
- Produce rate and processing capacity outside Kafkaesque.
- Partition skew and one unusually slow partition.
- Commit strategy and expected batch duration.

Kafkaesque reports current offsets only. It does not store history or calculate
messages per second, so one snapshot cannot establish whether lag is improving.
