#!/usr/bin/env bash
set -euo pipefail

readonly BOOTSTRAP_SERVER="broker-1:19092"
readonly KAFKA_BIN="/opt/kafka/bin"

create_topic() {
  local topic="$1"
  local partitions="$2"

  "${KAFKA_BIN}/kafka-topics.sh" \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor 3
}

topic_has_records() {
  local topic="$1"
  local offsets

  offsets="$("${KAFKA_BIN}/kafka-get-offsets.sh" \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --topic "${topic}")"

  awk -F: '{ total += $3 } END { exit total == 0 }' <<<"${offsets}"
}

seed_topic() {
  local topic="$1"
  local record_count="$2"

  if topic_has_records "${topic}"; then
    echo "Topic ${topic} already contains records; skipping seed"
    return
  fi

  for ((sequence = 1; sequence <= record_count; sequence++)); do
    printf '{"id":"%s-%03d","type":"%s","sequence":%d}\n' \
      "${topic}" "${sequence}" "${topic}" "${sequence}"
  done | "${KAFKA_BIN}/kafka-console-producer.sh" \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --topic "${topic}"
}

group_exists() {
  local group="$1"

  "${KAFKA_BIN}/kafka-consumer-groups.sh" \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --list | grep -Fxq "${group}"
}

create_consumer_group() {
  local group="$1"
  local topic="$2"
  local messages="$3"

  if group_exists "${group}"; then
    echo "Consumer group ${group} already exists; skipping"
    return
  fi

  "${KAFKA_BIN}/kafka-console-consumer.sh" \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --topic "${topic}" \
    --group "${group}" \
    --from-beginning \
    --max-messages "${messages}" \
    --timeout-ms 15000 \
    --consumer-property enable.auto.commit=true \
    --consumer-property auto.commit.interval.ms=100 \
    >/dev/null

  if ! group_exists "${group}"; then
    echo "Failed to establish consumer group ${group}" >&2
    exit 1
  fi
}

create_topic orders 6
create_topic payments 3
create_topic inventory 4
create_topic notifications 1
create_topic audit-events 8

seed_topic orders 12
seed_topic payments 8
seed_topic inventory 8
seed_topic notifications 5
seed_topic audit-events 10

create_consumer_group order-processor orders 4
create_consumer_group payment-worker payments 3
create_consumer_group inventory-sync inventory 8

echo "Kafka fixture environment is ready at localhost:9090"
