#!/usr/bin/env bash
set -euo pipefail

readonly BOOTSTRAP_SERVER="broker-1:19092"
readonly KAFKA_BIN="/opt/kafka/bin"
readonly PRODUCE_INTERVAL_SECONDS=2
readonly REQUIRED_TOPICS=(orders payments inventory notifications audit-events)

child_pids=()

cleanup() {
  trap - EXIT INT TERM

  if ((${#child_pids[@]} > 0)); then
    kill "${child_pids[@]}" 2>/dev/null || true
    wait "${child_pids[@]}" 2>/dev/null || true
  fi
}

trap cleanup EXIT
trap 'exit 0' INT TERM

if ! "${KAFKA_BIN}/kafka-topics.sh" \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --list >/dev/null 2>&1; then
  echo "Kafka fixture is not available; run 'make kafka-up' first." >&2
  exit 1
fi

for topic in "${REQUIRED_TOPICS[@]}"; do
  if ! "${KAFKA_BIN}/kafka-topics.sh" \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --describe \
    --topic "${topic}" >/dev/null 2>&1; then
    echo "Kafka topic ${topic} is missing; run 'make kafka-up' first." >&2
    exit 1
  fi
done

start_consumer() {
  local group="$1"
  local selector="$2"
  local value="$3"

  "${KAFKA_BIN}/kafka-console-consumer.sh" \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --group "${group}" \
    "${selector}" "${value}" \
    --command-property auto.offset.reset=latest \
    >/dev/null &
  child_pids+=("$!")
}

produce_records() {
  local topic="$1"
  local sequence=1

  while true; do
    printf '{"id":"sim-%s-%06d","type":"%s","sequence":%d,"produced_at":%(%Y-%m-%dT%H:%M:%SZ)T}\n' \
      "${topic}" "${sequence}" "${topic}" "${sequence}" -1
    sequence=$((sequence + 1))
    sleep "${PRODUCE_INTERVAL_SECONDS}"
  done
}

start_producer() {
  local topic="$1"

  produce_records "${topic}" | "${KAFKA_BIN}/kafka-console-producer.sh" \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --topic "${topic}" &
  child_pids+=("$!")
}

start_consumer order-processor --topic orders
start_consumer order-processor --topic orders
start_consumer payment-worker --topic payments
start_consumer inventory-sync --topic inventory
start_consumer notification-dispatcher --include '^(notifications|audit-events)$'

for topic in "${REQUIRED_TOPICS[@]}"; do
  start_producer "${topic}"
done

touch /tmp/kafkaesque-simulator-ready
echo "Kafka activity simulator is running"

if wait -n "${child_pids[@]}"; then
  echo "A simulator worker stopped unexpectedly." >&2
else
  echo "A simulator worker failed." >&2
fi
exit 1
