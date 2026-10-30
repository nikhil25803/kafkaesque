#!/usr/bin/env bash
set -euo pipefail

MODE="simulate"
if [[ "${1:-}" == "--resolve" ]]; then
  MODE="resolve"
  shift
fi
readonly MODE
readonly GROUP="${1:-}"
readonly TOPIC="${2:-}"
readonly COUNT="${3:-10}"
readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly COMPOSE_FILE="${SCRIPT_DIR}/compose.yaml"
readonly BOOTSTRAP_SERVER="broker-1:19092"
readonly KAFKA_BIN="/opt/kafka/bin"

if [[ -z "${GROUP}" || -z "${TOPIC}" ]]; then
  echo "Usage: $0 [--resolve] <consumer-group> <topic> [count]" >&2
  exit 2
fi
if [[ "${MODE}" == "simulate" && ! "${COUNT}" =~ ^[1-9][0-9]*$ ]]; then
  echo "Count must be a positive integer." >&2
  exit 2
fi

compose() {
  docker compose -f "${COMPOSE_FILE}" "$@"
}

kafka() {
  compose exec -T broker-1 "${KAFKA_BIN}/$1" "${@:2}"
}

if ! kafka kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVER}" --describe --topic "${TOPIC}" >/dev/null 2>&1; then
  echo "Kafka topic ${TOPIC} is unavailable; run 'make kafka-up' first." >&2
  exit 1
fi

describe_group() {
  kafka kafka-consumer-groups.sh --bootstrap-server "${BOOTSTRAP_SERVER}" --describe --group "${GROUP}" 2>/dev/null
}

if ! describe_group | awk -v topic="${TOPIC}" '$2 == topic { found = 1 } END { exit !found }'; then
  echo "Consumer group ${GROUP} has no committed offsets for topic ${TOPIC}." >&2
  exit 1
fi

if compose --profile simulator ps --status running --services | grep -Fxq simulator; then
  echo "Stopping the activity simulator."
  compose --profile simulator rm --stop --force simulator >/dev/null
fi

total_lag() {
  describe_group | awk -v topic="${TOPIC}" '
    $2 == topic && $6 ~ /^[0-9]+$/ { total += $6; found = 1 }
    END { if (!found) exit 1; print total }
  '
}

if ! BEFORE_LAG="$(total_lag)"; then
  echo "Unable to read lag for ${GROUP}/${TOPIC}." >&2
  exit 1
fi
readonly BEFORE_LAG

if [[ "${MODE}" == "resolve" ]]; then
  if ((BEFORE_LAG == 0)); then
    echo "Lag for ${GROUP}/${TOPIC} is already zero."
    exit 0
  fi

  echo "Consuming ${BEFORE_LAG} pending records for ${GROUP}/${TOPIC}."
  kafka kafka-console-consumer.sh \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --group "${GROUP}" \
    --topic "${TOPIC}" \
    --timeout-ms 5000 \
    --command-property enable.auto.commit=true \
    --command-property auto.commit.interval.ms=1000 \
    >/dev/null 2>&1 || true

  AFTER_LAG="${BEFORE_LAG}"
  for _ in {1..10}; do
    if AFTER_LAG="$(total_lag)" && ((AFTER_LAG == 0)); then
      echo "Lag for ${GROUP}/${TOPIC} decreased from ${BEFORE_LAG} to 0."
      echo "The activity simulator remains stopped so Kafkaesque can observe the resolution."
      exit 0
    fi
    sleep 1
  done
  echo "Expected lag to reach zero, but ${GROUP}/${TOPIC} still has lag ${AFTER_LAG}." >&2
  exit 1
fi

for ((sequence = 1; sequence <= COUNT; sequence++)); do
  printf '{"source":"kafka-lag-simulate","sequence":%d}\n' "${sequence}"
done | kafka kafka-console-producer.sh --bootstrap-server "${BOOTSTRAP_SERVER}" --topic "${TOPIC}" >/dev/null

if ! AFTER_LAG="$(total_lag)"; then
  echo "Unable to verify lag for ${GROUP}/${TOPIC}." >&2
  exit 1
fi
readonly AFTER_LAG
readonly INCREASE=$((AFTER_LAG - BEFORE_LAG))
if ((INCREASE != COUNT)); then
  echo "Expected lag to increase by ${COUNT}, but it changed from ${BEFORE_LAG} to ${AFTER_LAG}." >&2
  echo "Check for another active consumer or producer using ${GROUP}/${TOPIC}." >&2
  exit 1
fi

echo "Lag for ${GROUP}/${TOPIC} increased from ${BEFORE_LAG} to ${AFTER_LAG}."
echo "Run 'make kafka-lag-resolve GROUP=${GROUP} TOPIC=${TOPIC}' after the firing alert."
