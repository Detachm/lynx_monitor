#!/usr/bin/env bash
set -euo pipefail

BROKERS="${KAFKA_BOOTSTRAP_SERVERS:-${BROKERS:-redpanda:9092}}"
RPK_BIN="${RPK_BIN:-rpk}"
RETENTION_MS="${KAFKA_RETENTION_MS:-86400000}"
RAW_RETENTION_BYTES="${KAFKA_RAW_RETENTION_BYTES:-5368709120}"
PROCESSED_RETENTION_BYTES="${KAFKA_PROCESSED_RETENTION_BYTES:-5368709120}"
RAW_TOPIC="${KAFKA_RAW_TOPIC:-raw_market_events_v1}"
PROCESSED_TOPIC="${KAFKA_PROCESSED_TOPIC:-processed_market_events_v1}"

configure_topic() {
  local topic="$1"
  local retention_bytes="$2"

  "${RPK_BIN}" topic create "${topic}" \
    -X "brokers=${BROKERS}" \
    --topic-config "retention.ms=${RETENTION_MS}" \
    --topic-config "retention.bytes=${retention_bytes}" \
    >/dev/null 2>&1 || true

  "${RPK_BIN}" topic alter-config "${topic}" \
    -X "brokers=${BROKERS}" \
    --set "retention.ms=${RETENTION_MS}" \
    --set "retention.bytes=${retention_bytes}" \
    >/dev/null 2>&1 || {
      "${RPK_BIN}" topic alter-config "${topic}" -X "brokers=${BROKERS}" "retention.ms" "${RETENTION_MS}" >/dev/null 2>&1 || true
      "${RPK_BIN}" topic alter-config "${topic}" -X "brokers=${BROKERS}" "retention.bytes" "${retention_bytes}" >/dev/null 2>&1 || true
    }
}

configure_topic "${RAW_TOPIC}" "${RAW_RETENTION_BYTES}"
configure_topic "${PROCESSED_TOPIC}" "${PROCESSED_RETENTION_BYTES}"

echo "configured Redpanda retention for ${RAW_TOPIC},${PROCESSED_TOPIC} on ${BROKERS}"
