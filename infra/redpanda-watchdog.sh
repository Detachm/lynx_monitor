#!/usr/bin/env bash
set -euo pipefail

ROOT="${ROOT:-/home/hliu/thousand}"
LOG_DIR="${LOG_DIR:-${ROOT}/artifacts/production-validation/logs}"
CONFIG="${CONFIG:-${ROOT}/artifacts/production-validation/redpanda-local.yaml}"
LOADER="${LOADER:-${ROOT}/artifacts/production-validation/redpanda-full/lib/ld.so}"
LIB_DIR="${LIB_DIR:-${ROOT}/artifacts/production-validation/redpanda-full/lib}"
REDPANDA="${REDPANDA:-${ROOT}/artifacts/production-validation/redpanda-full/libexec/redpanda}"

mkdir -p "${LOG_DIR}"

while true; do
  if ss -ltn | grep -q ':9092'; then
    sleep 30
    continue
  fi
  echo "$(date --iso-8601=seconds) starting redpanda" >> "${LOG_DIR}/redpanda-watchdog.log"
  "${LOADER}" \
    --library-path "${LIB_DIR}" \
    "${REDPANDA}" \
    --redpanda-cfg "${CONFIG}" \
    --smp 1 \
    --memory 1G \
    --overprovisioned \
    >> "${LOG_DIR}/redpanda-watchdog.log" 2>&1 || true
  sleep 5
done
