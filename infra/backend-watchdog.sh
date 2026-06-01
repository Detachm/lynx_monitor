#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="${ROOT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
BACKEND_CONTAINER_NAME="${BACKEND_CONTAINER_NAME:-thousand-backend-$(TZ=Asia/Shanghai date +%Y%m%d)}"
HEALTH_SNAPSHOT_PATH="${HEALTH_SNAPSHOT_PATH:-${ROOT_DIR}/artifacts/runtime-health-live-$(TZ=Asia/Shanghai date +%Y%m%d).json}"
MAX_AGE_SECONDS="${RUNTIME_HEALTH_MAX_AGE_SECONDS:-30}"
CHECK_INTERVAL_SECONDS="${WATCHDOG_CHECK_INTERVAL_SECONDS:-15}"
FAILURE_THRESHOLD="${WATCHDOG_FAILURE_THRESHOLD:-3}"

failures=0

while true; do
  if PYTHONPATH="${ROOT_DIR}/backend" python -m beast_market.healthcheck \
    --path "${HEALTH_SNAPSHOT_PATH}" \
    --max-age-seconds "${MAX_AGE_SECONDS}" >/dev/null 2>&1; then
    failures=0
  else
    failures=$((failures + 1))
    echo "$(date --iso-8601=seconds) backend health failed (${failures}/${FAILURE_THRESHOLD}) for ${BACKEND_CONTAINER_NAME}"
  fi

  if (( failures >= FAILURE_THRESHOLD )); then
    echo "$(date --iso-8601=seconds) restarting ${BACKEND_CONTAINER_NAME}"
    docker restart "${BACKEND_CONTAINER_NAME}" >/dev/null
    failures=0
  fi

  sleep "${CHECK_INTERVAL_SECONDS}"
done
