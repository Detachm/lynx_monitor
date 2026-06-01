#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="${ROOT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
ENV_FILE="${ENV_FILE:-${ROOT_DIR}/infra/production.env}"

if [[ -f "${ENV_FILE}" ]]; then
  set -a
  # shellcheck disable=SC1090
  . "${ENV_FILE}"
  set +a
fi

runtime_trade_date="${RUNTIME_TRADE_DATE:-$(TZ=Asia/Shanghai date +%Y%m%d)}"
container_name="${BACKEND_CONTAINER_NAME:-thousand-backend-${runtime_trade_date}}"
host_artifact_dir="${HOST_ARTIFACT_DIR:-${ROOT_DIR}/artifacts}"
host_silver_root="${HOST_SILVER_ROOT:-${ROOT_DIR}/artifacts/production-silver-20260528}"
host_xtquant_sdk_path="${HOST_XTQUANT_SDK_PATH:-/home/hliu/xtbackend/vendor/xtquant_251211_interim-release_cp36m-37m-38-39-310-311-312_linux-gnu_x86_64}"
host_xtquant_data_home="${HOST_XTQUANT_DATA_HOME:-/home/hliu/xtbackend/.runtime/xtquant-live-${runtime_trade_date}}"
xtquant_config_path="${XTQUANT_CONFIG_PATH:-/home/hliu/beast/services/mammoth/historical-ingestion-service/config/bronze_ingest_routine.yaml}"
xtquant_token="${XTQUANT_TOKEN:-}"

if [[ -z "${xtquant_token}" && -f "${xtquant_config_path}" ]]; then
  xtquant_token="$(
    awk -F: '/^[[:space:]]*token:/{gsub(/^[[:space:]]+|[[:space:]]+$/, "", $2); gsub(/[\"\047]/, "", $2); print $2; exit}' "${xtquant_config_path}"
  )"
fi

if [[ -z "${RUNTIME_SYMBOLS:-}" ]]; then
  echo "RUNTIME_SYMBOLS is required in ${ENV_FILE}" >&2
  exit 2
fi

mkdir -p "${host_artifact_dir}" "${host_xtquant_data_home}"

existing_containers="$(docker ps -aq --filter "name=thousand-backend-" || true)"
if [[ -n "${existing_containers}" ]]; then
  docker rm -f ${existing_containers} >/dev/null
fi

docker run -d \
  --name "${container_name}" \
  --restart unless-stopped \
  --network host \
  -e TZ=Asia/Shanghai \
  -e PYTHONPATH=/app/backend \
  -e PYTHONUNBUFFERED=1 \
  -e ARTIFACT_DIR=/app/artifacts \
  -e CONFIG_PATH="/app/artifacts/runtime-config-live-${runtime_trade_date}.json" \
  -e CONFIG_VERIFICATION_PATH="/app/artifacts/runtime-config-verification-live-${runtime_trade_date}.json" \
  -e HEALTH_SNAPSHOT_PATH="/app/artifacts/runtime-health-live-${runtime_trade_date}.json" \
  -e HEALTH_SNAPSHOT_INTERVAL_SECONDS="${HEALTH_SNAPSHOT_INTERVAL_SECONDS:-5}" \
  -e RUNTIME_STATE_ROOT="/app/artifacts/runtime-state-live-${runtime_trade_date}-duckdb-2026-new-listings" \
  -e KAFKA_SPOOL_DIR="/app/artifacts/runtime-state-live-${runtime_trade_date}-duckdb-2026-new-listings/kafka-spool" \
  -e RUNTIME_TRADE_DATE="${runtime_trade_date}" \
  -e RUNTIME_START_AT="${RUNTIME_START_AT:-09:25}" \
  -e WAIT_FOR_MARKET_START="${WAIT_FOR_MARKET_START:-false}" \
  -e SILVER_ROOT=/data/silver \
  -e KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-127.0.0.1:9092}" \
  -e REDIS_URL="${REDIS_URL:-redis://127.0.0.1:6379/0}" \
  -e GATEWAY_HOST="${GATEWAY_HOST:-0.0.0.0}" \
  -e GATEWAY_PORT="${GATEWAY_PORT:-9020}" \
  -e ACTIVE_POOL_TARGET_SIZE="${ACTIVE_POOL_TARGET_SIZE:-200}" \
  -e ACTIVE_POOL_PINNED_MAX_SIZE="${ACTIVE_POOL_PINNED_MAX_SIZE:-100}" \
  -e ACTIVE_POOL_RANK_WINDOW_DAYS="${ACTIVE_POOL_RANK_WINDOW_DAYS:-5}" \
  -e ACTIVE_POOL_RANK_METRIC="${ACTIVE_POOL_RANK_METRIC:-avg_turnover}" \
  -e FRESHNESS_MAX_EVENT_AGE_SECONDS="${FRESHNESS_MAX_EVENT_AGE_SECONDS:-900}" \
  -e KAFKA_POLL_TIMEOUT_MS="${KAFKA_POLL_TIMEOUT_MS:-1}" \
  -e HEALTH_SNAPSHOT_EVERY_TICKS="${HEALTH_SNAPSHOT_EVERY_TICKS:-20}" \
  -e TICK_INTERVAL_SECONDS="${TICK_INTERVAL_SECONDS:-0.1}" \
  -e RAW_QUEUE_MAX_SIZE="${RAW_QUEUE_MAX_SIZE:-100000}" \
  -e MAX_RAW_RECORDS_PER_TICK="${MAX_RAW_RECORDS_PER_TICK:-50}" \
  -e STARTUP_INTRADAY_RECOVERY="${STARTUP_INTRADAY_RECOVERY:-false}" \
  -e PERSIST_REALTIME_EVENTS="${PERSIST_REALTIME_EVENTS:-false}" \
  -e COMMIT_RUNTIME_OWNED_RAW_OFFSETS="${COMMIT_RUNTIME_OWNED_RAW_OFFSETS:-false}" \
  -e HYDRATE_HISTORICAL_ALERTS="${HYDRATE_HISTORICAL_ALERTS:-true}" \
  -e XTQUANT_SDK_PATH=/xtquant/sdk \
  -e XTQUANT_DATA_HOME=/xtquant/data \
  -e XTQUANT_PORT="${XTQUANT_PORT:-58642}" \
  -e XTQUANT_TOKEN="${xtquant_token}" \
  -e RUNTIME_SYMBOLS="${RUNTIME_SYMBOLS}" \
  -v "${host_artifact_dir}:/app/artifacts" \
  -v "${host_silver_root}:/data/silver:ro" \
  -v "${host_xtquant_sdk_path}:/xtquant/sdk:ro" \
  -v "${host_xtquant_data_home}:/xtquant/data" \
  thousand-backend:production
