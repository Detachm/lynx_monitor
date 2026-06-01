#!/usr/bin/env bash
set -euo pipefail

ARTIFACT_DIR="${ARTIFACT_DIR:-/app/artifacts}"
CONFIG_PATH="${CONFIG_PATH:-${ARTIFACT_DIR}/runtime-config.json}"
CONFIG_VERIFICATION_PATH="${CONFIG_VERIFICATION_PATH:-${ARTIFACT_DIR}/runtime-config-verification.json}"
HEALTH_SNAPSHOT_PATH="${HEALTH_SNAPSHOT_PATH:-${ARTIFACT_DIR}/runtime-health.json}"
RUNTIME_STATE_ROOT="${RUNTIME_STATE_ROOT:-${ARTIFACT_DIR}/runtime-state}"
KAFKA_SPOOL_DIR="${KAFKA_SPOOL_DIR:-${RUNTIME_STATE_ROOT}/kafka-spool}"
SILVER_ROOT="${SILVER_ROOT:-/data/silver}"
RUNTIME_TRADE_DATE="${RUNTIME_TRADE_DATE:-$(TZ=Asia/Shanghai date +%Y%m%d)}"
RUNTIME_START_AT="${RUNTIME_START_AT:-09:25}"
WAIT_FOR_MARKET_START="${WAIT_FOR_MARKET_START:-true}"
export ARTIFACT_DIR CONFIG_PATH CONFIG_VERIFICATION_PATH HEALTH_SNAPSHOT_PATH
export RUNTIME_STATE_ROOT KAFKA_SPOOL_DIR SILVER_ROOT RUNTIME_TRADE_DATE
export RUNTIME_START_AT WAIT_FOR_MARKET_START

mkdir -p "${ARTIFACT_DIR}" "${RUNTIME_STATE_ROOT}" "${KAFKA_SPOOL_DIR}"

python - <<'PY'
import os
import shutil
from pathlib import Path

def int_env(name, default):
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default

artifact_dir = Path(os.environ["ARTIFACT_DIR"])
min_free_bytes = int_env("MIN_ARTIFACT_FREE_BYTES", 20 * 1024 * 1024 * 1024)
warn_free_bytes = int_env("WARN_ARTIFACT_FREE_BYTES", 100 * 1024 * 1024 * 1024)
usage = shutil.disk_usage(artifact_dir)
if usage.free < min_free_bytes:
    raise SystemExit(
        f"artifact disk free bytes {usage.free} is below MIN_ARTIFACT_FREE_BYTES={min_free_bytes}"
    )
if usage.free < warn_free_bytes:
    print(
        f"warning: artifact disk free bytes {usage.free} is below WARN_ARTIFACT_FREE_BYTES={warn_free_bytes}",
        flush=True,
    )
PY

if [[ "${WAIT_FOR_MARKET_START}" == "true" || "${WAIT_FOR_MARKET_START}" == "1" ]]; then
  python - <<'PY'
import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo

start_at = os.environ.get("RUNTIME_START_AT", "09:25")
trade_date = os.environ.get("RUNTIME_TRADE_DATE", "")
hour, minute = [int(part) for part in start_at.split(":", 1)]
tz = ZoneInfo("Asia/Shanghai")
now = datetime.now(tz)
target_day = datetime.strptime(trade_date, "%Y%m%d").replace(tzinfo=tz)
target = target_day.replace(hour=hour, minute=minute, second=0, microsecond=0)
if now < target:
    seconds = (target - now).total_seconds()
    print(f"waiting {seconds:.0f}s until runtime start {target.isoformat()}", flush=True)
    time.sleep(seconds)
PY
fi

python - <<'PY'
import json
import os
from pathlib import Path

def int_env(name, default):
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default

def float_env(name, default):
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return default

def bool_env(name, default):
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y"}

config = {
    "schema_version": 1,
    "trade_date": os.environ["RUNTIME_TRADE_DATE"],
    "silver_root": os.environ["SILVER_ROOT"],
    "runtime_state_root": os.environ["RUNTIME_STATE_ROOT"],
    "gateway": {
        "host": os.environ.get("GATEWAY_HOST", "0.0.0.0"),
        "port": int_env("GATEWAY_PORT", 9020),
        "path": os.environ.get("GATEWAY_PATH", "/ws"),
    },
    "kafka": {
        "raw_topic": os.environ.get("KAFKA_RAW_TOPIC", "raw_market_events_v1"),
        "processed_topic": os.environ.get("KAFKA_PROCESSED_TOPIC", "processed_market_events_v1"),
        "consumer_group": os.environ.get("KAFKA_CONSUMER_GROUP", "beast-terminal-v2"),
        "poll_timeout_ms": int_env("KAFKA_POLL_TIMEOUT_MS", 1),
        "auto_offset_reset": os.environ.get("KAFKA_AUTO_OFFSET_RESET", "latest"),
    },
    "redis": {
        "terminal_ttl_seconds": int_env("REDIS_TERMINAL_TTL_SECONDS", 28800),
        "history_ttl_seconds": int_env("REDIS_HISTORY_TTL_SECONDS", 604800),
    },
    "runtime": {
        "runtime_state_root": os.environ["RUNTIME_STATE_ROOT"],
        "kafka_spool_dir": os.environ["KAFKA_SPOOL_DIR"],
        "raw_queue_max_size": int_env("RAW_QUEUE_MAX_SIZE", 100000),
        "client_queue_size": int_env("CLIENT_QUEUE_SIZE", 100),
        "kafka_retries": int_env("KAFKA_RETRIES", 3),
        "symbol_eviction_grace_seconds": float_env("SYMBOL_EVICTION_GRACE_SECONDS", 300),
        "max_concurrent_hydrations": int_env("MAX_CONCURRENT_HYDRATIONS", 8),
        "max_raw_records_per_tick": int_env("MAX_RAW_RECORDS_PER_TICK", 50),
        "startup_intraday_recovery": bool_env("STARTUP_INTRADAY_RECOVERY", False),
        "persist_realtime_events": bool_env("PERSIST_REALTIME_EVENTS", False),
        "commit_runtime_owned_raw_offsets": bool_env("COMMIT_RUNTIME_OWNED_RAW_OFFSETS", False),
        "big_trade_volume_baseline_ratio": float_env("BIG_TRADE_VOLUME_BASELINE_RATIO", 0.0005),
        "install_signal_handlers": True,
    },
    "active_pool": {
        "target_size": int_env("ACTIVE_POOL_TARGET_SIZE", 200),
        "pinned_max_size": int_env("ACTIVE_POOL_PINNED_MAX_SIZE", 100),
        "rank_window_days": int_env("ACTIVE_POOL_RANK_WINDOW_DAYS", 5),
        "rank_metric": os.environ.get("ACTIVE_POOL_RANK_METRIC", "avg_turnover"),
        "exclude_instrument_types": [
            value.strip()
            for value in os.environ.get("ACTIVE_POOL_EXCLUDE_INSTRUMENT_TYPES", "ETF,WARRANT,CBBC,FUND,BOND,DERIVATIVE").split(",")
            if value.strip()
        ],
        "eviction_grace_seconds": float_env("ACTIVE_POOL_EVICTION_GRACE_SECONDS", 300),
    },
    "freshness": {
        "max_event_age_seconds": float_env("FRESHNESS_MAX_EVENT_AGE_SECONDS", 60),
        "max_queue_backlog": int_env("FRESHNESS_MAX_QUEUE_BACKLOG", 1000),
    },
    "production_clients": {
        "duckdb_connection": True,
        "kafka_producer": True,
        "kafka_consumer": True,
        "redis_client": True,
        "market_data_client": os.environ.get("DISABLE_XTQUANT", "false").lower() not in {"1", "true", "yes"},
    },
}
path = Path(os.environ["CONFIG_PATH"])
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(json.dumps(config, indent=2, sort_keys=True) + "\n", encoding="utf-8")
print(f"wrote runtime config: {path}", flush=True)
PY

python -m beast_market.ops_cli verify-runtime-config \
  --config-path "${CONFIG_PATH}" \
  --output-path "${CONFIG_VERIFICATION_PATH}"

args=(
  -m beast_market.production_runtime
  --config-path "${CONFIG_PATH}"
  --kafka-bootstrap-servers "${KAFKA_BOOTSTRAP_SERVERS:-redpanda:9092}"
  --redis-url "${REDIS_URL:-redis://redis:6379/0}"
  --kafka-degraded-spool-dir "${KAFKA_SPOOL_DIR}"
  --redis-maxmemory "${REDIS_MAXMEMORY:-1gb}"
  --redis-maxmemory-policy "${REDIS_MAXMEMORY_POLICY:-volatile-ttl}"
  --gateway-host "${GATEWAY_HOST:-0.0.0.0}"
  --gateway-port "${GATEWAY_PORT:-9020}"
  --health-snapshot-path "${HEALTH_SNAPSHOT_PATH}"
  --health-snapshot-every-ticks "${HEALTH_SNAPSHOT_EVERY_TICKS:-4}"
  --tick-interval-seconds "${TICK_INTERVAL_SECONDS:-0.25}"
  --xtquant-sdk-path "${XTQUANT_SDK_PATH:-/xtquant/sdk}"
  --xtquant-data-home "${XTQUANT_DATA_HOME:-/xtquant/data}"
  --xtquant-port "${XTQUANT_PORT:-58628}"
)

if [[ -n "${HEALTH_SNAPSHOT_INTERVAL_SECONDS:-}" ]]; then
  args+=(--health-snapshot-interval-seconds "${HEALTH_SNAPSHOT_INTERVAL_SECONDS}")
fi
if [[ "${ALLOW_KAFKA_DEGRADED:-true}" == "true" || "${ALLOW_KAFKA_DEGRADED:-true}" == "1" ]]; then
  args+=(--allow-kafka-degraded)
fi
if [[ -n "${RUNTIME_SYMBOLS:-}" ]]; then
  args+=(--symbols "${RUNTIME_SYMBOLS}")
fi
if [[ "${DISABLE_XTQUANT:-false}" == "true" || "${DISABLE_XTQUANT:-false}" == "1" ]]; then
  args+=(--disable-xtquant)
fi
if [[ "${DISABLE_DUCKDB:-false}" == "true" || "${DISABLE_DUCKDB:-false}" == "1" ]]; then
  args+=(--disable-duckdb)
fi
if [[ "${HYDRATE_HISTORICAL_ALERTS:-false}" == "true" || "${HYDRATE_HISTORICAL_ALERTS:-false}" == "1" ]]; then
  args+=(--hydrate-historical-alerts)
fi

exec python "${args[@]}"
