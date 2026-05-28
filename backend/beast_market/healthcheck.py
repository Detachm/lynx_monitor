from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Sequence


@dataclass(frozen=True)
class RuntimeHealthcheckResult:
    healthy: bool
    blockers: list[str] = field(default_factory=list)


def evaluate_runtime_health_snapshot(
    snapshot: dict[str, Any],
    *,
    now: datetime | None = None,
    max_age_seconds: float = 60.0,
    max_topic_lag: int = 0,
) -> RuntimeHealthcheckResult:
    blockers: list[str] = []
    if snapshot.get("schema_version") != 1:
        blockers.append("schema_version_invalid")

    generated_at = parse_iso_datetime(snapshot.get("generated_at"))
    current_time = now or datetime.now(UTC)
    if generated_at is None:
        blockers.append("generated_at_missing_or_invalid")
    else:
        age_seconds = (current_time - generated_at).total_seconds()
        if age_seconds < -5:
            blockers.append("generated_at_in_future")
        if age_seconds > max_age_seconds:
            blockers.append("runtime_health_stale")

    if snapshot.get("running") is not True:
        blockers.append("runtime_not_running")
    if snapshot.get("runtime_state") != "LIVE":
        blockers.append("runtime_state_not_live")

    supervisor = record_value(snapshot.get("supervisor"))
    if supervisor is None:
        blockers.append("supervisor_missing")
    else:
        if not string_value(supervisor.get("last_tick_at")):
            blockers.append("last_tick_at_missing")
        if supervisor.get("stop_reason") not in (None, ""):
            blockers.append("runtime_stop_reason_present")

    for topic, evidence in (record_value(snapshot.get("topics")) or {}).items():
        if not isinstance(evidence, dict):
            blockers.append(f"topic_{topic}_evidence_invalid")
            continue
        lag = non_negative_int(evidence.get("lag"))
        if lag is None:
            blockers.append(f"topic_{topic}_lag_invalid")
        elif lag > max_topic_lag:
            blockers.append(f"topic_{topic}_lag_exceeded")

    producer = record_value(snapshot.get("producer")) or {}
    if non_negative_int(producer.get("dead_letters"), default=0) > 0:
        blockers.append("producer_dead_letters_present")
    if non_negative_int(producer.get("spooled_records"), default=0) > 0:
        blockers.append("producer_spooled_records_present")

    redis = record_value(snapshot.get("redis")) or {}
    write_stats = record_value(redis.get("write_stats")) or {}
    if non_negative_int(write_stats.get("failures"), default=0) > 0:
        blockers.append("redis_write_failures_present")

    gateway = record_value(snapshot.get("gateway_websocket")) or {}
    if gateway.get("running") is not True:
        blockers.append("gateway_websocket_not_running")

    health = record_value(snapshot.get("health")) or {}
    for component in ("collector", "octopus", "gateway"):
        payload = record_value(health.get(component)) or {}
        if payload.get("process") == "degraded":
            blockers.append(f"{component}_degraded")

    return RuntimeHealthcheckResult(healthy=not blockers, blockers=sorted(set(blockers)))


def evaluate_runtime_health_file(
    path: str | Path,
    *,
    now: datetime | None = None,
    max_age_seconds: float = 60.0,
    max_topic_lag: int = 0,
) -> RuntimeHealthcheckResult:
    snapshot_path = Path(path)
    if not snapshot_path.exists():
        return RuntimeHealthcheckResult(False, ["runtime_health_missing"])
    try:
        decoded = json.loads(snapshot_path.read_text(encoding="utf-8"))
    except Exception:
        return RuntimeHealthcheckResult(False, ["runtime_health_unreadable"])
    if not isinstance(decoded, dict):
        return RuntimeHealthcheckResult(False, ["runtime_health_not_object"])
    return evaluate_runtime_health_snapshot(
        decoded,
        now=now,
        max_age_seconds=max_age_seconds,
        max_topic_lag=max_topic_lag,
    )


def parse_iso_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    candidate = value.strip()
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def record_value(value: Any) -> dict[str, Any] | None:
    return value if isinstance(value, dict) else None


def string_value(value: Any) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def non_negative_int(value: Any, default: int | None = None) -> int | None:
    if isinstance(value, bool):
        return default
    if isinstance(value, int) and value >= 0:
        return value
    return default


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m beast_market.healthcheck")
    parser.add_argument("--path", default="artifacts/runtime-health.json")
    parser.add_argument("--max-age-seconds", type=float, default=60.0)
    parser.add_argument("--max-topic-lag", type=int, default=0)
    args = parser.parse_args(argv)
    result = evaluate_runtime_health_file(
        args.path,
        max_age_seconds=args.max_age_seconds,
        max_topic_lag=args.max_topic_lag,
    )
    if result.healthy:
        print("ok")
        return 0
    print("unhealthy: " + ",".join(result.blockers))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
