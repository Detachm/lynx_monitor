from __future__ import annotations

import json
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha1
from pathlib import Path
from typing import Any

from .performance import evaluate_performance


@dataclass(frozen=True)
class ShadowRunThresholds:
    max_event_count_delta_ratio: float = 0.01
    max_duplicate_ratio: float = 0.001
    max_out_of_order_ratio: float = 0.001
    max_missing_symbol_count: int = 0
    max_latency_delta_ms: float = 250
    max_stale_gap_seconds: float = 60


@dataclass(frozen=True)
class ShadowRunFiles:
    metadata_path: Path
    legacy_events_path: Path
    v2_events_path: Path
    performance_samples_path: Path


class LegacyShadowTelemetryAdapter:
    """Normalizes legacy terminal telemetry before writing shadow-run evidence."""

    def __init__(self, recorder: Any, *, source: str = "legacy") -> None:
        self.recorder = recorder
        self.source = source
        self.seq_by_symbol: dict[str, int] = defaultdict(int)

    def record(self, message: dict[str, Any]) -> dict[str, Any]:
        event = normalize_legacy_terminal_event(
            message,
            source=self.source,
            next_seq=self._next_seq,
        )
        self.recorder.record_legacy_event(event)
        return event

    def _next_seq(self, symbol: str) -> int:
        self.seq_by_symbol[symbol] += 1
        return self.seq_by_symbol[symbol]


class ShadowRunRecorder:
    def __init__(
        self,
        *,
        session_id: str,
        trading_date: str,
        started_at: str,
        thresholds: ShadowRunThresholds | None = None,
    ) -> None:
        self.session_id = session_id
        self.trading_date = trading_date
        self.started_at = started_at
        self.thresholds = thresholds
        self.legacy_events: list[dict[str, Any]] = []
        self.v2_events: list[dict[str, Any]] = []
        self.performance_samples: dict[str, list[float]] = {
            "collector_to_kafka_ms": [],
            "processed_to_gateway_ms": [],
            "gateway_to_frontend_ms": [],
            "subscribe_snapshot_ms": [],
            "frontend_store_update_ms": [],
        }

    def record_legacy_event(self, event: dict[str, Any]) -> None:
        self.legacy_events.append(normalize_shadow_event(event))

    def record_v2_event(self, event: dict[str, Any]) -> None:
        self.v2_events.append(normalize_shadow_event(event))

    def record_performance_sample(self, key: str, value_ms: float) -> None:
        self.performance_samples.setdefault(key, []).append(performance_sample_value(key, value_ms))

    def build_report(self, *, finished_at: str) -> dict[str, Any]:
        return build_shadow_run_report(
            session_id=self.session_id,
            trading_date=self.trading_date,
            started_at=self.started_at,
            finished_at=finished_at,
            legacy_events=self.legacy_events,
            v2_events=self.v2_events,
            performance_samples=self.performance_samples,
            thresholds=self.thresholds,
        )


class FileBackedShadowRunRecorder:
    def __init__(
        self,
        *,
        directory: str | Path,
        session_id: str,
        trading_date: str,
        started_at: str,
        thresholds: ShadowRunThresholds | None = None,
        reset: bool = False,
    ) -> None:
        self.session_id = session_id
        self.trading_date = trading_date
        self.started_at = started_at
        self.thresholds = thresholds
        self.files = shadow_run_file_paths(directory, trading_date=trading_date, session_id=session_id)
        for path in (
            self.files.metadata_path,
            self.files.legacy_events_path,
            self.files.v2_events_path,
            self.files.performance_samples_path,
        ):
            path.parent.mkdir(parents=True, exist_ok=True)
            if reset and path.exists():
                path.unlink()
        write_json(
            self.files.metadata_path,
            {
                "schema_version": 1,
                "session_id": session_id,
                "trading_date": trading_date,
                "started_at": started_at,
                "thresholds": thresholds_to_dict(thresholds),
            },
        )

    def record_legacy_event(self, event: dict[str, Any]) -> None:
        append_json_line(self.files.legacy_events_path, normalize_shadow_event(event))

    def record_v2_event(self, event: dict[str, Any]) -> None:
        append_json_line(self.files.v2_events_path, normalize_shadow_event(event))

    def record_performance_sample(self, key: str, value_ms: float) -> None:
        append_json_line(self.files.performance_samples_path, {"key": key, "value_ms": performance_sample_value(key, value_ms)})

    def build_report(self, *, finished_at: str) -> dict[str, Any]:
        return build_shadow_run_report_from_files(self.files, finished_at=finished_at, thresholds=self.thresholds)


def shadow_run_file_paths(directory: str | Path, *, trading_date: str, session_id: str) -> ShadowRunFiles:
    root = Path(directory)
    prefix = f"{safe_path_part(trading_date)}.{safe_path_part(session_id)}"
    return ShadowRunFiles(
        metadata_path=root / f"{prefix}.metadata.json",
        legacy_events_path=root / f"{prefix}.legacy.ndjson",
        v2_events_path=root / f"{prefix}.v2.ndjson",
        performance_samples_path=root / f"{prefix}.performance.ndjson",
    )


def load_shadow_run_files(files: ShadowRunFiles) -> dict[str, Any]:
    metadata = json.loads(files.metadata_path.read_text(encoding="utf-8"))
    performance_samples: dict[str, list[float]] = {
        "collector_to_kafka_ms": [],
        "processed_to_gateway_ms": [],
        "gateway_to_frontend_ms": [],
        "subscribe_snapshot_ms": [],
        "frontend_store_update_ms": [],
    }
    for line_number, sample in enumerate(load_json_lines(files.performance_samples_path), start=1):
        if not isinstance(sample, dict):
            raise ValueError(f"performance sample line {line_number} must be an object")
        key = sample.get("key")
        if not isinstance(key, str) or not key.strip():
            raise ValueError(f"performance sample line {line_number} missing key")
        performance_samples.setdefault(key, []).append(
            performance_sample_value(key, sample.get("value_ms"), line_number=line_number)
        )
    return {
        "metadata": metadata,
        "legacy_events": load_shadow_event_lines(files.legacy_events_path, stream_name="legacy"),
        "v2_events": load_shadow_event_lines(files.v2_events_path, stream_name="v2"),
        "performance_samples": performance_samples,
    }


def load_shadow_event_lines(path: Path, *, stream_name: str) -> list[dict[str, Any]]:
    events = []
    for line_number, event in enumerate(load_json_lines(path), start=1):
        events.append(shadow_event_value(event, stream_name=stream_name, line_number=line_number))
    return events


def record_v2_runtime_tick(recorder: Any, tick_result: dict[str, Any]) -> None:
    """Record v2 runtime tick telemetry into a shadow-run recorder.

    The production parallel runner can call this after each supervisor tick. Legacy
    telemetry still needs to be fed from the legacy process, but v2 event and timing
    samples now come from the same runtime boundary that drives Gateway v2.
    """

    for message in tick_result.get("terminal_messages") or []:
        recorder.record_v2_event(message)
    for event in tick_result.get("raw_events") or []:
        record_duration_sample(recorder, "collector_to_kafka_ms", event)
    for event in tick_result.get("processed_event_payloads") or []:
        record_duration_sample(recorder, "processed_to_gateway_ms", event)
    for message in tick_result.get("terminal_messages") or []:
        record_duration_sample(recorder, "gateway_to_frontend_ms", message)


def record_duration_sample(recorder: Any, key: str, event: dict[str, Any]) -> None:
    source_ts = event.get("source_ts")
    ingest_ts = event.get("ingest_ts")
    if not source_ts or not ingest_ts:
        return
    value_ms = max(0.0, (parse_ts(str(ingest_ts)) - parse_ts(str(source_ts))) * 1000)
    recorder.record_performance_sample(key, value_ms)


def normalize_legacy_terminal_event(
    message: dict[str, Any],
    *,
    source: str = "legacy",
    next_seq: Any | None = None,
) -> dict[str, Any]:
    symbol = normalize_shadow_symbol(
        first_string(
            message,
            "symbol",
            "code",
            "stock_code",
            "stockCode",
            "ticker",
        )
    )
    if not symbol:
        payload = message.get("payload") if isinstance(message.get("payload"), dict) else {}
        symbol = normalize_shadow_symbol(
            first_string(payload, "symbol", "code", "stock_code", "stockCode", "ticker")
        )
    if not symbol:
        payload = message.get("payload") if isinstance(message.get("payload"), dict) else {}
        tick = payload.get("tick") if isinstance(payload.get("tick"), dict) else {}
        snapshot = payload.get("snapshot") if isinstance(payload.get("snapshot"), dict) else {}
        symbol = normalize_shadow_symbol(
            first_string(tick, "symbol", "code", "stock_code", "stockCode", "ticker")
            or first_string(snapshot, "symbol", "code", "stock_code", "stockCode", "ticker")
        )
    if not symbol:
        raise ValueError("legacy telemetry missing symbol")

    seq = message.get("seq") or message.get("sequence") or message.get("serial")
    if seq is None:
        seq = next_seq(symbol) if callable(next_seq) else 1

    source_ts = legacy_source_ts(message)
    ingest_ts = str(
        message.get("ingest_ts")
        or message.get("received_ts")
        or message.get("receivedAt")
        or message.get("server_ts")
        or source_ts
    )
    event_id = str(
        message.get("event_id")
        or message.get("eventId")
        or message.get("id")
        or legacy_event_id(source, symbol, int(seq), source_ts, message)
    )
    return {
        "event_id": event_id,
        "symbol": symbol,
        "seq": int(seq),
        "source_ts": source_ts,
        "ingest_ts": ingest_ts,
    }


def legacy_source_ts(message: dict[str, Any]) -> str:
    payload = message.get("payload") if isinstance(message.get("payload"), dict) else {}
    tick = payload.get("tick") if isinstance(payload.get("tick"), dict) else {}
    snapshot = payload.get("snapshot") if isinstance(payload.get("snapshot"), dict) else {}
    freshness = payload.get("freshness") if isinstance(payload.get("freshness"), dict) else {}
    value = (
        message.get("source_ts")
        or message.get("timestamp")
        or message.get("time")
        or message.get("updatedAt")
        or payload.get("source_ts")
        or payload.get("timestamp")
        or tick.get("timestamp")
        or snapshot.get("updatedAt")
        or freshness.get("source_ts")
        or freshness.get("updated_at")
    )
    if not value:
        raise ValueError("legacy telemetry missing source timestamp")
    return str(value)


def legacy_event_id(source: str, symbol: str, seq: int, source_ts: str, message: dict[str, Any]) -> str:
    fingerprint = sha1(json.dumps(message, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()[:12]
    return f"legacy-{source}-{symbol}-{seq}-{safe_path_part(source_ts)}-{fingerprint}"


def first_string(mapping: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = mapping.get(key)
        if value is not None and str(value).strip():
            return str(value)
    return ""


def normalize_shadow_symbol(raw_symbol: str) -> str:
    symbol = str(raw_symbol or "").strip().upper()
    if not symbol:
        return ""
    if "." in symbol:
        prefix, suffix = symbol.split(".", 1)
        if prefix.isdigit() and suffix == "HK":
            return f"{prefix.zfill(5)}.HK"
        return symbol
    if symbol.isdigit():
        return f"{symbol.zfill(5)}.HK"
    return symbol


def build_shadow_run_report_from_files(
    files: ShadowRunFiles,
    *,
    finished_at: str,
    thresholds: ShadowRunThresholds | None = None,
) -> dict[str, Any]:
    loaded = load_shadow_run_files(files)
    metadata = loaded["metadata"]
    report = build_shadow_run_report(
        session_id=str(metadata["session_id"]),
        trading_date=str(metadata["trading_date"]),
        started_at=str(metadata["started_at"]),
        finished_at=finished_at,
        legacy_events=loaded["legacy_events"],
        v2_events=loaded["v2_events"],
        performance_samples=loaded["performance_samples"],
        thresholds=thresholds or thresholds_from_dict(metadata.get("thresholds")),
    )
    report["evidence_source"] = file_backed_shadow_run_evidence_source(
        files=files,
        legacy_events=loaded["legacy_events"],
        v2_events=loaded["v2_events"],
        performance_samples=loaded["performance_samples"],
    )
    return report


def file_backed_shadow_run_evidence_source(
    *,
    files: ShadowRunFiles,
    legacy_events: list[dict[str, Any]],
    v2_events: list[dict[str, Any]],
    performance_samples: dict[str, list[float]],
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "kind": "file_backed_shadow_run",
        "files": {
            "metadata": str(files.metadata_path),
            "legacy_events": str(files.legacy_events_path),
            "v2_events": str(files.v2_events_path),
            "performance_samples": str(files.performance_samples_path),
        },
        "legacy_event_count": len(legacy_events),
        "v2_event_count": len(v2_events),
        "performance_sample_counts": {
            key: len(values) for key, values in performance_samples.items()
        },
    }


def compare_event_streams(
    legacy_events: list[dict[str, Any]],
    v2_events: list[dict[str, Any]],
    *,
    thresholds: ShadowRunThresholds | None = None,
) -> dict[str, Any]:
    thresholds = thresholds or ShadowRunThresholds()
    legacy_by_symbol = group_by_symbol(legacy_events)
    v2_by_symbol = group_by_symbol(v2_events)
    symbols = sorted(set(legacy_by_symbol) | set(v2_by_symbol))
    per_symbol = {}
    failed_symbols = []

    for symbol in symbols:
        legacy = legacy_by_symbol.get(symbol, [])
        v2 = v2_by_symbol.get(symbol, [])
        legacy_count = len(legacy)
        v2_count = len(v2)
        count_delta_ratio = 0.0 if legacy_count == 0 else abs(v2_count - legacy_count) / legacy_count
        duplicate_ratio = duplicate_ratio_for(v2)
        out_of_order_ratio = out_of_order_ratio_for(v2)
        latency_delta_ms = max_latency_delta_ms_for(v2)
        stale_gap_seconds = max_stale_gap_seconds_for(v2)
        legacy_source_coverage_seconds = source_coverage_seconds_for(legacy)
        v2_source_coverage_seconds = source_coverage_seconds_for(v2)
        missing = legacy_count > 0 and v2_count == 0
        passed = (
            count_delta_ratio <= thresholds.max_event_count_delta_ratio
            and duplicate_ratio <= thresholds.max_duplicate_ratio
            and out_of_order_ratio <= thresholds.max_out_of_order_ratio
            and latency_delta_ms <= thresholds.max_latency_delta_ms
            and stale_gap_seconds <= thresholds.max_stale_gap_seconds
            and not missing
        )
        per_symbol[symbol] = {
            "legacy_count": legacy_count,
            "v2_count": v2_count,
            "count_delta_ratio": count_delta_ratio,
            "duplicate_ratio": duplicate_ratio,
            "out_of_order_ratio": out_of_order_ratio,
            "max_latency_delta_ms": latency_delta_ms,
            "max_stale_gap_seconds": stale_gap_seconds,
            "legacy_source_coverage_seconds": legacy_source_coverage_seconds,
            "v2_source_coverage_seconds": v2_source_coverage_seconds,
            "missing": missing,
            "passed": passed,
        }
        if not passed:
            failed_symbols.append(symbol)

    missing_symbol_count = sum(1 for result in per_symbol.values() if result["missing"])
    passed = not failed_symbols and missing_symbol_count <= thresholds.max_missing_symbol_count
    return {
        "passed": passed,
        "symbols": per_symbol,
        "failed_symbols": failed_symbols,
        "missing_symbol_count": missing_symbol_count,
        "thresholds": {
            "max_event_count_delta_ratio": thresholds.max_event_count_delta_ratio,
            "max_duplicate_ratio": thresholds.max_duplicate_ratio,
            "max_out_of_order_ratio": thresholds.max_out_of_order_ratio,
            "max_missing_symbol_count": thresholds.max_missing_symbol_count,
            "max_latency_delta_ms": thresholds.max_latency_delta_ms,
            "max_stale_gap_seconds": thresholds.max_stale_gap_seconds,
        },
    }


def normalize_shadow_event(event: dict[str, Any]) -> dict[str, Any]:
    return shadow_event_value({
        "event_id": str(event.get("event_id", "")),
        "symbol": str(event["symbol"]),
        "seq": int(event.get("seq", 0)),
        "source_ts": str(event.get("source_ts") or event.get("timestamp") or ""),
        "ingest_ts": str(event.get("ingest_ts") or event.get("received_ts") or event.get("source_ts") or ""),
    }, stream_name="shadow")


def build_shadow_run_report(
    *,
    session_id: str,
    trading_date: str,
    started_at: str,
    finished_at: str,
    legacy_events: list[dict[str, Any]],
    v2_events: list[dict[str, Any]],
    performance_samples: dict[str, list[float]],
    thresholds: ShadowRunThresholds | None = None,
) -> dict[str, Any]:
    comparison = compare_event_streams(legacy_events, v2_events, thresholds=thresholds)
    performance = evaluate_performance(performance_samples)
    legacy_source_coverage_seconds = source_coverage_seconds_for(legacy_events)
    v2_source_coverage_seconds = source_coverage_seconds_for(v2_events)
    passed = comparison["passed"] and performance["passed"]
    return {
        "schema_version": 1,
        "session_id": session_id,
        "trading_date": trading_date,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": max(0.0, parse_ts(finished_at) - parse_ts(started_at)),
        "passed": passed,
        "comparison": comparison,
        "performance": performance,
        "legacy_event_count": len(legacy_events),
        "v2_event_count": len(v2_events),
        "legacy_source_coverage_seconds": legacy_source_coverage_seconds,
        "v2_source_coverage_seconds": v2_source_coverage_seconds,
    }


def group_by_symbol(events: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for event in events:
        grouped.setdefault(str(event["symbol"]), []).append(event)
    return grouped


def duplicate_ratio_for(events: list[dict[str, Any]]) -> float:
    if not events:
        return 0.0
    event_ids = [str(event.get("event_id", "")) for event in events]
    duplicate_count = len(event_ids) - len(set(event_ids))
    return duplicate_count / len(events)


def out_of_order_ratio_for(events: list[dict[str, Any]]) -> float:
    if len(events) < 2:
        return 0.0
    out_of_order = 0
    previous = int(events[0].get("seq", 0))
    for event in events[1:]:
        current = int(event.get("seq", 0))
        if current < previous:
            out_of_order += 1
        previous = current
    return out_of_order / (len(events) - 1)


def max_latency_delta_ms_for(events: list[dict[str, Any]]) -> float:
    latencies = [
        (parse_ts(str(event["ingest_ts"])) - parse_ts(str(event["source_ts"]))) * 1000
        for event in events
        if event.get("source_ts") and event.get("ingest_ts")
    ]
    return max(latencies) if latencies else 0.0


def max_stale_gap_seconds_for(events: list[dict[str, Any]]) -> float:
    timestamps = sorted(
        parse_ts(str(event["source_ts"]))
        for event in events
        if event.get("source_ts")
    )
    if len(timestamps) < 2:
        return 0.0
    return max(right - left for left, right in zip(timestamps, timestamps[1:]))


def source_coverage_seconds_for(events: list[dict[str, Any]]) -> float:
    timestamps = [
        parse_ts(str(event["source_ts"]))
        for event in events
        if event.get("source_ts")
    ]
    if len(timestamps) < 2:
        return 0.0
    return max(timestamps) - min(timestamps)


def parse_ts(value: str) -> float:
    normalized = value.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized).timestamp()


def append_json_line(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(value, sort_keys=True, ensure_ascii=False) + "\n")


def load_json_lines(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")


def thresholds_to_dict(thresholds: ShadowRunThresholds | None) -> dict[str, float | int] | None:
    if thresholds is None:
        return None
    return {
        "max_event_count_delta_ratio": thresholds.max_event_count_delta_ratio,
        "max_duplicate_ratio": thresholds.max_duplicate_ratio,
        "max_out_of_order_ratio": thresholds.max_out_of_order_ratio,
        "max_missing_symbol_count": thresholds.max_missing_symbol_count,
        "max_latency_delta_ms": thresholds.max_latency_delta_ms,
        "max_stale_gap_seconds": thresholds.max_stale_gap_seconds,
    }


def thresholds_from_dict(value: Any) -> ShadowRunThresholds | None:
    if not isinstance(value, dict):
        return None
    defaults = ShadowRunThresholds()
    return ShadowRunThresholds(
        max_event_count_delta_ratio=bounded_threshold_value(
            value,
            "max_event_count_delta_ratio",
            defaults.max_event_count_delta_ratio,
        ),
        max_duplicate_ratio=bounded_threshold_value(value, "max_duplicate_ratio", defaults.max_duplicate_ratio),
        max_out_of_order_ratio=bounded_threshold_value(value, "max_out_of_order_ratio", defaults.max_out_of_order_ratio),
        max_missing_symbol_count=non_negative_integer_threshold_value(
            value,
            "max_missing_symbol_count",
            defaults.max_missing_symbol_count,
        ),
        max_latency_delta_ms=non_negative_threshold_value(value, "max_latency_delta_ms", defaults.max_latency_delta_ms),
        max_stale_gap_seconds=non_negative_threshold_value(
            value,
            "max_stale_gap_seconds",
            defaults.max_stale_gap_seconds,
        ),
    )


def performance_sample_value(key: str, value: Any, *, line_number: int | None = None) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        location = f" line {line_number}" if line_number is not None else ""
        raise ValueError(f"performance sample{location} {key} value_ms must be a non-negative finite number") from None
    if not math.isfinite(numeric) or numeric < 0:
        location = f" line {line_number}" if line_number is not None else ""
        raise ValueError(f"performance sample{location} {key} value_ms must be a non-negative finite number")
    return numeric


def shadow_event_value(event: Any, *, stream_name: str, line_number: int | None = None) -> dict[str, Any]:
    location = f" line {line_number}" if line_number is not None else ""
    if not isinstance(event, dict):
        raise ValueError(f"{stream_name} event{location} must be an object")
    event_id = event.get("event_id")
    if not isinstance(event_id, str) or not event_id.strip():
        raise ValueError(f"{stream_name} event{location} event_id must be a non-empty string")
    symbol = event.get("symbol")
    if not isinstance(symbol, str) or not valid_shadow_symbol(symbol):
        raise ValueError(f"{stream_name} event{location} symbol must use canonical format 00700.HK")
    seq = event.get("seq")
    if not isinstance(seq, int) or isinstance(seq, bool) or seq < 1:
        raise ValueError(f"{stream_name} event{location} seq must be a positive integer")
    source_ts = event.get("source_ts")
    if not is_iso_datetime(source_ts):
        raise ValueError(f"{stream_name} event{location} source_ts must be an ISO-8601 datetime string")
    ingest_ts = event.get("ingest_ts")
    if not is_iso_datetime(ingest_ts):
        raise ValueError(f"{stream_name} event{location} ingest_ts must be an ISO-8601 datetime string")
    return {
        "event_id": event_id.strip(),
        "symbol": symbol,
        "seq": seq,
        "source_ts": str(source_ts).strip(),
        "ingest_ts": str(ingest_ts).strip(),
    }


def valid_shadow_symbol(symbol: str) -> bool:
    return len(symbol) == 8 and symbol[:5].isdigit() and symbol[5:] == ".HK"


def is_iso_datetime(value: Any) -> bool:
    if not isinstance(value, str) or "T" not in value:
        return False
    try:
        datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return False
    return True


def bounded_threshold_value(value: dict[str, Any], key: str, default: float) -> float:
    numeric = non_negative_threshold_value(value, key, default)
    if numeric > 1:
        raise ValueError(f"shadow-run threshold {key} must be between 0 and 1")
    return numeric


def non_negative_threshold_value(value: dict[str, Any], key: str, default: float) -> float:
    raw = value.get(key, default)
    if isinstance(raw, bool):
        raise ValueError(f"shadow-run threshold {key} must be a non-negative finite number")
    try:
        numeric = float(raw)
    except (TypeError, ValueError):
        raise ValueError(f"shadow-run threshold {key} must be a non-negative finite number") from None
    if not math.isfinite(numeric) or numeric < 0:
        raise ValueError(f"shadow-run threshold {key} must be a non-negative finite number")
    return numeric


def non_negative_integer_threshold_value(value: dict[str, Any], key: str, default: int) -> int:
    raw = value.get(key, default)
    if not isinstance(raw, int) or isinstance(raw, bool) or raw < 0:
        raise ValueError(f"shadow-run threshold {key} must be a non-negative integer")
    return raw


def safe_path_part(value: str) -> str:
    cleaned = "".join(character if character.isalnum() or character in {"-", "_"} else "-" for character in value)
    return cleaned.strip("-") or "unknown"
