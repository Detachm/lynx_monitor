from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
import json
from pathlib import Path
import re
from typing import Any, Protocol

from .contracts import CANONICAL_SYMBOL_PATTERN, PROCESSED_TOPIC, RAW_TOPIC, now_iso


TRADE_DATE_PATTERN = re.compile(r"^\d{8}$")


class EventBus(Protocol):
    def publish(self, topic: str, key: str, value: dict[str, Any]) -> None:
        ...

    def read(self, topic: str) -> list[dict[str, Any]]:
        ...

    def lag(self, topic: str, committed_offset: int = 0) -> int:
        ...

    def commit(self, topic: str, offset: int) -> None:
        ...

    def committed_offset(self, topic: str) -> int:
        ...


class EventConsumer(Protocol):
    def poll(self, topic: str, offset: int) -> list[dict[str, Any]]:
        ...

    def commit(self, topic: str, offset: int) -> None:
        ...

    def committed_offset(self, topic: str) -> int:
        ...


class SnapshotCache(Protocol):
    def set_terminal_snapshot(self, trade_date: str, symbol: str, snapshot: dict[str, Any]) -> None:
        ...

    def set_terminal_state(self, trade_date: str, symbol: str, state: dict[str, Any]) -> None:
        ...

    def get_terminal_snapshot(self, trade_date: str, symbol: str) -> dict[str, Any] | None:
        ...

    def set_holding_history(self, symbol: str, participant_id: str, history: list[dict[str, Any]]) -> None:
        ...


class EventPublishError(RuntimeError):
    pass


@dataclass
class PublishResult:
    topic: str
    key: str
    event_id: str
    acknowledged: bool
    attempts: int
    error: str | None = None


@dataclass
class DeadLetterRecord:
    topic: str
    key: str
    value: dict[str, Any]
    reason: str


@dataclass
class LocalSpool:
    records: list[DeadLetterRecord] = field(default_factory=list)

    def append(self, topic: str, key: str, value: dict[str, Any], reason: str) -> None:
        self.records.append(DeadLetterRecord(topic=topic, key=key, value=value, reason=reason))

    def drain(self) -> list[DeadLetterRecord]:
        drained = list(self.records)
        self.records.clear()
        return drained


class FileBackedSpool(LocalSpool):
    """Append-only JSONL spool for publish failures that must survive restarts."""

    def __init__(self, path: str | Path, quarantine_path: str | Path | None = None) -> None:
        self.path = Path(path)
        self.quarantine_path = Path(quarantine_path) if quarantine_path is not None else self.path.with_suffix(
            self.path.suffix + ".quarantine"
        )
        self.records = []
        self.quarantined_records = 0
        self._load_existing()

    def append(self, topic: str, key: str, value: dict[str, Any], reason: str) -> None:
        super().append(topic, key, value, reason)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "schema_version": 1,
            "spooled_at": now_iso(),
            "topic": topic,
            "key": key,
            "value": value,
            "reason": reason,
        }
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, separators=(",", ":"), ensure_ascii=False) + "\n")

    def drain(self) -> list[DeadLetterRecord]:
        drained = super().drain()
        if self.path.exists():
            self.path.unlink()
        return drained

    def replace(self, records: list[DeadLetterRecord]) -> None:
        self.records = list(records)
        if not self.records:
            if self.path.exists():
                self.path.unlink()
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("w", encoding="utf-8") as handle:
            for record in self.records:
                payload = {
                    "schema_version": 1,
                    "spooled_at": now_iso(),
                    "topic": record.topic,
                    "key": record.key,
                    "value": record.value,
                    "reason": record.reason,
                }
                handle.write(json.dumps(payload, separators=(",", ":"), ensure_ascii=False) + "\n")

    def _load_existing(self) -> None:
        if not self.path.exists():
            return
        with self.path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                raw_line = line.rstrip("\n")
                try:
                    decoded = json.loads(line)
                except json.JSONDecodeError:
                    self._quarantine_line(line_number, raw_line, "invalid_json")
                    continue
                if not isinstance(decoded, dict):
                    self._quarantine_line(line_number, raw_line, "record_not_object")
                    continue
                topic = decoded.get("topic")
                key = decoded.get("key")
                value = decoded.get("value")
                reason = decoded.get("reason")
                if isinstance(topic, str) and isinstance(key, str) and isinstance(value, dict) and isinstance(reason, str):
                    self.records.append(DeadLetterRecord(topic=topic, key=key, value=value, reason=reason))
                else:
                    self._quarantine_line(line_number, raw_line, "record_shape_invalid")

    def _quarantine_line(self, line_number: int, raw_line: str, reason: str) -> None:
        self.quarantined_records += 1
        self.quarantine_path.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "schema_version": 1,
            "quarantined_at": now_iso(),
            "source_path": str(self.path),
            "line_number": line_number,
            "raw_line": raw_line,
            "reason": reason,
        }
        with self.quarantine_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, separators=(",", ":"), ensure_ascii=False) + "\n")


class ReliableEventBus:
    """EventBus decorator that fixes producer ACK/retry/spool semantics.

    The wrapped bus can be Kafka or an in-memory adapter. A failed publish is retried;
    after the retry budget is exhausted the event is written to local spool and DLQ.
    """

    def __init__(self, inner: EventBus, *, retries: int = 3, spool: LocalSpool | None = None) -> None:
        if retries < 0:
            raise ValueError("retries must be >= 0")
        self.inner = inner
        self.retries = retries
        self.spool = spool or LocalSpool()
        self.dead_letters: list[DeadLetterRecord] = []
        self.results: list[PublishResult] = []

    def publish(self, topic: str, key: str, value: dict[str, Any]) -> None:
        attempts = 0
        last_error: Exception | None = None
        while attempts <= self.retries:
            attempts += 1
            try:
                self.inner.publish(topic, key, value)
                self.results.append(
                    PublishResult(
                        topic=topic,
                        key=key,
                        event_id=str(value.get("event_id", "")),
                        acknowledged=True,
                        attempts=attempts,
                    )
                )
                return
            except Exception as error:  # Adapter boundary: preserve failure and never drop silently.
                last_error = error

        reason = str(last_error) if last_error else "unknown publish failure"
        self.spool.append(topic, key, value, reason)
        record = DeadLetterRecord(topic=topic, key=key, value=value, reason=reason)
        self.dead_letters.append(record)
        self.results.append(
            PublishResult(
                topic=topic,
                key=key,
                event_id=str(value.get("event_id", "")),
                acknowledged=False,
                attempts=attempts,
                error=reason,
            )
        )
        raise EventPublishError(f"failed to publish {topic}/{key}: {reason}")

    def read(self, topic: str) -> list[dict[str, Any]]:
        return self.inner.read(topic)

    def lag(self, topic: str, committed_offset: int = 0) -> int:
        return self.inner.lag(topic, committed_offset)

    def commit(self, topic: str, offset: int) -> None:
        commit = getattr(self.inner, "commit", None)
        if callable(commit):
            commit(topic, offset)

    def committed_offset(self, topic: str) -> int:
        committed_offset = getattr(self.inner, "committed_offset", None)
        if callable(committed_offset):
            return int(committed_offset(topic) or 0)
        return 0


class InMemoryEventBus:
    """Kafka-shaped test adapter preserving per-topic records and symbol keys."""

    def __init__(self) -> None:
        self.records: dict[str, list[dict[str, Any]]] = defaultdict(list)

    def publish(self, topic: str, key: str, value: dict[str, Any]) -> None:
        validate_event_bus_publish_inputs(topic, key, value)
        self.records[topic].append({"key": key, "value": value})

    def read(self, topic: str) -> list[dict[str, Any]]:
        return list(self.records[topic])

    def lag(self, topic: str, committed_offset: int = 0) -> int:
        return max(0, len(self.records[topic]) - committed_offset)

    def poll(self, topic: str, offset: int) -> list[dict[str, Any]]:
        return list(self.records[topic][offset:])

    def commit(self, topic: str, offset: int) -> None:
        if not hasattr(self, "_committed_offsets"):
            self._committed_offsets: dict[str, int] = defaultdict(int)
        self._committed_offsets[topic] = offset

    def committed_offset(self, topic: str) -> int:
        if not hasattr(self, "_committed_offsets"):
            self._committed_offsets: dict[str, int] = defaultdict(int)
        return self._committed_offsets[topic]


class FailingEventBus(InMemoryEventBus):
    def __init__(self, *, fail_for_attempts: int) -> None:
        super().__init__()
        self.fail_for_attempts = fail_for_attempts
        self.attempts = 0

    def publish(self, topic: str, key: str, value: dict[str, Any]) -> None:
        self.attempts += 1
        if self.attempts <= self.fail_for_attempts:
            raise RuntimeError("simulated publish failure")
        super().publish(topic, key, value)


class InMemoryRedisSnapshotCache:
    """Redis-shaped cache using the Phase 1 key contract."""

    def __init__(self) -> None:
        self.values: dict[str, Any] = {}
        self.ttls: dict[str, int] = {}

    def set_terminal_snapshot(self, trade_date: str, symbol: str, snapshot: dict[str, Any]) -> None:
        validate_snapshot_key_inputs(trade_date, symbol)
        updated_at = snapshot_updated_at(snapshot)
        self.values[f"terminal:{trade_date}:snapshot:{symbol}"] = terminal_snapshot_record(
            trade_date,
            symbol,
            snapshot,
            updated_at,
        )
        self.values[f"terminal:{trade_date}:minute:{symbol}"] = read_model_record(
            trade_date,
            symbol,
            snapshot,
            snapshot.get("minute_bars", []),
            updated_at,
        )
        self.values[f"terminal:{trade_date}:alerts:{symbol}"] = read_model_record(
            trade_date,
            symbol,
            snapshot,
            snapshot.get("alerts", []),
            updated_at,
        )
        self.values[f"terminal:{trade_date}:queue:{symbol}"] = read_model_record(
            trade_date,
            symbol,
            snapshot,
            snapshot.get("broker_queue", {}),
            updated_at,
        )
        self.values[f"terminal:{trade_date}:state:{symbol}"] = read_model_record(
            trade_date,
            symbol,
            snapshot,
            terminal_state_record(trade_date, symbol, snapshot),
            updated_at,
        )
        self.values[f"ccass:holding:{symbol}"] = read_model_record(
            trade_date,
            symbol,
            snapshot,
            snapshot.get("ccass_holdings", []),
            updated_at,
        )
        for key in (
            f"terminal:{trade_date}:snapshot:{symbol}",
            f"terminal:{trade_date}:minute:{symbol}",
            f"terminal:{trade_date}:alerts:{symbol}",
            f"terminal:{trade_date}:queue:{symbol}",
            f"terminal:{trade_date}:state:{symbol}",
            f"ccass:holding:{symbol}",
        ):
            self.ttls[key] = 1

    def set_terminal_state(self, trade_date: str, symbol: str, state: dict[str, Any]) -> None:
        validate_snapshot_key_inputs(trade_date, symbol)
        key = f"terminal:{trade_date}:state:{symbol}"
        self.values[key] = terminal_runtime_state_record(trade_date, symbol, state)
        self.ttls[key] = 1

    def get_terminal_snapshot(self, trade_date: str, symbol: str) -> dict[str, Any] | None:
        validate_snapshot_key_inputs(trade_date, symbol)
        value = self.values.get(f"terminal:{trade_date}:snapshot:{symbol}")
        return value if isinstance(value, dict) else None

    def set_holding_history(self, symbol: str, participant_id: str, history: list[dict[str, Any]]) -> None:
        validate_snapshot_symbol(symbol)
        if not isinstance(participant_id, str) or not participant_id.strip():
            raise ValueError("participant_id must be a non-empty string")
        key = f"ccass:history:{symbol}:{participant_id}"
        self.values[key] = holding_history_record(symbol, participant_id, history)
        self.ttls[key] = 1


def validate_snapshot_key_inputs(trade_date: str, symbol: str) -> None:
    if not isinstance(trade_date, str) or not TRADE_DATE_PATTERN.fullmatch(trade_date):
        raise ValueError("trade_date must use YYYYMMDD format")
    validate_snapshot_symbol(symbol)


def validate_snapshot_symbol(symbol: str) -> None:
    if not isinstance(symbol, str) or not CANONICAL_SYMBOL_PATTERN.fullmatch(symbol):
        raise ValueError("symbol must use canonical format 00700.HK")


def cache_record(data: Any, updated_at: str) -> dict[str, Any]:
    return {"updated_at": updated_at, "data": data}


def read_model_record(
    trade_date: str,
    symbol: str,
    snapshot: dict[str, Any],
    data: Any,
    updated_at: str | None = None,
) -> dict[str, Any]:
    freshness = snapshot.get("freshness")
    freshness = freshness if isinstance(freshness, dict) else {}
    inner_snapshot = snapshot.get("snapshot")
    inner_snapshot = inner_snapshot if isinstance(inner_snapshot, dict) else {}
    resolved_updated_at = updated_at or freshness.get("updated_at") or snapshot_updated_at(snapshot)
    version = freshness.get("version") or freshness.get("last_event_id") or snapshot.get("last_event_id") or ""
    return {
        "schema_version": 1,
        "symbol": symbol,
        "requested_trade_date": freshness.get("requested_trade_date") or inner_snapshot.get("requestedTradeDate") or trade_date,
        "effective_trade_date": freshness.get("effective_trade_date") or inner_snapshot.get("tradeDate") or trade_date,
        "source_dates": freshness.get("source_dates") or {},
        "updated_at": resolved_updated_at,
        "version": version,
        "last_event_id": version,
        "freshness": dict(freshness),
        "degraded_reasons": freshness.get("degraded_reasons") or [],
        "data": data,
    }


def terminal_snapshot_record(
    trade_date: str,
    symbol: str,
    snapshot: dict[str, Any],
    updated_at: str | None = None,
) -> dict[str, Any]:
    metadata = read_model_record(trade_date, symbol, snapshot, None, updated_at)
    metadata.pop("data", None)
    return {**snapshot, **metadata}


def holding_history_record(symbol: str, participant_id: str, history: list[dict[str, Any]]) -> dict[str, Any]:
    updated_at = now_iso()
    dates = sorted(
        str(row.get("date") or row.get("trade_date") or "")
        for row in history
        if isinstance(row, dict) and str(row.get("date") or row.get("trade_date") or "").strip()
    )
    effective_trade_date = dates[-1] if dates else ""
    snapshot = {
        "freshness": {
            "updated_at": updated_at,
            "requested_trade_date": effective_trade_date,
            "effective_trade_date": effective_trade_date,
            "source_dates": {"ccass_history": effective_trade_date},
            "runtime_state": "WARM",
            "degraded_reasons": [],
        }
    }
    record = read_model_record(effective_trade_date, symbol, snapshot, history, updated_at)
    record["participant_id"] = participant_id
    return record


def terminal_state_record(trade_date: str, symbol: str, snapshot: dict[str, Any]) -> dict[str, Any]:
    freshness = snapshot.get("freshness")
    freshness = freshness if isinstance(freshness, dict) else {}
    inner_snapshot = snapshot.get("snapshot")
    inner_snapshot = inner_snapshot if isinstance(inner_snapshot, dict) else {}
    return {
        "schema_version": 1,
        "symbol": symbol,
        "requested_trade_date": freshness.get("requested_trade_date") or inner_snapshot.get("requestedTradeDate") or trade_date,
        "effective_trade_date": freshness.get("effective_trade_date") or inner_snapshot.get("tradeDate") or trade_date,
        "source_dates": freshness.get("source_dates") or {},
        "updated_at": freshness.get("updated_at") or snapshot_updated_at(snapshot),
        "version": freshness.get("version") or freshness.get("last_event_id") or "",
        "freshness": dict(freshness),
        "degraded_reasons": freshness.get("degraded_reasons") or [],
        "runtime_state": freshness.get("runtime_state") or "WARM",
    }


def terminal_runtime_state_record(trade_date: str, symbol: str, state: dict[str, Any]) -> dict[str, Any]:
    freshness = state.get("freshness")
    freshness = freshness if isinstance(freshness, dict) else {}
    updated_at = str(state.get("updated_at") or freshness.get("updated_at") or now_iso())
    version = str(state.get("version") or state.get("last_event_id") or freshness.get("last_event_id") or "")
    degraded_reasons = state.get("degraded_reasons")
    if not isinstance(degraded_reasons, list):
        degraded_reasons = freshness.get("degraded_reasons") if isinstance(freshness.get("degraded_reasons"), list) else []
    requested_trade_date = str(state.get("requested_trade_date") or freshness.get("requested_trade_date") or trade_date)
    effective_trade_date = str(state.get("effective_trade_date") or freshness.get("effective_trade_date") or requested_trade_date)
    source_dates = state.get("source_dates") or freshness.get("source_dates") or {}
    return {
        **state,
        "schema_version": 1,
        "symbol": symbol,
        "requested_trade_date": requested_trade_date,
        "effective_trade_date": effective_trade_date,
        "source_dates": source_dates if isinstance(source_dates, dict) else {},
        "updated_at": updated_at,
        "version": version,
        "last_event_id": version,
        "freshness": dict(freshness),
        "degraded_reasons": [str(reason) for reason in degraded_reasons],
    }


def snapshot_updated_at(snapshot: dict[str, Any]) -> str:
    freshness = snapshot.get("freshness")
    updated_at = freshness.get("updated_at") if isinstance(freshness, dict) else None
    if isinstance(updated_at, str) and updated_at.strip():
        return updated_at
    inner_snapshot = snapshot.get("snapshot")
    inner_updated_at = inner_snapshot.get("updatedAt") if isinstance(inner_snapshot, dict) else None
    if isinstance(inner_updated_at, str) and inner_updated_at.strip():
        return inner_updated_at
    return now_iso()


def validate_event_bus_publish_inputs(topic: str, key: str, value: dict[str, Any]) -> None:
    if topic not in {RAW_TOPIC, PROCESSED_TOPIC}:
        return
    if not isinstance(value, dict):
        raise ValueError("event value must be an object")
    if not isinstance(key, str) or not CANONICAL_SYMBOL_PATTERN.fullmatch(key):
        raise ValueError("Kafka event key must use canonical symbol format 00700.HK")
    symbol = value.get("symbol")
    if not isinstance(symbol, str) or not CANONICAL_SYMBOL_PATTERN.fullmatch(symbol):
        raise ValueError("Kafka event value symbol must use canonical format 00700.HK")
    if key != symbol:
        raise ValueError("Kafka event key must match event symbol")


def validate_event_bus_record_inputs(topic: str, record: dict[str, Any]) -> None:
    if topic not in {RAW_TOPIC, PROCESSED_TOPIC}:
        return
    if not isinstance(record, dict):
        raise ValueError("Kafka record must be an object")
    validate_event_bus_publish_inputs(topic, record.get("key"), record.get("value"))


class BoundedRawEventQueue:
    """Small queue abstraction for xtquant callback isolation."""

    def __init__(self, max_size: int) -> None:
        if max_size < 1:
            raise ValueError("max_size must be >= 1")
        self.max_size = max_size
        self.items: deque[dict[str, Any]] = deque()
        self.coalesced_items: dict[str, dict[str, Any]] = {}
        self.dropped: list[dict[str, Any]] = []

    def push(self, event: dict[str, Any]) -> bool:
        coalesce_key = raw_event_queue_coalesce_key(event)
        if coalesce_key and coalesce_key in self.coalesced_items:
            self.coalesced_items[coalesce_key] = event
            return True
        if len(self.items) >= self.max_size:
            self.dropped.append(event)
            return False
        if coalesce_key:
            self.coalesced_items[coalesce_key] = event
            self.items.append({"__coalesce_key__": coalesce_key})
        else:
            self.items.append(event)
        return True

    def pop(self) -> dict[str, Any] | None:
        while self.items:
            event = self.items.popleft()
            coalesce_key = event.get("__coalesce_key__")
            if isinstance(coalesce_key, str):
                coalesced = self.coalesced_items.pop(coalesce_key, None)
                if coalesced is None:
                    continue
                return coalesced
            return event
        return None

    @property
    def backlog(self) -> int:
        return len(self.items)


def raw_event_queue_coalesce_key(event: dict[str, Any]) -> str | None:
    symbol = str(event.get("symbol") or event.get("code") or "").strip().upper()
    period = str(event.get("period") or event.get("Period") or "").strip().lower()
    kind = str(event.get("kind") or "").strip().lower()
    if not symbol:
        return None
    if "." not in symbol and symbol.isdigit():
        symbol = f"{symbol.zfill(5)}.HK"
    coalesced_periods = {
        "hkbrokerqueueex",
        "broker_queue",
        "brokerqueue",
        "brokerqueue2",
        "l2thousand",
        "l2thousand_queue",
        "l2_order_book",
    }
    if period in coalesced_periods:
        return f"{symbol}|{period}"
    if kind in {"broker_queue", "l2_order_book"}:
        return f"{symbol}|{kind}"
    return None
