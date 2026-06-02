from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
import threading
import time
from typing import Any, Callable, Protocol

from .contracts import make_terminal_message, now_iso, validate_processed_market_event
from .trading_session import is_regular_hk_trading_minute

HK_TZ = timezone(timedelta(hours=8))


class SymbolRuntimeState(str, Enum):
    COLD = "COLD"
    HYDRATING = "HYDRATING"
    WARM = "WARM"
    LIVE = "LIVE"
    DEGRADED = "DEGRADED"
    EVICTING = "EVICTING"


class SymbolGateway(Protocol):
    def subscribe(self, symbol: str, trade_date: str) -> dict[str, Any]:
        ...

    def snapshot_message(self, symbol: str, payload: dict[str, Any]) -> dict[str, Any]:
        ...


HydrateSymbol = Callable[[str], dict[str, Any] | None]
SymbolHook = Callable[[str], bool | None]
RuntimeStateSink = Callable[[str, dict[str, Any]], None]
RuntimeSnapshotSink = Callable[[str, dict[str, Any]], None]
RawEventProcessor = Callable[[dict[str, Any], str, dict[str, Any]], list[dict[str, Any]]]
Clock = Callable[[], float]
MAX_MINUTE_BARS = 420
MAX_ALERTS = 500


@dataclass(frozen=True)
class HydrationKey:
    symbol: str
    data_type: str
    effective_trade_date: str


class HistoricalHydrationService:
    """Singleflight boundary keyed by symbol, data type, and effective date."""

    def __init__(self) -> None:
        self._active: set[HydrationKey] = set()
        self._condition = threading.Condition(threading.RLock())

    def hydrate(self, key: HydrationKey, loader: Callable[[], dict[str, Any] | None]) -> dict[str, Any] | None:
        with self._condition:
            while key in self._active:
                self._condition.wait()
            self._active.add(key)
        try:
            return loader()
        finally:
            with self._condition:
                self._active.discard(key)
                self._condition.notify_all()

    def snapshot(self) -> dict[str, Any]:
        with self._condition:
            keys = sorted(self._active, key=lambda item: (item.symbol, item.data_type, item.effective_trade_date))
            return {"active_count": len(keys), "active_keys": [key.__dict__ for key in keys]}


@dataclass
class SymbolRuntime:
    symbol: str
    state: SymbolRuntimeState = SymbolRuntimeState.COLD
    subscribers: set[str] = field(default_factory=set)
    hydrate_count: int = 0
    hydration_failures: int = 0
    last_hydration_latency_ms: float = 0.0
    max_hydration_latency_ms: float = 0.0
    last_hydration_error: str = ""
    degraded_reasons: list[str] = field(default_factory=list)
    eviction_started_at: float | None = None
    realtime_attached: bool = False
    snapshot_payload: dict[str, Any] | None = None
    delta_emitted: int = 0
    mailbox_depth: int = 0

    @property
    def ref_count(self) -> int:
        return len(self.subscribers)


class SymbolRuntimeManager:
    """Owns per-symbol subscription lifecycle for the terminal gateway.

    The first implementation intentionally keeps business computation in the
    existing Octopus/Gateway path. This class establishes the state ownership,
    ref-count, and singleflight hydrate boundary required by the roadmap.
    """

    def __init__(
        self,
        gateway: SymbolGateway,
        *,
        trade_date: str,
        hydrate_symbol: HydrateSymbol | None = None,
        attach_realtime: SymbolHook | None = None,
        release_symbol: SymbolHook | None = None,
        state_sink: RuntimeStateSink | None = None,
        snapshot_sink: RuntimeSnapshotSink | None = None,
        raw_event_processor: RawEventProcessor | None = None,
        hydration_service: HistoricalHydrationService | None = None,
        active_pool_manager: Any | None = None,
        eviction_grace_seconds: float = 300,
        max_concurrent_hydrations: int = 8,
        now: Clock | None = None,
    ) -> None:
        if max_concurrent_hydrations < 1:
            raise ValueError("max_concurrent_hydrations must be >= 1")
        self.gateway = gateway
        self.trade_date = trade_date
        self.hydrate_symbol = hydrate_symbol or (lambda symbol: None)
        self.attach_realtime = attach_realtime
        self.release_symbol = release_symbol
        self.state_sink = state_sink
        self.snapshot_sink = snapshot_sink
        self.raw_event_processor = raw_event_processor
        self.hydration_service = hydration_service or HistoricalHydrationService()
        self.active_pool_manager = active_pool_manager
        self.eviction_grace_seconds = eviction_grace_seconds
        self.max_concurrent_hydrations = max_concurrent_hydrations
        self.now = now or time.monotonic
        self.runtimes: dict[str, SymbolRuntime] = {}
        self._hydrating: set[HydrationKey] = set()
        self._active_hydrations = 0
        self.seq_by_symbol: dict[str, int] = defaultdict(int)
        self.raw_events_applied = 0
        self.runtime_delta_emitted = 0
        self.runtime_delta_delivered = 0
        self.capacity_rejections = 0
        self.state_sink_failures = 0
        self.last_state_sink_error = ""
        self.state_sink_failure_symbols: set[str] = set()
        self.snapshot_sink_failures = 0
        self.last_snapshot_sink_error = ""
        self.snapshot_sink_failure_symbols: set[str] = set()
        self._condition = threading.Condition(threading.RLock())

    def attach(self, symbol: str, subscriber_id: str) -> dict[str, Any]:
        state_payload: dict[str, Any] | None = None
        snapshot_payload: dict[str, Any] | None = None
        pool_change = self._note_query(symbol)
        with self._condition:
            runtime = self.runtimes.setdefault(symbol, SymbolRuntime(symbol=symbol))
            was_unreferenced = runtime.ref_count == 0
            was_subscribed = subscriber_id in runtime.subscribers
            runtime.subscribers.add(subscriber_id)
        try:
            self._ensure_hydrated(runtime)
            with self._condition:
                if runtime.state == SymbolRuntimeState.EVICTING:
                    runtime.state = SymbolRuntimeState.WARM
                    runtime.eviction_started_at = None
            if was_unreferenced:
                self._attach_realtime(runtime)
            with self._condition:
                snapshot = self._snapshot_message(runtime)
                if runtime.state == SymbolRuntimeState.HYDRATING:
                    runtime.state = SymbolRuntimeState.WARM
                state_payload = self._runtime_state_payload_locked(runtime)
                if isinstance(runtime.snapshot_payload, dict):
                    snapshot_payload = runtime.snapshot_payload
            self._publish_state(symbol, state_payload)
            self._publish_snapshot(symbol, snapshot_payload)
            self._deactivate_pool_evictions(pool_change)
            return snapshot
        except Exception:
            failure_state_payload: dict[str, Any] | None = None
            with self._condition:
                if not was_subscribed:
                    runtime.subscribers.discard(subscriber_id)
                    if runtime.ref_count == 0:
                        runtime.eviction_started_at = self.now()
                failure_state_payload = self._runtime_state_payload_locked(runtime)
            self._publish_state(symbol, failure_state_payload)
            raise

    def detach(self, symbol: str, subscriber_id: str) -> SymbolRuntime:
        state_payload: dict[str, Any] | None = None
        with self._condition:
            runtime = self.runtimes.setdefault(symbol, SymbolRuntime(symbol=symbol))
            runtime.subscribers.discard(subscriber_id)
            if runtime.ref_count == 0 and runtime.state != SymbolRuntimeState.COLD:
                runtime.state = SymbolRuntimeState.EVICTING
                runtime.eviction_started_at = self.now()
            state_payload = self._runtime_state_payload_locked(runtime)
        self._publish_state(symbol, state_payload)
        return runtime

    def activate_symbol(self, symbol: str, *, strict_realtime: bool = False) -> SymbolRuntime:
        state_payload: dict[str, Any] | None = None
        snapshot_payload: dict[str, Any] | None = None
        with self._condition:
            runtime = self.runtimes.setdefault(symbol, SymbolRuntime(symbol=symbol))
        self._ensure_hydrated(runtime)
        with self._condition:
            if runtime.state == SymbolRuntimeState.EVICTING:
                runtime.state = SymbolRuntimeState.WARM
                runtime.eviction_started_at = None
        self._attach_realtime(runtime, strict=strict_realtime)
        with self._condition:
            state_payload = self._runtime_state_payload_locked(runtime)
            if isinstance(runtime.snapshot_payload, dict):
                snapshot_payload = runtime.snapshot_payload
        self._publish_state(symbol, state_payload)
        self._publish_snapshot(symbol, snapshot_payload)
        return runtime

    def deactivate_symbol(self, symbol: str) -> SymbolRuntime | None:
        state_payload: dict[str, Any] | None = None
        with self._condition:
            runtime = self.runtimes.get(symbol)
            if runtime is None:
                return None
            if runtime.ref_count == 0 and runtime.state != SymbolRuntimeState.COLD:
                runtime.state = SymbolRuntimeState.EVICTING
                runtime.eviction_started_at = self.now()
            state_payload = self._runtime_state_payload_locked(runtime)
        self._publish_state(symbol, state_payload)
        return runtime

    def seed_snapshot(self, symbol: str, payload: dict[str, Any]) -> SymbolRuntime:
        state_payload: dict[str, Any] | None = None
        with self._condition:
            runtime = self.runtimes.setdefault(symbol, SymbolRuntime(symbol=symbol))
            runtime.snapshot_payload = payload
            if runtime.state == SymbolRuntimeState.COLD:
                runtime.state = SymbolRuntimeState.WARM
            self._sync_degraded_from_snapshot(runtime)
            state_payload = self._runtime_state_payload_locked(runtime)
        self._publish_state(symbol, state_payload)
        return runtime

    def evict_expired(self) -> list[str]:
        evicted: list[str] = []
        with self._condition:
            candidates = list(self.runtimes.items())
        for symbol, runtime in candidates:
            with self._condition:
                if runtime.state != SymbolRuntimeState.EVICTING or runtime.ref_count != 0:
                    continue
                if runtime.eviction_started_at is None:
                    runtime.eviction_started_at = self.now()
                    continue
                if self.now() - runtime.eviction_started_at < self.eviction_grace_seconds:
                    continue
            self._release_realtime(runtime)
            with self._condition:
                current = self.runtimes.get(symbol)
                if current is runtime and runtime.state == SymbolRuntimeState.EVICTING and runtime.ref_count == 0:
                    del self.runtimes[symbol]
                    self._release_temporary_pool_symbol(symbol)
                    evicted.append(symbol)
        return evicted

    def apply_terminal_message(self, message: dict[str, Any]) -> bool:
        state_payload: dict[str, Any] | None = None
        applied = False
        with self._condition:
            symbol = str(message.get("symbol") or "")
            runtime = self.runtimes.get(symbol)
            if runtime is None or runtime.snapshot_payload is None:
                return False
            payload = message.get("payload")
            if not isinstance(payload, dict):
                return False

            message_type = str(message.get("type") or "")
            if message_type == "snapshot":
                runtime.snapshot_payload = dict(payload)
                self._sync_degraded_from_snapshot(runtime)
                applied = True
            elif message_type == "tick_realtime":
                snapshot = payload.get("snapshot")
                tick = payload.get("tick")
                if isinstance(snapshot, dict):
                    runtime.snapshot_payload["snapshot"] = snapshot
                if isinstance(tick, dict):
                    trade_date = terminal_payload_trade_date(payload, runtime.snapshot_payload, self.trade_date)
                    runtime.snapshot_payload["minute_bars"] = upsert_minute_bar(
                        runtime.snapshot_payload.get("minute_bars"),
                        tick,
                        trade_date=trade_date,
                    )
                self._update_freshness(runtime, payload)
                applied = True
            elif message_type == "alert_realtime":
                alert = payload.get("alert")
                if isinstance(alert, dict):
                    alerts = runtime.snapshot_payload.get("alerts")
                    existing_alerts = list(alerts) if isinstance(alerts, list) else []
                    runtime.snapshot_payload["alerts"] = [alert, *existing_alerts][:MAX_ALERTS]
                self._update_freshness(runtime, payload)
                applied = True
            elif message_type == "queue_realtime":
                broker_queue = payload.get("broker_queue")
                if isinstance(broker_queue, dict):
                    current = runtime.snapshot_payload.get("broker_queue")
                    merged = dict(current) if isinstance(current, dict) else {"ask": [], "bid": []}
                    for side in ("ask", "bid"):
                        if isinstance(broker_queue.get(side), list):
                            merged[side] = broker_queue[side]
                    runtime.snapshot_payload["broker_queue"] = merged
                self._update_freshness(runtime, payload)
                applied = True
            if applied:
                state_payload = self._runtime_state_payload_locked(runtime)
        if applied:
            self._publish_state(symbol, state_payload)
        return applied

    def apply_terminal_messages(self, messages: list[dict[str, Any]]) -> int:
        return sum(1 for message in messages if self.apply_terminal_message(message))

    def apply_processed_event(self, event: dict[str, Any]) -> bool:
        validate_processed_market_event(event)
        symbol = str(event.get("symbol") or "")
        state_payload: dict[str, Any] | None = None
        applied = False
        with self._condition:
            runtime = self.runtimes.get(symbol)
            if runtime is None or runtime.snapshot_payload is None:
                return False
            payload = event["payload"]
            result_type = str(event.get("result_type") or "")
            if result_type in {"snapshot", "l2_order_book"}:
                runtime.snapshot_payload = dict(payload)
                self._sync_degraded_from_snapshot(runtime)
                applied = True
            elif result_type == "big_trade_alert":
                alert = payload.get("alert")
                if isinstance(alert, dict):
                    alerts = runtime.snapshot_payload.get("alerts")
                    existing_alerts = list(alerts) if isinstance(alerts, list) else []
                    runtime.snapshot_payload["alerts"] = prepend_unique_alert(alert, existing_alerts)
                self._update_freshness(runtime, payload)
                applied = True
            elif result_type == "broker_queue":
                broker_queue = payload.get("broker_queue")
                if isinstance(broker_queue, dict):
                    current = runtime.snapshot_payload.get("broker_queue")
                    merged = dict(current) if isinstance(current, dict) else {"ask": [], "bid": []}
                    for side in ("ask", "bid"):
                        if isinstance(broker_queue.get(side), list):
                            merged[side] = broker_queue[side]
                    runtime.snapshot_payload["broker_queue"] = merged
                self._update_freshness(runtime, payload)
                applied = True
            if applied:
                state_payload = self._runtime_state_payload_locked(runtime)
        if applied:
            self._publish_state(symbol, state_payload)
        return applied

    def apply_processed_events(self, events: list[dict[str, Any]]) -> int:
        return sum(1 for event in events if self.apply_processed_event(event))

    def apply_raw_event(
        self,
        raw_event: dict[str, Any],
        trade_date: str,
        processor: RawEventProcessor | None = None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        symbol = str(raw_event.get("symbol") or "")
        raw_processor = processor or self.raw_event_processor
        if raw_processor is None:
            raise RuntimeError("runtime raw event processor is not configured")
        with self._condition:
            runtime = self.runtimes.setdefault(symbol, SymbolRuntime(symbol=symbol))
            needs_hydration = not isinstance(runtime.snapshot_payload, dict)
        if needs_hydration:
            self._ensure_hydrated(runtime)
        with self._condition:
            runtime = self.runtimes.get(symbol)
            if runtime is None or not isinstance(runtime.snapshot_payload, dict):
                raise RuntimeError(f"runtime snapshot must be hydrated before raw processing: {symbol}")
            state = runtime.snapshot_payload

        processed_events = raw_processor(raw_event, trade_date, state)
        terminal_messages = self._terminal_messages_from_processed(processed_events)

        state_payload: dict[str, Any] | None = None
        snapshot_payload: dict[str, Any] | None = None
        with self._condition:
            runtime = self.runtimes.get(symbol)
            if runtime is not None:
                runtime.snapshot_payload = state
                self._sync_degraded_from_snapshot(runtime)
                if runtime.state not in {SymbolRuntimeState.DEGRADED, SymbolRuntimeState.EVICTING}:
                    runtime.state = SymbolRuntimeState.LIVE
                runtime.delta_emitted += len(terminal_messages)
                runtime.mailbox_depth += len(terminal_messages)
                self.raw_events_applied += 1
                self.runtime_delta_emitted += len(terminal_messages)
                state_payload = self._runtime_state_payload_locked(runtime)
                snapshot_payload = runtime.snapshot_payload
        self._publish_state(symbol, state_payload)
        self._publish_snapshot(symbol, snapshot_payload)
        return processed_events, terminal_messages

    def mark_deltas_delivered(self, messages: list[dict[str, Any]], delivered: int) -> None:
        if not messages and delivered <= 0:
            return
        by_symbol: dict[str, int] = defaultdict(int)
        for message in messages:
            by_symbol[str(message.get("symbol") or "")] += 1
        with self._condition:
            self.runtime_delta_delivered += delivered
            for symbol, count in by_symbol.items():
                runtime = self.runtimes.get(symbol)
                if runtime is not None:
                    runtime.mailbox_depth = max(0, runtime.mailbox_depth - count)

    def has_runtime(self, symbol: str) -> bool:
        with self._condition:
            return symbol in self.runtimes

    def snapshot_payload(self, symbol: str) -> dict[str, Any] | None:
        with self._condition:
            runtime = self.runtimes.get(symbol)
            if runtime is None or not isinstance(runtime.snapshot_payload, dict):
                return None
            return runtime.snapshot_payload

    def retain_existing_runtime(self, symbol: str, subscriber_id: str) -> bool:
        state_payload: dict[str, Any] | None = None
        with self._condition:
            runtime = self.runtimes.get(symbol)
            if runtime is None or runtime.snapshot_payload is None or not runtime.realtime_attached:
                return False
            runtime.subscribers.add(subscriber_id)
            if runtime.state == SymbolRuntimeState.EVICTING:
                runtime.state = SymbolRuntimeState.LIVE if runtime.realtime_attached else SymbolRuntimeState.WARM
                runtime.eviction_started_at = None
            state_payload = self._runtime_state_payload_locked(runtime)
        self._publish_state(symbol, state_payload)
        return True

    def runtime_state_payload(self, symbol: str) -> dict[str, Any] | None:
        with self._condition:
            runtime = self.runtimes.get(symbol)
            if runtime is None:
                return None
            return self._runtime_state_payload_locked(runtime)

    def flush_runtime_state(self, symbol: str) -> bool:
        state_payload = self.runtime_state_payload(symbol)
        if state_payload is None:
            return False
        self._publish_state(symbol, state_payload)
        return True

    def snapshot(self) -> dict[str, dict[str, Any]]:
        with self._condition:
            return {
                symbol: {
                    "symbol": runtime.symbol,
                    "state": runtime.state.value,
                    "ref_count": runtime.ref_count,
                    "subscribers": sorted(runtime.subscribers),
                    "hydrate_count": runtime.hydrate_count,
                    "hydration_failures": runtime.hydration_failures,
                    "last_hydration_latency_ms": runtime.last_hydration_latency_ms,
                    "max_hydration_latency_ms": runtime.max_hydration_latency_ms,
                    "last_hydration_error": runtime.last_hydration_error,
                    "degraded_reasons": list(runtime.degraded_reasons),
                    "eviction_started_at": runtime.eviction_started_at,
                    "eviction_grace_seconds": self.eviction_grace_seconds,
                    "max_concurrent_hydrations": self.max_concurrent_hydrations,
                    "capacity_rejections": self.capacity_rejections,
                    "realtime_attached": runtime.realtime_attached,
                    "delta_emitted": runtime.delta_emitted,
                    "mailbox_depth": runtime.mailbox_depth,
                    "has_snapshot_payload": runtime.snapshot_payload is not None,
                    "freshness": dict(runtime.snapshot_payload.get("freshness", {}))
                    if isinstance(runtime.snapshot_payload, dict)
                    else {},
                }
                for symbol, runtime in sorted(self.runtimes.items())
            }

    def manager_snapshot(self) -> dict[str, Any]:
        with self._condition:
            states: dict[str, int] = {state.value: 0 for state in SymbolRuntimeState}
            total_ref_count = 0
            realtime_attached_symbols = []
            for symbol, runtime in self.runtimes.items():
                states[runtime.state.value] = states.get(runtime.state.value, 0) + 1
                total_ref_count += runtime.ref_count
                if runtime.realtime_attached:
                    realtime_attached_symbols.append(symbol)
            return {
                "runtime_count": len(self.runtimes),
                "state_counts": states,
                "total_ref_count": total_ref_count,
                "active_hydrations": self._active_hydrations,
                "hydrating_symbols": sorted({key.symbol for key in self._hydrating}),
                "active_hydration_keys": [
                    key.__dict__
                    for key in sorted(self._hydrating, key=lambda item: (item.symbol, item.data_type, item.effective_trade_date))
                ],
                "hydration_service": self.hydration_service.snapshot(),
                "max_concurrent_hydrations": self.max_concurrent_hydrations,
                "capacity_rejections": self.capacity_rejections,
                "raw_events_applied": self.raw_events_applied,
                "runtime_delta_emitted": self.runtime_delta_emitted,
                "runtime_delta_delivered": self.runtime_delta_delivered,
                "state_sink_failures": self.state_sink_failures,
                "last_state_sink_error": self.last_state_sink_error,
                "state_sink_failure_symbols": sorted(self.state_sink_failure_symbols),
                "snapshot_sink_failures": self.snapshot_sink_failures,
                "last_snapshot_sink_error": self.last_snapshot_sink_error,
                "snapshot_sink_failure_symbols": sorted(self.snapshot_sink_failure_symbols),
                "realtime_attached_symbols": sorted(realtime_attached_symbols),
                "eviction_grace_seconds": self.eviction_grace_seconds,
            }

    def _snapshot_message(self, runtime: SymbolRuntime) -> dict[str, Any]:
        if runtime.snapshot_payload is not None:
            return self.gateway.snapshot_message(runtime.symbol, runtime.snapshot_payload)
        return self.gateway.subscribe(runtime.symbol, self.trade_date)

    def _note_query(self, symbol: str) -> Any | None:
        if self.active_pool_manager is None:
            return None
        note_query = getattr(self.active_pool_manager, "note_query", None)
        if not callable(note_query):
            return None
        return note_query(symbol)

    def _deactivate_pool_evictions(self, pool_change: Any | None) -> None:
        if pool_change is None:
            return
        evicted_symbols = getattr(pool_change, "evicted_symbols", None)
        if not isinstance(evicted_symbols, list):
            return
        for evicted_symbol in evicted_symbols:
            self.deactivate_symbol(str(evicted_symbol))

    def _release_temporary_pool_symbol(self, symbol: str) -> None:
        if self.active_pool_manager is None:
            return
        release_temporary = getattr(self.active_pool_manager, "release_temporary", None)
        if callable(release_temporary):
            release_temporary(symbol)

    def _update_freshness(self, runtime: SymbolRuntime, payload: dict[str, Any]) -> None:
        freshness = payload.get("freshness")
        if isinstance(freshness, dict):
            runtime.snapshot_payload["freshness"] = freshness
            self._sync_degraded_from_snapshot(runtime)

    def _attach_realtime(self, runtime: SymbolRuntime, *, strict: bool = False) -> None:
        if self.attach_realtime is None or runtime.realtime_attached:
            return
        try:
            attached = self.attach_realtime(runtime.symbol)
            if attached is False:
                return
            runtime.realtime_attached = True
            if not runtime.degraded_reasons:
                runtime.state = SymbolRuntimeState.LIVE
        except Exception as error:
            runtime.state = SymbolRuntimeState.DEGRADED
            reason = f"realtime_attach_failed: {error}"
            if reason not in runtime.degraded_reasons:
                runtime.degraded_reasons = [*runtime.degraded_reasons, reason]
            if strict:
                raise

    def _release_realtime(self, runtime: SymbolRuntime) -> None:
        if self.release_symbol is None or not runtime.realtime_attached:
            return
        try:
            self.release_symbol(runtime.symbol)
            runtime.realtime_attached = False
        except Exception as error:
            runtime.state = SymbolRuntimeState.DEGRADED
            reason = f"realtime_release_failed: {error}"
            if reason not in runtime.degraded_reasons:
                runtime.degraded_reasons = [*runtime.degraded_reasons, reason]

    def _ensure_hydrated(self, runtime: SymbolRuntime) -> None:
        key = self._hydration_key(runtime)
        with self._condition:
            while key in self._hydrating:
                self._condition.wait()
            if runtime.state != SymbolRuntimeState.COLD:
                return
        self._hydrate_once(runtime)

    def _hydrate_once(self, runtime: SymbolRuntime) -> None:
        capacity_state_payload: dict[str, Any] | None = None
        key = self._hydration_key(runtime)
        with self._condition:
            if key in self._hydrating:
                while key in self._hydrating:
                    self._condition.wait()
                return
            if runtime.state != SymbolRuntimeState.COLD:
                return
            if self._active_hydrations >= self.max_concurrent_hydrations:
                reason = f"hydration_capacity_exceeded:max_concurrent_hydrations={self.max_concurrent_hydrations}"
                self.capacity_rejections += 1
                runtime.state = SymbolRuntimeState.DEGRADED
                runtime.degraded_reasons = [reason]
                runtime.hydration_failures += 1
                runtime.last_hydration_error = reason
                if runtime.snapshot_payload is None:
                    runtime.snapshot_payload = degraded_snapshot_payload(runtime.symbol, self.trade_date, reason)
                capacity_state_payload = self._runtime_state_payload_locked(runtime)
                self._condition.notify_all()
            else:
                self._hydrating.add(key)
                self._active_hydrations += 1
                runtime.state = SymbolRuntimeState.HYDRATING
                started = self.now()
        if capacity_state_payload is not None:
            self._publish_state(runtime.symbol, capacity_state_payload)
            return
        snapshot_payload: dict[str, Any] | None = None
        error: Exception | None = None
        try:
            hydrated = self.hydration_service.hydrate(key, lambda: self.hydrate_symbol(runtime.symbol))
            if isinstance(hydrated, dict):
                snapshot_payload = hydrated
        except Exception as caught:
            error = caught

        with self._condition:
            latency_ms = max(0.0, (self.now() - started) * 1000)
            runtime.last_hydration_latency_ms = latency_ms
            runtime.max_hydration_latency_ms = max(runtime.max_hydration_latency_ms, latency_ms)
            if error is None:
                if snapshot_payload is not None:
                    runtime.snapshot_payload = snapshot_payload
                runtime.hydrate_count += 1
                runtime.last_hydration_error = ""
                runtime.state = SymbolRuntimeState.WARM
                runtime.degraded_reasons = []
            else:
                runtime.state = SymbolRuntimeState.DEGRADED
                runtime.degraded_reasons = [str(error)]
                runtime.hydration_failures += 1
                runtime.last_hydration_error = str(error)
            self._sync_degraded_from_snapshot(runtime)
            self._active_hydrations = max(0, self._active_hydrations - 1)
            self._hydrating.discard(key)
            state_payload = self._runtime_state_payload_locked(runtime)
            self._condition.notify_all()
        self._publish_state(runtime.symbol, state_payload)
        if error is not None:
            raise error

    def _hydration_key(self, runtime: SymbolRuntime, data_type: str = "snapshot") -> HydrationKey:
        effective_trade_date = self.trade_date
        if isinstance(runtime.snapshot_payload, dict):
            freshness = runtime.snapshot_payload.get("freshness")
            if isinstance(freshness, dict):
                effective_trade_date = str(freshness.get("effective_trade_date") or effective_trade_date)
        return HydrationKey(runtime.symbol, data_type, effective_trade_date)

    def _sync_degraded_from_snapshot(self, runtime: SymbolRuntime) -> None:
        if not isinstance(runtime.snapshot_payload, dict):
            return
        freshness = runtime.snapshot_payload.get("freshness")
        if not isinstance(freshness, dict):
            return
        degraded_reasons = [str(reason) for reason in freshness.get("degraded_reasons") or []]
        if freshness.get("degraded") is True and not degraded_reasons:
            degraded_reasons = ["freshness_degraded"]
        if degraded_reasons:
            runtime.state = SymbolRuntimeState.DEGRADED
            runtime.degraded_reasons = degraded_reasons
        elif runtime.state == SymbolRuntimeState.DEGRADED:
            runtime.degraded_reasons = []
        runtime_state = freshness.get("runtime_state")
        if (
            runtime_state in {SymbolRuntimeState.WARM.value, SymbolRuntimeState.LIVE.value}
            and runtime.state not in {SymbolRuntimeState.EVICTING, SymbolRuntimeState.HYDRATING}
            and not degraded_reasons
        ):
            runtime.state = SymbolRuntimeState(runtime_state)

    def _runtime_state_payload_locked(self, runtime: SymbolRuntime) -> dict[str, Any]:
        timestamp = now_iso()
        freshness: dict[str, Any] = {}
        if isinstance(runtime.snapshot_payload, dict) and isinstance(runtime.snapshot_payload.get("freshness"), dict):
            freshness = dict(runtime.snapshot_payload["freshness"])
        source_dates = freshness.get("source_dates") if isinstance(freshness.get("source_dates"), dict) else {}
        requested_trade_date = str(freshness.get("requested_trade_date") or self.trade_date)
        effective_trade_date = str(freshness.get("effective_trade_date") or requested_trade_date)
        updated_at = str(freshness.get("updated_at") or timestamp)
        version = str(freshness.get("version") or freshness.get("last_event_id") or "")
        return {
            "schema_version": 1,
            "symbol": runtime.symbol,
            "requested_trade_date": requested_trade_date,
            "effective_trade_date": effective_trade_date,
            "source_dates": source_dates,
            "updated_at": updated_at,
            "version": version,
            "last_event_id": version,
            "freshness": freshness,
            "degraded_reasons": list(runtime.degraded_reasons),
            "runtime_state": runtime.state.value,
            "ref_count": runtime.ref_count,
            "subscribers": sorted(runtime.subscribers),
            "hydrate_count": runtime.hydrate_count,
            "hydration_failures": runtime.hydration_failures,
            "last_hydration_latency_ms": runtime.last_hydration_latency_ms,
            "max_hydration_latency_ms": runtime.max_hydration_latency_ms,
            "last_hydration_error": runtime.last_hydration_error,
            "realtime_attached": runtime.realtime_attached,
            "eviction_started_at": runtime.eviction_started_at,
            "eviction_grace_seconds": self.eviction_grace_seconds,
            "max_concurrent_hydrations": self.max_concurrent_hydrations,
            "capacity_rejections": self.capacity_rejections,
            "has_snapshot_payload": runtime.snapshot_payload is not None,
            "delta_emitted": runtime.delta_emitted,
            "mailbox_depth": runtime.mailbox_depth,
        }

    def _terminal_messages_from_processed(self, processed_events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        messages: list[dict[str, Any]] = []
        for processed in processed_events:
            validate_processed_market_event(processed)
            symbol = processed["symbol"]
            result_type = processed["result_type"]
            if result_type == "snapshot":
                payload = processed["payload"]
                tick = payload.get("last_tick")
                if not isinstance(tick, dict):
                    minute_bars = payload.get("minute_bars")
                    tick = minute_bars[-1] if isinstance(minute_bars, list) and minute_bars else {}
                messages.append(
                    self._terminal(
                        "tick_realtime",
                        symbol,
                        {
                            "tick": tick,
                            "snapshot": payload.get("snapshot", {}),
                            "freshness": payload.get("freshness", {}),
                        },
                        source_ts=processed["source_ts"],
                    )
                )
            elif result_type == "l2_order_book":
                messages.append(self._terminal("snapshot", symbol, processed["payload"], source_ts=processed["source_ts"]))
            elif result_type == "big_trade_alert":
                messages.append(self._terminal("alert_realtime", symbol, processed["payload"], source_ts=processed["source_ts"]))
            elif result_type == "broker_queue":
                messages.append(self._terminal("queue_realtime", symbol, processed["payload"], source_ts=processed["source_ts"]))
        return messages

    def _terminal(
        self,
        message_type: str,
        symbol: str,
        payload: dict[str, Any],
        *,
        source_ts: str | None = None,
    ) -> dict[str, Any]:
        self.seq_by_symbol[symbol] += 1
        return make_terminal_message(
            message_type=message_type,
            symbol=symbol,
            source="symbol-runtime",
            seq=self.seq_by_symbol[symbol],
            source_ts=source_ts,
            payload=payload,
        )

    def _publish_state(self, symbol: str, state_payload: dict[str, Any] | None) -> None:
        if self.state_sink is None or state_payload is None:
            return
        try:
            self.state_sink(symbol, state_payload)
        except Exception as error:
            # State read model writes are diagnostic; they must not break
            # subscribe/detach or realtime processing paths.
            reason = f"runtime_state_sink_write_failed: {error}"
            with self._condition:
                self.state_sink_failures += 1
                self.last_state_sink_error = reason
                self.state_sink_failure_symbols.add(symbol)
                runtime = self.runtimes.get(symbol)
                if runtime is not None:
                    runtime.state = SymbolRuntimeState.DEGRADED
                    if reason not in runtime.degraded_reasons:
                        runtime.degraded_reasons = [*runtime.degraded_reasons, reason]
            return

    def _publish_snapshot(self, symbol: str, snapshot_payload: dict[str, Any] | None) -> None:
        if self.snapshot_sink is None or snapshot_payload is None:
            return
        try:
            self.snapshot_sink(symbol, snapshot_payload)
        except Exception as error:
            reason = f"runtime_snapshot_sink_write_failed: {error}"
            with self._condition:
                self.snapshot_sink_failures += 1
                self.last_snapshot_sink_error = reason
                self.snapshot_sink_failure_symbols.add(symbol)
                runtime = self.runtimes.get(symbol)
                if runtime is not None:
                    runtime.state = SymbolRuntimeState.DEGRADED
                    if reason not in runtime.degraded_reasons:
                        runtime.degraded_reasons = [*runtime.degraded_reasons, reason]
            return


def upsert_minute_bar(existing: Any, tick: dict[str, Any], *, trade_date: str | None = None) -> list[dict[str, Any]]:
    bars = [dict(item) for item in existing if isinstance(item, dict)] if isinstance(existing, list) else []
    timestamp = str(tick.get("timestamp") or "")
    if not timestamp:
        return bars[-MAX_MINUTE_BARS:]
    minute_ts = minute_bucket(timestamp)
    if not is_regular_hk_trading_minute(minute_ts, trade_date):
        return bars[-MAX_MINUTE_BARS:]
    tick_price = float(tick.get("price") or tick.get("close") or 0)
    tick_volume = int(tick.get("volume") or 0)
    tick_turnover = float(tick.get("turnover") or 0)
    for index, bar in enumerate(bars):
        if minute_bucket(str(bar.get("timestamp") or "")) == minute_ts:
            previous_price = float(bar.get("price") or bar.get("close") or tick_price)
            previous_high = float(bar.get("high") or previous_price)
            previous_low = float(bar.get("low") or previous_price)
            if tick.get("replace") is True:
                bars[index] = {
                    **bar,
                    **tick,
                    "timestamp": minute_ts,
                    "price": tick_price,
                    "close": tick_price,
                    "open": float(tick.get("open") or bar.get("open") or previous_price),
                    "high": float(tick.get("high") or tick_price),
                    "low": float(tick.get("low") or tick_price),
                    "volume": tick_volume,
                    "turnover": tick_turnover,
                }
                break
            bars[index] = {
                **bar,
                **tick,
                "timestamp": minute_ts,
                "price": tick_price,
                "close": tick_price,
                "open": float(bar.get("open") or previous_price),
                "high": max(previous_high, tick_price),
                "low": min(previous_low, tick_price),
                "volume": int(bar.get("volume") or 0) + tick_volume,
                "turnover": float(bar.get("turnover") or 0) + tick_turnover,
            }
            break
    else:
        bars.append(
            {
                **tick,
                "timestamp": minute_ts,
                "price": tick_price,
                "open": float(tick.get("open") or tick_price),
                "high": float(tick.get("high") or tick_price),
                "low": float(tick.get("low") or tick_price),
                "close": tick_price,
                "volume": tick_volume,
                "turnover": tick_turnover,
            }
        )
    bars.sort(key=lambda item: minute_bucket(str(item.get("timestamp") or "")))
    return bars[-MAX_MINUTE_BARS:]


def prepend_unique_alert(alert: dict[str, Any], existing_alerts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    alert_id = str(alert.get("id") or "")
    if alert_id:
        deduped = [existing for existing in existing_alerts if str(existing.get("id") or "") != alert_id]
    else:
        deduped = [existing for existing in existing_alerts if existing != alert]
    return [alert, *deduped][:MAX_ALERTS]


def terminal_payload_trade_date(
    payload: dict[str, Any],
    snapshot_payload: dict[str, Any] | None,
    fallback: str,
) -> str:
    for candidate in (
        extract_freshness_trade_date(payload.get("freshness")),
        extract_snapshot_trade_date(payload.get("snapshot")),
        extract_freshness_trade_date((snapshot_payload or {}).get("freshness")),
        extract_snapshot_trade_date((snapshot_payload or {}).get("snapshot")),
    ):
        if candidate:
            return candidate
    return fallback


def extract_freshness_trade_date(value: Any) -> str:
    if not isinstance(value, dict):
        return ""
    for key in ("effective_trade_date", "effectiveTradeDate", "requested_trade_date", "requestedTradeDate"):
        candidate = value.get(key)
        if isinstance(candidate, str) and len(candidate) == 8 and candidate.isdigit():
            return candidate
    return ""


def extract_snapshot_trade_date(value: Any) -> str:
    if not isinstance(value, dict):
        return ""
    for key in ("tradeDate", "trade_date", "requestedTradeDate", "requested_trade_date"):
        candidate = value.get(key)
        if isinstance(candidate, str) and len(candidate) == 8 and candidate.isdigit():
            return candidate
    return ""


def minute_bucket(timestamp: str) -> str:
    try:
        parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError:
        return timestamp
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=HK_TZ)
    return parsed.astimezone(HK_TZ).replace(second=0, microsecond=0).isoformat()


def degraded_snapshot_payload(symbol: str, trade_date: str, reason: str) -> dict[str, Any]:
    timestamp = now_iso()
    return {
        "snapshot": {
            "symbol": symbol,
            "name": symbol,
            "currency": "HKD",
            "tradeDate": trade_date,
            "requestedTradeDate": trade_date,
            "isHistoricalSession": False,
            "price": 0.0,
            "previousClose": 0.0,
            "open": 0.0,
            "high": 0.0,
            "low": 0.0,
            "volume": 0,
            "turnover": 0.0,
            "change": 0.0,
            "changePercent": 0.0,
            "updatedAt": timestamp,
        },
        "minute_bars": [],
        "alerts": [],
        "broker_queue": {"ask": [], "bid": []},
        "ccass_holdings": [],
        "freshness": {
            "updated_at": timestamp,
            "requested_trade_date": trade_date,
            "effective_trade_date": trade_date,
            "runtime_state": SymbolRuntimeState.DEGRADED.value,
            "source_dates": {},
            "degraded": True,
            "degraded_reasons": [reason],
        },
    }
