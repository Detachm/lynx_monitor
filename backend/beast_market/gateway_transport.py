from __future__ import annotations

import json
from dataclasses import dataclass, field
from time import perf_counter
from typing import Any, Callable

from .contracts import CANONICAL_SYMBOL_PATTERN, SCHEMA_VERSION, TERMINAL_MESSAGE_PROTOCOL
from .gateway import GatewayClientQueue
from .pipeline import GatewayV2
from .symbol_runtime import SymbolRuntimeManager


HistoryProvider = Callable[[str, str, int], list[dict[str, Any]]]
RealtimeSeedProvider = Callable[[str], Any]
MAX_PERFORMANCE_SAMPLES = 500


@dataclass
class GatewaySession:
    client_id: str
    queue: GatewayClientQueue
    subscribed_symbols: set[str] = field(default_factory=set)
    attached_runtime_symbols: set[str] = field(default_factory=set)


class GatewayV2SessionManager:
    """Testable WebSocket session boundary for Gateway v2.

    A real WebSocket server can map connection open/close/message/send calls to this
    class without changing Gateway v2 contract behavior.
    """

    def __init__(
        self,
        gateway: GatewayV2,
        *,
        trade_date: str,
        history_provider: HistoryProvider | None = None,
        client_queue_size: int = 100,
        symbol_runtime_manager: SymbolRuntimeManager | None = None,
        realtime_seed_provider: RealtimeSeedProvider | None = None,
        consume_processed_on_broadcast: bool = True,
    ) -> None:
        self.gateway = gateway
        self.trade_date = trade_date
        self.history_provider = history_provider or empty_history
        self.client_queue_size = client_queue_size
        self.symbol_runtime_manager = symbol_runtime_manager
        self.realtime_seed_provider = realtime_seed_provider
        self.consume_processed_on_broadcast = consume_processed_on_broadcast
        self.sessions: dict[str, GatewaySession] = {}
        self.observed_client_ids: set[str] = set()
        self.observed_declared_client_ids: set[str] = set()
        self.max_connected_clients = 0
        self.performance_samples: dict[str, list[float]] = {"subscribe_snapshot_ms": []}
        self.recent_performance_samples: dict[str, list[float]] = {"subscribe_snapshot_ms": []}

    def connect(self, client_id: str) -> GatewaySession:
        session = GatewaySession(
            client_id=client_id,
            queue=GatewayClientQueue(client_id, max_size=self.client_queue_size),
        )
        self.sessions[client_id] = session
        self.observed_client_ids.add(client_id)
        self.max_connected_clients = max(self.max_connected_clients, len(self.sessions))
        session.queue.enqueue(self.gateway.health.as_message(source="gateway"))
        return session

    def disconnect(self, client_id: str) -> None:
        session = self.sessions.pop(client_id, None)
        if session is not None and self.symbol_runtime_manager is not None:
            for symbol in list(session.attached_runtime_symbols):
                self.symbol_runtime_manager.detach(symbol, client_id)

    def handle_message(self, client_id: str, raw_message: str | dict[str, Any]) -> None:
        session = self._session(client_id)
        request = parse_request(raw_message)
        validate_gateway_request_protocol(request)
        self._record_declared_client_id(request)
        action = str(request.get("action", ""))
        symbol = normalize_symbol(str(request.get("symbol", "")))

        if action == "subscribe":
            require_gateway_symbol(symbol, "subscribe")
            started = perf_counter()
            fast_snapshot = self._active_runtime_snapshot(symbol) if self.symbol_runtime_manager is not None else None
            if fast_snapshot is not None:
                if self.symbol_runtime_manager is not None and self.symbol_runtime_manager.retain_existing_runtime(symbol, client_id):
                    session.attached_runtime_symbols.add(symbol)
                snapshot = fast_snapshot
            elif self.symbol_runtime_manager is not None:
                snapshot = self.symbol_runtime_manager.attach(symbol, client_id)
                session.attached_runtime_symbols.add(symbol)
            else:
                snapshot = self.gateway.subscribe(symbol, self.trade_date)
            session.subscribed_symbols.add(symbol)
            session.queue.enqueue(snapshot)
            self._record_performance_sample("subscribe_snapshot_ms", (perf_counter() - started) * 1000)
        elif action == "unsubscribe":
            require_gateway_symbol(symbol, "unsubscribe")
            session.subscribed_symbols.discard(symbol)
            if self.symbol_runtime_manager is not None and symbol in session.attached_runtime_symbols:
                session.attached_runtime_symbols.discard(symbol)
                self.symbol_runtime_manager.detach(symbol, client_id)
        elif action == "holding_name_click":
            require_gateway_symbol(symbol, "holding_name_click")
            participant_name = str(request.get("participant_name") or request.get("participantName") or "").strip()
            if not participant_name:
                raise ValueError("holding_name_click requires participant_name")
            days = request.get("days", 30)
            if not isinstance(days, int) or isinstance(days, bool) or days < 1:
                raise ValueError("holding_name_click requires positive integer days")
            history = self.history_provider(symbol, participant_name, days)
            session.queue.enqueue(self.gateway.holding_history_response(symbol, participant_name, days, history))
        elif action == "health":
            session.queue.enqueue(self.gateway.health.as_message(source="gateway"))
        else:
            raise ValueError(f"unsupported gateway action: {action}")

    def broadcast_processed(self) -> int:
        if not self.consume_processed_on_broadcast:
            return 0
        messages = self.gateway.to_terminal_messages()
        return self.broadcast_runtime_messages(messages)

    def broadcast_runtime_messages(self, messages: list[dict[str, Any]], *, update_symbol_runtime: bool = True) -> int:
        if update_symbol_runtime and self.symbol_runtime_manager is not None:
            for message in messages:
                self.symbol_runtime_manager.apply_terminal_message(message)
        delivered = 0
        for message in messages:
            symbol = str(message.get("symbol", ""))
            for session in self.sessions.values():
                if symbol in session.subscribed_symbols:
                    session.queue.enqueue(message)
                    delivered += 1
        return delivered

    def flush(self, client_id: str, limit: int | None = None) -> list[str]:
        session = self._session(client_id)
        return [json.dumps(message, separators=(",", ":"), ensure_ascii=False) for message in session.queue.drain(limit)]

    def pop_performance_samples(self) -> dict[str, list[float]]:
        samples = {key: list(values) for key, values in self.performance_samples.items()}
        for values in self.performance_samples.values():
            values.clear()
        return samples

    def performance_snapshot(self) -> dict[str, list[float]]:
        return {key: list(values) for key, values in self.recent_performance_samples.items()}

    def client_queue_snapshot(self) -> dict[str, Any]:
        current_backlogs = {
            client_id: len(session.queue.queue) for client_id, session in self.sessions.items()
        }
        stats = [session.queue.stats for session in self.sessions.values()]
        return {
            "connected_clients": len(self.sessions),
            "observed_client_count": len(self.observed_client_ids),
            "observed_client_ids": sorted(self.observed_client_ids),
            "observed_declared_client_count": len(self.observed_declared_client_ids),
            "observed_declared_client_ids": sorted(self.observed_declared_client_ids),
            "max_connected_clients": self.max_connected_clients,
            "client_queue_max_size": self.client_queue_size,
            "current_backlog_by_client": current_backlogs,
            "total_current_backlog": sum(current_backlogs.values()),
            "max_current_backlog": max(current_backlogs.values(), default=0),
            "enqueued": sum(item.enqueued for item in stats),
            "coalesced": sum(item.coalesced for item in stats),
            "dropped": sum(item.dropped for item in stats),
            "alerts_enqueued": sum(item.alerts_enqueued for item in stats),
            "alert_overflow": sum(item.alert_overflow for item in stats),
            "alert_dropped": sum(item.alert_dropped for item in stats),
            "critical_overflow": sum(item.critical_overflow for item in stats),
        }

    def _record_performance_sample(self, key: str, value: float) -> None:
        self.performance_samples.setdefault(key, []).append(value)
        recent = self.recent_performance_samples.setdefault(key, [])
        recent.append(value)
        if len(recent) > MAX_PERFORMANCE_SAMPLES:
            del recent[: len(recent) - MAX_PERFORMANCE_SAMPLES]

    def _record_declared_client_id(self, request: dict[str, Any]) -> None:
        client_id = request.get("client_id") or request.get("clientId") or request.get("machine_id") or request.get("machineId")
        if isinstance(client_id, str) and client_id.strip():
            self.observed_declared_client_ids.add(client_id.strip())

    def _active_runtime_snapshot(self, symbol: str) -> dict[str, Any] | None:
        if self.symbol_runtime_manager is None:
            return None
        state = self.symbol_runtime_manager.runtime_state_payload(symbol)
        if not isinstance(state, dict) or state.get("realtime_attached") is not True:
            return None
        payload = self.symbol_runtime_manager.snapshot_payload(symbol)
        if not isinstance(payload, dict):
            return None
        if snapshot_needs_realtime_seed(payload, self.trade_date) and self.realtime_seed_provider is not None:
            try:
                self.realtime_seed_provider(symbol)
            except Exception:
                pass
            refreshed = self.symbol_runtime_manager.snapshot_payload(symbol)
            if isinstance(refreshed, dict):
                payload = refreshed
        return self.gateway.snapshot_message(symbol, payload)

    def _session(self, client_id: str) -> GatewaySession:
        if client_id not in self.sessions:
            raise KeyError(f"unknown gateway client: {client_id}")
        return self.sessions[client_id]


def parse_request(raw_message: str | dict[str, Any]) -> dict[str, Any]:
    if isinstance(raw_message, str):
        decoded = json.loads(raw_message)
    else:
        decoded = raw_message
    if not isinstance(decoded, dict):
        raise ValueError("gateway request must be an object")
    return decoded


def validate_gateway_request_protocol(request: dict[str, Any]) -> None:
    if request.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(f"unsupported gateway request schema_version: {request.get('schema_version')}")
    if request.get("protocol") != TERMINAL_MESSAGE_PROTOCOL:
        raise ValueError(f"unsupported gateway request protocol: {request.get('protocol')}")


def normalize_symbol(symbol: str) -> str:
    value = symbol.strip().upper()
    if not value:
        return ""
    if "." in value:
        return value
    if value.isdigit():
        return f"{value.zfill(5)}.HK"
    return value


def require_gateway_symbol(symbol: str, action: str) -> None:
    if not symbol:
        raise ValueError(f"{action} requires symbol")
    if not CANONICAL_SYMBOL_PATTERN.fullmatch(symbol):
        raise ValueError(f"{action} requires canonical symbol 00700.HK")


def empty_history(symbol: str, participant_name: str, days: int) -> list[dict[str, Any]]:
    return []


def snapshot_needs_realtime_seed(payload: dict[str, Any], trade_date: str) -> bool:
    freshness = payload.get("freshness")
    source_dates = freshness.get("source_dates") if isinstance(freshness, dict) else {}
    degraded_reasons = freshness.get("degraded_reasons") if isinstance(freshness, dict) else []
    minute_bars = payload.get("minute_bars")
    if isinstance(degraded_reasons, list) and "intraday_gap_before_attach" in degraded_reasons:
        return True
    if not isinstance(minute_bars, list) or not minute_bars:
        return True
    if not isinstance(source_dates, dict):
        return True
    return source_dates.get("minute_bars") != trade_date and source_dates.get("latest_bar") != trade_date
