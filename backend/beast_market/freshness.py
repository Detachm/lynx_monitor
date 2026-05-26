from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .contracts import now_iso


@dataclass
class SymbolFreshness:
    symbol: str
    period: str | None = None
    stream_kind: str | None = None
    subscribed: bool = False
    latest_event_at: str | None = None
    latest_ingest_at: str | None = None
    queue_backlog: int = 0
    degraded: bool = False
    degraded_reason: str | None = None
    resubscribe_requested: bool = False

    @property
    def stream_id(self) -> str:
        if self.period or self.stream_kind:
            return f"{self.symbol}|{self.period or ''}|{self.stream_kind or ''}"
        return self.symbol

    def as_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "period": self.period,
            "stream_kind": self.stream_kind,
            "stream_id": self.stream_id,
            "subscribed": self.subscribed,
            "latest_event_at": self.latest_event_at,
            "latest_ingest_at": self.latest_ingest_at,
            "queue_backlog": self.queue_backlog,
            "degraded": self.degraded,
            "degraded_reason": self.degraded_reason,
            "resubscribe_requested": self.resubscribe_requested,
        }


@dataclass(frozen=True)
class FreshnessPolicy:
    max_event_age_seconds: float = 60
    max_queue_backlog: int = 1_000


class SymbolFreshnessTracker:
    def __init__(self, policy: FreshnessPolicy | None = None) -> None:
        self.policy = policy or FreshnessPolicy()
        self.state_by_symbol: dict[str, SymbolFreshness] = {}

    def mark_subscribed(
        self,
        symbol: str,
        *,
        period: str | None = None,
        stream_kind: str | None = None,
    ) -> SymbolFreshness:
        state = self._state(symbol, period=period, stream_kind=stream_kind)
        state.subscribed = True
        state.resubscribe_requested = False
        state.degraded = False
        state.degraded_reason = None
        return state

    def mark_unsubscribed(
        self,
        symbol: str,
        *,
        period: str | None = None,
        stream_kind: str | None = None,
    ) -> SymbolFreshness:
        state = self._state(symbol, period=period, stream_kind=stream_kind)
        state.subscribed = False
        state.resubscribe_requested = False
        state.degraded = False
        state.degraded_reason = None
        return state

    def record_event(
        self,
        symbol: str,
        *,
        source_ts: str,
        ingest_ts: str | None = None,
        period: str | None = None,
        stream_kind: str | None = None,
    ) -> SymbolFreshness:
        state = self._state(symbol, period=period, stream_kind=stream_kind)
        state.latest_event_at = source_ts
        state.latest_ingest_at = ingest_ts or now_iso()
        state.degraded = False
        state.degraded_reason = None
        state.resubscribe_requested = False
        return state

    def record_backlog(
        self,
        symbol: str,
        queue_backlog: int,
        *,
        period: str | None = None,
        stream_kind: str | None = None,
    ) -> SymbolFreshness:
        state = self._state(symbol, period=period, stream_kind=stream_kind)
        state.queue_backlog = max(0, queue_backlog)
        return state

    def evaluate(self, *, now: str | None = None) -> dict[str, Any]:
        current = now or now_iso()
        resubscribe_symbols = []

        for state in self.state_by_symbol.values():
            if not state.subscribed:
                continue

            reason = self._degraded_reason(state, current)
            if reason:
                state.degraded = True
                state.degraded_reason = reason
                state.resubscribe_requested = True
                if state.symbol not in resubscribe_symbols:
                    resubscribe_symbols.append(state.symbol)
            else:
                state.degraded = False
                state.degraded_reason = None
                state.resubscribe_requested = False

        return {
            "evaluated_at": current,
            "resubscribe_symbols": resubscribe_symbols,
            "symbol_freshness": self.snapshot(),
        }

    def snapshot(self) -> dict[str, dict[str, Any]]:
        return {stream_id: state.as_dict() for stream_id, state in sorted(self.state_by_symbol.items())}

    def latest_event_at_by_symbol(self) -> dict[str, str]:
        latest: dict[str, str] = {}
        for state in self.state_by_symbol.values():
            if state.latest_event_at and state.latest_event_at > latest.get(state.symbol, ""):
                latest[state.symbol] = state.latest_event_at
        return latest

    def _state(
        self,
        symbol: str,
        *,
        period: str | None = None,
        stream_kind: str | None = None,
    ) -> SymbolFreshness:
        key = freshness_key(symbol, period=period, stream_kind=stream_kind)
        if key not in self.state_by_symbol:
            self.state_by_symbol[key] = SymbolFreshness(symbol=symbol, period=period, stream_kind=stream_kind)
        return self.state_by_symbol[key]

    def _degraded_reason(self, state: SymbolFreshness, current: str) -> str | None:
        if state.queue_backlog > self.policy.max_queue_backlog:
            return "queue_backlog_exceeded"
        if not state.latest_event_at:
            return "no_events_after_subscribe"
        if parse_ts(current) - parse_ts(state.latest_event_at) > self.policy.max_event_age_seconds:
            return "stale_event_stream"
        return None


def parse_ts(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def freshness_key(symbol: str, *, period: str | None = None, stream_kind: str | None = None) -> str:
    if period or stream_kind:
        return f"{symbol}|{period or ''}|{stream_kind or ''}"
    return symbol
