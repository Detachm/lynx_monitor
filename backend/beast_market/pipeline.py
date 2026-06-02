from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import re
from typing import Any

from .adapters import EventBus, SnapshotCache, validate_event_bus_record_inputs
from .contracts import (
    PROCESSED_TOPIC,
    RAW_TOPIC,
    make_processed_market_event,
    make_raw_market_event,
    make_terminal_message,
    now_iso,
    validate_processed_market_event,
    validate_terminal_message,
)
from .freshness import FreshnessPolicy, SymbolFreshnessTracker
from .mammoth_api import MammothAPI
from .trading_session import is_regular_hk_trading_minute

HK_TZ = timezone(timedelta(hours=8))
LATEST_BAR_WRITE_INTERVAL_SECONDS = 1.0


@dataclass
class HealthStatus:
    process: str = "starting"
    kafka: str = "unknown"
    redis: str = "unknown"
    kafka_lag: int | None = None
    latest_event_at_by_symbol: dict[str, str] = field(default_factory=dict)
    symbol_freshness: dict[str, dict[str, Any]] = field(default_factory=dict)

    def as_message(self, source: str = "backend") -> dict[str, Any]:
        return {
            "schema_version": 1,
            "type": "health",
            "source": source,
            "payload": {
                "process": self.process,
                "kafka": self.kafka,
                "redis": self.redis,
                "kafka_lag": self.kafka_lag,
                "latest_event_at_by_symbol": dict(self.latest_event_at_by_symbol),
                "symbol_freshness": dict(self.symbol_freshness),
            },
        }


@dataclass
class BodState:
    volume_baseline: float
    broker_mapping_by_code: dict[str, dict[str, Any]]
    highlighted_participants: set[str]
    participant_history_by_id: dict[str, list[dict[str, Any]]]


@dataclass
class TickStateUpdate:
    state: dict[str, Any]
    tick: dict[str, Any]
    alert: dict[str, Any] | None = None


@dataclass
class BrokerQueueStateUpdate:
    state: dict[str, Any]
    broker_queue: dict[str, list[dict[str, Any]]]
    applied: bool = True


@dataclass
class L2OrderBookStateUpdate:
    state: dict[str, Any]
    order_book: dict[str, Any]
    applied: bool = True


class RealtimeCollectorV2:
    def __init__(
        self,
        bus: EventBus,
        source: str = "xtquant",
        *,
        freshness_policy: FreshnessPolicy | None = None,
    ) -> None:
        self.bus = bus
        self.source = source
        self.seq_by_symbol: dict[str, int] = defaultdict(int)
        self.health = HealthStatus(process="running", kafka="connected", redis="unknown", kafka_lag=0)
        self.freshness = SymbolFreshnessTracker(freshness_policy)

    def subscribe_symbol(self, symbol: str) -> None:
        self.freshness.mark_subscribed(symbol)
        self._sync_freshness_health()

    def unsubscribe_symbol(self, symbol: str) -> None:
        self.freshness.mark_unsubscribed(symbol)
        self._sync_freshness_health()

    def record_queue_backlog(
        self,
        symbol: str,
        backlog: int,
        *,
        period: str | None = None,
        stream_kind: str | None = None,
    ) -> None:
        self.freshness.record_backlog(symbol, backlog, period=period, stream_kind=stream_kind)
        self._sync_freshness_health()

    def evaluate_freshness(self, *, now: str | None = None) -> dict[str, Any]:
        result = self.freshness.evaluate(now=now)
        self._sync_freshness_health()
        if result["resubscribe_symbols"]:
            self.health.process = "degraded"
        return result

    def ingest_tick(self, symbol: str, tick: dict[str, Any]) -> dict[str, Any]:
        payload = {
            "price": tick["price"],
            "volume": tick["volume"],
            "turnover": tick.get("turnover", tick["price"] * tick["volume"]),
            "side": tick.get("side", "neutral"),
            "broker_code": tick.get("broker_code", ""),
        }
        for optional_key in (
            "participant_id",
            "participant_name",
            "broker_name",
            "is_highlighted",
            "trade_type",
            "active_broker_code",
            "broker_code_source",
            "source_kind",
            "source_table",
            "trade_id",
            "row_hash",
            "source_event_id",
            "tick_ts",
        ):
            if optional_key in tick:
                payload[optional_key] = tick[optional_key]
        return self.ingest_event(
            kind="tick",
            symbol=symbol,
            period=str(tick.get("period") or "hktransaction"),
            source_ts=str(tick.get("timestamp") or now_iso()),
            payload=payload,
        )

    def ingest_broker_queue(self, symbol: str, broker_queue: dict[str, Any]) -> dict[str, Any]:
        return self.ingest_event(
            kind="broker_queue",
            symbol=symbol,
            period=str(broker_queue.get("period") or "hkbrokerqueueex"),
            source_ts=str(broker_queue.get("timestamp") or now_iso()),
            payload={
                "side": broker_queue.get("side"),
                "entries": broker_queue.get("entries", []),
            },
        )

    def ingest_l2_order_book(self, symbol: str, order_book: dict[str, Any]) -> dict[str, Any]:
        return self.ingest_event(
            kind="l2_order_book",
            symbol=symbol,
            period=str(order_book.get("period") or "l2thousand"),
            source_ts=str(order_book.get("timestamp") or now_iso()),
            payload={
                "ask": order_book.get("ask") or order_book.get("asks") or [],
                "bid": order_book.get("bid") or order_book.get("bids") or [],
            },
        )

    def ingest_event(
        self,
        *,
        kind: str,
        symbol: str,
        payload: dict[str, Any],
        period: str | None = None,
        source_ts: str | None = None,
    ) -> dict[str, Any]:
        self.freshness.mark_subscribed(symbol, period=period, stream_kind=kind)
        self.seq_by_symbol[symbol] += 1
        event = make_raw_market_event(
            kind=kind,
            symbol=symbol,
            source=self.source,
            period=period,
            seq=self.seq_by_symbol[symbol],
            source_ts=source_ts or now_iso(),
            payload=payload,
        )
        self.bus.publish(RAW_TOPIC, symbol, event)
        self.freshness.record_event(
            symbol,
            source_ts=event["source_ts"],
            ingest_ts=event["ingest_ts"],
            period=period,
            stream_kind=kind,
        )
        self.freshness.record_event(symbol, source_ts=event["source_ts"], ingest_ts=event["ingest_ts"])
        self._sync_freshness_health()
        return event

    def _sync_freshness_health(self) -> None:
        self.health.latest_event_at_by_symbol = self.freshness.latest_event_at_by_symbol()
        self.health.symbol_freshness = self.freshness.snapshot()


class OctopusComputeV2:
    def __init__(
        self,
        mammoth: MammothAPI,
        bus: EventBus,
        cache: SnapshotCache,
        *,
        big_trade_volume_baseline_ratio: float = 0.0005,
        big_trade_min_volume_threshold: int = 5_000,
        big_trade_turnover_threshold: float = 1_000_000.0,
        hydrate_historical_alerts: bool = False,
    ) -> None:
        self.mammoth = mammoth
        self.bus = bus
        self.cache = cache
        self.big_trade_volume_baseline_ratio = big_trade_volume_baseline_ratio
        self.big_trade_min_volume_threshold = big_trade_min_volume_threshold
        self.big_trade_turnover_threshold = big_trade_turnover_threshold
        self.hydrate_historical_alerts = hydrate_historical_alerts
        self.seq_by_symbol: dict[str, int] = defaultdict(int)
        self.state_by_symbol: dict[str, dict[str, Any]] = {}
        self.bod_by_symbol: dict[str, BodState] = {}
        self.latest_bar_flush_at_by_symbol: dict[str, float] = {}
        self.latest_bar_flushed_minute_by_symbol: dict[str, str] = {}
        self.health = HealthStatus(process="running", kafka="connected", redis="connected", kafka_lag=0)

    def get_state(self, symbol: str) -> dict[str, Any] | None:
        return self.state_by_symbol.get(symbol)

    def set_state(self, symbol: str, state: dict[str, Any]) -> dict[str, Any]:
        self.sanitize_state_for_source_policy(symbol, state)
        self.state_by_symbol[symbol] = state
        return state

    def has_state(self, symbol: str) -> bool:
        return symbol in self.state_by_symbol

    def is_big_trade(self, tick: dict[str, Any], bod: BodState | None) -> bool:
        return is_big_trade(
            tick,
            bod,
            volume_baseline_ratio=self.big_trade_volume_baseline_ratio,
            min_volume_threshold=self.big_trade_min_volume_threshold,
            turnover_threshold=self.big_trade_turnover_threshold,
        )

    def sanitize_state_for_source_policy(self, symbol: str, state: dict[str, Any]) -> None:
        self.filter_state_alerts_for_policy(symbol, state)
        sanitize_broker_queue_snapshot(state)

    def filter_state_alerts_for_policy(self, symbol: str, state: dict[str, Any]) -> None:
        alerts = state.get("alerts")
        if not isinstance(alerts, list):
            return
        bod = self.bod_by_symbol.get(symbol)
        filtered: list[dict[str, Any]] = []
        for alert in alerts:
            if not isinstance(alert, dict):
                continue
            if not is_canonical_big_trade_alert(alert):
                continue
            tick = {
                "price": float(alert.get("price") or 0),
                "volume": int(alert.get("volume") or 0),
                "turnover": float(alert.get("turnover") or 0),
            }
            if self.is_big_trade(tick, bod):
                filtered.append(alert)
        state["alerts"] = filtered[:500]

    def preload_symbols(self, symbols: list[str], trade_date: str) -> dict[str, dict[str, Any]]:
        return {symbol: self.preload_bod(symbol, trade_date) for symbol in symbols}

    def preload_bod(
        self,
        symbol: str,
        trade_date: str,
        *,
        cache_trade_date: str | None = None,
        requested_trade_date: str | None = None,
    ) -> dict[str, Any]:
        cache_date = cache_trade_date or trade_date
        requested_date = requested_trade_date or cache_date
        context = self.ensure_bod_context(symbol, trade_date, hydrate_participant_history=True)
        ccass_pair = context["ccass_pair"]
        holdings = context["holdings"]
        broker_queue = to_broker_queue(self.mammoth.get_broker_queue(symbol, trade_date))
        daily_bars = context["daily_bars"]
        previous_daily_bar = context["previous_daily_bar"]
        current_daily_bar = context["current_daily_bar"]
        previous_close = float(previous_daily_bar["close"]) if previous_daily_bar else (
            float(current_daily_bar["close"]) if current_daily_bar else 0.0
        )
        opening_reference = current_daily_bar if current_daily_bar and current_daily_bar["trade_date"] == trade_date else None
        try:
            minute_bars = to_minute_bars(self.mammoth.get_minute_bars(symbol, trade_date))
        except Exception:
            minute_bars = []
        latest_minute_bar = minute_bars[-1] if minute_bars else None
        initial_price = float(latest_minute_bar["price"]) if latest_minute_bar else previous_close
        initial_volume = sum(int(bar.get("volume") or 0) for bar in minute_bars)
        initial_turnover = sum(float(bar.get("turnover") or 0) for bar in minute_bars)
        alerts, latest_tick_ts = (
            self._historical_big_trade_alerts(symbol, trade_date)
            if self.hydrate_historical_alerts
            else ([], "")
        )
        instrument_name = ""
        get_instrument_name = getattr(self.mammoth, "get_instrument_name", None)
        try:
            instrument_name = get_instrument_name(symbol) if callable(get_instrument_name) else ""
        except Exception:
            instrument_name = ""
        snapshot = {
            "snapshot": {
                "symbol": symbol,
                "name": instrument_name or symbol,
                "currency": "HKD",
                "tradeDate": trade_date,
                "requestedTradeDate": requested_date,
                "isHistoricalSession": requested_date != trade_date,
                "price": initial_price,
                "previousClose": previous_close,
                "open": float(opening_reference["open"]) if opening_reference else previous_close,
                "high": float(opening_reference["high"]) if opening_reference else previous_close,
                "low": float(opening_reference["low"]) if opening_reference else previous_close,
                "volume": initial_volume,
                "turnover": initial_turnover,
                "change": initial_price - previous_close,
                "changePercent": 0.0 if previous_close == 0 else (initial_price - previous_close) / previous_close * 100,
                "updatedAt": now_iso(),
                "nextSessionPreviousClose": float(current_daily_bar["close"]) if current_daily_bar else initial_price,
            },
            "minute_bars": minute_bars,
            "alerts": alerts,
            "broker_queue": broker_queue,
            "l2_order_book": empty_l2_order_book(),
            "ccass_holdings": holdings,
            "ccass_evidence": {
                "date": ccass_pair["current_date"],
                "current_date": ccass_pair["current_date"],
                "previous_date": ccass_pair["previous_date"],
            },
            "freshness": {
                "updated_at": now_iso(),
                "requested_trade_date": requested_date,
                "effective_trade_date": trade_date,
                "runtime_state": "WARM",
                "source_dates": {
                    "minute_bars": trade_date if minute_bars else "",
                    "daily_bars": str(current_daily_bar["trade_date"]) if current_daily_bar else "",
                    "ccass_current": ccass_pair["current_date"],
                    "ccass_previous": ccass_pair["previous_date"],
                    "trade_ticks": trade_date if latest_tick_ts else "",
                },
                "degraded_reasons": [] if minute_bars else ["missing_minute_bars"],
            },
        }
        if latest_tick_ts:
            snapshot["freshness"]["source_ts"] = latest_tick_ts
        self.state_by_symbol[symbol] = snapshot
        self._cache_set_terminal_snapshot(cache_date, symbol, snapshot)
        return snapshot

    def _historical_big_trade_alerts(self, symbol: str, trade_date: str) -> tuple[list[dict[str, Any]], str]:
        bod = self.bod_by_symbol.get(symbol)
        if bod is None:
            return [], ""
        try:
            rows = self.mammoth.get_trade_ticks(symbol, trade_date)
        except Exception:
            return [], ""

        alerts: list[dict[str, Any]] = []
        latest_tick_ts = ""
        rows.sort(key=lambda row: str(row.get("tick_ts") or ""))
        for index, row in enumerate(rows, start=1):
            tick_ts = str(row.get("tick_ts") or "")
            if tick_ts:
                latest_tick_ts = tick_ts
            tick = {
                "timestamp": tick_ts,
                "price": float(row.get("price") or 0),
                "volume": int(float(row.get("volume") or 0)),
                "turnover": float(row.get("turnover") or 0),
                "direction": "flat",
            }
            if not self.is_big_trade(tick, bod):
                continue
            payload = {
                "source_kind": "canonical_trade_tick",
                "source_table": "trade_ticks",
                "source_event_id": str(row.get("row_hash") or row.get("trade_id") or ""),
                "price": tick["price"],
                "volume": tick["volume"],
                "turnover": tick["turnover"],
                "side": row.get("side"),
                "broker_code": row.get("broker_code"),
                "broker_name": row.get("broker_name"),
                "participant_id": row.get("participant_id"),
                "participant_name": row.get("participant_name"),
                "trade_type": row.get("trade_type"),
                "trade_id": row.get("trade_id"),
                "row_hash": row.get("row_hash"),
            }
            raw_event = {
                "event_id": (
                    f"historical-alert-{symbol}-{safe_alert_id_part(tick_ts)}-"
                    f"{safe_alert_id_part(str(row.get('trade_id') or row.get('row_hash') or index))}"
                ),
                "kind": "tick",
                "payload": payload,
            }
            if not is_trade_tick_alert_source(raw_event):
                continue
            alerts.append(make_big_trade_alert(raw_event, tick, bod))
        alerts.sort(key=lambda alert: str(alert.get("timestamp") or ""), reverse=True)
        return alerts[:500], latest_tick_ts

    def ensure_bod_context(
        self,
        symbol: str,
        trade_date: str,
        *,
        hydrate_participant_history: bool = False,
    ) -> dict[str, Any]:
        ccass_pair = self.mammoth.get_ccass_holding_pair(symbol, trade_date)
        previous_holdings_by_id = {
            str(row["participant_id"]): row for row in ccass_pair["previous_rows"]
        }
        holdings = [
            to_holding(
                row,
                current_date=ccass_pair["current_date"],
                previous_date=ccass_pair["previous_date"],
                previous_row=previous_holdings_by_id.get(str(row["participant_id"])),
            )
            for row in ccass_pair["current_rows"]
        ]
        holdings.sort(key=lambda holding: int(holding["shares"]), reverse=True)
        daily_bars = self.mammoth.get_recent_daily_bars(symbol, trade_date, 2)
        previous_daily_bar = self.mammoth.get_previous_daily_bar(symbol, trade_date)
        current_daily_bar = latest_row_on_or_before(daily_bars, trade_date)
        broker_mapping_by_code = {
            str(row["broker_code"]): row for row in self.mammoth.get_broker_mapping()
        }
        highlighted_participants = {
            holding["participantCode"]
            for holding in holdings
            if holding["isHighlighted"]
        } | {
            holding["participantName"]
            for holding in holdings
            if holding["isHighlighted"]
        }
        participant_history_by_id = {}
        if hydrate_participant_history:
            participant_history_by_id = {
                holding["participantCode"]: participant_history_points(
                    self.mammoth.get_participant_history(symbol, holding["participantCode"], 7, trade_date=trade_date)
                )
                for holding in holdings
            }
            for participant_id, history in participant_history_by_id.items():
                self._cache_set_holding_history(symbol, participant_id, history)
        self.bod_by_symbol[symbol] = BodState(
            volume_baseline=volume_baseline(daily_bars, trade_date),
            broker_mapping_by_code=broker_mapping_by_code,
            highlighted_participants=highlighted_participants,
            participant_history_by_id=participant_history_by_id,
        )
        return {
            "ccass_pair": ccass_pair,
            "holdings": holdings,
            "daily_bars": daily_bars,
            "previous_daily_bar": previous_daily_bar,
            "current_daily_bar": current_daily_bar,
        }

    def _cache_set_holding_history(self, symbol: str, participant_id: str, history: list[dict[str, Any]]) -> None:
        try:
            self.cache.set_holding_history(symbol, participant_id, history)
        except Exception as error:
            self._mark_redis_degraded(symbol, f"redis_holding_history_write_failed: {error}")

    def _cache_set_terminal_snapshot(self, trade_date: str, symbol: str, state: dict[str, Any]) -> None:
        try:
            self.cache.set_terminal_snapshot(trade_date, symbol, state)
            if self.health.redis != "degraded":
                self.health.redis = "connected"
        except Exception as error:
            self._mark_redis_degraded(symbol, f"redis_terminal_snapshot_write_failed: {error}", state)

    def _mark_redis_degraded(self, symbol: str, reason: str, state: dict[str, Any] | None = None) -> None:
        self.health.process = "degraded"
        self.health.redis = "degraded"
        if state is None:
            state = self.state_by_symbol.get(symbol)
        if not isinstance(state, dict):
            return
        freshness = dict(state.get("freshness") or {})
        reasons = list(freshness.get("degraded_reasons") or [])
        if reason not in reasons:
            reasons.append(reason)
        freshness["degraded"] = True
        freshness["degraded_reasons"] = reasons
        state["freshness"] = freshness

    def process_raw_event(self, raw_event: dict[str, Any], trade_date: str) -> list[dict[str, Any]]:
        symbol = raw_event["symbol"]
        state = self.get_state(symbol)
        if state is None:
            raise RuntimeError(f"BOD state must be preloaded before realtime processing: {symbol}")
        return self.process_raw_event_with_state(raw_event, trade_date, state)

    def process_raw_event_with_state(
        self,
        raw_event: dict[str, Any],
        trade_date: str,
        state: dict[str, Any],
    ) -> list[dict[str, Any]]:
        symbol = raw_event["symbol"]
        if raw_event["kind"] == "broker_queue":
            return self._process_broker_queue(raw_event, trade_date, state)

        if raw_event["kind"] == "l2_order_book":
            return self._process_l2_order_book(raw_event, trade_date, state)

        if raw_event["kind"] != "tick":
            return []

        if is_minute_bar_source(raw_event):
            return self.process_minute_bar_event(raw_event, trade_date, state)

        if is_trade_tick_alert_source(raw_event):
            return self.process_trade_tick_alert_event(raw_event, trade_date, state)

        return self.process_realtime_tick_event(raw_event, trade_date, state)

    def process_realtime_tick_event(
        self,
        raw_event: dict[str, Any],
        trade_date: str,
        state: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Apply a non-canonical realtime price tick without alert side effects."""
        symbol = raw_event["symbol"]
        self.seq_by_symbol[symbol] += 1
        period = str(raw_event.get("period") or "").strip().lower()
        ordinary_transaction = period in {"hktransaction", "tick", "trade_tick"}
        update = self.apply_tick_to_state(
            state,
            raw_event,
            trade_date,
            minute_freshness_key="latest_bar" if ordinary_transaction else "minute_bars",
            updates_chart=not ordinary_transaction,
            updates_snapshot_totals=not ordinary_transaction,
            chart_update=not ordinary_transaction,
            updates_alerts=False,
        )
        tick = update.tick
        snapshot_seq = self.seq_by_symbol[symbol]
        if ordinary_transaction and not self.should_publish_latest_bar(symbol, tick):
            self.health.latest_event_at_by_symbol[symbol] = raw_event["source_ts"]
            self.health.kafka_lag = self.bus.lag(RAW_TOPIC, committed_offset=self.seq_by_symbol[symbol])
            return []

        self._cache_set_terminal_snapshot(trade_date, symbol, state)

        snapshot_event = make_processed_market_event(
            result_type="snapshot",
            symbol=symbol,
            source="octopus",
            seq=snapshot_seq,
            source_ts=raw_event["source_ts"],
            payload=state,
        )
        self.bus.publish(PROCESSED_TOPIC, symbol, snapshot_event)
        self.health.latest_event_at_by_symbol[symbol] = raw_event["source_ts"]
        self.health.kafka_lag = self.bus.lag(RAW_TOPIC, committed_offset=self.seq_by_symbol[symbol])
        return [snapshot_event]

    def apply_tick_to_state(
        self,
        state: dict[str, Any],
        raw_event: dict[str, Any],
        trade_date: str,
        *,
        minute_freshness_key: str = "minute_bars",
        updates_chart: bool = True,
        updates_snapshot_totals: bool = True,
        chart_update: bool = True,
        updates_alerts: bool = True,
    ) -> TickStateUpdate:
        symbol = raw_event["symbol"]
        payload = raw_event["payload"]
        state["minute_bars"] = prune_future_minute_bars(state.get("minute_bars", []))
        tick = {
            "timestamp": raw_event["source_ts"],
            "price": float(payload["price"]),
            "volume": int(payload["volume"]),
            "turnover": float(payload["turnover"]),
            "direction": "flat",
            "chart_update": chart_update,
        }
        is_full_tick_seed = is_full_tick_seed_event(raw_event)
        rollover = snapshot_trade_date(state) != trade_date
        if rollover:
            rollover_previous_close = float(
                state["snapshot"].get("nextSessionPreviousClose")
                or state["snapshot"].get("price")
                or state["snapshot"].get("previousClose")
                or 0
            )
            state["minute_bars"] = []
            state["alerts"] = []
            state["snapshot"] = {
                **state["snapshot"],
                "tradeDate": trade_date,
                "requestedTradeDate": trade_date,
                "isHistoricalSession": False,
                "previousClose": rollover_previous_close,
                "open": tick["price"],
                "high": tick["price"],
                "low": tick["price"],
                "volume": 0,
                "turnover": 0.0,
            }
        previous_price = float(state["snapshot"]["price"])
        tick["direction"] = "up" if tick["price"] > previous_price else "down" if tick["price"] < previous_price else "flat"
        state["snapshot"] = {
            **state["snapshot"],
            "price": tick["price"],
            "high": max(float(state["snapshot"].get("high") or tick["price"]), tick["price"]),
            "low": min(float(state["snapshot"].get("low") or tick["price"]), tick["price"]),
            "volume": (
                tick["volume"]
                if is_full_tick_seed
                else (
                    int(state["snapshot"]["volume"]) + tick["volume"]
                    if updates_snapshot_totals
                    else int(state["snapshot"].get("volume") or 0)
                )
            ),
            "turnover": (
                tick["turnover"]
                if is_full_tick_seed
                else (
                    float(state["snapshot"]["turnover"]) + tick["turnover"]
                    if updates_snapshot_totals
                    else float(state["snapshot"].get("turnover") or 0.0)
                )
            ),
            "change": tick["price"] - float(state["snapshot"]["previousClose"]),
            "updatedAt": raw_event["ingest_ts"],
        }
        previous_close = float(state["snapshot"]["previousClose"])
        state["snapshot"]["changePercent"] = 0.0 if previous_close == 0 else state["snapshot"]["change"] / previous_close * 100
        writes_minute_bar = updates_chart and (not is_full_tick_seed) and is_regular_hk_trading_minute(tick["timestamp"], trade_date)
        if writes_minute_bar:
            state["minute_bars"] = upsert_minute_bar(state["minute_bars"], tick, trade_date)
        state["last_tick"] = tick
        state["freshness"] = realtime_freshness(
            state,
            raw_event,
            trade_date,
            updates_minute_bars=writes_minute_bar and minute_freshness_key == "minute_bars",
            updates_latest_bar=minute_freshness_key == "latest_bar",
        )

        alert = None
        bod = self.bod_by_symbol.get(symbol)
        if updates_alerts and is_trade_tick_alert_source(raw_event) and not is_full_tick_seed and self.is_big_trade(tick, bod):
            alert = make_big_trade_alert(raw_event, tick, bod)
            state["alerts"] = prepend_unique_alert(alert, state.get("alerts", []))
        return TickStateUpdate(state=state, tick=tick, alert=alert)

    def process_minute_bar_event(
        self,
        raw_event: dict[str, Any],
        trade_date: str,
        state: dict[str, Any],
    ) -> list[dict[str, Any]]:
        symbol = raw_event["symbol"]
        self.apply_minute_bar_to_state(state, raw_event, trade_date)
        self.seq_by_symbol[symbol] += 1
        self._cache_set_terminal_snapshot(trade_date, symbol, state)
        event = make_processed_market_event(
            result_type="snapshot",
            symbol=symbol,
            source="octopus",
            seq=self.seq_by_symbol[symbol],
            source_ts=raw_event["source_ts"],
            payload=state,
        )
        self.bus.publish(PROCESSED_TOPIC, symbol, event)
        self.health.latest_event_at_by_symbol[symbol] = raw_event["source_ts"]
        self.health.kafka_lag = self.bus.lag(RAW_TOPIC, committed_offset=self.seq_by_symbol[symbol])
        return [event]

    def process_minute_bar_backfill(
        self,
        symbol: str,
        bars: list[dict[str, Any]],
        trade_date: str,
        *,
        source: str = "xtquant_1m_backfill",
    ) -> list[dict[str, Any]]:
        state = self.get_state(symbol)
        if state is None:
            raise RuntimeError(f"BOD state must be preloaded before minute backfill: {symbol}")
        state["minute_bars"] = prune_future_minute_bars(state.get("minute_bars", []))
        normalized = [
            bar
            for bar in (
                minute_bar_from_payload({"source_ts": str(raw_bar.get("timestamp") or ""), "payload": raw_bar})
                for raw_bar in bars
            )
            if bar is not None and not is_current_or_future_minute_bar(str(bar["timestamp"]))
        ]
        if not normalized:
            return []
        normalized.sort(key=lambda bar: minute_bucket(str(bar["timestamp"])))
        latest_source_ts = str(normalized[-1]["timestamp"])
        raw_event = {
            "kind": "tick",
            "symbol": symbol,
            "source": source,
            "period": "1m",
            "source_ts": latest_source_ts,
            "ingest_ts": now_iso(),
            "payload": normalized[-1],
            "event_id": f"raw-{source}-{symbol}-{safe_alert_id_part(latest_source_ts)}",
            "seq": self.seq_by_symbol[symbol] + 1,
        }
        for bar in normalized:
            self.apply_minute_bar_to_state(
                state,
                {**raw_event, "source_ts": str(bar["timestamp"]), "payload": bar},
                trade_date,
            )
        self.seq_by_symbol[symbol] += 1
        self._cache_set_terminal_snapshot(trade_date, symbol, state)
        event = make_processed_market_event(
            result_type="snapshot",
            symbol=symbol,
            source="octopus",
            seq=self.seq_by_symbol[symbol],
            source_ts=latest_source_ts,
            payload=state,
        )
        self.bus.publish(PROCESSED_TOPIC, symbol, event)
        self.health.latest_event_at_by_symbol[symbol] = latest_source_ts
        self.health.kafka_lag = self.bus.lag(RAW_TOPIC, committed_offset=self.seq_by_symbol[symbol])
        return [event]

    def apply_minute_bar_to_state(
        self,
        state: dict[str, Any],
        raw_event: dict[str, Any],
        trade_date: str,
    ) -> TickStateUpdate:
        rollover_realtime_session(state, trade_date, raw_event["ingest_ts"])
        state["minute_bars"] = prune_future_minute_bars(state.get("minute_bars", []))
        bar = minute_bar_from_payload(raw_event)
        if bar is None or is_current_or_future_minute_bar(str(bar["timestamp"])):
            return TickStateUpdate(state=state, tick={})
        previous_price = float(state["snapshot"].get("price") or bar["price"])
        bar["direction"] = "up" if bar["price"] > previous_price else "down" if bar["price"] < previous_price else "flat"
        bar["is_confirmed_minute_bar"] = True
        bar["replace"] = True
        state["minute_bars"] = upsert_confirmed_minute_bar(state.get("minute_bars", []), bar, trade_date)
        update_snapshot_from_minute_bars(state, raw_event["ingest_ts"])
        state["last_tick"] = bar
        state["freshness"] = realtime_freshness(
            state,
            raw_event,
            trade_date,
            updates_minute_bars=True,
            updates_latest_bar=False,
        )
        return TickStateUpdate(state=state, tick=bar)

    def process_historical_alert_event(self, raw_event: dict[str, Any], trade_date: str) -> list[dict[str, Any]]:
        """Generate alert state from historical ticks without rebuilding chart bars.

        Historical ticks may be replayed for alert continuity, but production
        chart initialization must stay on native 1m bars.
        """
        symbol = raw_event["symbol"]
        if symbol not in self.state_by_symbol or raw_event["kind"] != "tick" or not is_trade_tick_alert_source(raw_event):
            return []
        return self.process_trade_tick_alert_only_event(raw_event, trade_date, self.state_by_symbol[symbol])

    def process_trade_tick_alert_only_event(
        self,
        raw_event: dict[str, Any],
        trade_date: str,
        state: dict[str, Any],
    ) -> list[dict[str, Any]]:
        symbol = raw_event["symbol"]
        payload = raw_event["payload"]
        tick = {
            "timestamp": raw_event["source_ts"],
            "price": float(payload["price"]),
            "volume": int(payload["volume"]),
            "turnover": float(payload["turnover"]),
            "direction": "flat",
        }
        bod = self.bod_by_symbol.get(symbol)
        if not self.is_big_trade(tick, bod):
            self.health.latest_event_at_by_symbol[symbol] = raw_event["source_ts"]
            return []

        self.seq_by_symbol[symbol] += 1
        alert = make_big_trade_alert(raw_event, tick, bod)
        state["alerts"] = prepend_unique_alert(alert, state.get("alerts", []))
        freshness = dict(state.get("freshness") or {})
        source_dates = dict(freshness.get("source_dates") or {})
        source_dates["trade_ticks"] = trade_date
        state["freshness"] = {
            **freshness,
            "updated_at": raw_event["ingest_ts"],
            "source_ts": raw_event["source_ts"],
            "ingest_ts": raw_event["ingest_ts"],
            "source_dates": source_dates,
        }
        alert_event = make_processed_market_event(
            result_type="big_trade_alert",
            symbol=symbol,
            source="octopus",
            seq=self.seq_by_symbol[symbol],
            source_ts=raw_event["source_ts"],
            payload={"alert": alert, "freshness": state["freshness"]},
        )
        self._cache_set_terminal_snapshot(trade_date, symbol, state)
        self.bus.publish(PROCESSED_TOPIC, symbol, alert_event)
        self.health.latest_event_at_by_symbol[symbol] = raw_event["source_ts"]
        self.health.kafka_lag = self.bus.lag(RAW_TOPIC, committed_offset=self.seq_by_symbol[symbol])
        return [alert_event]

    def process_trade_tick_alert_event(
        self,
        raw_event: dict[str, Any],
        trade_date: str,
        state: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Apply hktransaction as trade metadata/alerts without mutating chart bars.

        xt hktransaction volume is not authoritative for chart aggregation in this
        runtime. Native 1m bars are the only writer for minute_bars and cumulative
        snapshot volume/turnover after startup/full_tick seeding.
        """
        symbol = raw_event["symbol"]
        update = self.apply_tick_to_state(
            state,
            raw_event,
            trade_date,
            minute_freshness_key="latest_bar",
            updates_chart=False,
            updates_snapshot_totals=False,
            chart_update=False,
        )
        tick = update.tick
        alert = update.alert
        publish_snapshot = self.should_publish_latest_bar(symbol, tick)
        processed_events: list[dict[str, Any]] = []

        if publish_snapshot:
            self.seq_by_symbol[symbol] += 1
            snapshot_event = make_processed_market_event(
                result_type="snapshot",
                symbol=symbol,
                source="octopus",
                seq=self.seq_by_symbol[symbol],
                source_ts=raw_event["source_ts"],
                payload=state,
            )
            self.bus.publish(PROCESSED_TOPIC, symbol, snapshot_event)
            processed_events.append(snapshot_event)

        if alert is not None:
            self.seq_by_symbol[symbol] += 1
            alert_event = make_processed_market_event(
                result_type="big_trade_alert",
                symbol=symbol,
                source="octopus",
                seq=self.seq_by_symbol[symbol],
                source_ts=raw_event["source_ts"],
                payload={"alert": alert, "freshness": state["freshness"]},
            )
            self.bus.publish(PROCESSED_TOPIC, symbol, alert_event)
            processed_events.append(alert_event)

        if processed_events:
            self._cache_set_terminal_snapshot(trade_date, symbol, state)
        self.health.latest_event_at_by_symbol[symbol] = raw_event["source_ts"]
        self.health.kafka_lag = self.bus.lag(RAW_TOPIC, committed_offset=self.seq_by_symbol[symbol])
        return processed_events

    def should_publish_latest_bar(self, symbol: str, tick: dict[str, Any]) -> bool:
        timestamp = str(tick.get("timestamp") or "")
        if not timestamp:
            return False
        minute_ts = minute_bucket(timestamp)
        previous_minute = self.latest_bar_flushed_minute_by_symbol.get(symbol)
        now_value = iso_timestamp_seconds(timestamp)
        previous_flush = self.latest_bar_flush_at_by_symbol.get(symbol)
        if previous_minute != minute_ts or previous_flush is None or now_value - previous_flush >= LATEST_BAR_WRITE_INTERVAL_SECONDS:
            self.latest_bar_flushed_minute_by_symbol[symbol] = minute_ts
            self.latest_bar_flush_at_by_symbol[symbol] = now_value
            return True
        return False

    def _process_broker_queue(
        self,
        raw_event: dict[str, Any],
        trade_date: str,
        state: dict[str, Any],
    ) -> list[dict[str, Any]]:
        symbol = raw_event["symbol"]
        update = self.apply_broker_queue_to_state(state, raw_event, trade_date)
        if not update.applied:
            return []
        self.seq_by_symbol[symbol] += 1
        self._cache_set_terminal_snapshot(trade_date, symbol, state)
        delta = broker_queue_delta_for_event(update.broker_queue, raw_event["payload"].get("side"))
        if not delta:
            return []
        event = make_processed_market_event(
            result_type="broker_queue",
            symbol=symbol,
            source="octopus",
            seq=self.seq_by_symbol[symbol],
            source_ts=raw_event["source_ts"],
            payload={
                "side": raw_event["payload"].get("side"),
                "broker_queue": delta,
                "freshness": state["freshness"],
            },
        )
        self.bus.publish(PROCESSED_TOPIC, symbol, event)
        self.health.latest_event_at_by_symbol[symbol] = raw_event["source_ts"]
        self.health.kafka_lag = self.bus.lag(RAW_TOPIC, committed_offset=self.seq_by_symbol[symbol])
        return [event]

    def _process_l2_order_book(
        self,
        raw_event: dict[str, Any],
        trade_date: str,
        state: dict[str, Any],
    ) -> list[dict[str, Any]]:
        symbol = raw_event["symbol"]
        update = self.apply_l2_order_book_to_state(state, raw_event, trade_date)
        if not update.applied:
            return []
        self.seq_by_symbol[symbol] += 1
        self._cache_set_terminal_snapshot(trade_date, symbol, state)
        event = make_processed_market_event(
            result_type="l2_order_book",
            symbol=symbol,
            source="octopus",
            seq=self.seq_by_symbol[symbol],
            source_ts=raw_event["source_ts"],
            payload=state,
        )
        self.bus.publish(PROCESSED_TOPIC, symbol, event)
        self.health.latest_event_at_by_symbol[symbol] = raw_event["source_ts"]
        self.health.kafka_lag = self.bus.lag(RAW_TOPIC, committed_offset=self.seq_by_symbol[symbol])
        return [event]

    def apply_broker_queue_to_state(
        self,
        state: dict[str, Any],
        raw_event: dict[str, Any],
        trade_date: str,
    ) -> BrokerQueueStateUpdate:
        rollover_realtime_session(state, trade_date, raw_event["ingest_ts"])
        broker_queue = normalize_raw_broker_queue(raw_event["payload"], self.bod_by_symbol.get(raw_event["symbol"]))
        if not queue_has_entries(broker_queue):
            return BrokerQueueStateUpdate(state=state, broker_queue={"ask": [], "bid": []}, applied=False)
        merged = merge_broker_queue(state["broker_queue"], broker_queue)
        state["broker_queue"] = enrich_broker_queue_with_depth(merged, state.get("l2_order_book"))
        broker_queue = enrich_broker_queue_with_depth(broker_queue, state.get("l2_order_book"))
        state["freshness"] = realtime_freshness(state, raw_event, trade_date)
        return BrokerQueueStateUpdate(state=state, broker_queue=broker_queue)

    def apply_l2_order_book_to_state(
        self,
        state: dict[str, Any],
        raw_event: dict[str, Any],
        trade_date: str,
    ) -> L2OrderBookStateUpdate:
        rollover_realtime_session(state, trade_date, raw_event["ingest_ts"])
        order_book = normalize_l2_order_book(raw_event["payload"])
        if not order_book_has_valid_depth(order_book):
            existing = state.get("l2_order_book") if isinstance(state.get("l2_order_book"), dict) else empty_l2_order_book()
            return L2OrderBookStateUpdate(state=state, order_book=existing, applied=False)
        state["l2_order_book"] = order_book
        state["broker_queue"] = enrich_broker_queue_with_depth(state.get("broker_queue"), order_book)
        state["freshness"] = realtime_freshness(state, raw_event, trade_date)
        return L2OrderBookStateUpdate(state=state, order_book=order_book)


class GatewayV2:
    def __init__(self, bus: EventBus, cache: SnapshotCache, *, runtime_epoch: str = "") -> None:
        self.bus = bus
        self.cache = cache
        self.runtime_epoch = runtime_epoch or now_iso()
        self.seq_by_symbol: dict[str, int] = defaultdict(int)
        self.processed_records_consumed = 0
        self.shadow_processed_records_drained = 0
        self.direct_runtime_messages_emitted = 0
        self.terminal_messages_emitted = 0
        self.last_terminal_messages: list[dict[str, Any]] = []
        self.health = HealthStatus(process="running", kafka="connected", redis="connected", kafka_lag=0)

    def subscribe(self, symbol: str, trade_date: str) -> dict[str, Any]:
        snapshot = self.cache.get_terminal_snapshot(trade_date, symbol)
        if snapshot is None:
            raise KeyError(f"missing terminal snapshot for {symbol} on {trade_date}")
        return self.snapshot_message(symbol, snapshot)

    def snapshot_message(self, symbol: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._terminal("snapshot", symbol, payload)

    def holding_history_response(
        self,
        symbol: str,
        participant_name: str,
        days: int,
        history: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return self._terminal(
            "holding_name_click_response",
            symbol,
            {
                "participant_name": participant_name,
                "days": days,
                "history": history,
                "freshness": {"updated_at": now_iso()},
            },
        )

    def to_terminal_messages(self) -> list[dict[str, Any]]:
        messages = []
        if hasattr(self.bus, "poll"):
            records = self.bus.poll(PROCESSED_TOPIC, self.bus.committed_offset(PROCESSED_TOPIC))
        else:
            records = self.bus.read(PROCESSED_TOPIC)
        for record in records:
            validate_event_bus_record_inputs(PROCESSED_TOPIC, record)
            processed = record["value"]
            messages.extend(self.terminal_messages_from_processed([processed]))
        if records:
            self.bus.commit(PROCESSED_TOPIC, next_committed_offset(records, self.bus.committed_offset(PROCESSED_TOPIC)))
        self.processed_records_consumed += len(records)
        self.terminal_messages_emitted += len(messages)
        self.last_terminal_messages = messages
        self.health.kafka_lag = self.bus.lag(PROCESSED_TOPIC, committed_offset=self.bus.committed_offset(PROCESSED_TOPIC))
        return messages

    def drain_processed_shadow_records(self) -> int:
        if hasattr(self.bus, "poll"):
            records = self.bus.poll(PROCESSED_TOPIC, self.bus.committed_offset(PROCESSED_TOPIC))
        else:
            records = self.bus.read(PROCESSED_TOPIC)
        for record in records:
            validate_event_bus_record_inputs(PROCESSED_TOPIC, record)
            validate_processed_market_event(record["value"])
        if records:
            self.bus.commit(PROCESSED_TOPIC, next_committed_offset(records, self.bus.committed_offset(PROCESSED_TOPIC)))
        self.shadow_processed_records_drained += len(records)
        self.health.kafka_lag = self.bus.lag(PROCESSED_TOPIC, committed_offset=self.bus.committed_offset(PROCESSED_TOPIC))
        return len(records)

    def record_direct_terminal_messages(self, messages: list[dict[str, Any]]) -> None:
        if not messages:
            return
        for message in messages:
            validate_terminal_message(message)
        self.last_terminal_messages = messages
        self.direct_runtime_messages_emitted += len(messages)
        self.terminal_messages_emitted += len(messages)

    def terminal_messages_from_processed(self, processed_events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        messages = []
        for processed in processed_events:
            validate_processed_market_event(processed)
            if processed["result_type"] == "snapshot":
                messages.append(self._terminal("tick_realtime", processed["symbol"], {
                    "tick": processed["payload"].get("last_tick") or processed["payload"]["minute_bars"][-1],
                    "snapshot": processed["payload"]["snapshot"],
                    "freshness": processed["payload"]["freshness"],
                }, source_ts=processed["source_ts"]))
            elif processed["result_type"] == "l2_order_book":
                messages.append(self._terminal("snapshot", processed["symbol"], processed["payload"], source_ts=processed["source_ts"]))
            elif processed["result_type"] == "big_trade_alert":
                messages.append(self._terminal("alert_realtime", processed["symbol"], processed["payload"], source_ts=processed["source_ts"]))
            elif processed["result_type"] == "broker_queue":
                messages.append(self._terminal("queue_realtime", processed["symbol"], processed["payload"], source_ts=processed["source_ts"]))
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
        message = make_terminal_message(
            message_type=message_type,
            symbol=symbol,
            source="gateway",
            seq=self.seq_by_symbol[symbol],
            source_ts=source_ts,
            payload=payload,
        )
        message["runtime_epoch"] = self.runtime_epoch
        validate_terminal_message(message)
        self.health.latest_event_at_by_symbol[symbol] = message["source_ts"]
        return message


def next_committed_offset(records: list[dict[str, Any]], current_offset: int) -> int:
    offsets = [int(record["offset"]) for record in records if "offset" in record]
    if offsets:
        return max(offsets) + 1
    return current_offset + len(records)


def to_holding(
    row: dict[str, Any],
    *,
    current_date: str = "",
    previous_date: str = "",
    previous_row: dict[str, Any] | None = None,
) -> dict[str, Any]:
    shares = int(row["shares"])
    change = shares - int(previous_row["shares"]) if previous_row is not None else int(row.get("change") or 0)
    date = str(current_date or row["trade_date"])
    return {
        "participantName": row["participant_name"],
        "participantCode": row["participant_id"],
        "shares": shares,
        "percent": float(row["percent"]),
        "change": change,
        "date": date,
        "current_date": date,
        "previous_date": previous_date,
        "isHighlighted": bool(row.get("is_highlighted", False)),
    }


def to_holding_history_point(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "date": str(row["trade_date"]),
        "shares": int(row["shares"]),
        "percent": float(row["percent"]),
        "change": int(row.get("change") or 0),
    }


def participant_history_points(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    points = []
    previous_row: dict[str, Any] | None = None
    for row in rows:
        point = to_holding_history_point(row)
        if previous_row is not None:
            point["change"] = int(row["shares"]) - int(previous_row["shares"])
        points.append(point)
        previous_row = row
    return points[-7:]


def to_minute_bars(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    bars: list[dict[str, Any]] = []
    previous_close: float | None = None
    for row in rows:
        if not is_regular_hk_trading_minute(str(row["bar_ts"]), str(row.get("trade_date") or "")):
            continue
        close = float(row["close"])
        direction = "flat"
        if previous_close is not None:
            direction = "up" if close > previous_close else "down" if close < previous_close else "flat"
        bars.append(
            {
                "timestamp": str(row["bar_ts"]),
                "price": close,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": close,
                "volume": int(row["volume"]),
                "turnover": float(row["turnover"]),
                "direction": direction,
                "is_confirmed_minute_bar": True,
                "replace": True,
            }
        )
        previous_close = close
    return bars[-420:]


def latest_row_on_or_before(rows: list[dict[str, Any]], trade_date: str) -> dict[str, Any] | None:
    candidates = [row for row in rows if row.get("trade_date") <= trade_date]
    if not candidates:
        return None
    candidates.sort(key=lambda row: row["trade_date"])
    return candidates[-1]


def to_broker_queue(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped = {"ask": [], "bid": []}
    for row in rows:
        side = str(row["side"])
        if side not in grouped:
            continue
        grouped[side].append(row)
    queue = {"ask": [], "bid": []}
    for side, side_rows in grouped.items():
        side_rows.sort(key=lambda row: (int(row.get("position") or 0), str(row.get("queue_ts") or "")))
        for index, row in enumerate(side_rows, start=1):
            price = float(row.get("price") or 0)
            volume = nullable_positive_int(row.get("volume"))
            broker_code = str(row.get("broker_code") or "")
            queue[side].append(
                {
                    "id": f"{row['symbol']}-{side}-{index}-{broker_code}-{price}-{volume if volume is not None else 'unknown'}",
                    "position": index,
                    "side": side,
                    "participantName": normalized_participant_display(
                        row.get("participant_name") or row.get("broker_name"),
                        broker_code,
                        row,
                    ),
                    "brokerCode": row["broker_code"],
                    "price": price,
                    "volume": volume,
                    "volume_unknown": volume is None,
                }
            )
    return queue


def is_big_trade(
    tick: dict[str, Any],
    bod: BodState | None,
    volume_baseline_ratio: float,
    min_volume_threshold: int,
    turnover_threshold: float,
) -> bool:
    volume_candidates: list[float] = []
    if bod and bod.volume_baseline > 0 and volume_baseline_ratio > 0:
        volume_candidates.append(bod.volume_baseline * volume_baseline_ratio)
    if min_volume_threshold > 0:
        volume_candidates.append(float(min_volume_threshold))
    volume_hit = bool(volume_candidates) and int(tick["volume"]) >= max(volume_candidates)
    turnover_hit = turnover_threshold > 0 and float(tick.get("turnover") or 0) >= turnover_threshold
    return volume_hit or turnover_hit


def is_full_tick_seed_event(raw_event: dict[str, Any]) -> bool:
    return raw_event.get("period") == "full_tick" or raw_event.get("source") == "xtquant_full_tick"


def is_minute_bar_source(raw_event: dict[str, Any]) -> bool:
    period = str(raw_event.get("period") or "").strip().lower()
    return period in {"1m", "1min", "minute", "minute_bar", "minute_bars"}


def is_trade_tick_alert_source(raw_event: dict[str, Any]) -> bool:
    if raw_event.get("kind") != "tick":
        return False
    payload = raw_event.get("payload")
    if not isinstance(payload, dict):
        return False
    if str(payload.get("source_kind") or "").strip() != "canonical_trade_tick":
        return False
    if not str(payload.get("source_table") or "").strip():
        return False
    if not canonical_trade_tick_payload_provenance(payload):
        return False
    if normalize_trade_side(payload.get("side")) == "neutral":
        return False
    volume = nullable_positive_int(payload.get("volume"))
    if volume is None:
        return False
    try:
        return float(payload.get("turnover") or 0) > 0 and float(payload.get("price") or 0) > 0
    except (TypeError, ValueError):
        return False


def make_big_trade_alert(raw_event: dict[str, Any], tick: dict[str, Any], bod: BodState | None = None) -> dict[str, Any]:
    payload = raw_event["payload"]
    broker_code = str(payload.get("broker_code") or "")
    mapping = bod.broker_mapping_by_code.get(broker_code, {}) if bod and broker_code else {}
    participant_name = (
        payload.get("participant_name")
        or mapping.get("participant_name")
        or mapping.get("broker_name")
        or payload.get("broker_name")
    )
    participant_name = normalized_participant_display(participant_name, broker_code, payload)
    broker_name = participant_name
    participant_id = str(mapping.get("participant_id") or payload.get("participant_id") or "")
    highlighted = bool(payload.get("is_highlighted", False))
    if bod:
        highlighted = highlighted or participant_id in bod.highlighted_participants or participant_name in bod.highlighted_participants
    provenance = canonical_trade_tick_provenance(payload, raw_event)
    return {
        "id": f"alert-{raw_event['event_id']}",
        "timestamp": tick["timestamp"],
        "price": tick["price"],
        "volume": tick["volume"],
        "turnover": tick["turnover"],
        "side": normalize_trade_side(payload.get("side")),
        "participantName": participant_name,
        "brokerName": broker_name,
        "brokerCode": broker_code,
        "isHighlighted": highlighted,
        "source_kind": "canonical_trade_tick",
        "source_table": str(payload.get("source_table") or "trade_ticks"),
        "source_event_id": provenance,
        "trade_id": str(payload.get("trade_id") or ""),
        "row_hash": str(payload.get("row_hash") or ""),
    }


def canonical_trade_tick_provenance(payload: dict[str, Any], raw_event: dict[str, Any]) -> str:
    payload_provenance = canonical_trade_tick_payload_provenance(payload)
    if payload_provenance:
        return payload_provenance
    return str(raw_event.get("event_id") or "").strip()


def canonical_trade_tick_payload_provenance(payload: dict[str, Any]) -> str:
    for key in ("trade_id", "row_hash", "source_event_id"):
        value = str(payload.get(key) or "").strip()
        if value:
            return value
    return ""


def prepend_unique_alert(alert: dict[str, Any], existing_alerts: Any) -> list[dict[str, Any]]:
    alerts = [item for item in existing_alerts if isinstance(item, dict)] if isinstance(existing_alerts, list) else []
    alert_id = str(alert.get("id") or "")
    if alert_id:
        alerts = [item for item in alerts if str(item.get("id") or "") != alert_id]
    elif alert in alerts:
        alerts = [item for item in alerts if item != alert]
    return [alert, *alerts][:500]


def is_canonical_big_trade_alert(alert: dict[str, Any]) -> bool:
    if str(alert.get("source_kind") or "") != "canonical_trade_tick":
        return False
    if not any(str(alert.get(key) or "").strip() for key in ("trade_id", "row_hash", "source_event_id")):
        return False
    return normalize_trade_side(alert.get("side")) != "neutral"


def sanitize_broker_queue_snapshot(state: dict[str, Any]) -> None:
    broker_queue = state.get("broker_queue")
    if not isinstance(broker_queue, dict):
        return
    for side in ("ask", "bid"):
        entries = broker_queue.get(side)
        if not isinstance(entries, list):
            continue
        sanitized = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            current = dict(entry)
            volume = nullable_positive_int(current.get("volume"))
            current["volume"] = volume
            current["volume_unknown"] = volume is None
            if "levelVolume" in current or "level_volume" in current:
                level_volume = nullable_positive_int(current.get("levelVolume", current.get("level_volume")))
                current["levelVolume"] = level_volume
                current["depthAvailable"] = level_volume is not None and bool(current.get("depthAvailable", True))
            sanitized.append(current)
        broker_queue[side] = sanitized


def normalized_participant_display(participant_name: Any, broker_code: str, payload: dict[str, Any]) -> str:
    candidate = str(participant_name or "").strip()
    if candidate and candidate != "--" and not candidate.startswith("Broker "):
        return candidate
    trade_type = str(payload.get("trade_type") or "")
    if trade_type == "101" or broker_code == "101":
        return "集合竞价"
    return "未披露"


def normalize_trade_side(value: Any) -> str:
    normalized = str(value or "").lower()
    if normalized in {"buy", "b"}:
        return "buy"
    if normalized in {"sell", "s"}:
        return "sell"
    return "neutral"


def safe_alert_id_part(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", value).strip("-") or "unknown"


def upsert_minute_bar(
    minute_bars: list[dict[str, Any]],
    tick: dict[str, Any],
    trade_date: str | None = None,
) -> list[dict[str, Any]]:
    minute_ts = minute_bucket(tick["timestamp"])
    if not is_regular_hk_trading_minute(minute_ts, trade_date):
        return minute_bars[-420:]
    for index, previous in enumerate(minute_bars):
        if minute_bucket(str(previous["timestamp"])) != minute_ts:
            continue
        merged = {
            **previous,
            "timestamp": minute_ts,
            "price": tick["price"],
            "close": tick["price"],
            "high": max(float(previous.get("high", previous.get("price", tick["price"]))), float(tick["price"])),
            "low": min(float(previous.get("low", previous.get("price", tick["price"]))), float(tick["price"])),
            "volume": int(previous.get("volume") or 0) + int(tick["volume"]),
            "turnover": float(previous.get("turnover") or 0) + float(tick["turnover"]),
            "direction": tick["direction"],
        }
        updated = [*minute_bars[:index], merged, *minute_bars[index + 1:]]
        updated.sort(key=lambda bar: minute_bucket(str(bar["timestamp"])))
        return updated[-420:]

    updated = [
        *minute_bars,
        {
            **tick,
            "timestamp": minute_ts,
            "open": tick["price"],
            "high": tick["price"],
            "low": tick["price"],
            "close": tick["price"],
        },
    ]
    updated.sort(key=lambda bar: minute_bucket(str(bar["timestamp"])))
    return updated[-420:]


def upsert_confirmed_minute_bar(
    minute_bars: list[dict[str, Any]],
    bar: dict[str, Any],
    trade_date: str | None = None,
) -> list[dict[str, Any]]:
    minute_ts = minute_bucket(str(bar["timestamp"]))
    if not is_regular_hk_trading_minute(minute_ts, trade_date):
        return minute_bars[-420:]
    normalized = {
        **bar,
        "timestamp": minute_ts,
        "price": float(bar.get("price") or bar.get("close") or 0),
        "open": float(bar.get("open") or bar.get("price") or bar.get("close") or 0),
        "high": float(bar.get("high") or bar.get("price") or bar.get("close") or 0),
        "low": float(bar.get("low") or bar.get("price") or bar.get("close") or 0),
        "close": float(bar.get("close") or bar.get("price") or 0),
        "volume": int(bar.get("volume") or 0),
        "turnover": float(bar.get("turnover") or 0),
        "is_confirmed_minute_bar": True,
        "replace": True,
    }
    updated: list[dict[str, Any]] = []
    replaced = False
    for previous in minute_bars:
        if not isinstance(previous, dict):
            continue
        if minute_bucket(str(previous.get("timestamp") or "")) == minute_ts:
            updated.append(normalized)
            replaced = True
        else:
            updated.append(previous)
    if not replaced:
        updated.append(normalized)
    updated.sort(key=lambda item: minute_bucket(str(item.get("timestamp") or "")))
    return updated[-420:]


def minute_bar_from_payload(raw_event: dict[str, Any]) -> dict[str, Any] | None:
    payload = raw_event.get("payload")
    if not isinstance(payload, dict):
        return None
    close = first_float(payload, "close", "price", "last_price", "lastPrice")
    if close is None or close <= 0:
        return None
    timestamp = str(payload.get("timestamp") or payload.get("bar_ts") or raw_event.get("source_ts") or "")
    if not timestamp:
        return None
    return {
        "timestamp": timestamp,
        "price": close,
        "open": first_float(payload, "open") or close,
        "high": first_float(payload, "high") or close,
        "low": first_float(payload, "low") or close,
        "close": close,
        "volume": int(first_float(payload, "volume", "qty", "quantity") or 0),
        "turnover": float(first_float(payload, "turnover", "amount") or 0.0),
        "direction": str(payload.get("direction") or "flat"),
        "is_confirmed_minute_bar": True,
        "replace": True,
    }


def update_snapshot_from_minute_bars(state: dict[str, Any], updated_at: str) -> None:
    minute_bars = prune_future_minute_bars(state.get("minute_bars", []))
    if not minute_bars:
        return
    minute_bars.sort(key=lambda bar: minute_bucket(str(bar.get("timestamp") or "")))
    state["minute_bars"] = minute_bars[-420:]
    latest = minute_bars[-1]
    previous_close = float(state["snapshot"].get("previousClose") or 0)
    price = float(latest.get("price") or latest.get("close") or previous_close)
    high = max(float(bar.get("high") or bar.get("price") or price) for bar in minute_bars)
    low = min(float(bar.get("low") or bar.get("price") or price) for bar in minute_bars)
    open_price = float(minute_bars[0].get("open") or minute_bars[0].get("price") or price)
    volume = sum(int(bar.get("volume") or 0) for bar in minute_bars)
    turnover = sum(float(bar.get("turnover") or 0.0) for bar in minute_bars)
    state["snapshot"] = {
        **state["snapshot"],
        "price": price,
        "open": open_price,
        "high": high,
        "low": low,
        "volume": volume,
        "turnover": turnover,
        "change": price - previous_close,
        "updatedAt": updated_at,
    }
    state["snapshot"]["changePercent"] = 0.0 if previous_close == 0 else state["snapshot"]["change"] / previous_close * 100


def first_float(data: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = data.get(key)
        if value in (None, ""):
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def minute_bucket(timestamp: str) -> str:
    parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=HK_TZ)
    return parsed.astimezone(HK_TZ).replace(second=0, microsecond=0).isoformat()


def is_future_minute_bar(timestamp: str, *, now: datetime | None = None) -> bool:
    parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=HK_TZ)
    bar_minute = parsed.astimezone(HK_TZ).replace(second=0, microsecond=0)
    current_minute = (now or datetime.now(timezone.utc)).astimezone(HK_TZ).replace(second=0, microsecond=0)
    return bar_minute > current_minute


def is_current_or_future_minute_bar(timestamp: str, *, now: datetime | None = None) -> bool:
    parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=HK_TZ)
    bar_minute = parsed.astimezone(HK_TZ).replace(second=0, microsecond=0)
    current_minute = (now or datetime.now(timezone.utc)).astimezone(HK_TZ).replace(second=0, microsecond=0)
    return bar_minute >= current_minute


def prune_future_minute_bars(minute_bars: Any, *, now: datetime | None = None) -> list[dict[str, Any]]:
    if not isinstance(minute_bars, list):
        return []
    pruned: list[dict[str, Any]] = []
    for bar in minute_bars:
        if not isinstance(bar, dict):
            continue
        timestamp = str(bar.get("timestamp") or "")
        if not timestamp:
            continue
        try:
            if is_future_minute_bar(timestamp, now=now):
                continue
        except ValueError:
            continue
        pruned.append(bar)
    return pruned[-420:]


def iso_timestamp_seconds(timestamp: str) -> float:
    try:
        parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError:
        return datetime.now(timezone.utc).timestamp()
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=HK_TZ)
    return parsed.timestamp()


def normalize_raw_broker_queue(payload: dict[str, Any], bod: BodState | None = None) -> dict[str, list[dict[str, Any]]]:
    side = str(payload.get("side", ""))
    entries = payload.get("entries")
    if not isinstance(entries, list):
        entries = payload.get("queues", [])
    queue = {"ask": [], "bid": []}
    for index, item in enumerate(entries if isinstance(entries, list) else []):
        if not isinstance(item, dict):
            continue
        item_side = str(item.get("side") or side)
        if item_side not in queue:
            continue
        broker_code = str(item.get("brokerCode") or item.get("broker_code") or "")
        volume = nullable_positive_int(first_present_dict(item, "volume", "qty", "quantity"))
        mapping = bod.broker_mapping_by_code.get(broker_code, {}) if bod and broker_code else {}
        participant_name = (
            item.get("participantName")
            or item.get("participant_name")
            or mapping.get("participant_name")
            or mapping.get("broker_name")
        )
        queue[item_side].append(
            {
                "id": str(item.get("id") or f"{item_side}-{index + 1}"),
                "position": int(item.get("position") or item.get("rank") or index + 1),
                "side": item_side,
                "participantName": normalized_participant_display(participant_name, broker_code, item),
                "brokerCode": broker_code,
                "price": float(item.get("price") or 0),
                "volume": volume,
                "volume_unknown": volume is None,
            }
        )
    queue["ask"].sort(key=lambda row: row["position"])
    queue["bid"].sort(key=lambda row: row["position"])
    return queue


def merge_broker_queue(
    current: dict[str, list[dict[str, Any]]],
    update: dict[str, list[dict[str, Any]]],
) -> dict[str, list[dict[str, Any]]]:
    return {
        "ask": update["ask"] if update["ask"] else current.get("ask", []),
        "bid": update["bid"] if update["bid"] else current.get("bid", []),
    }


def queue_has_entries(queue: dict[str, list[dict[str, Any]]]) -> bool:
    return any(bool(queue.get(side)) for side in ("ask", "bid"))


def broker_queue_delta_for_event(queue: dict[str, list[dict[str, Any]]], side: Any = None) -> dict[str, list[dict[str, Any]]]:
    normalized_side = str(side or "").strip().lower()
    if normalized_side in {"ask", "bid"}:
        entries = queue.get(normalized_side) or []
        return {normalized_side: entries} if entries else {}
    return {side_key: entries for side_key in ("ask", "bid") if (entries := queue.get(side_key) or [])}


def enrich_broker_queue_with_depth(current: Any, order_book: Any) -> dict[str, list[dict[str, Any]]]:
    queue = current if isinstance(current, dict) else {"ask": [], "bid": []}
    book = order_book if isinstance(order_book, dict) else {}
    enriched: dict[str, list[dict[str, Any]]] = {"ask": [], "bid": []}
    for side in ("ask", "bid"):
        raw_entries = queue.get(side) if isinstance(queue.get(side), list) else []
        entries = [dict(entry) for entry in raw_entries if isinstance(entry, dict)]
        levels = [level for level in book.get(side, []) if isinstance(level, dict)]
        level_by_price = {price_key(level.get("price")): level for level in levels if price_key(level.get("price"))}
        broker_count_by_price: dict[str, int] = defaultdict(int)
        for entry in entries:
            key = price_key(entry.get("price"))
            if key:
                broker_count_by_price[key] += 1
        used_prices: set[str] = set()
        for entry in entries:
            key = price_key(entry.get("price"))
            level = level_by_price.get(key)
            if level is not None:
                used_prices.add(key)
                entry["levelVolume"] = int(level.get("volume") or 0)
                entry["depthPosition"] = int(level.get("position") or entry.get("position") or 0)
                entry["depthAvailable"] = True
                entry["brokerCountAtPrice"] = broker_count_by_price.get(key, 0)
            else:
                entry.setdefault("levelVolume", None)
                entry.setdefault("depthAvailable", False)
            enriched[side].append(entry)
        for level in levels:
            key = price_key(level.get("price"))
            if not key or key in used_prices:
                continue
            position = int(level.get("position") or len(enriched[side]) + 1)
            volume = int(level.get("volume") or 0)
            enriched[side].append(
                {
                    "id": f"depth-{side}-{position}-{key}",
                    "position": position,
                    "side": side,
                    "participantName": "档位总量",
                    "brokerCode": "",
                    "price": float(level.get("price") or 0),
                    "volume": None,
                    "volume_unknown": True,
                    "levelVolume": volume,
                    "depthPosition": position,
                    "depthAvailable": True,
                    "brokerCountAtPrice": broker_count_by_price.get(key, 0),
                    "isDepthLevel": True,
                }
            )
        enriched[side].sort(key=lambda row: (int(row.get("depthPosition") or row.get("position") or 0), int(row.get("position") or 0), str(row.get("id") or "")))
    return enriched


def price_key(value: Any) -> str:
    try:
        price = float(value)
    except (TypeError, ValueError):
        return ""
    return f"{price:.6f}"


def nullable_positive_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        parsed = int(float(value))
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def first_present_dict(data: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in data and data[key] is not None:
            return data[key]
    return None


def order_book_has_valid_depth(order_book: dict[str, Any]) -> bool:
    for side in ("ask", "bid"):
        levels = order_book.get(side)
        if not isinstance(levels, list):
            continue
        for level in levels:
            if not isinstance(level, dict):
                continue
            try:
                price = float(level.get("price") or 0)
                volume = int(level.get("volume") or 0)
            except (TypeError, ValueError):
                continue
            if price > 0 and volume > 0:
                return True
    return False


def realtime_freshness(
    state: dict[str, Any],
    raw_event: dict[str, Any],
    trade_date: str,
    *,
    updates_minute_bars: bool = True,
    updates_latest_bar: bool = False,
) -> dict[str, Any]:
    existing = state.get("freshness") if isinstance(state.get("freshness"), dict) else {}
    source_dates = dict(existing.get("source_dates") or {})
    if raw_event.get("kind") == "tick":
        if updates_minute_bars:
            source_dates["minute_bars"] = trade_date
        if updates_latest_bar:
            source_dates["latest_bar"] = trade_date
        if is_full_tick_seed_event(raw_event):
            source_dates["full_tick"] = trade_date
        if is_trade_tick_alert_source(raw_event):
            source_dates["trade_ticks"] = trade_date
        source_dates["realtime"] = trade_date
    elif raw_event.get("kind") == "broker_queue":
        source_dates["broker_queue"] = trade_date
        source_dates["realtime"] = trade_date
        existing["broker_queue_last_valid_ts"] = raw_event["source_ts"]
    elif raw_event.get("kind") == "l2_order_book":
        source_dates["l2_order_book"] = trade_date
        source_dates["realtime"] = trade_date
        existing["depth_last_valid_ts"] = raw_event["source_ts"]
    degraded_reasons = freshness_degraded_reasons(state, source_dates, trade_date)
    return {
        **existing,
        "updated_at": raw_event["ingest_ts"],
        "source_ts": raw_event["source_ts"],
        "ingest_ts": raw_event["ingest_ts"],
        "requested_trade_date": trade_date,
        "effective_trade_date": trade_date,
        "runtime_state": "LIVE",
        "source_dates": source_dates,
        "degraded": bool(degraded_reasons),
        "degraded_reasons": degraded_reasons,
    }


def freshness_degraded_reasons(state: dict[str, Any], source_dates: dict[str, Any], trade_date: str) -> list[str]:
    freshness = state.get("freshness") if isinstance(state.get("freshness"), dict) else {}
    existing_reasons = [str(reason) for reason in freshness.get("degraded_reasons") or []]
    minute_bars = state.get("minute_bars") if isinstance(state.get("minute_bars"), list) else []
    has_confirmed_minute_bars = bool(minute_bars) and source_dates.get("minute_bars") == trade_date
    reasons: list[str] = []
    for reason in existing_reasons:
        if reason in {"missing_minute_bars", "intraday_gap_before_attach"} and has_confirmed_minute_bars:
            continue
        if reason not in reasons:
            reasons.append(reason)
    if not has_confirmed_minute_bars and "missing_minute_bars" in existing_reasons and "missing_minute_bars" not in reasons:
        reasons.append("missing_minute_bars")
    return reasons


def rollover_realtime_session(state: dict[str, Any], trade_date: str, updated_at: str) -> None:
    if snapshot_trade_date(state) == trade_date:
        return
    snapshot = state.get("snapshot") if isinstance(state.get("snapshot"), dict) else {}
    reference_price = float(
        snapshot.get("nextSessionPreviousClose")
        or snapshot.get("price")
        or snapshot.get("previousClose")
        or 0
    )
    state["minute_bars"] = []
    state["alerts"] = []
    state["snapshot"] = {
        **snapshot,
        "tradeDate": trade_date,
        "requestedTradeDate": trade_date,
        "isHistoricalSession": False,
        "price": reference_price,
        "previousClose": reference_price,
        "open": reference_price,
        "high": reference_price,
        "low": reference_price,
        "volume": 0,
        "turnover": 0.0,
        "change": 0.0,
        "changePercent": 0.0,
        "updatedAt": updated_at,
    }
    freshness = state.get("freshness") if isinstance(state.get("freshness"), dict) else {}
    source_dates = freshness.get("source_dates") if isinstance(freshness.get("source_dates"), dict) else {}
    state["freshness"] = {
        **freshness,
        "updated_at": updated_at,
        "requested_trade_date": trade_date,
        "effective_trade_date": trade_date,
        "source_dates": {
            **source_dates,
            "minute_bars": "",
            "trade_ticks": "",
            "broker_queue": "",
            "realtime_session": trade_date,
        },
    }


def snapshot_trade_date(state: dict[str, Any]) -> str:
    snapshot = state.get("snapshot") if isinstance(state.get("snapshot"), dict) else {}
    value = snapshot.get("tradeDate") or snapshot.get("trade_date")
    return str(value or "")


def empty_l2_order_book() -> dict[str, Any]:
    return {"ask": [], "bid": [], "best_ask": None, "best_bid": None, "spread": None}


def normalize_l2_order_book(payload: dict[str, Any]) -> dict[str, Any]:
    ask = normalize_l2_levels(payload.get("ask") or payload.get("asks") or [], "ask")
    bid = normalize_l2_levels(payload.get("bid") or payload.get("bids") or [], "bid")
    ask.sort(key=lambda level: level["price"])
    bid.sort(key=lambda level: level["price"], reverse=True)
    for index, level in enumerate(ask, start=1):
        level["position"] = index
    for index, level in enumerate(bid, start=1):
        level["position"] = index
    best_ask = ask[0]["price"] if ask else None
    best_bid = bid[0]["price"] if bid else None
    spread = None if best_ask is None or best_bid is None else best_ask - best_bid
    return {
        "ask": ask,
        "bid": bid,
        "best_ask": best_ask,
        "best_bid": best_bid,
        "spread": spread,
    }


def normalize_l2_levels(levels: Any, side: str) -> list[dict[str, Any]]:
    if not isinstance(levels, list):
        return []
    normalized = []
    for index, level in enumerate(levels):
        if not isinstance(level, dict):
            continue
        try:
            price = float(level.get("price") or 0)
        except (TypeError, ValueError):
            continue
        if price <= 0:
            continue
        try:
            volume = int(level.get("volume") or level.get("qty") or level.get("quantity") or 0)
        except (TypeError, ValueError):
            volume = 0
        normalized.append(
            {
                "position": int(level.get("position") or level.get("rank") or index + 1),
                "side": side,
                "price": price,
                "volume": max(0, volume),
                "order_count": int(level.get("order_count") or level.get("orders") or 0),
            }
        )
    return normalized


def volume_baseline(daily_bars: list[dict[str, Any]], trade_date: str | None = None) -> float:
    rows = sorted(daily_bars, key=lambda row: str(row.get("trade_date") or ""))
    if trade_date:
        previous_rows = [row for row in rows if str(row.get("trade_date") or "") < trade_date]
        if previous_rows:
            return float(previous_rows[-1].get("volume") or 0)
    if rows:
        return float(rows[-1].get("volume") or 0)
    return 0.0
