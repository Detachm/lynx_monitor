from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Protocol

from .adapters import (
    BoundedRawEventQueue,
    DeadLetterRecord,
    EventConsumer,
    EventPublishError,
    validate_event_bus_record_inputs,
)
from .contracts import RAW_TOPIC, validate_raw_market_event
from .pipeline import OctopusComputeV2, RealtimeCollectorV2


CallbackRejectSink = Callable[[dict[str, Any], str], None]
StateProvider = Callable[[str], dict[str, Any] | None]
RuntimeRawEventProcessor = Callable[[dict[str, Any], str], tuple[list[dict[str, Any]], list[dict[str, Any]]]]
RawDeadLetterSink = Callable[[DeadLetterRecord], None]


@dataclass
class WorkerStats:
    received: int = 0
    enqueued: int = 0
    rejected: int = 0
    processed: int = 0
    failed: int = 0
    committed_offset: int = 0
    dead_letters: list[DeadLetterRecord] = field(default_factory=list)


class MarketDataSubscriptionClient(Protocol):
    def start(self) -> None:
        ...

    def stop(self) -> None:
        ...

    def subscribe(self, symbol: str) -> None:
        ...

    def unsubscribe(self, symbol: str) -> None:
        ...


@dataclass
class SubscriptionStats:
    starts: int = 0
    stops: int = 0
    subscribes: int = 0
    unsubscribes: int = 0
    resubscribes: int = 0
    failed: int = 0
    errors: list[str] = field(default_factory=list)


class XtQuantSubscriptionManager:
    """Owns xtquant subscription lifecycle without hiding client failures."""

    def __init__(self, client: MarketDataSubscriptionClient, collector: RealtimeCollectorV2) -> None:
        self.client = client
        self.collector = collector
        self.stats = SubscriptionStats()
        self.running = False
        self.subscribed_symbols: set[str] = set()

    def start(self) -> None:
        try:
            self.client.start()
            self.running = True
            self.stats.starts += 1
            self.collector.health.process = "running"
        except Exception as error:
            self._record_failure(error)
            raise

    def stop(self) -> None:
        try:
            self.client.stop()
            self.running = False
            self.stats.stops += 1
            self.collector.health.process = "stopped"
        except Exception as error:
            self._record_failure(error)
            raise

    def subscribe(self, raw_symbol: str) -> None:
        symbol = normalize_subscription_symbol(raw_symbol)
        try:
            self.client.subscribe(symbol)
            self.subscribed_symbols.add(symbol)
            self.collector.subscribe_symbol(symbol)
            self.stats.subscribes += 1
        except Exception as error:
            self._record_failure(error)
            raise

    def unsubscribe(self, raw_symbol: str) -> None:
        symbol = normalize_subscription_symbol(raw_symbol)
        try:
            self.client.unsubscribe(symbol)
            self.subscribed_symbols.discard(symbol)
            self.collector.unsubscribe_symbol(symbol)
            self.stats.unsubscribes += 1
        except Exception as error:
            self._record_failure(error)
            raise

    def check_freshness_and_resubscribe(self, *, now: str | None = None) -> dict[str, Any]:
        result = self.collector.evaluate_freshness(now=now)
        for symbol in result["resubscribe_symbols"]:
            if symbol in self.subscribed_symbols:
                self._resubscribe(symbol)
        return result

    def _resubscribe(self, symbol: str) -> None:
        try:
            self.client.unsubscribe(symbol)
            self.client.subscribe(symbol)
            self.collector.subscribe_symbol(symbol)
            self.stats.resubscribes += 1
        except Exception as error:
            self._record_failure(error)
            raise

    def _record_failure(self, error: Exception) -> None:
        self.stats.failed += 1
        self.stats.errors.append(str(error))
        self.collector.health.process = "degraded"


class RealtimeIngestWorker:
    """Moves xtquant callback payloads through a bounded queue into RawMarketEvent v1."""

    def __init__(
        self,
        queue: BoundedRawEventQueue,
        collector: RealtimeCollectorV2,
        *,
        normalizer: Callable[[dict[str, Any]], Any],
        reject_sink: CallbackRejectSink | None = None,
    ) -> None:
        self.queue = queue
        self.collector = collector
        self.normalizer = normalizer
        self.reject_sink = reject_sink
        self.stats = WorkerStats()

    def receive_callback(self, payload: dict[str, Any]) -> bool:
        self.stats.received += 1
        accepted = self.queue.push(payload)
        if accepted:
            self.stats.enqueued += 1
        else:
            self.stats.rejected += 1
            self._record_rejected_callback(payload, "raw_callback_queue_full")
        symbol = callback_symbol(payload)
        if symbol:
            self.collector.record_queue_backlog(symbol, self.queue.backlog)
        return accepted

    def drain_once(self) -> dict[str, Any] | None:
        payload = self.queue.pop()
        if payload is None:
            return None
        symbol_hint = callback_symbol(payload)
        try:
            normalized = self.normalizer(payload)
            event = self._ingest_normalized(normalized)
            self.collector.record_queue_backlog(
                event["symbol"],
                self.queue.backlog,
                period=event.get("period"),
                stream_kind=event.get("kind"),
            )
            self.collector.record_queue_backlog(event["symbol"], self.queue.backlog)
            self.stats.processed += 1
            return event
        except Exception as error:
            if symbol_hint:
                self.collector.record_queue_backlog(symbol_hint, self.queue.backlog)
            self.stats.failed += 1
            failure_prefix = "raw_publish_failed" if isinstance(error, EventPublishError) else "normalization_failed"
            if isinstance(error, EventPublishError):
                self.collector.health.process = "degraded"
                self.collector.health.kafka = "degraded"
            self.stats.dead_letters.append(
                DeadLetterRecord(topic=RAW_TOPIC, key=str(payload.get("symbol", "")), value=payload, reason=str(error))
            )
            self._record_rejected_callback(payload, f"{failure_prefix}: {error}")
            return None

    def drain_all(self) -> list[dict[str, Any]]:
        events = []
        while self.queue.backlog:
            event = self.drain_once()
            if event is not None:
                events.append(event)
        return events

    def _ingest_normalized(self, normalized: Any) -> dict[str, Any]:
        if isinstance(normalized, tuple) and len(normalized) == 2:
            symbol, tick = normalized
            return self.collector.ingest_tick(symbol, tick)
        if not isinstance(normalized, dict):
            raise ValueError("normalizer must return a (symbol, tick) tuple or normalized event object")

        kind = str(normalized.get("kind") or "")
        symbol = str(normalized.get("symbol") or "")
        payload = normalized.get("payload")
        if not isinstance(payload, dict):
            raise ValueError("normalized event payload must be an object")
        return self.collector.ingest_event(
            kind=kind,
            symbol=symbol,
            period=normalized.get("period"),
            source_ts=normalized.get("source_ts"),
            payload=payload,
        )

    def _record_rejected_callback(self, payload: dict[str, Any], reason: str) -> None:
        if self.reject_sink is None:
            return
        try:
            self.reject_sink(payload, reason)
        except Exception as error:
            self.stats.dead_letters.append(
                DeadLetterRecord(topic=RAW_TOPIC, key=str(payload.get("symbol", "")), value=payload, reason=f"callback_rejection_quarantine_failed: {error}")
            )


class RawEventConsumerWorker:
    """Consumes RawMarketEvent v1 and commits offsets only after successful processing."""

    def __init__(
        self,
        consumer: EventConsumer,
        octopus: OctopusComputeV2,
        *,
        topic: str = RAW_TOPIC,
        skip_bad_records: bool = True,
        state_provider: StateProvider | None = None,
        runtime_event_processor: RuntimeRawEventProcessor | None = None,
        dead_letter_sink: RawDeadLetterSink | None = None,
    ) -> None:
        self.consumer = consumer
        self.octopus = octopus
        self.topic = topic
        self.skip_bad_records = skip_bad_records
        self.state_provider = state_provider
        self.runtime_event_processor = runtime_event_processor
        self.dead_letter_sink = dead_letter_sink
        self.stats = WorkerStats(committed_offset=consumer.committed_offset(topic))
        self.last_terminal_messages: list[dict[str, Any]] = []

    def poll_and_process(self, trade_date: str, *, max_records: int | None = None) -> list[dict[str, Any]]:
        offset = self.consumer.committed_offset(self.topic)
        records = self.consumer.poll(self.topic, offset)
        if max_records is not None:
            records = records[:max_records]

        processed_events: list[dict[str, Any]] = []
        self.last_terminal_messages = []
        next_offset = offset
        for record in records:
            try:
                validate_event_bus_record_inputs(self.topic, record)
                raw_event = record["value"]
                validate_raw_market_event(raw_event)
                processed, terminal_messages = self._process_raw_event(raw_event, trade_date)
                processed_events.extend(processed)
                self.last_terminal_messages.extend(terminal_messages)
                next_offset += 1
                self.consumer.commit(self.topic, next_offset)
                self.stats.committed_offset = next_offset
                self.stats.processed += 1
            except Exception as error:
                self.stats.failed += 1
                dead_letter = DeadLetterRecord(
                    topic=self.topic,
                    key=str(record.get("key", "")),
                    value=record.get("value", {}) if isinstance(record.get("value"), dict) else {},
                    reason=str(error),
                )
                self.stats.dead_letters.append(dead_letter)
                self._write_dead_letter(dead_letter)
                if self.skip_bad_records:
                    next_offset += 1
                    self.consumer.commit(self.topic, next_offset)
                    self.stats.committed_offset = next_offset
                    continue
                break
        return processed_events

    def _process_raw_event(self, raw_event: dict[str, Any], trade_date: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        if self.runtime_event_processor is not None:
            return self.runtime_event_processor(raw_event, trade_date)
        if self.state_provider is not None:
            state = self.state_provider(str(raw_event.get("symbol") or ""))
            if isinstance(state, dict):
                return self.octopus.process_raw_event_with_state(raw_event, trade_date, state), []
        return self.octopus.process_raw_event(raw_event, trade_date), []

    def _write_dead_letter(self, dead_letter: DeadLetterRecord) -> None:
        if self.dead_letter_sink is None:
            return
        try:
            self.dead_letter_sink(dead_letter)
        except Exception as error:
            self.stats.dead_letters.append(
                DeadLetterRecord(
                    topic=dead_letter.topic,
                    key=dead_letter.key,
                    value=dead_letter.value,
                    reason=f"raw_consumer_dead_letter_quarantine_failed: {error}",
                )
            )


def normalize_xtquant_tick(payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    symbol = str(payload.get("symbol") or payload.get("code") or "")
    if not symbol:
        raise ValueError("missing symbol")
    if "." not in symbol and symbol.isdigit():
        symbol = f"{symbol.zfill(5)}.HK"

    price = payload.get("price") or payload.get("last_price")
    volume = payload.get("volume") or payload.get("qty") or payload.get("quantity")
    if price is None or volume is None:
        raise ValueError("missing price or volume")
    tick = {
        "timestamp": normalize_source_timestamp(payload.get("timestamp") or payload.get("time") or ""),
        "price": float(price),
        "volume": int(volume),
        "turnover": float(payload.get("turnover") or float(price) * int(volume)),
        "side": str(payload.get("side") or "neutral"),
        "broker_code": str(payload.get("broker_code") or ""),
    }
    return symbol, tick


def normalize_xtquant_callback(payload: dict[str, Any]) -> dict[str, Any]:
    """Normalize legacy xtquant/Mammoth callback messages into RawMarketEvent inputs.

    The old collector emitted `{symbol, period, data}` records. This function keeps
    that business shape at the boundary and converts it to the strict v1 raw event
    categories used by the rebuilt pipeline.
    """

    if not isinstance(payload, dict):
        raise ValueError("callback payload must be an object")
    period = str(payload.get("period") or payload.get("Period") or "").lower()
    symbol = normalize_subscription_symbol(str(payload.get("symbol") or payload.get("code") or infer_symbol_from_data(payload.get("data")) or ""))
    data = payload.get("data", payload)
    if not isinstance(data, dict):
        raise ValueError("callback data must be an object")

    if not period:
        period = infer_period(data)

    if period in {"hktransaction", "trade_tick", "tick"}:
        tick_payload = normalize_legacy_tick(data)
        return {
            "kind": "tick",
            "symbol": symbol,
            "period": "hktransaction",
            "source_ts": tick_payload.pop("timestamp"),
            "payload": tick_payload,
        }

    if period in {"hkbrokerqueueex", "broker_queue", "brokerqueue"}:
        source_ts = normalize_source_timestamp(first_present(data, "_Collect_time", "_collect_time", "Time", "time"))
        return {
            "kind": "broker_queue",
            "symbol": symbol,
            "period": "hkbrokerqueueex",
            "source_ts": source_ts,
            "payload": {"side": data.get("Side") or data.get("side"), "entries": normalize_legacy_broker_queue(data)},
        }

    if period in {"l2thousand", "l2thousand_queue", "l2_order_book"}:
        source_ts = normalize_source_timestamp(first_present(data, "_Collect_time", "_collect_time", "Time", "time"))
        return {
            "kind": "l2_order_book",
            "symbol": symbol,
            "period": period,
            "source_ts": source_ts,
            "payload": normalize_legacy_l2_order_book(symbol, data),
        }

    raise ValueError(f"unsupported callback period: {period}")


def normalize_legacy_tick(data: dict[str, Any]) -> dict[str, Any]:
    price = first_present(data, "Price", "price", "last_price", "LastPrice")
    volume = first_present(data, "Volume", "volume", "qty", "quantity")
    if price is None or volume is None:
        raise ValueError("missing price or volume")
    turnover = first_present(data, "Turnover", "turnover", "Amount", "amount")
    price_value = float(price)
    volume_value = int(volume)
    side = first_present(data, "Side", "side", "Dir", "dir")
    broker_code = first_present(data, "BrokerID", "brokerID", "BrokerNo", "brokerNo", "broker_code")
    result = {
        "timestamp": normalize_source_timestamp(
            first_present(data, "_Collect_time", "_collect_time", "timestamp", "Timestamp", "Time", "time")
        ),
        "price": price_value,
        "volume": volume_value,
        "turnover": float(turnover) if turnover is not None else price_value * volume_value,
        "side": normalize_legacy_side(side),
        "broker_code": str(broker_code or ""),
    }
    for source_key, target_key in (
        ("ParticipantID", "participant_id"),
        ("participant_id", "participant_id"),
        ("ParticipantName", "participant_name"),
        ("participant_name", "participant_name"),
        ("BrokerName", "broker_name"),
        ("broker_name", "broker_name"),
    ):
        if source_key in data and data[source_key] is not None:
            result[target_key] = data[source_key]
    return result


def normalize_legacy_broker_queue(data: dict[str, Any]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    explicit_entries = data.get("entries")
    if isinstance(explicit_entries, list):
        return [normalize_broker_queue_entry(item, index) for index, item in enumerate(explicit_entries) if isinstance(item, dict)]

    for side_key, side in (("AskQueues", "ask"), ("askQueues", "ask"), ("BidQueues", "bid"), ("bidQueues", "bid")):
        queues = data.get(side_key)
        if not isinstance(queues, list):
            continue
        for queue_index, queue in enumerate(queues):
            if not isinstance(queue, dict):
                continue
            brokers = queue.get("Brokers") or queue.get("brokers") or []
            volumes = queue.get("Volumes") or queue.get("volumes") or []
            price = queue.get("Price") or queue.get("price")
            if not isinstance(brokers, list):
                brokers = []
            if not isinstance(volumes, list):
                volumes = []
            for index, broker in enumerate(brokers):
                volume = volumes[index] if index < len(volumes) else 0
                entries.append(
                    {
                        "id": f"{side}-{queue_index + 1}-{index + 1}",
                        "position": queue_index + 1,
                        "side": side,
                        "broker_code": str(broker or ""),
                        "price": float(price or 0),
                        "volume": int(float(volume or 0)),
                    }
                )
    return entries


def normalize_broker_queue_entry(item: dict[str, Any], index: int) -> dict[str, Any]:
    return {
        "id": str(item.get("id") or f"entry-{index + 1}"),
        "position": int(item.get("position") or item.get("rank") or index + 1),
        "side": str(item.get("side") or "bid").lower(),
        "broker_code": str(item.get("broker_code") or item.get("brokerCode") or item.get("BrokerID") or ""),
        "price": float(item.get("price") or item.get("Price") or 0),
        "volume": int(float(item.get("volume") or item.get("Volume") or item.get("qty") or 0)),
    }


def normalize_legacy_l2_order_book(symbol: str, data: dict[str, Any]) -> dict[str, Any]:
    candidate = data
    nested = data.get(symbol)
    if isinstance(nested, list) and nested and isinstance(nested[0], dict):
        candidate = nested[0]
    elif isinstance(nested, dict):
        candidate = nested

    ask_prices = first_present(candidate, "AskPrice", "askPrice", "ask_price") or []
    ask_volumes = first_present(candidate, "AskVolume", "askVolume", "ask_volume") or []
    bid_prices = first_present(candidate, "BidPrice", "bidPrice", "bid_price") or []
    bid_volumes = first_present(candidate, "BidVolume", "bidVolume", "bid_volume") or []
    ask_counts = first_present(candidate, "AskOrderCount", "askOrderCount", "AskCount", "askCount") or []
    bid_counts = first_present(candidate, "BidOrderCount", "bidOrderCount", "BidCount", "bidCount") or []
    return {
        "ask": normalize_l2_side(ask_prices, ask_volumes, ask_counts),
        "bid": normalize_l2_side(bid_prices, bid_volumes, bid_counts),
    }


def normalize_l2_side(prices: Any, volumes: Any, counts: Any) -> list[dict[str, Any]]:
    if not isinstance(prices, list):
        return []
    if not isinstance(volumes, list):
        volumes = []
    if not isinstance(counts, list):
        counts = []
    levels = []
    for index, price in enumerate(prices):
        volume = volumes[index] if index < len(volumes) else 0
        count = counts[index] if index < len(counts) else 0
        levels.append(
            {
                "position": index + 1,
                "price": float(price or 0),
                "volume": int(float(volume or 0)),
                "order_count": int(float(count or 0)),
            }
        )
    return levels


def normalize_source_timestamp(value: Any) -> str:
    if isinstance(value, str) and "T" in value:
        return value
    if value in (None, ""):
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    if numeric > 10_000_000_000:
        numeric = numeric / 1000
    return datetime.fromtimestamp(numeric, tz=timezone.utc).isoformat(timespec="milliseconds")


def first_present(data: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in data and data[key] is not None:
            return data[key]
    return None


def infer_period(data: dict[str, Any]) -> str:
    if any(key in data for key in ("AskQueues", "askQueues", "BidQueues", "bidQueues")):
        return "hkbrokerqueueex"
    if any(key in data for key in ("AskPrice", "askPrice", "BidPrice", "bidPrice")):
        return "l2thousand"
    return "hktransaction"


def infer_symbol_from_data(data: Any) -> str:
    if isinstance(data, dict):
        for key in data:
            if isinstance(key, str) and key.endswith(".HK"):
                return key
    return ""


def normalize_legacy_side(value: Any) -> str:
    normalized = str(value or "").lower()
    if normalized in {"b", "buy", "1"}:
        return "buy"
    if normalized in {"s", "sell", "2"}:
        return "sell"
    return "neutral"


def callback_symbol(payload: dict[str, Any]) -> str:
    symbol = str(payload.get("symbol") or payload.get("code") or "")
    if symbol and "." not in symbol and symbol.isdigit():
        symbol = f"{symbol.zfill(5)}.HK"
    return symbol


def normalize_subscription_symbol(raw_symbol: str) -> str:
    symbol = str(raw_symbol or "").strip().upper()
    if not symbol:
        raise ValueError("missing subscription symbol")
    if "." not in symbol and symbol.isdigit():
        symbol = f"{symbol.zfill(5)}.HK"
    return symbol
