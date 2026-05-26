from __future__ import annotations

import json
import inspect
from dataclasses import asdict, dataclass
from time import perf_counter
from typing import Any

from .adapters import (
    holding_history_record,
    read_model_record,
    snapshot_updated_at,
    terminal_snapshot_record,
    terminal_runtime_state_record,
    terminal_state_record,
    validate_event_bus_publish_inputs,
    validate_snapshot_key_inputs,
    validate_snapshot_symbol,
)
from .contracts import now_iso


@dataclass(frozen=True)
class KafkaAdapterConfig:
    raw_topic: str = "raw_market_events_v1"
    processed_topic: str = "processed_market_events_v1"
    consumer_group: str = "beast-terminal-v2"
    poll_timeout_ms: int = 1000
    auto_offset_reset: str = "latest"
    delivery_timeout_seconds: float = 5.0


@dataclass(frozen=True)
class RedisAdapterConfig:
    terminal_ttl_seconds: int = 60 * 60 * 8
    history_ttl_seconds: int = 60 * 60 * 24 * 30


@dataclass
class RedisWriteStats:
    writes: int = 0
    failures: int = 0
    last_latency_ms: float = 0.0
    max_latency_ms: float = 0.0
    last_error: str = ""


class KafkaEventBusAdapter:
    """Production-shaped Kafka adapter.

    This wrapper intentionally depends on a caller-supplied Kafka client so the
    baseline has no hard dependency on a specific library. The producer must expose
    `produce(topic, key, value)` and optionally `flush()`.
    """

    def __init__(self, producer: Any, consumer: Any | None = None, config: KafkaAdapterConfig | None = None) -> None:
        self.producer = producer
        self.consumer = consumer
        self.config = config or KafkaAdapterConfig()
        self._committed_offsets: dict[str, int] = {}

    def publish(self, topic: str, key: str, value: dict[str, Any]) -> None:
        validate_event_bus_publish_inputs(topic, key, value)
        encoded = json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        delivery_state: dict[str, Any] = {"called": False, "error": None}

        def on_delivery(error: Any, message: Any) -> None:
            delivery_state["called"] = True
            delivery_state["error"] = error

        delivery = produce_with_optional_delivery_callback(
            self.producer,
            topic,
            key=key.encode("utf-8"),
            value=encoded,
            callback=on_delivery,
        )
        if delivery_state["called"] or producer_accepts_delivery_callback(self.producer):
            wait_for_delivery_callback(
                self.producer,
                delivery_state,
                timeout_seconds=self.config.delivery_timeout_seconds,
            )
        wait_for_delivery_ack(delivery)

    def read(self, topic: str) -> list[dict[str, Any]]:
        if self.consumer is None:
            return []
        return self.poll(topic, self.committed_offset(topic))

    def poll(self, topic: str, offset: int) -> list[dict[str, Any]]:
        if self.consumer is None:
            return []
        poll = getattr(self.consumer, "poll", None)
        if not callable(poll):
            return []
        records = poll(topic=topic, offset=offset, timeout_ms=self.config.poll_timeout_ms)
        return [
            {
                "key": decode_key(record.get("key")),
                "value": decode_value(record.get("value")),
                "offset": int(record.get("offset", offset + index)),
            }
            for index, record in enumerate(records or [])
        ]

    def commit(self, topic: str, offset: int) -> None:
        self._committed_offsets[topic] = offset
        commit = getattr(self.consumer, "commit", None) if self.consumer is not None else None
        if callable(commit):
            commit(topic=topic, offset=offset)

    def committed_offset(self, topic: str) -> int:
        if topic in self._committed_offsets:
            return self._committed_offsets[topic]
        committed = getattr(self.consumer, "committed", None) if self.consumer is not None else None
        if callable(committed):
            return int(committed(topic=topic) or 0)
        return 0

    def lag(self, topic: str, committed_offset: int = 0) -> int:
        high_watermark = getattr(self.consumer, "high_watermark", None) if self.consumer is not None else None
        if callable(high_watermark):
            return max(0, int(high_watermark(topic=topic)) - committed_offset)
        return 0


class RedisSnapshotCacheAdapter:
    """Production-shaped Redis adapter for terminal snapshot keys."""

    def __init__(self, redis_client: Any, config: RedisAdapterConfig | None = None) -> None:
        self.redis = redis_client
        self.config = config or RedisAdapterConfig()
        self.write_stats = RedisWriteStats()

    def set_terminal_snapshot(self, trade_date: str, symbol: str, snapshot: dict[str, Any]) -> None:
        validate_snapshot_key_inputs(trade_date, symbol)
        ttl = self.config.terminal_ttl_seconds
        updated_at = snapshot_updated_at(snapshot)
        self._set_many_json(
            [
                (
                    f"terminal:{trade_date}:snapshot:{symbol}",
                    terminal_snapshot_record(trade_date, symbol, snapshot, updated_at),
                    ttl,
                ),
                (
                    f"terminal:{trade_date}:minute:{symbol}",
                    read_model_record(trade_date, symbol, snapshot, snapshot.get("minute_bars", []), updated_at),
                    ttl,
                ),
                (
                    f"terminal:{trade_date}:alerts:{symbol}",
                    read_model_record(trade_date, symbol, snapshot, snapshot.get("alerts", []), updated_at),
                    ttl,
                ),
                (
                    f"terminal:{trade_date}:queue:{symbol}",
                    read_model_record(trade_date, symbol, snapshot, snapshot.get("broker_queue", {}), updated_at),
                    ttl,
                ),
                (
                    f"terminal:{trade_date}:state:{symbol}",
                    read_model_record(
                        trade_date,
                        symbol,
                        snapshot,
                        terminal_state_record(trade_date, symbol, snapshot),
                        updated_at,
                    ),
                    ttl,
                ),
                (
                    f"ccass:holding:{symbol}",
                    read_model_record(trade_date, symbol, snapshot, snapshot.get("ccass_holdings", []), updated_at),
                    ttl,
                ),
            ]
        )

    def set_terminal_state(self, trade_date: str, symbol: str, state: dict[str, Any]) -> None:
        validate_snapshot_key_inputs(trade_date, symbol)
        self._set_json(
            f"terminal:{trade_date}:state:{symbol}",
            terminal_runtime_state_record(trade_date, symbol, state),
            self.config.terminal_ttl_seconds,
        )

    def get_terminal_snapshot(self, trade_date: str, symbol: str) -> dict[str, Any] | None:
        validate_snapshot_key_inputs(trade_date, symbol)
        value = self.redis.get(f"terminal:{trade_date}:snapshot:{symbol}")
        if value is None:
            return None
        decoded = decode_redis_json(value)
        return decoded if isinstance(decoded, dict) else None

    def set_holding_history(self, symbol: str, participant_id: str, history: list[dict[str, Any]]) -> None:
        validate_snapshot_symbol(symbol)
        if not isinstance(participant_id, str) or not participant_id.strip():
            raise ValueError("participant_id must be a non-empty string")
        self._set_json(f"ccass:history:{symbol}:{participant_id}", holding_history_record(symbol, participant_id, history), self.config.history_ttl_seconds)

    def _set_json(self, key: str, value: Any, ttl_seconds: int) -> None:
        self._recorded_write(lambda: self._set_json_unrecorded(key, value, ttl_seconds))

    def _set_many_json(self, records: list[tuple[str, Any, int]]) -> None:
        self._recorded_write(lambda: self._set_many_json_unrecorded(records))

    def stats_snapshot(self) -> dict[str, Any]:
        return asdict(self.write_stats)

    def _set_json_unrecorded(self, key: str, value: Any, ttl_seconds: int) -> None:
        encoded = json.dumps(value, separators=(",", ":"), ensure_ascii=False)
        self.redis.set(key, encoded, ex=ttl_seconds)

    def _set_many_json_unrecorded(self, records: list[tuple[str, Any, int]]) -> None:
        pipeline_factory = getattr(self.redis, "pipeline", None)
        if not callable(pipeline_factory):
            for key, value, ttl_seconds in records:
                self._set_json_unrecorded(key, value, ttl_seconds)
            return
        pipeline = pipeline_factory(transaction=True)
        for key, value, ttl_seconds in records:
            encoded = json.dumps(value, separators=(",", ":"), ensure_ascii=False)
            pipeline.set(key, encoded, ex=ttl_seconds)
        pipeline.execute()

    def _recorded_write(self, operation) -> None:
        started = perf_counter()
        try:
            operation()
        except Exception as error:
            self.write_stats.failures += 1
            self.write_stats.last_error = str(error)
            raise
        finally:
            latency_ms = max(0.0, (perf_counter() - started) * 1000)
            self.write_stats.writes += 1
            self.write_stats.last_latency_ms = latency_ms
            self.write_stats.max_latency_ms = max(self.write_stats.max_latency_ms, latency_ms)


def decode_key(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def decode_value(value: Any) -> dict[str, Any]:
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    decoded = json.loads(value)
    if not isinstance(decoded, dict):
        raise ValueError("Kafka record value must decode to an object")
    return decoded


def decode_redis_json(value: Any) -> Any:
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    return json.loads(value)


def wait_for_delivery_ack(delivery: Any) -> None:
    if delivery is None:
        return
    get = getattr(delivery, "get", None)
    if callable(get):
        get(timeout=5)
        return
    result = getattr(delivery, "result", None)
    if callable(result):
        result(timeout=5)


def produce_with_optional_delivery_callback(
    producer: Any,
    topic: str,
    *,
    key: bytes,
    value: bytes,
    callback: Any,
) -> Any:
    produce = producer.produce
    callback_parameter = delivery_callback_parameter(produce)
    if callback_parameter == "on_delivery":
        return produce(topic, key=key, value=value, on_delivery=callback)
    if callback_parameter == "callback":
        return produce(topic, key=key, value=value, callback=callback)
    return produce(topic, key=key, value=value)


def producer_accepts_delivery_callback(producer: Any) -> bool:
    return delivery_callback_parameter(producer.produce) is not None


def delivery_callback_parameter(produce: Any) -> str | None:
    try:
        signature = inspect.signature(produce)
    except (TypeError, ValueError):
        return "on_delivery"
    parameters = signature.parameters
    if "on_delivery" in parameters:
        return "on_delivery"
    if "callback" in parameters:
        return "callback"
    if any(parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in parameters.values()):
        return "on_delivery"
    return None


def wait_for_delivery_callback(
    producer: Any,
    state: dict[str, Any],
    *,
    timeout_seconds: float,
) -> None:
    poll = getattr(producer, "poll", None)
    if callable(poll):
        deadline = perf_counter() + max(0.0, timeout_seconds)
        while not state["called"] and perf_counter() <= deadline:
            poll(0.01)
    if not state["called"]:
        raise TimeoutError("Kafka delivery callback did not ACK before timeout")
    if state["error"] is not None:
        raise RuntimeError(f"Kafka delivery failed: {state['error']}")
