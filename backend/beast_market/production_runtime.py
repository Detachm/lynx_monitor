from __future__ import annotations

import argparse
import asyncio
import importlib
from pathlib import Path
import time
from typing import Sequence

from .app_runtime import (
    BeastMarketRuntimeClients,
    BeastMarketRuntimeConfig,
    BeastMarketRuntimeSupervisor,
    load_runtime_config_artifact,
    run_supervised_runtime,
    runtime_config_from_artifact,
)
from .production_adapters import KafkaAdapterConfig
from .xtquant_client import DeferredCallbackSink, XtQuantMarketDataClient

DEFAULT_XTQUANT_SDK_PATH = Path(
    "/home/hliu/xtbackend/vendor/"
    "xtquant_251211_interim-release_cp36m-37m-38-39-310-311-312_linux-gnu_x86_64"
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    config = runtime_config_from_artifact(load_runtime_config_artifact(args.config_path))
    if args.gateway_host:
        config = with_gateway_host(config, args.gateway_host)
    if args.gateway_port is not None:
        config = with_gateway_port(config, args.gateway_port)
    if args.hydrate_historical_alerts:
        config = with_hydrate_historical_alerts(config, True)
    clients = build_production_clients(
        kafka_bootstrap_servers=args.kafka_bootstrap_servers,
        redis_url=args.redis_url,
        kafka_config=config.kafka,
        enable_xtquant=not args.disable_xtquant,
        xtquant_sdk_path=args.xtquant_sdk_path,
        xtquant_data_home=args.xtquant_data_home,
        xtquant_port=args.xtquant_port,
        enable_duckdb=not args.disable_duckdb,
    )
    runtime = build_runtime_with_deferred_xtquant(config, clients)
    supervisor = BeastMarketRuntimeSupervisor(runtime)
    symbols = parse_symbols(args.symbols)
    result = asyncio.run(
        run_supervised_runtime(
            supervisor,
            symbols=symbols,
            tick_interval_seconds=args.tick_interval_seconds,
            health_snapshot_path=args.health_snapshot_path,
            health_snapshot_every_ticks=args.health_snapshot_every_ticks,
            health_snapshot_interval_seconds=args.health_snapshot_interval_seconds,
            max_ticks=args.max_ticks,
            install_signal_handlers=True,
        )
    )
    return 0 if result.stop_reason.startswith(("signal:", "stop_event", "max_ticks", "finished")) else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m beast_market.production_runtime")
    parser.add_argument("--config-path", required=True, help="Verified runtime config JSON artifact.")
    parser.add_argument("--symbols", default="", help="Comma-separated BOD warm symbols, e.g. 00700.HK,00939.HK.")
    parser.add_argument("--kafka-bootstrap-servers", default="127.0.0.1:9092")
    parser.add_argument("--redis-url", default="redis://127.0.0.1:6379/0")
    parser.add_argument("--gateway-host", default="", help="Optional override for the verified config gateway host.")
    parser.add_argument("--gateway-port", type=int, default=None, help="Optional override for the verified config gateway port.")
    parser.add_argument("--health-snapshot-path", default="artifacts/runtime-health.json")
    parser.add_argument("--health-snapshot-every-ticks", type=int, default=4)
    parser.add_argument(
        "--health-snapshot-interval-seconds",
        type=float,
        default=None,
        help="Optional wall-clock interval for writing runtime health independently from tick count.",
    )
    parser.add_argument("--tick-interval-seconds", type=float, default=0.25)
    parser.add_argument("--max-ticks", type=int, default=None, help="Stop after N runtime ticks; intended for validation runs.")
    parser.add_argument("--disable-xtquant", action="store_true", help="Run without live xtquant subscriptions.")
    parser.add_argument("--xtquant-sdk-path", type=Path, default=DEFAULT_XTQUANT_SDK_PATH)
    parser.add_argument("--xtquant-data-home", type=Path, default=Path("/home/hliu/xtbackend/.runtime/xtquant"))
    parser.add_argument("--xtquant-port", type=int, default=58628)
    parser.add_argument("--disable-duckdb", action="store_true", help="Use CSV silver reader instead of DuckDB.")
    parser.add_argument(
        "--hydrate-historical-alerts",
        action="store_true",
        help="Hydrate big-trade alerts from silver trade ticks into initial snapshots.",
    )
    return parser


def build_production_clients(
    *,
    kafka_bootstrap_servers: str,
    redis_url: str,
    kafka_config: KafkaAdapterConfig | None = None,
    enable_xtquant: bool = True,
    xtquant_sdk_path: str | Path | None = DEFAULT_XTQUANT_SDK_PATH,
    xtquant_data_home: str | Path = Path("/home/hliu/xtbackend/.runtime/xtquant"),
    xtquant_port: int = 58628,
    enable_duckdb: bool = True,
) -> BeastMarketRuntimeClients:
    try:
        import redis
    except ImportError as error:
        raise RuntimeError("redis package is required to start the production runtime") from error
    try:
        from confluent_kafka import Consumer, Producer, TopicPartition
    except ImportError as error:
        raise RuntimeError("confluent-kafka package is required to start the production runtime") from error

    duckdb_connection = None
    if enable_duckdb:
        try:
            duckdb = importlib.import_module("duckdb")
        except ImportError as error:
            raise RuntimeError("duckdb package is required unless --disable-duckdb is used") from error
        duckdb_connection = duckdb.connect()

    callback_sink = DeferredCallbackSink()
    resolved_kafka = kafka_config or KafkaAdapterConfig()
    consumer = Consumer(
        {
            "bootstrap.servers": kafka_bootstrap_servers,
            "group.id": resolved_kafka.consumer_group,
            "auto.offset.reset": resolved_kafka.auto_offset_reset,
            "enable.auto.commit": False,
        }
    )
    return BeastMarketRuntimeClients(
        kafka_producer=Producer({"bootstrap.servers": kafka_bootstrap_servers}),
        kafka_consumer=ConfluentKafkaConsumerAdapter(consumer, TopicPartition),
        redis_client=redis.Redis.from_url(redis_url),
        duckdb_connection=duckdb_connection,
        market_data_client=XtQuantMarketDataClient(
            callback_sink=callback_sink,
            sdk_path=xtquant_sdk_path,
            data_home=xtquant_data_home,
            port=xtquant_port,
        )
        if enable_xtquant
        else None,
    )


def build_runtime_with_deferred_xtquant(config: BeastMarketRuntimeConfig, clients: BeastMarketRuntimeClients):
    from .app_runtime import build_beast_market_runtime

    runtime = build_beast_market_runtime(config, clients)
    callback_sink = getattr(clients.market_data_client, "callback_sink", None)
    if isinstance(callback_sink, DeferredCallbackSink):
        callback_sink.set(runtime.ingest_worker.receive_callback)
    return runtime


def with_gateway_host(config: BeastMarketRuntimeConfig, gateway_host: str) -> BeastMarketRuntimeConfig:
    return BeastMarketRuntimeConfig(
        **{
            **config.__dict__,
            "gateway_host": gateway_host,
        }
    )


def with_gateway_port(config: BeastMarketRuntimeConfig, gateway_port: int) -> BeastMarketRuntimeConfig:
    return BeastMarketRuntimeConfig(
        **{
            **config.__dict__,
            "gateway_port": gateway_port,
        }
    )


def with_hydrate_historical_alerts(config: BeastMarketRuntimeConfig, enabled: bool) -> BeastMarketRuntimeConfig:
    return BeastMarketRuntimeConfig(
        **{
            **config.__dict__,
            "hydrate_historical_alerts": enabled,
        }
    )


def parse_symbols(raw: str) -> list[str]:
    return [symbol.strip().upper() for symbol in raw.split(",") if symbol.strip()]


class ConfluentKafkaConsumerAdapter:
    def __init__(self, consumer, topic_partition_factory, *, max_poll_records: int = 100) -> None:
        self.consumer = consumer
        self.topic_partition_factory = topic_partition_factory
        self.max_poll_records = max_poll_records
        self._committed_offsets: dict[str, int] = {}
        self._subscribed_topics: set[str] = set()
        self._buffers: dict[str, list[dict]] = {}

    def poll(self, topic: str, offset: int, timeout_ms: int = 1000) -> list[dict]:
        self._ensure_subscribed(topic)
        records: list[dict] = []
        buffered = self._buffers.get(topic)
        while buffered and len(records) < self.max_poll_records:
            records.append(buffered.pop(0))
        if len(records) >= self.max_poll_records:
            return records

        deadline = time.monotonic() + max(0, timeout_ms) / 1000
        first_poll = True
        while len(records) < self.max_poll_records:
            remaining = max(0.0, deadline - time.monotonic())
            message = self.consumer.poll(remaining if first_poll else 0)
            first_poll = False
            if message is None:
                break
            error = message.error()
            if error is not None:
                raise RuntimeError(error)
            record_topic = message.topic() if callable(getattr(message, "topic", None)) else topic
            record = {
                "key": message.key(),
                "value": message.value(),
                "offset": int(message.offset()),
            }
            if record_topic == topic:
                records.append(record)
            else:
                self._buffers.setdefault(record_topic, []).append(record)
        return records

    def commit(self, topic: str, offset: int) -> None:
        self._committed_offsets[topic] = offset
        partition = self.topic_partition_factory(topic, 0, offset)
        self.consumer.commit(offsets=[partition], asynchronous=False)

    def committed(self, topic: str) -> int:
        return self._committed_offsets.get(topic, 0)

    def high_watermark(self, topic: str) -> int:
        partition = self.topic_partition_factory(topic, 0)
        low, high = self.consumer.get_watermark_offsets(partition, timeout=1.0, cached=False)
        return int(high)

    def _ensure_subscribed(self, topic: str) -> None:
        if topic in self._subscribed_topics:
            return
        self._subscribed_topics.add(topic)
        self.consumer.subscribe(sorted(self._subscribed_topics))


if __name__ == "__main__":
    raise SystemExit(main())
