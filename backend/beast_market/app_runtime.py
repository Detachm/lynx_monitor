from __future__ import annotations

import asyncio
import json
import re
import signal
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from json import JSONDecodeError
from pathlib import Path
from typing import Any, Callable, Iterable

from .adapters import BoundedRawEventQueue, DeadLetterRecord, FileBackedSpool, ReliableEventBus
from .active_pool import ActivePoolConfig, ActiveSymbolPoolManager, DEFAULT_EXCLUDE_INSTRUMENT_TYPES
from .contracts import (
    PROCESSED_TOPIC,
    RAW_TOPIC,
    REDIS_RUNTIME_SNAPSHOT_KEY_FAMILIES,
    REDIS_RUNTIME_SNAPSHOT_KEY_TEMPLATES,
    SCHEMA_VERSION,
    TERMINAL_MESSAGE_PROTOCOL,
    now_iso,
    make_raw_market_event,
)
from .freshness import FreshnessPolicy
from .gateway_transport import GatewayV2SessionManager
from .mammoth_api import DuckDBParquetSilverTableReader, MammothAPI
from .pipeline import GatewayV2, OctopusComputeV2, RealtimeCollectorV2, minute_bucket, rollover_realtime_session, snapshot_trade_date
from .production_adapters import (
    KafkaAdapterConfig,
    KafkaEventBusAdapter,
    RedisAdapterConfig,
    RedisSnapshotCacheAdapter,
)
from .runtime import (
    MarketDataSubscriptionClient,
    RawEventConsumerWorker,
    RealtimeIngestWorker,
    XtQuantSubscriptionManager,
    normalize_subscription_symbol,
    normalize_source_timestamp,
    normalize_xtquant_callback,
)
from .runtime_state import RuntimeStateStore
from .shadow_run import record_v2_runtime_tick
from .symbol_runtime import SymbolRuntimeManager
from .websocket_server import GatewayV2WebSocketService


TRADE_DATE_PATTERN = re.compile(r"^\d{8}$")
SECRET_KEY_FRAGMENTS = ("password", "passwd", "secret", "token", "credential", "apikey", "api_key")


class RuntimeLifecycleState(str, Enum):
    BOOTSTRAP = "BOOTSTRAP"
    HISTORICAL_READY = "HISTORICAL_READY"
    CACHE_WARMING = "CACHE_WARMING"
    RECOVERING_INTRADAY = "RECOVERING_INTRADAY"
    LIVE = "LIVE"
    DEGRADED = "DEGRADED"
    CLOSING = "CLOSING"


@dataclass(frozen=True)
class BeastMarketRuntimeConfig:
    trade_date: str
    silver_root: str | Path
    runtime_state_root: str | Path = "artifacts/runtime-state"
    kafka_spool_dir: str | Path | None = None
    gateway_host: str = "0.0.0.0"
    gateway_port: int = 9020
    gateway_path: str = "/ws"
    raw_queue_max_size: int = 10_000
    client_queue_size: int = 100
    kafka_retries: int = 3
    symbol_eviction_grace_seconds: float = 300
    max_concurrent_hydrations: int = 8
    max_raw_records_per_tick: int = 50
    startup_intraday_recovery: bool = True
    persist_realtime_events: bool = True
    commit_runtime_owned_raw_offsets: bool = True
    big_trade_volume_baseline_ratio: float = 0.0005
    hydrate_historical_alerts: bool = False
    active_pool: ActivePoolConfig = field(default_factory=ActivePoolConfig)
    freshness_policy: FreshnessPolicy = FreshnessPolicy()
    kafka: KafkaAdapterConfig = KafkaAdapterConfig()
    redis: RedisAdapterConfig = RedisAdapterConfig()


@dataclass(frozen=True)
class BeastMarketRuntimeClients:
    kafka_producer: Any
    kafka_consumer: Any
    redis_client: Any
    duckdb_connection: Any | None = None
    market_data_client: MarketDataSubscriptionClient | None = None


@dataclass
class BeastMarketRuntime:
    config: BeastMarketRuntimeConfig
    mammoth: MammothAPI
    kafka_adapter: KafkaEventBusAdapter
    event_bus: ReliableEventBus
    cache: RedisSnapshotCacheAdapter
    runtime_state: RuntimeStateStore
    raw_queue: BoundedRawEventQueue
    collector: RealtimeCollectorV2
    ingest_worker: RealtimeIngestWorker
    subscription_manager: XtQuantSubscriptionManager | None
    octopus: OctopusComputeV2
    raw_consumer_worker: RawEventConsumerWorker
    gateway: GatewayV2
    active_pool_manager: ActiveSymbolPoolManager
    symbol_runtime_manager: SymbolRuntimeManager
    session_manager: GatewayV2SessionManager
    websocket_service: GatewayV2WebSocketService
    effective_trade_date_by_symbol: dict[str, str] = field(default_factory=dict)
    startup_cache_read_errors_by_symbol: dict[str, str] = field(default_factory=dict)


@dataclass
class BeastMarketSupervisorStats:
    starts: int = 0
    stops: int = 0
    ticks: int = 0
    ingested_events: int = 0
    processed_events: int = 0
    broadcast_messages: int = 0
    freshness_checks: int = 0
    started_at: str | None = None
    last_tick_at: str | None = None
    stopped_at: str | None = None
    stop_reason: str | None = None
    runtime_state: str = RuntimeLifecycleState.BOOTSTRAP.value
    blockers: list[str] | None = None


@dataclass(frozen=True)
class BeastMarketRuntimeRunResult:
    ticks: int
    health_snapshot_path: Path | None
    final_health_snapshot: dict[str, Any] | None
    stop_reason: str


@dataclass
class RuntimeSignalStopController:
    stop_event: asyncio.Event
    received_signal: str | None = None
    _cleanup_callbacks: list[Callable[[], None]] | None = None

    def request_stop(self, signal_name: str) -> None:
        self.received_signal = signal_name
        self.stop_event.set()

    def cleanup(self) -> None:
        for callback in self._cleanup_callbacks or []:
            callback()
        self._cleanup_callbacks = []


class BeastMarketRuntimeSupervisor:
    def __init__(self, runtime: BeastMarketRuntime) -> None:
        self.runtime = runtime
        self.stats = BeastMarketSupervisorStats()
        self.running = False

    def start(self, symbols: list[str] | None = None) -> None:
        requested_symbols = normalize_startup_symbols(symbols or [])
        if requested_symbols:
            symbol_list = self.runtime.active_pool_manager.bootstrap_explicit_symbols(requested_symbols)
        else:
            symbol_list = self.runtime.active_pool_manager.rebuild_base_active()
        self._transition(RuntimeLifecycleState.BOOTSTRAP)
        readiness = evaluate_monitoring_historical_readiness(
            self.runtime.mammoth,
            symbols=symbol_list,
            trade_date=self.runtime.config.trade_date,
        )
        if not readiness["passed"]:
            self.stats.blockers = list(readiness["blockers"])
            self._mark_degraded()
            raise RuntimeError(f"historical data not ready: {', '.join(readiness['blockers'])}")

        self._transition(RuntimeLifecycleState.HISTORICAL_READY)
        cached_snapshots = {
            symbol: startup_cached_snapshot(self.runtime, symbol)
            for symbol in symbol_list
        }
        effective_trade_dates = {
            symbol: effective_trade_date_for_symbol(
                self.runtime.mammoth,
                symbol=symbol,
                requested_trade_date=self.runtime.config.trade_date,
            )
            for symbol in symbol_list
        }
        self.runtime.effective_trade_date_by_symbol = dict(effective_trade_dates)
        try:
            self._transition(RuntimeLifecycleState.CACHE_WARMING)
            for symbol in symbol_list:
                cached_snapshot = cached_snapshots.get(symbol)
                if isinstance(cached_snapshot, dict):
                    apply_symbol_display_name(self.runtime, symbol, cached_snapshot)
                effective_trade_date = effective_trade_dates[symbol]
                if cached_snapshot is not None and is_terminal_snapshot_usable(
                    cached_snapshot,
                    requested_trade_date=self.runtime.config.trade_date,
                    effective_trade_date=effective_trade_date,
                ):
                    self.runtime.octopus.ensure_bod_context(
                        symbol,
                        effective_trade_date,
                        hydrate_participant_history=True,
                    )
                    self.runtime.octopus.set_state(symbol, cached_snapshot)
                else:
                    self.runtime.octopus.preload_bod(
                        symbol,
                        effective_trade_date,
                        cache_trade_date=self.runtime.config.trade_date,
                        requested_trade_date=self.runtime.config.trade_date,
                    )
                cache_read_error = self.runtime.startup_cache_read_errors_by_symbol.get(symbol)
                current_snapshot = self.runtime.octopus.get_state(symbol)
                if cache_read_error and current_snapshot is not None:
                    mark_snapshot_degraded(current_snapshot, cache_read_error)
            self._transition(RuntimeLifecycleState.RECOVERING_INTRADAY)
            for symbol in symbol_list:
                if self.runtime.config.startup_intraday_recovery:
                    recover_symbol_intraday(
                        self.runtime,
                        symbol,
                        cached_snapshot=cached_snapshots.get(symbol),
                        data_trade_date=effective_trade_dates[symbol],
                    )
                current_snapshot = self.runtime.octopus.get_state(symbol)
                if isinstance(current_snapshot, dict):
                    if should_attach_realtime_for_symbol(self.runtime, symbol):
                        promote_snapshot_to_realtime_session(self.runtime, symbol, current_snapshot)
                    self.runtime.symbol_runtime_manager.seed_snapshot(symbol, current_snapshot)
            if self.runtime.subscription_manager is not None:
                self.runtime.subscription_manager.start()
                for symbol in symbol_list:
                    if should_attach_realtime_for_symbol(self.runtime, symbol):
                        self.runtime.symbol_runtime_manager.activate_symbol(symbol, strict_realtime=True)
                        seed_symbol_from_market_full_tick(self.runtime, symbol)
            self.running = True
            self.stats.starts += 1
            self.stats.started_at = now_iso()
            self.stats.stopped_at = None
            self.stats.stop_reason = None
            self.stats.blockers = []
            self._transition(RuntimeLifecycleState.LIVE)
        except Exception:
            self._mark_degraded()
            raise

    def stop(self, *, reason: str | None = None) -> None:
        self._transition(RuntimeLifecycleState.CLOSING)
        if self.runtime.subscription_manager is not None and self.runtime.subscription_manager.running:
            self.runtime.subscription_manager.stop()
        self.running = False
        self.stats.stops += 1
        self.stats.stopped_at = now_iso()
        self.stats.stop_reason = reason

    def _transition(self, state: RuntimeLifecycleState) -> None:
        self.stats.runtime_state = state.value

    def _mark_degraded(self) -> None:
        self._transition(RuntimeLifecycleState.DEGRADED)
        self.runtime.collector.health.process = "degraded"
        self.runtime.octopus.health.process = "degraded"
        self.runtime.gateway.health.process = "degraded"

    async def tick_once(
        self,
        *,
        now: str | None = None,
        max_raw_records: int | None = None,
    ) -> dict[str, Any]:
        ingested = drain_ingest_worker(self.runtime.ingest_worker, max_raw_records)
        runtime_owned_raw_path = self.runtime.raw_consumer_worker.runtime_event_processor is not None
        if runtime_owned_raw_path:
            processed, direct_terminal_messages = process_runtime_owned_raw_events(
                self.runtime.raw_consumer_worker,
                ingested,
                self.runtime.config.trade_date,
                commit_offsets=self.runtime.config.commit_runtime_owned_raw_offsets,
            )
        else:
            processed = self.runtime.raw_consumer_worker.poll_and_process(
                self.runtime.config.trade_date,
                max_records=max_raw_records,
            )
            direct_terminal_messages = self.runtime.gateway.terminal_messages_from_processed(processed)
        if self.runtime.config.persist_realtime_events:
            for raw_event in ingested:
                self.runtime.runtime_state.append_raw_event(self.runtime.config.trade_date, raw_event["symbol"], raw_event)
            for processed_event in processed:
                self.runtime.runtime_state.append_processed_event(
                    self.runtime.config.trade_date,
                    processed_event["symbol"],
                    processed_event,
                )
        runtime_processed_events_applied = (
            len(processed)
            if runtime_owned_raw_path
            else self.runtime.symbol_runtime_manager.apply_processed_events(processed)
        )
        if runtime_owned_raw_path:
            self.runtime.raw_consumer_worker.last_terminal_messages = list(direct_terminal_messages)
        self.runtime.gateway.record_direct_terminal_messages(direct_terminal_messages)
        direct_terminal_enqueued = self.runtime.session_manager.broadcast_runtime_messages(
            direct_terminal_messages,
            update_symbol_runtime=False,
        )
        self.runtime.symbol_runtime_manager.mark_deltas_delivered(direct_terminal_messages, direct_terminal_enqueued)
        if runtime_owned_raw_path and not self.runtime.config.commit_runtime_owned_raw_offsets:
            shadow_processed_drained = 0
        else:
            try:
                shadow_processed_drained = self.runtime.gateway.drain_processed_shadow_records()
            except Exception:
                shadow_processed_drained = 0
        freshness = None
        if self.runtime.subscription_manager is not None:
            freshness = self.runtime.subscription_manager.check_freshness_and_resubscribe(now=now)
            self.stats.freshness_checks += 1
        evicted_symbols = self.runtime.symbol_runtime_manager.evict_expired()
        broadcast = await self.runtime.websocket_service.broadcast_once()

        self.stats.ticks += 1
        self.stats.ingested_events += len(ingested)
        self.stats.processed_events += len(processed)
        self.stats.broadcast_messages += direct_terminal_enqueued + broadcast
        self.stats.last_tick_at = now or now_iso()
        return {
            "ingested_events": len(ingested),
            "processed_events": len(processed),
            "broadcast_messages": direct_terminal_enqueued + broadcast,
            "runtime_terminal_messages": direct_terminal_messages,
            "runtime_processed_events_applied": runtime_processed_events_applied,
            "runtime_terminal_messages_enqueued": direct_terminal_enqueued,
            "shadow_processed_drained": shadow_processed_drained,
            "freshness": freshness,
            "evicted_symbols": evicted_symbols,
            "raw_events": ingested,
            "processed_event_payloads": processed,
            "terminal_messages": [*direct_terminal_messages, *self.runtime.gateway.last_terminal_messages],
        }

    def health_snapshot(self, *, generated_at: str | None = None) -> dict[str, Any]:
        return build_runtime_health_snapshot(self, generated_at=generated_at)


def drain_ingest_worker(worker: RealtimeIngestWorker, max_records: int | None = None) -> list[dict[str, Any]]:
    if max_records is not None and max_records <= 0:
        return []
    events: list[dict[str, Any]] = []
    while worker.queue.backlog and (max_records is None or len(events) < max_records):
        event = worker.drain_once()
        if event is not None:
            events.append(event)
    return events


def process_runtime_owned_raw_events(
    worker: RawEventConsumerWorker,
    raw_events: list[dict[str, Any]],
    trade_date: str,
    *,
    commit_offsets: bool = True,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    processed_events: list[dict[str, Any]] = []
    terminal_messages: list[dict[str, Any]] = []
    for raw_event in raw_events:
        try:
            processed, messages = worker._process_raw_event(raw_event, trade_date)
        except Exception as error:
            worker.stats.failed += 1
            worker.stats.dead_letters.append(
                DeadLetterRecord(
                    topic=worker.topic,
                    key=str(raw_event.get("symbol") or ""),
                    value=raw_event,
                    reason=str(error),
                )
            )
            continue
        processed_events.extend(processed)
        terminal_messages.extend(messages)
        worker.stats.processed += 1
    if raw_events and commit_offsets:
        next_offset = worker.consumer.committed_offset(worker.topic) + len(raw_events)
        try:
            worker.consumer.commit(worker.topic, next_offset)
        except Exception as error:
            worker.stats.failed += 1
            worker.stats.dead_letters.append(
                DeadLetterRecord(
                    topic=worker.topic,
                    key="",
                    value={},
                    reason=f"runtime_owned_raw_commit_failed: {error}",
                )
            )
        else:
            worker.stats.committed_offset = next_offset
    return processed_events, terminal_messages


async def run_supervised_runtime(
    supervisor: BeastMarketRuntimeSupervisor,
    *,
    symbols: list[str] | None = None,
    tick_interval_seconds: float = 0.25,
    health_snapshot_path: str | Path | None = None,
    health_snapshot_every_ticks: int = 1,
    health_snapshot_interval_seconds: float | None = None,
    stop: asyncio.Event | None = None,
    serve_factory: Any | None = None,
    shadow_recorder: Any | None = None,
    max_ticks: int | None = None,
    install_signal_handlers: bool = False,
) -> BeastMarketRuntimeRunResult:
    """Run the v2 runtime lifecycle until stopped.

    Production process managers can either own signal handling and pass a stop
    event, or let this runner bind SIGINT/SIGTERM to the same graceful stop path.
    Tests and smoke runs can use max_ticks for a bounded execution.
    """

    if health_snapshot_every_ticks <= 0:
        raise ValueError("health_snapshot_every_ticks must be positive")
    if health_snapshot_interval_seconds is not None and health_snapshot_interval_seconds <= 0:
        raise ValueError("health_snapshot_interval_seconds must be positive")
    if tick_interval_seconds < 0:
        raise ValueError("tick_interval_seconds must be non-negative")

    stop_event = stop or asyncio.Event()
    ticks = 0
    stop_reason = "not_started"
    final_snapshot: dict[str, Any] | None = None
    snapshot_path = Path(health_snapshot_path) if health_snapshot_path is not None else None
    websocket_service = supervisor.runtime.websocket_service
    previous_shadow_recorder = websocket_service.shadow_recorder
    signal_controller: RuntimeSignalStopController | None = None
    health_snapshot_task: asyncio.Task | None = None

    supervisor.start(symbols or [])
    try:
        stop_reason = "running"
        if install_signal_handlers:
            signal_controller = install_runtime_signal_handlers(stop_event)
        if shadow_recorder is not None:
            websocket_service.shadow_recorder = shadow_recorder
        if snapshot_path is not None and health_snapshot_interval_seconds is not None:
            health_snapshot_task = asyncio.create_task(
                runtime_health_snapshot_loop(
                    supervisor,
                    snapshot_path,
                    interval_seconds=health_snapshot_interval_seconds,
                    stop=stop_event,
                )
            )
        async with websocket_service.serve(serve_factory=serve_factory):
            while not stop_event.is_set():
                tick_result = await supervisor.tick_once(
                    max_raw_records=supervisor.runtime.config.max_raw_records_per_tick,
                )
                ticks += 1
                if shadow_recorder is not None:
                    record_v2_runtime_tick(shadow_recorder, tick_result)
                if (
                    snapshot_path is not None
                    and health_snapshot_interval_seconds is None
                    and ticks % health_snapshot_every_ticks == 0
                ):
                    final_snapshot = write_runtime_health_snapshot(supervisor, snapshot_path)
                if max_ticks is not None and ticks >= max_ticks:
                    stop_reason = "max_ticks"
                    break
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=tick_interval_seconds)
                except TimeoutError:
                    continue
            if stop_event.is_set() and stop_reason == "running":
                if signal_controller is not None and signal_controller.received_signal:
                    stop_reason = f"signal:{signal_controller.received_signal}"
                else:
                    stop_reason = "stop_event"
    finally:
        if health_snapshot_task is not None:
            health_snapshot_task.cancel()
            try:
                await health_snapshot_task
            except asyncio.CancelledError:
                pass
        if signal_controller is not None:
            signal_controller.cleanup()
        websocket_service.shadow_recorder = previous_shadow_recorder
        if stop_reason == "running":
            stop_reason = "finished"
        supervisor.stats.stop_reason = stop_reason
        if snapshot_path is not None:
            final_snapshot = write_runtime_health_snapshot(supervisor, snapshot_path)
        supervisor.stop(reason=stop_reason)

    return BeastMarketRuntimeRunResult(
        ticks=ticks,
        health_snapshot_path=snapshot_path,
        final_health_snapshot=final_snapshot,
        stop_reason=stop_reason,
    )


async def runtime_health_snapshot_loop(
    supervisor: BeastMarketRuntimeSupervisor,
    path: str | Path,
    *,
    interval_seconds: float,
    stop: asyncio.Event,
) -> None:
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    while not stop.is_set():
        write_runtime_health_snapshot(supervisor, path)
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_seconds)
        except TimeoutError:
            continue


def install_runtime_signal_handlers(
    stop_event: asyncio.Event,
    *,
    loop: Any | None = None,
    signals: Iterable[signal.Signals] | None = None,
) -> RuntimeSignalStopController:
    """Bind process signals to the runtime stop event and return a cleanup handle."""

    event_loop = loop or asyncio.get_running_loop()
    signal_values = tuple(signals or (signal.SIGINT, signal.SIGTERM))
    controller = RuntimeSignalStopController(stop_event=stop_event, _cleanup_callbacks=[])
    for signal_value in signal_values:
        signal_name = signal_value.name
        handler = lambda name=signal_name: controller.request_stop(name)
        try:
            event_loop.add_signal_handler(signal_value, handler)
        except (NotImplementedError, RuntimeError, ValueError):
            continue
        controller._cleanup_callbacks.append(
            lambda value=signal_value: event_loop.remove_signal_handler(value)
        )
    return controller


def build_beast_market_runtime(
    config: BeastMarketRuntimeConfig,
    clients: BeastMarketRuntimeClients,
) -> BeastMarketRuntime:
    kafka_adapter = KafkaEventBusAdapter(
        clients.kafka_producer,
        clients.kafka_consumer,
        config.kafka,
    )
    kafka_spool_dir = Path(config.kafka_spool_dir) if config.kafka_spool_dir is not None else Path(config.runtime_state_root) / "kafka-spool"
    event_bus = ReliableEventBus(
        kafka_adapter,
        retries=config.kafka_retries,
        spool=FileBackedSpool(kafka_spool_dir / "publish-failures.jsonl"),
    )
    cache = RedisSnapshotCacheAdapter(clients.redis_client, config.redis)
    runtime_state = RuntimeStateStore(config.runtime_state_root)
    silver_reader = build_silver_reader(config, clients)
    mammoth = MammothAPI(config.silver_root) if silver_reader is None else MammothAPI(reader=silver_reader)
    raw_queue = BoundedRawEventQueue(config.raw_queue_max_size)
    collector = RealtimeCollectorV2(
        event_bus,
        freshness_policy=config.freshness_policy,
    )
    ingest_worker = RealtimeIngestWorker(
        raw_queue,
        collector,
        normalizer=normalize_xtquant_callback,
        reject_sink=lambda payload, reason: runtime_state.append_callback_rejection(config.trade_date, payload, reason),
    )
    subscription_manager = (
        XtQuantSubscriptionManager(clients.market_data_client, collector)
        if clients.market_data_client is not None
        else None
    )
    octopus = OctopusComputeV2(
        mammoth,
        event_bus,
        cache,
        big_trade_volume_baseline_ratio=config.big_trade_volume_baseline_ratio,
        hydrate_historical_alerts=config.hydrate_historical_alerts,
    )
    raw_consumer_worker = RawEventConsumerWorker(
        kafka_adapter,
        octopus,
        topic=config.kafka.raw_topic,
        dead_letter_sink=lambda dead_letter: runtime_state.append_raw_consumer_dead_letter(
            config.trade_date,
            topic=dead_letter.topic,
            key=dead_letter.key,
            value=dead_letter.value,
            reason=dead_letter.reason,
        ),
    )
    gateway = GatewayV2(event_bus, cache)
    active_pool_manager = ActiveSymbolPoolManager(
        mammoth,
        trade_date=config.trade_date,
        config=config.active_pool,
    )
    symbol_runtime_manager = SymbolRuntimeManager(
        gateway,
        trade_date=config.trade_date,
        release_symbol=live_release_symbol(subscription_manager),
        state_sink=lambda symbol, state: cache.set_terminal_state(config.trade_date, symbol, state),
        snapshot_sink=lambda symbol, snapshot: restore_terminal_snapshot_if_missing(
            cache,
            config.trade_date,
            symbol,
            snapshot,
        ),
        active_pool_manager=active_pool_manager,
        eviction_grace_seconds=config.active_pool.eviction_grace_seconds,
        max_concurrent_hydrations=config.max_concurrent_hydrations,
    )
    raw_consumer_worker.state_provider = symbol_runtime_manager.snapshot_payload
    raw_consumer_worker.runtime_event_processor = (
        lambda raw_event, trade_date: symbol_runtime_manager.apply_raw_event(
            raw_event,
            trade_date,
            octopus.process_raw_event_with_state,
        )
    )
    session_manager = GatewayV2SessionManager(
        gateway,
        trade_date=config.trade_date,
        history_provider=history_provider(mammoth, config.trade_date),
        client_queue_size=config.client_queue_size,
        symbol_runtime_manager=symbol_runtime_manager,
        consume_processed_on_broadcast=False,
    )
    websocket_service = GatewayV2WebSocketService(
        session_manager,
        host=config.gateway_host,
        port=config.gateway_port,
        path=config.gateway_path,
    )

    runtime = BeastMarketRuntime(
        config=config,
        mammoth=mammoth,
        kafka_adapter=kafka_adapter,
        event_bus=event_bus,
        cache=cache,
        runtime_state=runtime_state,
        raw_queue=raw_queue,
        collector=collector,
        ingest_worker=ingest_worker,
        subscription_manager=subscription_manager,
        octopus=octopus,
        raw_consumer_worker=raw_consumer_worker,
        gateway=gateway,
        active_pool_manager=active_pool_manager,
        symbol_runtime_manager=symbol_runtime_manager,
        session_manager=session_manager,
        websocket_service=websocket_service,
    )
    symbol_runtime_manager.attach_realtime = live_attach_symbol(
        subscription_manager,
        should_attach=lambda symbol: should_attach_realtime_for_symbol(runtime, symbol),
    )
    symbol_runtime_manager.hydrate_symbol = lambda symbol: hydrate_symbol_snapshot(runtime, symbol)
    session_manager.realtime_seed_provider = lambda symbol: seed_symbol_from_market_full_tick(runtime, symbol)
    return runtime


def evaluate_monitoring_historical_readiness(
    mammoth: MammothAPI,
    *,
    symbols: list[str],
    trade_date: str,
) -> dict[str, Any]:
    blockers: list[str] = []
    evidence: dict[str, Any] = {
        "trade_date": trade_date,
        "symbols": list(symbols),
        "daily_bars": {},
        "minute_bars": {},
        "ccass_holdings": {},
        "broker_mapping": {},
        "effective_trade_date_by_symbol": {},
    }
    if not TRADE_DATE_PATTERN.fullmatch(trade_date):
        blockers.append("trade_date_invalid")

    try:
        broker_mapping = mammoth.get_broker_mapping()
        evidence["broker_mapping"] = {"row_count": len(broker_mapping)}
        if not broker_mapping:
            blockers.append("missing_broker_mapping")
    except Exception as error:
        evidence["broker_mapping"] = {"error": str(error)}
        blockers.append("missing_broker_mapping")

    missing_daily_symbols: list[str] = []
    missing_minute_symbols: list[str] = []
    missing_ccass_symbols: list[str] = []
    for symbol in symbols:
        effective_trade_date = trade_date
        try:
            effective_trade_date = mammoth.get_latest_available_trade_date(symbol, trade_date) or trade_date
        except Exception as error:
            evidence["effective_trade_date_by_symbol"][symbol] = {"error": str(error), "effective_trade_date": ""}
        else:
            evidence["effective_trade_date_by_symbol"][symbol] = effective_trade_date
        try:
            daily_bars = mammoth.get_recent_daily_bars(symbol, trade_date, 2)
            evidence["daily_bars"][symbol] = {
                "dates": [str(row["trade_date"]) for row in daily_bars],
                "row_count": len(daily_bars),
            }
            if len(daily_bars) < 2:
                missing_daily_symbols.append(symbol)
        except Exception as error:
            evidence["daily_bars"][symbol] = {"error": str(error), "row_count": 0, "dates": []}
            missing_daily_symbols.append(symbol)

        try:
            minute_bars = mammoth.get_minute_bars(symbol, effective_trade_date)
            evidence["minute_bars"][symbol] = {
                "effective_trade_date": effective_trade_date,
                "row_count": len(minute_bars),
            }
            if not minute_bars:
                missing_minute_symbols.append(symbol)
        except Exception as error:
            evidence["minute_bars"][symbol] = {
                "effective_trade_date": effective_trade_date,
                "error": str(error),
                "row_count": 0,
            }
            missing_minute_symbols.append(symbol)

        try:
            ccass_pair = mammoth.get_ccass_holding_pair(symbol, trade_date)
            evidence["ccass_holdings"][symbol] = {
                "current_date": ccass_pair["current_date"],
                "previous_date": ccass_pair["previous_date"],
                "current_row_count": len(ccass_pair["current_rows"]),
                "previous_row_count": len(ccass_pair["previous_rows"]),
            }
            if not ccass_pair["current_rows"] or not ccass_pair["previous_rows"]:
                missing_ccass_symbols.append(symbol)
        except Exception as error:
            evidence["ccass_holdings"][symbol] = {"error": str(error), "current_row_count": 0, "previous_row_count": 0}
            missing_ccass_symbols.append(symbol)

    if missing_daily_symbols:
        blockers.append("missing_daily_bars")
        evidence["missing_daily_symbols"] = sorted(set(missing_daily_symbols))
    if missing_minute_symbols:
        blockers.append("missing_minute_bars")
        evidence["missing_minute_symbols"] = sorted(set(missing_minute_symbols))
    if missing_ccass_symbols:
        blockers.append("missing_ccass_holdings")
        evidence["missing_ccass_symbols"] = sorted(set(missing_ccass_symbols))

    return {
        "schema_version": 1,
        "passed": not blockers,
        "blockers": sorted(set(blockers)),
        "evidence": evidence,
    }


def effective_trade_date_for_symbol(
    mammoth: MammothAPI,
    *,
    symbol: str,
    requested_trade_date: str,
) -> str:
    try:
        latest_date = mammoth.get_latest_available_trade_date(symbol, requested_trade_date)
    except Exception:
        latest_date = ""
    return latest_date or requested_trade_date


def normalize_startup_symbols(symbols: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_symbol in symbols:
        try:
            symbol = normalize_subscription_symbol(raw_symbol)
        except ValueError:
            continue
        if symbol not in seen:
            seen.add(symbol)
            normalized.append(symbol)
    return normalized


def should_attach_realtime_for_symbol(runtime: BeastMarketRuntime, symbol: str) -> bool:
    requested_trade_date = runtime.config.trade_date
    if runtime.effective_trade_date_by_symbol.get(symbol, requested_trade_date) == requested_trade_date:
        return True
    trading_day = runtime.mammoth.is_trading_day(requested_trade_date, market="HK")
    return bool(trading_day)


def promote_snapshot_to_realtime_session(runtime: BeastMarketRuntime, symbol: str, snapshot: dict[str, Any]) -> dict[str, Any]:
    trade_date = runtime.config.trade_date
    apply_symbol_display_name(runtime, symbol, snapshot)
    if snapshot_trade_date(snapshot) == trade_date:
        return snapshot
    updated_at = now_iso()
    rollover_realtime_session(snapshot, trade_date, updated_at)
    freshness = dict(snapshot.get("freshness") or {})
    source_dates = dict(freshness.get("source_dates") or {})
    source_dates["realtime_session"] = trade_date
    degraded_reasons = list(freshness.get("degraded_reasons") or [])
    if "intraday_gap_before_attach" not in degraded_reasons:
        degraded_reasons.append("intraday_gap_before_attach")
    snapshot["freshness"] = {
        **freshness,
        "updated_at": updated_at,
        "requested_trade_date": trade_date,
        "effective_trade_date": trade_date,
        "runtime_state": "LIVE",
        "source_dates": source_dates,
        "degraded": True,
        "degraded_reasons": degraded_reasons,
    }
    runtime.octopus.set_state(symbol, snapshot)
    runtime.cache.set_terminal_snapshot(trade_date, symbol, snapshot)
    return snapshot


def apply_symbol_display_name(runtime: BeastMarketRuntime, symbol: str, snapshot: dict[str, Any]) -> dict[str, Any]:
    inner_snapshot = snapshot.get("snapshot")
    if not isinstance(inner_snapshot, dict):
        return snapshot
    current_name = str(inner_snapshot.get("name") or "")
    if current_name and current_name != symbol:
        return snapshot
    get_instrument_name = getattr(runtime.mammoth, "get_instrument_name", None)
    try:
        display_name = get_instrument_name(symbol) if callable(get_instrument_name) else ""
    except Exception:
        display_name = ""
    if isinstance(display_name, str) and display_name.strip():
        inner_snapshot["name"] = display_name.strip()
    return snapshot


def seed_symbol_from_market_full_tick(runtime: BeastMarketRuntime, symbol: str) -> list[dict[str, Any]]:
    if runtime.subscription_manager is None:
        return []
    get_full_ticks = getattr(runtime.subscription_manager.client, "get_full_ticks", None)
    if not callable(get_full_ticks):
        return []
    try:
        ticks = get_full_ticks([symbol])
    except Exception:
        return []
    full_tick = ticks.get(symbol)
    if not isinstance(full_tick, dict):
        return []
    price = first_numeric(full_tick, "lastPrice", "last_price", "price")
    volume = first_numeric(full_tick, "volume", "pvolume", "Volume")
    turnover = first_numeric(full_tick, "amount", "turnover", "Amount")
    if price is None or volume is None:
        return []
    raw_event = make_raw_market_event(
        kind="tick",
        symbol=symbol,
        source="xtquant_full_tick",
        seq=runtime.octopus.seq_by_symbol.get(symbol, 0) + 1,
        source_ts=full_tick_source_ts(full_tick),
        payload={
            "price": float(price),
            "volume": int(float(volume)),
            "turnover": float(turnover if turnover is not None else float(price) * float(volume)),
            "side": "",
            "broker_code": "",
        },
        period="full_tick",
    )
    processed = runtime.octopus.process_raw_event(raw_event, runtime.config.trade_date)
    runtime.runtime_state.append_raw_event(runtime.config.trade_date, symbol, raw_event)
    for event in processed:
        runtime.runtime_state.append_processed_event(runtime.config.trade_date, symbol, event)
    runtime.symbol_runtime_manager.apply_processed_events(processed)
    return processed


def first_numeric(data: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = data.get(key)
        if value in (None, ""):
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def full_tick_source_ts(full_tick: dict[str, Any]) -> str:
    timetag = full_tick.get("timetag")
    if isinstance(timetag, str) and len(timetag) >= 17:
        try:
            parsed = datetime.strptime(timetag[:17], "%Y%m%d %H:%M:%S")
            return parsed.isoformat(timespec="milliseconds") + "+08:00"
        except ValueError:
            pass
    return normalize_source_timestamp(full_tick.get("time") or full_tick.get("timestamp"))


def mark_snapshot_degraded(snapshot: dict[str, Any], reason: str) -> dict[str, Any]:
    freshness = dict(snapshot.get("freshness") or {})
    reasons = list(freshness.get("degraded_reasons") or [])
    if reason not in reasons:
        reasons.append(reason)
    freshness["degraded"] = True
    freshness["degraded_reasons"] = reasons
    snapshot["freshness"] = freshness
    return snapshot


def startup_cached_snapshot(runtime: BeastMarketRuntime, symbol: str) -> dict[str, Any] | None:
    try:
        snapshot = runtime.cache.get_terminal_snapshot(runtime.config.trade_date, symbol)
        runtime.startup_cache_read_errors_by_symbol.pop(symbol, None)
        return snapshot
    except Exception as error:
        runtime.octopus.health.process = "degraded"
        runtime.octopus.health.redis = "degraded"
        runtime.collector.health.process = "degraded"
        runtime.gateway.health.process = "degraded"
        runtime.startup_cache_read_errors_by_symbol[symbol] = f"redis_terminal_snapshot_read_failed: {error}"
        return None


def restore_terminal_snapshot_if_missing(
    cache: SnapshotCache,
    trade_date: str,
    symbol: str,
    snapshot: dict[str, Any],
) -> None:
    try:
        existing = cache.get_terminal_snapshot(trade_date, symbol)
    except Exception as error:
        raise RuntimeError(f"redis_terminal_snapshot_read_failed: {error}") from error
    if existing is None:
        cache.set_terminal_snapshot(trade_date, symbol, snapshot)


def hydrate_symbol_snapshot(runtime: BeastMarketRuntime, symbol: str) -> dict[str, Any]:
    requested_trade_date = runtime.config.trade_date
    try:
        cached = runtime.cache.get_terminal_snapshot(requested_trade_date, symbol)
    except Exception as error:
        runtime.octopus.health.process = "degraded"
        runtime.octopus.health.redis = "degraded"
        cached = None
        cached_read_error = f"redis_terminal_snapshot_read_failed: {error}"
    else:
        cached_read_error = ""
    effective_trade_date = effective_trade_date_for_symbol(
        runtime.mammoth,
        symbol=symbol,
        requested_trade_date=requested_trade_date,
    )
    runtime.effective_trade_date_by_symbol[symbol] = effective_trade_date
    if cached is not None and is_terminal_snapshot_usable(
        cached,
        requested_trade_date=requested_trade_date,
        effective_trade_date=effective_trade_date,
    ):
        normalize_snapshot_minute_bars(cached)
        apply_symbol_display_name(runtime, symbol, cached)
        runtime.octopus.ensure_bod_context(symbol, effective_trade_date, hydrate_participant_history=True)
        runtime.octopus.set_state(symbol, cached)
        return cached
    snapshot = runtime.octopus.preload_bod(
        symbol,
        effective_trade_date,
        cache_trade_date=requested_trade_date,
        requested_trade_date=requested_trade_date,
    )
    apply_symbol_display_name(runtime, symbol, snapshot)
    if should_attach_realtime_for_symbol(runtime, symbol):
        promote_snapshot_to_realtime_session(runtime, symbol, snapshot)
        seed_symbol_from_market_full_tick(runtime, symbol)
        seeded_snapshot = runtime.octopus.get_state(symbol)
        if isinstance(seeded_snapshot, dict):
            snapshot = seeded_snapshot
    return snapshot if not cached_read_error else mark_snapshot_degraded(snapshot, cached_read_error)


def recover_symbol_intraday(
    runtime: BeastMarketRuntime,
    symbol: str,
    *,
    cached_snapshot: dict[str, Any] | None = None,
    data_trade_date: str | None = None,
) -> dict[str, Any]:
    request_trade_date = runtime.config.trade_date
    state_trade_date = data_trade_date or request_trade_date
    recovered_raw = 0
    recovered_processed = 0
    replayed_local_raw = 0
    backfilled_ticks = 0
    runtime_processed_applied = 0
    seen = set()
    last_recovered_ts = ""

    if cached_snapshot is not None and is_terminal_snapshot_fresh(cached_snapshot, state_trade_date):
        runtime.octopus.set_state(symbol, cached_snapshot)
        runtime.symbol_runtime_manager.seed_snapshot(symbol, cached_snapshot)
        try:
            runtime.cache.set_terminal_snapshot(request_trade_date, symbol, cached_snapshot)
        except Exception as error:
            runtime.octopus.health.process = "degraded"
            runtime.octopus.health.redis = "degraded"
            mark_snapshot_degraded(cached_snapshot, f"redis_terminal_snapshot_write_failed: {error}")
            runtime.symbol_runtime_manager.seed_snapshot(symbol, cached_snapshot)
        last_recovered_ts = snapshot_recovered_ts(cached_snapshot)
    else:
        current_snapshot = runtime.octopus.get_state(symbol)
        if isinstance(current_snapshot, dict):
            runtime.symbol_runtime_manager.seed_snapshot(symbol, current_snapshot)
        for raw_event in sorted(runtime.runtime_state.load_raw_events(state_trade_date, symbol), key=raw_event_sort_key):
            keys = raw_event_dedupe_keys(raw_event)
            if any(key in seen for key in keys):
                continue
            for key in keys:
                seen.add(key)
            processed = process_recovery_raw_event(runtime, raw_event, request_trade_date)
            replayed_local_raw += 1
            recovered_processed += len(processed)
            runtime_processed_applied += runtime.symbol_runtime_manager.apply_processed_events(processed)
            for processed_event in processed:
                runtime.runtime_state.append_processed_event(state_trade_date, symbol, processed_event)
            source_ts = str(raw_event.get("source_ts") or "")
            if source_ts and iso_after(source_ts, last_recovered_ts):
                last_recovered_ts = source_ts

    for raw_event in backfill_trade_tick_events(runtime, symbol, last_recovered_ts, trade_date=state_trade_date):
        keys = raw_event_dedupe_keys(raw_event)
        if any(key in seen for key in keys):
            continue
        for key in keys:
            seen.add(key)
        processed = process_recovery_raw_event(runtime, raw_event, request_trade_date)
        runtime.runtime_state.append_raw_event(state_trade_date, symbol, raw_event)
        runtime_processed_applied += runtime.symbol_runtime_manager.apply_processed_events(processed)
        for processed_event in processed:
            runtime.runtime_state.append_processed_event(state_trade_date, symbol, processed_event)
        recovered_raw += 1
        recovered_processed += len(processed)
        backfilled_ticks += 1

    return {
        "symbol": symbol,
        "requested_trade_date": request_trade_date,
        "effective_trade_date": state_trade_date,
        "cached_snapshot_used": cached_snapshot is not None and is_terminal_snapshot_fresh(cached_snapshot, state_trade_date),
        "local_raw_replayed": replayed_local_raw,
        "backfilled_ticks": backfilled_ticks,
        "recovered_raw_events": recovered_raw,
        "recovered_processed_events": recovered_processed,
        "runtime_processed_events_applied": runtime_processed_applied,
        "last_recovered_ts": last_recovered_ts,
    }


def process_recovery_raw_event(
    runtime: BeastMarketRuntime,
    raw_event: dict[str, Any],
    request_trade_date: str,
) -> list[dict[str, Any]]:
    if raw_event.get("kind") == "tick":
        return runtime.octopus.process_historical_alert_event(raw_event, request_trade_date)
    return runtime.octopus.process_raw_event(raw_event, request_trade_date)


def backfill_trade_tick_events(
    runtime: BeastMarketRuntime,
    symbol: str,
    last_recovered_ts: str,
    *,
    trade_date: str | None = None,
) -> list[dict[str, Any]]:
    data_trade_date = trade_date or runtime.config.trade_date
    try:
        rows = runtime.mammoth.get_trade_ticks(symbol, data_trade_date)
    except Exception:
        return []
    rows.sort(key=lambda row: str(row.get("tick_ts") or ""))
    events: list[dict[str, Any]] = []
    for row in rows:
        tick_ts = str(row.get("tick_ts") or "")
        if last_recovered_ts and not iso_after(tick_ts, last_recovered_ts):
            continue
        runtime.octopus.seq_by_symbol[symbol] += 1
        payload = {
            "price": float(row["price"]),
            "volume": int(row["volume"]),
            "turnover": float(row["turnover"]),
            "side": str(row.get("side") or "neutral"),
            "broker_code": str(row.get("broker_code") or ""),
        }
        for source_key, target_key in (
            ("participant_id", "participant_id"),
            ("participant_name", "participant_name"),
            ("broker_name", "broker_name"),
            ("trade_id", "trade_id"),
            ("row_hash", "row_hash"),
        ):
            if source_key in row and row[source_key] not in (None, ""):
                payload[target_key] = row[source_key]
        events.append(
            make_raw_market_event(
                kind="tick",
                symbol=symbol,
                source="mammoth",
                seq=runtime.octopus.seq_by_symbol[symbol],
                source_ts=tick_ts,
                payload=payload,
                event_id=f"raw-mammoth-backfill-{symbol}-{safe_event_id_part(tick_ts)}-{safe_event_id_part(str(row.get('trade_id') or row.get('row_hash') or len(events) + 1))}",
            )
        )
    return events


def is_terminal_snapshot_fresh(snapshot: dict[str, Any], trade_date: str) -> bool:
    freshness = snapshot.get("freshness")
    if not isinstance(freshness, dict):
        return False
    effective_trade_date = freshness.get("effective_trade_date") or snapshot_trade_date(snapshot)
    if effective_trade_date != trade_date:
        return False
    source_dates = freshness.get("source_dates")
    if not isinstance(source_dates, dict) or source_dates.get("minute_bars") != trade_date:
        return False
    return isinstance(snapshot.get("minute_bars"), list) and bool(snapshot["minute_bars"])


def is_terminal_snapshot_usable(
    snapshot: dict[str, Any],
    *,
    requested_trade_date: str,
    effective_trade_date: str,
) -> bool:
    return is_terminal_snapshot_fresh(snapshot, effective_trade_date) or is_terminal_snapshot_fresh(
        snapshot,
        requested_trade_date,
    )


def normalize_snapshot_minute_bars(snapshot: dict[str, Any]) -> None:
    raw_bars = snapshot.get("minute_bars")
    if not isinstance(raw_bars, list):
        return
    merged_by_minute: dict[str, dict[str, Any]] = {}
    for raw_bar in raw_bars:
        if not isinstance(raw_bar, dict):
            continue
        timestamp = str(raw_bar.get("timestamp") or "")
        if not timestamp:
            continue
        bucket = minute_bucket(timestamp)
        bar = dict(raw_bar)
        bar["timestamp"] = bucket
        previous = merged_by_minute.get(bucket)
        if previous is None:
            merged_by_minute[bucket] = bar
            continue
        previous["price"] = bar.get("price", bar.get("close", previous.get("price")))
        previous["close"] = bar.get("close", bar.get("price", previous.get("close")))
        previous["high"] = max(float(previous.get("high") or previous.get("price") or 0), float(bar.get("high") or bar.get("price") or 0))
        previous["low"] = min(float(previous.get("low") or previous.get("price") or 0), float(bar.get("low") or bar.get("price") or 0))
        previous["volume"] = int(previous.get("volume") or 0) + int(bar.get("volume") or 0)
        previous["turnover"] = float(previous.get("turnover") or 0) + float(bar.get("turnover") or 0)
        previous["direction"] = bar.get("direction", previous.get("direction", "flat"))
    snapshot["minute_bars"] = [merged_by_minute[key] for key in sorted(merged_by_minute)]


def snapshot_trade_date(snapshot: dict[str, Any]) -> str:
    inner_snapshot = snapshot.get("snapshot")
    if not isinstance(inner_snapshot, dict):
        return ""
    for key in ("tradeDate", "trade_date", "displayTradeDate", "display_trade_date"):
        value = inner_snapshot.get(key)
        if isinstance(value, str) and TRADE_DATE_PATTERN.fullmatch(value):
            return value
    return ""


def snapshot_recovered_ts(snapshot: dict[str, Any]) -> str:
    candidates: list[str] = []
    freshness = snapshot.get("freshness")
    if isinstance(freshness, dict):
        for key in ("source_ts", "ingest_ts", "updated_at"):
            value = freshness.get(key)
            if isinstance(value, str) and value.strip():
                candidates.append(value)
    inner_snapshot = snapshot.get("snapshot")
    if isinstance(inner_snapshot, dict):
        value = inner_snapshot.get("updatedAt") or inner_snapshot.get("updated_at")
        if isinstance(value, str) and value.strip():
            candidates.append(value)
    for minute_bar in snapshot.get("minute_bars", []) if isinstance(snapshot.get("minute_bars"), list) else []:
        if isinstance(minute_bar, dict):
            value = minute_bar.get("timestamp")
            if isinstance(value, str) and value.strip():
                candidates.append(value)
    return max(candidates, key=iso_sort_value) if candidates else ""


def snapshot_latest_minute_ts(snapshot: dict[str, Any]) -> str:
    candidates: list[str] = []
    for minute_bar in snapshot.get("minute_bars", []) if isinstance(snapshot.get("minute_bars"), list) else []:
        if isinstance(minute_bar, dict):
            value = minute_bar.get("timestamp")
            if isinstance(value, str) and value.strip():
                candidates.append(value)
    return max(candidates, key=iso_sort_value) if candidates else ""


def raw_event_sort_key(raw_event: dict[str, Any]) -> tuple[float, str]:
    return (iso_sort_value(str(raw_event.get("source_ts") or "")), str(raw_event.get("event_id") or ""))


def raw_event_dedupe_keys(raw_event: dict[str, Any]) -> set[tuple[str, str]]:
    keys = {("event_id", str(raw_event.get("event_id") or ""))}
    payload = raw_event.get("payload") if isinstance(raw_event.get("payload"), dict) else {}
    if raw_event.get("kind") == "tick":
        trade_id = str(payload.get("trade_id") or payload.get("row_hash") or "")
        if trade_id:
            keys.add(("trade_id", trade_id))
        keys.add((
            "tick",
            "|".join(
                [
                    str(raw_event.get("source_ts") or ""),
                    str(payload.get("price") or ""),
                    str(payload.get("volume") or ""),
                    str(payload.get("turnover") or ""),
                ]
            ),
        ))
    return {key for key in keys if key[1]}


def iso_after(left: str, right: str) -> bool:
    if not right:
        return bool(left)
    return iso_sort_value(left) > iso_sort_value(right)


def iso_sort_value(value: str) -> float:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0.0


def compact_date_from_iso(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return ""
    return parsed.strftime("%Y%m%d")


def safe_event_id_part(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", value).strip("-") or "unknown"


def build_silver_reader(config: BeastMarketRuntimeConfig, clients: BeastMarketRuntimeClients):
    if clients.duckdb_connection is None:
        return None
    return DuckDBParquetSilverTableReader(config.silver_root, clients.duckdb_connection)


def load_runtime_config_artifact(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        decoded = json.load(handle)
    if not isinstance(decoded, dict):
        raise ValueError("runtime config artifact must be a JSON object")
    return decoded


def runtime_config_from_artifact(config: dict[str, Any]) -> BeastMarketRuntimeConfig:
    verification = evaluate_runtime_config_artifact(config)
    if verification.get("passed") is not True:
        blockers = ", ".join(str(blocker) for blocker in verification.get("blockers") or [])
        raise ValueError(f"runtime config artifact failed verification: {blockers}")
    kafka = config.get("kafka") if isinstance(config.get("kafka"), dict) else {}
    redis = config.get("redis") if isinstance(config.get("redis"), dict) else {}
    gateway = config.get("gateway") if isinstance(config.get("gateway"), dict) else {}
    runtime = config.get("runtime") if isinstance(config.get("runtime"), dict) else {}
    freshness = config.get("freshness") if isinstance(config.get("freshness"), dict) else {}
    active_pool = config.get("active_pool") if isinstance(config.get("active_pool"), dict) else {}
    return BeastMarketRuntimeConfig(
        trade_date=str(config.get("trade_date") or ""),
        silver_root=str(config.get("silver_root") or ""),
        runtime_state_root=str(config.get("runtime_state_root") or runtime.get("runtime_state_root") or "artifacts/runtime-state"),
        kafka_spool_dir=runtime.get("kafka_spool_dir"),
        gateway_host=str(gateway.get("host") or "0.0.0.0"),
        gateway_port=int_value(gateway.get("port"), default=9020),
        gateway_path=str(gateway.get("path") or "/ws"),
        raw_queue_max_size=int_value(runtime.get("raw_queue_max_size"), default=10_000),
        client_queue_size=int_value(runtime.get("client_queue_size"), default=100),
        kafka_retries=int_value(runtime.get("kafka_retries"), default=3),
        symbol_eviction_grace_seconds=float_value(runtime.get("symbol_eviction_grace_seconds"), default=300),
        max_concurrent_hydrations=int_value(runtime.get("max_concurrent_hydrations"), default=8),
        max_raw_records_per_tick=int_value(runtime.get("max_raw_records_per_tick"), default=50),
        startup_intraday_recovery=bool_value(runtime.get("startup_intraday_recovery"), default=True),
        persist_realtime_events=bool_value(runtime.get("persist_realtime_events"), default=True),
        commit_runtime_owned_raw_offsets=bool_value(runtime.get("commit_runtime_owned_raw_offsets"), default=True),
        big_trade_volume_baseline_ratio=float_value(runtime.get("big_trade_volume_baseline_ratio"), default=0.0005),
        active_pool=ActivePoolConfig(
            target_size=int_value(active_pool.get("target_size"), default=200),
            pinned_max_size=int_value(active_pool.get("pinned_max_size"), default=100),
            rank_window_days=int_value(active_pool.get("rank_window_days"), default=5),
            rank_metric=str(active_pool.get("rank_metric") or "avg_turnover"),
            exclude_instrument_types=tuple(
                str(value)
                for value in (
                    active_pool.get("exclude_instrument_types")
                    if isinstance(active_pool.get("exclude_instrument_types"), list)
                    else DEFAULT_EXCLUDE_INSTRUMENT_TYPES
                )
            ),
            eviction_grace_seconds=float_value(active_pool.get("eviction_grace_seconds"), default=300),
        ),
        freshness_policy=FreshnessPolicy(
            max_event_age_seconds=float_value(freshness.get("max_event_age_seconds"), default=60),
            max_queue_backlog=int_value(freshness.get("max_queue_backlog"), default=1_000),
        ),
        kafka=KafkaAdapterConfig(
            raw_topic=str(kafka.get("raw_topic") or RAW_TOPIC),
            processed_topic=str(kafka.get("processed_topic") or PROCESSED_TOPIC),
            consumer_group=str(kafka.get("consumer_group") or "beast-terminal-v2"),
            poll_timeout_ms=int_value(kafka.get("poll_timeout_ms"), default=1),
            auto_offset_reset=str(kafka.get("auto_offset_reset") or "latest"),
        ),
        redis=RedisAdapterConfig(
            terminal_ttl_seconds=int_value(redis.get("terminal_ttl_seconds"), default=60 * 60 * 8),
            history_ttl_seconds=int_value(redis.get("history_ttl_seconds"), default=60 * 60 * 24 * 30),
        ),
    )


def evaluate_runtime_config_artifact(config: dict[str, Any]) -> dict[str, Any]:
    blockers: list[str] = []
    if not isinstance(config, dict):
        return {"schema_version": 1, "passed": False, "blockers": ["runtime_config_not_object"], "evidence": {}}

    gateway = config.get("gateway") if isinstance(config.get("gateway"), dict) else {}
    kafka = config.get("kafka") if isinstance(config.get("kafka"), dict) else {}
    redis = config.get("redis") if isinstance(config.get("redis"), dict) else {}
    runtime = config.get("runtime") if isinstance(config.get("runtime"), dict) else {}
    freshness = config.get("freshness") if isinstance(config.get("freshness"), dict) else {}
    active_pool = config.get("active_pool") if isinstance(config.get("active_pool"), dict) else {}
    production_clients = config.get("production_clients") if isinstance(config.get("production_clients"), dict) else {}
    secret_paths = secret_like_paths(config)

    trade_date = str(config.get("trade_date") or "")
    silver_root = str(config.get("silver_root") or "")
    runtime_state_root = str(config.get("runtime_state_root") or runtime.get("runtime_state_root") or "")
    kafka_spool_dir = str(runtime.get("kafka_spool_dir") or "")
    gateway_path = str(gateway.get("path") or "")
    raw_topic = str(kafka.get("raw_topic") or "")
    processed_topic = str(kafka.get("processed_topic") or "")
    auto_offset_reset = str(kafka.get("auto_offset_reset") or "")
    required_client_flags = ("duckdb_connection", "kafka_producer", "kafka_consumer", "redis_client", "market_data_client")
    missing_production_clients = [
        name for name in required_client_flags if production_clients.get(name) is not True
    ]

    if config.get("schema_version") != 1:
        blockers.append("runtime_config_schema_version_invalid")
    if not TRADE_DATE_PATTERN.fullmatch(trade_date):
        blockers.append("runtime_config_trade_date_invalid")
    if not silver_root:
        blockers.append("runtime_config_silver_root_missing")
    elif not Path(silver_root).exists():
        blockers.append("runtime_config_silver_root_missing_on_disk")
    if not isinstance(gateway, dict) or not gateway:
        blockers.append("runtime_config_gateway_missing")
    if not isinstance(gateway.get("host"), str) or not gateway.get("host", "").strip():
        blockers.append("runtime_config_gateway_host_invalid")
    elif is_loopback_gateway_host(str(gateway.get("host"))):
        blockers.append("runtime_config_gateway_host_loopback")
    if not positive_integer(gateway.get("port")):
        blockers.append("runtime_config_gateway_port_invalid")
    if gateway_path != "/ws":
        blockers.append("runtime_config_gateway_path_mismatch")
    if raw_topic != RAW_TOPIC:
        blockers.append("runtime_config_raw_topic_mismatch")
    if processed_topic and processed_topic != PROCESSED_TOPIC:
        blockers.append("runtime_config_processed_topic_mismatch")
    if not isinstance(kafka.get("consumer_group"), str) or not kafka.get("consumer_group", "").strip():
        blockers.append("runtime_config_kafka_consumer_group_invalid")
    if not positive_integer(kafka.get("poll_timeout_ms")):
        blockers.append("runtime_config_kafka_poll_timeout_invalid")
    if auto_offset_reset not in {"latest", "earliest"}:
        blockers.append("runtime_config_kafka_auto_offset_reset_invalid")
    if not positive_integer(redis.get("terminal_ttl_seconds")):
        blockers.append("runtime_config_redis_terminal_ttl_invalid")
    if not positive_integer(redis.get("history_ttl_seconds")):
        blockers.append("runtime_config_redis_history_ttl_invalid")
    for field in ("raw_queue_max_size", "client_queue_size", "max_concurrent_hydrations", "max_raw_records_per_tick"):
        if not positive_integer(runtime.get(field)):
            blockers.append(f"runtime_config_{field}_invalid")
    if not non_negative_integer(runtime.get("kafka_retries")):
        blockers.append("runtime_config_kafka_retries_invalid")
    if not positive_number(runtime.get("symbol_eviction_grace_seconds")):
        blockers.append("runtime_config_symbol_eviction_grace_invalid")
    if not positive_number(runtime.get("big_trade_volume_baseline_ratio")):
        blockers.append("runtime_config_big_trade_volume_ratio_invalid")
    if "big_trade_turnover_threshold" in runtime:
        blockers.append("runtime_config_big_trade_turnover_threshold_deprecated")
    if active_pool:
        if not positive_integer(active_pool.get("target_size")):
            blockers.append("runtime_config_active_pool_target_size_invalid")
        if not positive_integer(active_pool.get("pinned_max_size")):
            blockers.append("runtime_config_active_pool_pinned_max_size_invalid")
        if not positive_integer(active_pool.get("rank_window_days")):
            blockers.append("runtime_config_active_pool_rank_window_days_invalid")
        if active_pool.get("rank_metric") not in {"avg_turnover", "avg_volume"}:
            blockers.append("runtime_config_active_pool_rank_metric_invalid")
        if not isinstance(active_pool.get("exclude_instrument_types"), list) or not active_pool.get("exclude_instrument_types"):
            blockers.append("runtime_config_active_pool_exclude_types_invalid")
        if not positive_number(active_pool.get("eviction_grace_seconds")):
            blockers.append("runtime_config_active_pool_eviction_grace_invalid")
    if runtime.get("install_signal_handlers") is not True:
        blockers.append("runtime_config_signal_handlers_not_enabled")
    if not isinstance(runtime.get("startup_intraday_recovery"), bool):
        blockers.append("runtime_config_startup_intraday_recovery_invalid")
    if not isinstance(runtime.get("persist_realtime_events"), bool):
        blockers.append("runtime_config_persist_realtime_events_invalid")
    if not isinstance(runtime.get("commit_runtime_owned_raw_offsets"), bool):
        blockers.append("runtime_config_commit_runtime_owned_raw_offsets_invalid")
    if not positive_number(freshness.get("max_event_age_seconds")):
        blockers.append("runtime_config_freshness_event_age_invalid")
    if not positive_integer(freshness.get("max_queue_backlog")):
        blockers.append("runtime_config_freshness_queue_backlog_invalid")
    if missing_production_clients:
        blockers.append("runtime_config_production_clients_missing")
    if secret_paths:
        blockers.append("runtime_config_contains_secret_like_keys")

    evidence = {
        "trade_date": trade_date,
        "silver_root": silver_root,
        "runtime_state_root": runtime_state_root,
        "gateway": {"host": gateway.get("host"), "port": gateway.get("port"), "path": gateway_path},
        "kafka": {
            "raw_topic": raw_topic,
            "processed_topic": processed_topic,
            "consumer_group": kafka.get("consumer_group"),
            "poll_timeout_ms": kafka.get("poll_timeout_ms"),
            "auto_offset_reset": auto_offset_reset,
        },
        "redis": {
            "terminal_ttl_seconds": redis.get("terminal_ttl_seconds"),
            "history_ttl_seconds": redis.get("history_ttl_seconds"),
        },
        "runtime": {
            "raw_queue_max_size": runtime.get("raw_queue_max_size"),
            "client_queue_size": runtime.get("client_queue_size"),
            "kafka_spool_dir": kafka_spool_dir,
            "kafka_retries": runtime.get("kafka_retries"),
            "symbol_eviction_grace_seconds": runtime.get("symbol_eviction_grace_seconds"),
            "max_concurrent_hydrations": runtime.get("max_concurrent_hydrations"),
            "max_raw_records_per_tick": runtime.get("max_raw_records_per_tick"),
            "startup_intraday_recovery": runtime.get("startup_intraday_recovery"),
            "persist_realtime_events": runtime.get("persist_realtime_events"),
            "commit_runtime_owned_raw_offsets": runtime.get("commit_runtime_owned_raw_offsets"),
            "big_trade_volume_baseline_ratio": runtime.get("big_trade_volume_baseline_ratio"),
            "install_signal_handlers": runtime.get("install_signal_handlers"),
        },
        "active_pool": {
            "target_size": active_pool.get("target_size"),
            "pinned_max_size": active_pool.get("pinned_max_size"),
            "rank_window_days": active_pool.get("rank_window_days"),
            "rank_metric": active_pool.get("rank_metric"),
            "exclude_instrument_types": active_pool.get("exclude_instrument_types"),
            "eviction_grace_seconds": active_pool.get("eviction_grace_seconds"),
        },
        "freshness": {
            "max_event_age_seconds": freshness.get("max_event_age_seconds"),
            "max_queue_backlog": freshness.get("max_queue_backlog"),
        },
        "production_clients": {name: production_clients.get(name) is True for name in required_client_flags},
        "missing_production_clients": missing_production_clients,
        "secret_like_key_paths": secret_paths,
    }
    return {"schema_version": 1, "passed": not blockers, "blockers": blockers, "evidence": evidence}


def write_runtime_config_verification(
    *,
    config_path: str | Path,
    output_path: str | Path,
) -> dict[str, Any]:
    config = load_runtime_config_artifact(config_path)
    result = evaluate_runtime_config_artifact(config)
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
    return result


def int_value(value: Any, *, default: int) -> int:
    if isinstance(value, bool):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def float_value(value: Any, *, default: float) -> float:
    if isinstance(value, bool):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def bool_value(value: Any, *, default: bool) -> bool:
    return value if isinstance(value, bool) else default


def is_loopback_gateway_host(value: str) -> bool:
    normalized = value.strip().lower()
    return normalized in {"localhost", "127.0.0.1", "::1"} or normalized.startswith("127.")


def positive_integer(value: Any) -> bool:
    return non_negative_integer(value) and int(value) > 0


def non_negative_integer(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def positive_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0


def secret_like_paths(value: Any, *, prefix: str = "") -> list[str]:
    paths: list[str] = []
    if isinstance(value, dict):
        for key, nested in value.items():
            key_text = str(key)
            path = f"{prefix}.{key_text}" if prefix else key_text
            if any(fragment in key_text.lower() for fragment in SECRET_KEY_FRAGMENTS):
                paths.append(path)
            paths.extend(secret_like_paths(nested, prefix=path))
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            paths.extend(secret_like_paths(nested, prefix=f"{prefix}[{index}]"))
    return paths


def history_provider(mammoth: MammothAPI, trade_date: str):
    def load_history(symbol: str, participant_name: str, days: int) -> list[dict[str, Any]]:
        participant_id = participant_id_for_name(mammoth, symbol, participant_name, trade_date)
        if not participant_id:
            return []
        return [
            {
                "date": str(row["trade_date"]),
                "shares": int(row["shares"]),
                "percent": float(row["percent"]),
                "change": int(row.get("change") or 0),
            }
            for row in participant_history_with_computed_change(
                mammoth.get_participant_history(symbol, participant_id, 2, trade_date=trade_date)
            )
        ]

    return load_history


def live_attach_symbol(
    manager: XtQuantSubscriptionManager | None,
    *,
    should_attach: Callable[[str], bool] | None = None,
) -> Callable[[str], bool | None] | None:
    if manager is None:
        return None

    def attach(symbol: str) -> bool:
        if should_attach is not None and not should_attach(symbol):
            return False
        if manager.running and symbol not in manager.subscribed_symbols:
            manager.subscribe(symbol)
            return True
        return symbol in manager.subscribed_symbols

    return attach


def live_release_symbol(manager: XtQuantSubscriptionManager | None) -> Callable[[str], None] | None:
    if manager is None:
        return None

    def release(symbol: str) -> None:
        if manager.running and symbol in manager.subscribed_symbols:
            manager.unsubscribe(symbol)

    return release


def participant_id_for_name(mammoth: MammothAPI, symbol: str, participant_name: str, trade_date: str) -> str:
    normalized = participant_name.strip().casefold()
    for holding in mammoth.get_latest_ccass_holdings(symbol, trade_date):
        if str(holding.get("participant_name", "")).strip().casefold() == normalized:
            return str(holding.get("participant_id") or "")
    return ""


def participant_history_with_computed_change(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    previous_row: dict[str, Any] | None = None
    for row in rows:
        enriched = dict(row)
        if previous_row is not None:
            enriched["change"] = int(row["shares"]) - int(previous_row["shares"])
        result.append(enriched)
        previous_row = row
    return result[-2:]


def build_runtime_health_snapshot(
    supervisor: BeastMarketRuntimeSupervisor,
    *,
    generated_at: str | None = None,
) -> dict[str, Any]:
    runtime = supervisor.runtime
    raw_topic = runtime.config.kafka.raw_topic or RAW_TOPIC
    processed_topic = runtime.config.kafka.processed_topic or PROCESSED_TOPIC
    raw_committed = runtime.kafka_adapter.committed_offset(raw_topic)
    processed_committed = runtime.kafka_adapter.committed_offset(processed_topic)
    subscription = subscription_snapshot(runtime.subscription_manager)
    redis_snapshot = redis_snapshot_probe(runtime)
    return {
        "schema_version": 1,
        "generated_at": generated_at or now_iso(),
        "trade_date": runtime.config.trade_date,
        "effective_trade_date_by_symbol": dict(runtime.effective_trade_date_by_symbol),
        "runtime_state": supervisor.stats.runtime_state,
        "runtime_state_root": str(runtime.config.runtime_state_root),
        "running": supervisor.running,
        "supervisor": asdict(supervisor.stats),
        "topics": {
            raw_topic: {
                "committed_offset": raw_committed,
                "lag": runtime.kafka_adapter.lag(raw_topic, raw_committed),
            },
            processed_topic: {
                "committed_offset": processed_committed,
                "lag": runtime.kafka_adapter.lag(processed_topic, processed_committed),
            },
        },
        "queues": {
            "raw_callback_backlog": runtime.raw_queue.backlog,
            "raw_callback_rejected": len(runtime.raw_queue.dropped),
            "raw_callback_rejection_path": str(runtime.runtime_state.callback_rejections_path(runtime.config.trade_date)),
            "raw_consumer_dead_letter_path": str(runtime.runtime_state.raw_consumer_dead_letters_path(runtime.config.trade_date)),
        },
        "workers": {
            "ingest": worker_stats_snapshot(runtime.ingest_worker.stats),
            "raw_consumer": worker_stats_snapshot(runtime.raw_consumer_worker.stats),
        },
        "producer": {
            "publish_attempts": len(runtime.event_bus.results),
            "dead_letters": len(runtime.event_bus.dead_letters),
            "spooled_records": len(runtime.event_bus.spool.records),
            "spool_path": str(getattr(runtime.event_bus.spool, "path", "")),
            "quarantined_spool_records": int(getattr(runtime.event_bus.spool, "quarantined_records", 0) or 0),
            "spool_quarantine_path": str(getattr(runtime.event_bus.spool, "quarantine_path", "")),
        },
        "redis": {
            "write_stats": runtime.cache.stats_snapshot(),
        },
        "subscription": subscription,
        "active_pool": runtime.active_pool_manager.snapshot(),
        "symbol_runtime_manager": runtime.symbol_runtime_manager.manager_snapshot(),
        "symbol_runtime": runtime.symbol_runtime_manager.snapshot(),
        "redis_snapshot": redis_snapshot,
        "gateway_websocket": {
            "host": runtime.websocket_service.host,
            "port": runtime.websocket_service.port,
            "path": runtime.websocket_service.path,
            "request_schema_version": SCHEMA_VERSION,
            "accepted_protocol": TERMINAL_MESSAGE_PROTOCOL,
            "running": supervisor.running,
            "connected_clients": len(runtime.websocket_service.clients),
            "failed_client_sends": runtime.websocket_service.failed_client_sends,
            "send_timeout_seconds": runtime.websocket_service.send_timeout_seconds,
        },
        "gateway_activity": {
            "processed_records_consumed": runtime.gateway.processed_records_consumed,
            "shadow_processed_records_drained": runtime.gateway.shadow_processed_records_drained,
            "direct_runtime_messages_emitted": runtime.gateway.direct_runtime_messages_emitted,
            "terminal_messages_emitted": runtime.gateway.terminal_messages_emitted,
            "terminal_messages_delivered": runtime.websocket_service.terminal_messages_delivered,
            "delivered_terminal_symbols": sorted(runtime.websocket_service.delivered_terminal_symbols),
            "last_terminal_message_delivered_at": runtime.websocket_service.last_terminal_message_delivered_at,
            "client_queue": runtime.session_manager.client_queue_snapshot(),
        },
        "performance_samples": runtime.session_manager.performance_snapshot(),
        "health": {
            "collector": runtime.collector.health.as_message()["payload"],
            "octopus": runtime.octopus.health.as_message()["payload"],
            "gateway": runtime.gateway.health.as_message()["payload"],
        },
    }


def write_runtime_health_snapshot(
    supervisor: BeastMarketRuntimeSupervisor,
    path: str | Path,
    *,
    generated_at: str | None = None,
) -> dict[str, Any]:
    snapshot = build_runtime_health_snapshot(supervisor, generated_at=generated_at)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(
        json.dumps(snapshot, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return snapshot


def worker_stats_snapshot(stats: Any) -> dict[str, Any]:
    value = asdict(stats)
    callback_samples = [
        float(sample)
        for sample in value.get("callback_enqueue_latency_ms", [])
        if isinstance(sample, (int, float)) and not isinstance(sample, bool) and sample >= 0
    ]
    value["callback_enqueue_sample_count"] = len(callback_samples)
    value["callback_enqueue_p95_latency_ms"] = percentile(callback_samples, 95)
    value["dead_letters"] = [
        {
            "topic": record.topic,
            "key": record.key,
            "reason": record.reason,
        }
        for record in stats.dead_letters
    ]
    return value


def percentile(samples: list[float], percentile_value: float) -> float:
    if not samples:
        return 0.0
    ordered = sorted(samples)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * percentile_value / 100
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    weight = rank - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def subscription_snapshot(manager: XtQuantSubscriptionManager | None) -> dict[str, Any] | None:
    if manager is None:
        return None
    client_stats = None
    stats_snapshot = getattr(manager.client, "stats_snapshot", None)
    if callable(stats_snapshot):
        client_stats = stats_snapshot()
    return {
        "running": manager.running,
        "subscribed_symbols": sorted(manager.subscribed_symbols),
        "stats": asdict(manager.stats),
        "client": client_stats,
    }


def redis_snapshot_probe(runtime: BeastMarketRuntime) -> dict[str, Any]:
    symbols = set(runtime.active_pool_manager.active_symbols())
    if runtime.subscription_manager is not None:
        symbols.update(runtime.subscription_manager.subscribed_symbols)
    for state in runtime.collector.freshness.snapshot().values():
        symbol = state.get("symbol") if isinstance(state, dict) else None
        if isinstance(symbol, str) and re.fullmatch(r"\d{5}\.HK", symbol):
            symbols.add(symbol)
    checked_symbols = sorted(symbols)
    present_symbols = []
    missing_symbols = []
    for symbol in checked_symbols:
        snapshot = runtime.cache.get_terminal_snapshot(runtime.config.trade_date, symbol)
        if snapshot is None:
            missing_symbols.append(symbol)
        else:
            present_symbols.append(symbol)
    key_family_coverage = redis_key_family_coverage(
        runtime.cache,
        runtime.config.trade_date,
        checked_symbols,
        runtime.octopus.bod_by_symbol,
    )
    return {
        "trade_date": runtime.config.trade_date,
        "checked_symbols": checked_symbols,
        "present_symbols": present_symbols,
        "missing_symbols": missing_symbols,
        "required_key_families": list(REDIS_RUNTIME_SNAPSHOT_KEY_FAMILIES),
        "key_family_coverage": key_family_coverage,
    }


def redis_key_family_coverage(
    cache: Any,
    trade_date: str,
    checked_symbols: list[str],
    bod_by_symbol: dict[str, Any] | None = None,
) -> dict[str, dict[str, Any]]:
    coverage: dict[str, dict[str, Any]] = {}
    for family in REDIS_RUNTIME_SNAPSHOT_KEY_FAMILIES:
        if family == "ccass_history":
            coverage[family] = redis_history_key_family_coverage(cache, checked_symbols, bod_by_symbol or {})
            continue
        present_symbols = []
        missing_symbols = []
        updated_at_by_symbol = {}
        missing_updated_at_symbols = []
        ttl_seconds_by_symbol = {}
        missing_ttl_symbols = []
        contract_missing_by_symbol = {}
        template = REDIS_RUNTIME_SNAPSHOT_KEY_TEMPLATES[family]
        for symbol in checked_symbols:
            key = template.format(trade_date=trade_date, symbol=symbol)
            if redis_cache_key_exists(cache, key):
                present_symbols.append(symbol)
                missing_contract_fields = redis_cache_key_contract_missing_fields(cache, key)
                if missing_contract_fields:
                    contract_missing_by_symbol[symbol] = missing_contract_fields
                updated_at = redis_cache_key_updated_at(cache, key)
                if updated_at:
                    updated_at_by_symbol[symbol] = updated_at
                else:
                    missing_updated_at_symbols.append(symbol)
                ttl_seconds = redis_cache_key_ttl_seconds(cache, key)
                if ttl_seconds is not None and ttl_seconds > 0:
                    ttl_seconds_by_symbol[symbol] = ttl_seconds
                else:
                    missing_ttl_symbols.append(symbol)
            else:
                missing_symbols.append(symbol)
        coverage[family] = {
            "checked_symbols": list(checked_symbols),
            "present_symbols": present_symbols,
            "missing_symbols": missing_symbols,
            "updated_at_by_symbol": updated_at_by_symbol,
            "missing_updated_at_symbols": missing_updated_at_symbols,
            "ttl_seconds_by_symbol": ttl_seconds_by_symbol,
            "missing_ttl_symbols": missing_ttl_symbols,
            "contract_missing_by_symbol": contract_missing_by_symbol,
        }
    return coverage


def redis_history_key_family_coverage(
    cache: Any,
    checked_symbols: list[str],
    bod_by_symbol: dict[str, Any],
) -> dict[str, Any]:
    present_symbols = []
    missing_symbols = []
    participants_by_symbol: dict[str, list[str]] = {}
    missing_keys: dict[str, list[str]] = {}
    updated_at_by_symbol: dict[str, str] = {}
    missing_updated_at_symbols = []
    ttl_seconds_by_symbol: dict[str, int] = {}
    missing_ttl_symbols = []
    contract_missing_by_symbol: dict[str, list[str]] = {}
    for symbol in checked_symbols:
        participant_ids = redis_history_participant_ids(cache, symbol, bod_by_symbol)
        participants_by_symbol[symbol] = participant_ids
        missing_participant_ids = [
            participant_id
            for participant_id in participant_ids
            if not redis_cache_key_exists(cache, f"ccass:history:{symbol}:{participant_id}")
        ]
        if participant_ids and not missing_participant_ids:
            present_symbols.append(symbol)
            missing_contract_fields = [
                field
                for participant_id in participant_ids
                for field in redis_cache_key_contract_missing_fields(cache, f"ccass:history:{symbol}:{participant_id}")
            ]
            if missing_contract_fields:
                contract_missing_by_symbol[symbol] = sorted(set(missing_contract_fields))
            updated_at_values = [
                redis_cache_key_updated_at(cache, f"ccass:history:{symbol}:{participant_id}")
                for participant_id in participant_ids
            ]
            valid_updated_at_values = [updated_at for updated_at in updated_at_values if updated_at]
            if len(valid_updated_at_values) == len(participant_ids):
                updated_at_by_symbol[symbol] = max(valid_updated_at_values)
            else:
                missing_updated_at_symbols.append(symbol)
            ttl_values = [
                redis_cache_key_ttl_seconds(cache, f"ccass:history:{symbol}:{participant_id}")
                for participant_id in participant_ids
            ]
            valid_ttl_values = [ttl for ttl in ttl_values if ttl is not None and ttl > 0]
            if len(valid_ttl_values) == len(participant_ids):
                ttl_seconds_by_symbol[symbol] = min(valid_ttl_values)
            else:
                missing_ttl_symbols.append(symbol)
        else:
            missing_symbols.append(symbol)
        if missing_participant_ids:
            missing_keys[symbol] = [
                f"ccass:history:{symbol}:{participant_id}" for participant_id in missing_participant_ids
            ]
    return {
        "checked_symbols": list(checked_symbols),
        "present_symbols": present_symbols,
        "missing_symbols": missing_symbols,
        "participants_by_symbol": participants_by_symbol,
        "missing_keys": missing_keys,
        "updated_at_by_symbol": updated_at_by_symbol,
        "missing_updated_at_symbols": missing_updated_at_symbols,
        "ttl_seconds_by_symbol": ttl_seconds_by_symbol,
        "missing_ttl_symbols": missing_ttl_symbols,
        "contract_missing_by_symbol": contract_missing_by_symbol,
    }


def redis_history_participant_ids(cache: Any, symbol: str, bod_by_symbol: dict[str, Any]) -> list[str]:
    bod = bod_by_symbol.get(symbol)
    participant_history_by_id = getattr(bod, "participant_history_by_id", None)
    if isinstance(participant_history_by_id, dict):
        return sorted(str(participant_id) for participant_id in participant_history_by_id if str(participant_id).strip())
    values = getattr(cache, "values", None)
    if not isinstance(values, dict):
        return []
    prefix = f"ccass:history:{symbol}:"
    return sorted(key.removeprefix(prefix) for key in values if isinstance(key, str) and key.startswith(prefix))


def redis_cache_key_exists(cache: Any, key: str) -> bool:
    values = getattr(cache, "values", None)
    if isinstance(values, dict):
        return key in values
    redis_client = getattr(cache, "redis", None)
    exists = getattr(redis_client, "exists", None) if redis_client is not None else None
    if callable(exists):
        return bool(exists(key))
    get = getattr(redis_client, "get", None) if redis_client is not None else None
    if callable(get):
        return get(key) is not None
    return False


def redis_cache_key_updated_at(cache: Any, key: str) -> str:
    value = redis_cache_key_value(cache, key)
    if not isinstance(value, dict):
        return ""
    updated_at = value.get("updated_at")
    if isinstance(updated_at, str) and updated_at.strip():
        return updated_at
    freshness = value.get("freshness")
    freshness_updated_at = freshness.get("updated_at") if isinstance(freshness, dict) else None
    return freshness_updated_at if isinstance(freshness_updated_at, str) and freshness_updated_at.strip() else ""


def redis_cache_key_contract_missing_fields(cache: Any, key: str) -> list[str]:
    value = redis_cache_key_value(cache, key)
    if not isinstance(value, dict):
        return ["<record>"]
    missing_fields = []
    if value.get("schema_version") != 1:
        missing_fields.append("schema_version")
    for field in ("requested_trade_date", "effective_trade_date", "updated_at"):
        field_value = value.get(field)
        if not isinstance(field_value, str) or not field_value.strip():
            missing_fields.append(field)
    if not isinstance(value.get("source_dates"), dict):
        missing_fields.append("source_dates")
    if not isinstance(value.get("freshness"), dict):
        missing_fields.append("freshness")
    if not isinstance(value.get("degraded_reasons"), list):
        missing_fields.append("degraded_reasons")
    if "version" not in value and "last_event_id" not in value:
        missing_fields.append("version_or_last_event_id")
    return missing_fields


def redis_cache_key_value(cache: Any, key: str) -> Any:
    values = getattr(cache, "values", None)
    if isinstance(values, dict):
        return values.get(key)
    redis_client = getattr(cache, "redis", None)
    get = getattr(redis_client, "get", None) if redis_client is not None else None
    if not callable(get):
        return None
    value = get(key)
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    if isinstance(value, str):
        try:
            return json.loads(value)
        except JSONDecodeError:
            return None
    return value


def redis_cache_key_ttl_seconds(cache: Any, key: str) -> int | None:
    ttls = getattr(cache, "ttls", None)
    if isinstance(ttls, dict):
        value = ttls.get(key)
        return int(value) if isinstance(value, int) and not isinstance(value, bool) else None
    redis_client = getattr(cache, "redis", None)
    client_ttls = getattr(redis_client, "ttls", None) if redis_client is not None else None
    if isinstance(client_ttls, dict):
        value = client_ttls.get(key)
        return int(value) if isinstance(value, int) and not isinstance(value, bool) else None
    ttl = getattr(redis_client, "ttl", None) if redis_client is not None else None
    if callable(ttl):
        value = ttl(key)
        return int(value) if isinstance(value, int) and not isinstance(value, bool) else None
    return None
