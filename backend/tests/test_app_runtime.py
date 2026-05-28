import csv
import asyncio
import json
import signal
import tempfile
import unittest
from pathlib import Path

from beast_market import (
    BeastMarketRuntimeClients,
    BeastMarketRuntimeConfig,
    BeastMarketRuntimeSupervisor,
    DuckDBParquetSilverTableReader,
    FileBackedShadowRunRecorder,
    InMemoryEventBus,
    InMemoryRedisSnapshotCache,
    KafkaAdapterConfig,
    MammothAPI,
    OctopusComputeV2,
    RAW_TOPIC,
    RedisAdapterConfig,
    DeferredCallbackSink,
    XtQuantMarketDataClient,
    build_beast_market_runtime,
    clear_runtime_cache,
    evaluate_monitoring_historical_readiness,
    install_runtime_signal_handlers,
    load_shadow_run_files,
    make_raw_market_event,
    run_supervised_runtime,
    shadow_run_file_paths,
    write_runtime_health_snapshot,
)
from beast_market.app_runtime import (
    hydrate_symbol_snapshot,
    is_terminal_snapshot_fresh,
    recover_symbol_intraday,
    promote_snapshot_to_realtime_session,
    seed_symbol_from_market_full_tick,
    should_attach_realtime_for_symbol,
)
from beast_market.production_runtime import (
    ConfluentKafkaConsumerAdapter,
    build_parser,
    build_runtime_with_deferred_xtquant,
    with_gateway_port,
)


class AppRuntimeTest(unittest.TestCase):
    def test_builds_runtime_with_csv_silver_kafka_redis_and_gateway_service(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_ccass(root)
            runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260522",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                    gateway_host="0.0.0.0",
                    gateway_port=9021,
                    gateway_path="/ws",
                    raw_queue_max_size=7,
                    client_queue_size=5,
                    kafka_retries=2,
                    kafka=KafkaAdapterConfig(raw_topic="raw_market_events_v1"),
                    redis=RedisAdapterConfig(terminal_ttl_seconds=3600),
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=FakeKafkaProducer(),
                    kafka_consumer=FakeKafkaConsumer(),
                    redis_client=FakeRedis(),
                    market_data_client=FakeMarketDataClient(),
                ),
            )

            self.assertEqual(runtime.raw_queue.max_size, 7)
            self.assertEqual(runtime.event_bus.retries, 2)
            self.assertEqual(runtime.raw_consumer_worker.topic, "raw_market_events_v1")
            self.assertIs(runtime.websocket_service.manager, runtime.session_manager)
            self.assertEqual(runtime.websocket_service.host, "0.0.0.0")
            self.assertEqual(runtime.websocket_service.port, 9021)
            self.assertEqual(runtime.websocket_service.path, "/ws")
            self.assertEqual(runtime.session_manager.client_queue_size, 5)
            self.assertEqual(runtime.symbol_runtime_manager.max_concurrent_hydrations, 8)
            self.assertIs(runtime.session_manager.symbol_runtime_manager, runtime.symbol_runtime_manager)
            self.assertIs(runtime.symbol_runtime_manager.gateway, runtime.gateway)
            self.assertFalse(runtime.session_manager.consume_processed_on_broadcast)
            self.assertEqual(runtime.event_bus.spool.path, root / "artifacts" / "runtime-state" / "kafka-spool" / "publish-failures.jsonl")
            self.assertIsNotNone(runtime.subscription_manager)

            history = runtime.session_manager.history_provider("00700.HK", "JPMorgan", 1)
            self.assertEqual(history, [{"date": "20260522", "shares": 1000, "percent": 1.1, "change": 10}])

    def test_builds_runtime_with_duckdb_parquet_reader_when_connection_is_supplied(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            connection = FakeDuckDBConnection()
            runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260522",
                    silver_root=directory,
                    runtime_state_root=Path(directory) / "artifacts" / "runtime-state",
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=FakeKafkaProducer(),
                    kafka_consumer=FakeKafkaConsumer(),
                    redis_client=FakeRedis(),
                    duckdb_connection=connection,
                ),
            )

            self.assertIsInstance(runtime.mammoth.reader, DuckDBParquetSilverTableReader)
            self.assertIs(runtime.mammoth.reader.connection, connection)

    def test_build_runtime_with_deferred_xtquant_callback_sink_targets_ingest_queue(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            callback_sink = DeferredCallbackSink()
            runtime = build_runtime_with_deferred_xtquant(
                BeastMarketRuntimeConfig(
                    trade_date="20260522",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=FakeKafkaProducer(),
                    kafka_consumer=FakeKafkaConsumer(),
                    redis_client=FakeRedis(),
                    market_data_client=XtQuantMarketDataClient(callback_sink=callback_sink),
                ),
            )

            accepted = callback_sink(
                {
                    "symbol": "00700.HK",
                    "period": "hktransaction",
                    "data": {"Price": 388.4, "Volume": 1000},
                }
            )

            self.assertTrue(accepted)
            self.assertEqual(runtime.raw_queue.backlog, 1)

    def test_production_runtime_cli_exposes_validation_port_and_tick_limit(self) -> None:
        args = build_parser().parse_args(
            [
                "--config-path",
                "runtime.json",
                "--gateway-port",
                "19020",
                "--health-snapshot-interval-seconds",
                "5",
                "--max-ticks",
                "3",
            ]
        )
        config = BeastMarketRuntimeConfig(
            trade_date="20260522",
            silver_root="silver",
            runtime_state_root="state",
            gateway_port=9020,
        )

        updated = with_gateway_port(config, args.gateway_port)

        self.assertEqual(args.max_ticks, 3)
        self.assertEqual(args.health_snapshot_interval_seconds, 5)
        self.assertEqual(updated.gateway_port, 19020)
        self.assertEqual(config.gateway_port, 9020)

    def test_confluent_consumer_adapter_keeps_raw_and_processed_subscriptions(self) -> None:
        consumer = FakeConfluentConsumer()
        adapter = ConfluentKafkaConsumerAdapter(consumer, FakeTopicPartition)

        adapter.poll("raw_market_events_v1", 0, timeout_ms=0)
        adapter.poll("processed_market_events_v1", 0, timeout_ms=0)

        self.assertEqual(
            consumer.subscriptions,
            [["raw_market_events_v1"], ["processed_market_events_v1", "raw_market_events_v1"]],
        )

    def test_confluent_consumer_adapter_buffers_cross_topic_records(self) -> None:
        consumer = FakeConfluentConsumer(
            messages=[
                FakeConfluentMessage(
                    topic="raw_market_events_v1",
                    key=b"00700.HK",
                    value=b'{"symbol":"00700.HK"}',
                    offset=4,
                )
            ]
        )
        adapter = ConfluentKafkaConsumerAdapter(consumer, FakeTopicPartition)

        processed = adapter.poll("processed_market_events_v1", 0, timeout_ms=0)
        raw = adapter.poll("raw_market_events_v1", 0, timeout_ms=0)

        self.assertEqual(processed, [])
        self.assertEqual(raw[0]["key"], b"00700.HK")
        self.assertEqual(raw[0]["offset"], 4)

    def test_confluent_consumer_adapter_polls_available_records_as_batch(self) -> None:
        consumer = FakeConfluentConsumer(
            messages=[
                FakeConfluentMessage(topic="raw_market_events_v1", key=b"00700.HK", value=b"{}", offset=1),
                FakeConfluentMessage(topic="raw_market_events_v1", key=b"00700.HK", value=b"{}", offset=2),
                FakeConfluentMessage(topic="raw_market_events_v1", key=b"00700.HK", value=b"{}", offset=3),
            ]
        )
        adapter = ConfluentKafkaConsumerAdapter(consumer, FakeTopicPartition, max_poll_records=2)

        records = adapter.poll("raw_market_events_v1", 0, timeout_ms=0)

        self.assertEqual([record["offset"] for record in records], [1, 2])

    def test_confluent_consumer_adapter_commits_explicit_topic_offset(self) -> None:
        consumer = FakeConfluentConsumer()
        adapter = ConfluentKafkaConsumerAdapter(consumer, FakeTopicPartition)

        adapter.commit("raw_market_events_v1", 42)

        self.assertEqual(consumer.committed_offsets[0].topic, "raw_market_events_v1")
        self.assertEqual(consumer.committed_offsets[0].offset, 42)

    def test_runtime_defaults_bind_gateway_for_lan_access(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260522",
                    silver_root=directory,
                    runtime_state_root=Path(directory) / "artifacts" / "runtime-state",
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=FakeKafkaProducer(),
                    kafka_consumer=FakeKafkaConsumer(),
                    redis_client=FakeRedis(),
                    market_data_client=FakeMarketDataClient(),
                ),
            )

            self.assertEqual(runtime.config.gateway_host, "0.0.0.0")
            self.assertEqual(runtime.websocket_service.host, "0.0.0.0")

    def test_gateway_subscribe_hydrates_cold_symbol_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_minimal_silver(root)
            runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260522",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=FakeKafkaProducer(),
                    kafka_consumer=FakeKafkaConsumer(),
                    redis_client=RecordingRedis(),
                    market_data_client=FakeMarketDataClient(),
                ),
            )

            runtime.session_manager.connect("client")
            runtime.session_manager.flush("client")
            runtime.session_manager.handle_message(
                "client",
                {
                    "schema_version": 1,
                    "protocol": "terminal-message-v1",
                    "action": "subscribe",
                    "symbol": "00700.HK",
                },
            )
            messages = [json.loads(item) for item in runtime.session_manager.flush("client")]

        self.assertEqual(messages[0]["type"], "snapshot")
        self.assertEqual(messages[0]["symbol"], "00700.HK")
        self.assertEqual(runtime.symbol_runtime_manager.runtimes["00700.HK"].hydrate_count, 1)
        self.assertEqual(runtime.symbol_runtime_manager.runtimes["00700.HK"].ref_count, 1)
        self.assertEqual(runtime.effective_trade_date_by_symbol["00700.HK"], "20260522")
        self.assertIsNotNone(runtime.cache.get_terminal_snapshot("20260522", "00700.HK"))

    def test_hot_runtime_subscribe_restores_cleared_redis_snapshot_without_restart(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_minimal_silver(root)
            redis = RecordingRedis()
            runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260522",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=FakeKafkaProducer(),
                    kafka_consumer=FakeKafkaConsumer(),
                    redis_client=redis,
                    market_data_client=FakeMarketDataClient(),
                ),
            )

            runtime.session_manager.connect("first")
            runtime.session_manager.flush("first")
            runtime.session_manager.handle_message(
                "first",
                {
                    "schema_version": 1,
                    "protocol": "terminal-message-v1",
                    "action": "subscribe",
                    "symbol": "00700.HK",
                },
            )
            runtime.session_manager.flush("first")
            clear_result = clear_runtime_cache(
                redis_client=redis,
                trade_date="20260522",
                symbols=["00700.HK"],
                dry_run=False,
                confirm=True,
            )

            runtime.session_manager.connect("second")
            runtime.session_manager.flush("second")
            runtime.session_manager.handle_message(
                "second",
                {
                    "schema_version": 1,
                    "protocol": "terminal-message-v1",
                    "action": "subscribe",
                    "symbol": "00700.HK",
                },
            )
            messages = [json.loads(item) for item in runtime.session_manager.flush("second")]

        minute_record = json.loads(redis.values["terminal:20260522:minute:00700.HK"])
        self.assertIn("terminal:20260522:snapshot:00700.HK", clear_result.deleted_keys)
        self.assertEqual(messages[0]["type"], "snapshot")
        self.assertEqual(messages[0]["payload"]["snapshot"]["price"], 388.8)
        self.assertIn("terminal:20260522:snapshot:00700.HK", redis.values)
        self.assertEqual([bar["volume"] for bar in minute_record["data"]], [1000, 2000])
        self.assertEqual(runtime.symbol_runtime_manager.manager_snapshot()["snapshot_sink_failures"], 0)

    def test_gateway_subscribe_falls_back_to_historical_hydration_when_redis_read_fails(self) -> None:
        class FailingReadRedis(RecordingRedis):
            def get(self, key: str):
                raise RuntimeError("redis read unavailable")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_minimal_silver(root)
            runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260522",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=FakeKafkaProducer(),
                    kafka_consumer=FakeKafkaConsumer(),
                    redis_client=FailingReadRedis(),
                    market_data_client=FakeMarketDataClient(),
                ),
            )

            runtime.session_manager.connect("client")
            runtime.session_manager.flush("client")
            runtime.session_manager.handle_message(
                "client",
                {
                    "schema_version": 1,
                    "protocol": "terminal-message-v1",
                    "action": "subscribe",
                    "symbol": "00700.HK",
                },
            )
            messages = [json.loads(item) for item in runtime.session_manager.flush("client")]

        symbol_runtime = runtime.symbol_runtime_manager.runtimes["00700.HK"]
        self.assertEqual(messages[0]["type"], "snapshot")
        self.assertEqual(messages[0]["payload"]["minute_bars"][0]["timestamp"], "2026-05-22T09:30:00+08:00")
        self.assertEqual(symbol_runtime.state.value, "DEGRADED")
        self.assertIn("redis_terminal_snapshot_read_failed", symbol_runtime.degraded_reasons[0])
        self.assertEqual(runtime.octopus.health.redis, "degraded")

    def test_supervisor_start_falls_back_to_historical_hydration_when_redis_read_fails(self) -> None:
        class FailingReadRedis(RecordingRedis):
            def get(self, key: str):
                raise RuntimeError("redis read unavailable")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_minimal_silver(root)
            runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260522",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=FakeKafkaProducer(),
                    kafka_consumer=FakeKafkaConsumer(),
                    redis_client=FailingReadRedis(),
                    market_data_client=FakeMarketDataClient(),
                ),
            )
            supervisor = BeastMarketRuntimeSupervisor(runtime)

            supervisor.start(["00700.HK"])
            state = runtime.octopus.get_state("00700.HK")
            symbol_runtime = runtime.symbol_runtime_manager.runtimes["00700.HK"]
            supervisor.stop()

        self.assertIsNotNone(state)
        self.assertEqual(state["snapshot"]["price"], 388.8)
        self.assertEqual(symbol_runtime.snapshot_payload["snapshot"]["price"], 388.8)
        self.assertEqual(symbol_runtime.ref_count, 0)
        self.assertEqual(symbol_runtime.state.value, "DEGRADED")
        self.assertIn("redis_terminal_snapshot_read_failed", symbol_runtime.degraded_reasons[0])
        self.assertEqual([bar["timestamp"] for bar in state["minute_bars"]], [
            "2026-05-22T09:30:00+08:00",
            "2026-05-22T09:31:00+08:00",
        ])
        self.assertEqual(runtime.octopus.health.redis, "degraded")

    def test_hot_cached_subscribe_restores_bod_context_for_realtime_alerts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            redis = RecordingRedis()
            write_minimal_silver(root)

            warm_runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260522",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                    big_trade_volume_baseline_ratio=0.5,
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=FakeKafkaBroker(),
                    kafka_consumer=FakeKafkaBroker(),
                    redis_client=redis,
                    market_data_client=FakeMarketDataClient(),
                ),
            )
            warm_runtime.octopus.preload_bod("00700.HK", "20260522")

            broker = FakeKafkaBroker()
            runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260522",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                    big_trade_volume_baseline_ratio=0.5,
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=broker,
                    kafka_consumer=broker,
                    redis_client=redis,
                    market_data_client=FakeMarketDataClient(),
                ),
            )
            snapshot = runtime.symbol_runtime_manager.attach("00700.HK", "client")
            first_screen_price = snapshot["payload"]["snapshot"]["price"]
            processed = runtime.octopus.process_raw_event(
                make_raw_market_event(
                    kind="tick",
                    symbol="00700.HK",
                    source="xtquant",
                    seq=1,
                    source_ts="2026-05-22T09:32:00+08:00",
                    payload={
                        "price": 389.2,
                        "volume": 1000,
                        "turnover": 389200,
                        "side": "buy",
                        "broker_code": "JPM",
                    },
                ),
                "20260522",
            )
            alert = runtime.cache.get_terminal_snapshot("20260522", "00700.HK")["alerts"][0]

        self.assertEqual(first_screen_price, 388.8)
        self.assertIn("00700.HK", runtime.octopus.bod_by_symbol)
        self.assertEqual([event["result_type"] for event in processed], ["snapshot", "big_trade_alert"])
        self.assertEqual(alert["participantName"], "JPMorgan")

    def test_hot_cached_subscribe_rehydrates_when_effective_trade_date_has_advanced(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            redis = RecordingRedis()
            write_minimal_silver(root)

            stale_runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260525",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=FakeKafkaProducer(),
                    kafka_consumer=FakeKafkaConsumer(),
                    redis_client=redis,
                    market_data_client=FakeMarketDataClient(),
                ),
            )
            stale_runtime.octopus.preload_bod(
                "00700.HK",
                "20260522",
                cache_trade_date="20260525",
                requested_trade_date="20260525",
            )
            write_table(
                root / "silver_daily_bars_v1.csv",
                [
                    {
                        "schema_version": 1,
                        "symbol": "00700.HK",
                        "trade_date": "20260521",
                        "open": 382,
                        "high": 384,
                        "low": 380,
                        "close": 382,
                        "volume": 2000,
                        "turnover": 764000,
                        "source": "fixture",
                        "ingest_ts": "2026-05-21T00:00:00Z",
                        "row_hash": "daily-previous",
                    },
                    {
                        "schema_version": 1,
                        "symbol": "00700.HK",
                        "trade_date": "20260522",
                        "open": 386,
                        "high": 389,
                        "low": 385,
                        "close": 386.2,
                        "volume": 1000,
                        "turnover": 386200,
                        "source": "fixture",
                        "ingest_ts": "2026-05-22T00:00:00Z",
                        "row_hash": "daily",
                    },
                    {
                        "schema_version": 1,
                        "symbol": "00700.HK",
                        "trade_date": "20260525",
                        "open": 390,
                        "high": 393,
                        "low": 389,
                        "close": 392,
                        "volume": 3000,
                        "turnover": 1176000,
                        "source": "fixture",
                        "ingest_ts": "2026-05-25T00:00:00Z",
                        "row_hash": "daily-current",
                    },
                ],
            )
            write_table(
                root / "silver_minute_bars_v1.csv",
                [
                    {
                        "schema_version": 1,
                        "symbol": "00700.HK",
                        "trade_date": "20260522",
                        "bar_ts": "2026-05-22T09:30:00+08:00",
                        "open": 388.0,
                        "high": 388.6,
                        "low": 387.8,
                        "close": 388.4,
                        "volume": 1000,
                        "turnover": 388400,
                        "source": "fixture",
                        "ingest_ts": "2026-05-22T09:31:00+08:00",
                        "row_hash": "minute-1",
                    },
                    {
                        "schema_version": 1,
                        "symbol": "00700.HK",
                        "trade_date": "20260525",
                        "bar_ts": "2026-05-25T09:30:00+08:00",
                        "open": 391.0,
                        "high": 392.8,
                        "low": 390.8,
                        "close": 392.4,
                        "volume": 3000,
                        "turnover": 1177200,
                        "source": "fixture",
                        "ingest_ts": "2026-05-25T09:31:00+08:00",
                        "row_hash": "minute-current",
                    },
                ],
            )

            runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260525",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=FakeKafkaProducer(),
                    kafka_consumer=FakeKafkaConsumer(),
                    redis_client=redis,
                    market_data_client=FakeMarketDataClient(),
                ),
            )
            snapshot = runtime.symbol_runtime_manager.attach("00700.HK", "client")

        self.assertEqual(runtime.effective_trade_date_by_symbol["00700.HK"], "20260525")
        self.assertEqual(snapshot["payload"]["snapshot"]["tradeDate"], "20260525")
        self.assertEqual(snapshot["payload"]["snapshot"]["requestedTradeDate"], "20260525")
        self.assertFalse(snapshot["payload"]["snapshot"]["isHistoricalSession"])
        self.assertEqual(snapshot["payload"]["snapshot"]["price"], 392.4)

    def test_supervisor_start_uses_fresh_redis_snapshot_without_rehydrating_from_minute_bars(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            redis = RecordingRedis()
            write_minimal_silver(root)

            warm_runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260522",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=FakeKafkaProducer(),
                    kafka_consumer=FakeKafkaConsumer(),
                    redis_client=redis,
                    market_data_client=FakeMarketDataClient(),
                ),
            )
            warm_runtime.octopus.preload_bod("00700.HK", "20260522")
            cached_before_restart = warm_runtime.cache.get_terminal_snapshot("20260522", "00700.HK")
            write_table(
                root / "silver_minute_bars_v1.csv",
                [
                    {
                        "schema_version": 1,
                        "symbol": "00700.HK",
                        "trade_date": "20260522",
                        "bar_ts": "2026-05-22T09:30:00+08:00",
                        "open": 399.0,
                        "high": 399.0,
                        "low": 399.0,
                        "close": 399.0,
                        "volume": 9999,
                        "turnover": 3990000,
                        "source": "fixture",
                        "ingest_ts": "2026-05-22T09:31:00+08:00",
                        "row_hash": "minute-mutated",
                    },
                ],
            )

            runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260522",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=FakeKafkaProducer(),
                    kafka_consumer=FakeKafkaConsumer(),
                    redis_client=redis,
                    market_data_client=FakeMarketDataClient(),
                ),
            )
            supervisor = BeastMarketRuntimeSupervisor(runtime)
            supervisor.start(["00700.HK"])
            restored = runtime.cache.get_terminal_snapshot("20260522", "00700.HK")
            supervisor.stop()

        self.assertEqual(cached_before_restart["snapshot"]["price"], 388.8)
        self.assertEqual(restored["snapshot"]["price"], 388.8)
        self.assertEqual([bar["volume"] for bar in restored["minute_bars"]], [1000, 2000])
        self.assertIn("00700.HK", runtime.octopus.bod_by_symbol)

    def test_cached_snapshot_requires_minute_bar_source_evidence_to_be_fresh(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_minimal_silver(root)
            runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260522",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=FakeKafkaProducer(),
                    kafka_consumer=FakeKafkaConsumer(),
                    redis_client=RecordingRedis(),
                    market_data_client=FakeMarketDataClient(),
                ),
            )
            snapshot = runtime.octopus.preload_bod("00700.HK", "20260522")

        self.assertTrue(is_terminal_snapshot_fresh(snapshot, "20260522"))

        missing_source = {**snapshot, "freshness": {**snapshot["freshness"], "source_dates": {}}}
        self.assertFalse(is_terminal_snapshot_fresh(missing_source, "20260522"))

        wrong_effective = {**snapshot, "freshness": {**snapshot["freshness"], "effective_trade_date": "20260521"}}
        self.assertFalse(is_terminal_snapshot_fresh(wrong_effective, "20260522"))

    def test_supervisor_start_uses_fresh_redis_snapshot_when_redis_rewrite_fails(self) -> None:
        class FailingWriteRedis(RecordingRedis):
            def set(self, key: str, value: str, ex: int) -> None:
                raise RuntimeError("redis write unavailable")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            seed_redis = RecordingRedis()
            write_minimal_silver(root)
            warm_runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260522",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=FakeKafkaProducer(),
                    kafka_consumer=FakeKafkaConsumer(),
                    redis_client=seed_redis,
                    market_data_client=FakeMarketDataClient(),
                ),
            )
            warm_runtime.octopus.preload_bod("00700.HK", "20260522")

            redis = FailingWriteRedis()
            redis.values.update(seed_redis.values)
            redis.ttls.update(seed_redis.ttls)
            runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260522",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=FakeKafkaProducer(),
                    kafka_consumer=FakeKafkaConsumer(),
                    redis_client=redis,
                    market_data_client=FakeMarketDataClient(),
                ),
            )
            supervisor = BeastMarketRuntimeSupervisor(runtime)

            supervisor.start(["00700.HK"])
            state = runtime.octopus.get_state("00700.HK")
            supervisor.stop()

        self.assertIsNotNone(state)
        self.assertEqual(state["snapshot"]["price"], 388.8)
        self.assertEqual(runtime.octopus.health.redis, "degraded")
        self.assertTrue(state["freshness"]["degraded"])
        self.assertIn("redis_terminal_snapshot_write_failed", state["freshness"]["degraded_reasons"][0])

    def test_raw_callback_backpressure_writes_persistent_quarantine_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_minimal_silver(root)
            runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260522",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                    raw_queue_max_size=1,
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=FakeKafkaProducer(),
                    kafka_consumer=FakeKafkaConsumer(),
                    redis_client=RecordingRedis(),
                    market_data_client=FakeMarketDataClient(),
                ),
            )

            self.assertTrue(
                runtime.ingest_worker.receive_callback(
                    {"code": "700", "timestamp": "2026-05-22T09:30:00+08:00", "price": 388.4, "volume": 1000}
                )
            )
            self.assertFalse(
                runtime.ingest_worker.receive_callback(
                    {"code": "700", "timestamp": "2026-05-22T09:30:01+08:00", "price": 388.5, "volume": 1000}
                )
            )

            path = root / "artifacts" / "runtime-state" / "20260522" / "callback-rejections.jsonl"
            rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["reason"], "raw_callback_queue_full")
        self.assertEqual(rows[0]["payload"]["price"], 388.5)

    def test_raw_consumer_bad_record_writes_persistent_dead_letter_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_minimal_silver(root)
            broker = FakeKafkaBroker()
            runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260522",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=broker,
                    kafka_consumer=broker,
                    redis_client=RecordingRedis(),
                    market_data_client=FakeMarketDataClient(),
                ),
            )
            event = make_raw_market_event(
                kind="tick",
                symbol="00700.HK",
                source="xtquant",
                seq=1,
                payload={"price": 388.4, "volume": 1000, "turnover": 388400},
            )
            broker.records.setdefault(RAW_TOPIC, []).append(
                {"key": b"00939.HK", "value": json.dumps(event).encode("utf-8"), "offset": 0}
            )

            processed = runtime.raw_consumer_worker.poll_and_process("20260522")
            path = root / "artifacts" / "runtime-state" / "20260522" / "raw-consumer-dead-letters.jsonl"
            rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]

        self.assertEqual(processed, [])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["topic"], RAW_TOPIC)
        self.assertEqual(rows[0]["key"], "00939.HK")
        self.assertEqual(rows[0]["reason"], "Kafka event key must match event symbol")
        self.assertEqual(rows[0]["value"]["symbol"], "00700.HK")

    def test_supervisor_tick_drains_ingest_processes_raw_and_updates_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_minimal_silver(root)
            broker = FakeKafkaBroker()
            redis = RecordingRedis()
            runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260522",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                    big_trade_volume_baseline_ratio=2.0,
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=broker,
                    kafka_consumer=broker,
                    redis_client=redis,
                    market_data_client=FakeMarketDataClient(),
                ),
            )
            supervisor = BeastMarketRuntimeSupervisor(runtime)

            supervisor.start(["00700.HK"])
            runtime.session_manager.connect("client")
            runtime.session_manager.flush("client")
            runtime.session_manager.handle_message(
                "client",
                {
                    "schema_version": 1,
                    "protocol": "terminal-message-v1",
                    "action": "subscribe",
                    "symbol": "00700.HK",
                },
            )
            runtime.session_manager.flush("client")
            runtime.ingest_worker.receive_callback(
                {
                    "code": "700",
                    "timestamp": "2026-05-22T09:30:00+08:00",
                    "price": 388.4,
                    "volume": 1000,
                    "turnover": 388400,
                }
            )
            result = asyncio.run(supervisor.tick_once(now="2026-05-22T09:30:01+08:00"))
            supervisor.stop()

            snapshot = runtime.cache.get_terminal_snapshot("20260522", "00700.HK")
            self.assertEqual(result["ingested_events"], 1)
            self.assertEqual(result["processed_events"], 1)
            self.assertEqual(result["raw_events"][0]["symbol"], "00700.HK")
            self.assertEqual(result["processed_event_payloads"][0]["symbol"], "00700.HK")
            self.assertEqual(result["terminal_messages"][0]["type"], "tick_realtime")
            self.assertEqual(result["terminal_messages"][0]["source_ts"], "2026-05-22T09:30:00+08:00")
            self.assertEqual(result["runtime_terminal_messages"][0]["type"], "tick_realtime")
            self.assertEqual(result["runtime_processed_events_applied"], 1)
            self.assertEqual(result["runtime_terminal_messages_enqueued"], 1)
            self.assertEqual(result["shadow_processed_drained"], 1)
            self.assertEqual(runtime.gateway.processed_records_consumed, 0)
            self.assertEqual(runtime.gateway.shadow_processed_records_drained, 1)
            self.assertEqual(runtime.gateway.direct_runtime_messages_emitted, 1)
            self.assertEqual(runtime.gateway.terminal_messages_emitted, 1)
            self.assertIsNotNone(runtime.raw_consumer_worker.state_provider)
            self.assertIs(
                runtime.raw_consumer_worker.state_provider("00700.HK"),
                runtime.symbol_runtime_manager.runtimes["00700.HK"].snapshot_payload,
            )
            runtime_state_record = json.loads(redis.values["terminal:20260522:state:00700.HK"])
            self.assertEqual(runtime_state_record["runtime_state"], "LIVE")
            self.assertEqual(runtime_state_record["ref_count"], 1)
            self.assertTrue(runtime_state_record["realtime_attached"])
            self.assertEqual(runtime_state_record["requested_trade_date"], "20260522")
            self.assertEqual(snapshot["snapshot"]["price"], 388.4)
            symbol_runtime_payload = runtime.symbol_runtime_manager.runtimes["00700.HK"].snapshot_payload
            self.assertIsNotNone(symbol_runtime_payload)
            self.assertEqual(symbol_runtime_payload["snapshot"]["price"], 388.4)
            self.assertEqual(symbol_runtime_payload["freshness"]["runtime_state"], "LIVE")
            self.assertEqual(supervisor.stats.ticks, 1)
            self.assertEqual(runtime.subscription_manager.stats.starts, 1)
            self.assertEqual(runtime.subscription_manager.stats.stops, 1)

    def test_supervisor_tick_without_limit_drains_all_available_raw_events(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_minimal_silver(root)
            broker = FakeKafkaBroker()
            runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260522",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                    big_trade_volume_baseline_ratio=2.0,
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=broker,
                    kafka_consumer=broker,
                    redis_client=RecordingRedis(),
                    market_data_client=FakeMarketDataClient(),
                ),
            )
            supervisor = BeastMarketRuntimeSupervisor(runtime)
            supervisor.start(["00700.HK"])
            for index in range(7):
                runtime.ingest_worker.receive_callback(
                    {
                        "code": "700",
                        "timestamp": f"2026-05-22T09:3{index}:00+08:00",
                        "price": 388.4 + index,
                        "volume": 1000 + index,
                        "turnover": (388.4 + index) * (1000 + index),
                    }
                )

            result = asyncio.run(supervisor.tick_once(now="2026-05-22T09:40:00+08:00"))
            supervisor.stop()

        self.assertEqual(result["ingested_events"], 7)
        self.assertEqual(result["processed_events"], 7)
        self.assertEqual(broker.committed("raw_market_events_v1"), 7)

    def test_runtime_owned_raw_commit_failure_does_not_drop_realtime_snapshot(self) -> None:
        class CommitFailingBroker(FakeKafkaBroker):
            def commit(self, topic: str, offset: int) -> None:
                raise RuntimeError("commit failed")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_minimal_silver(root)
            broker = CommitFailingBroker()
            runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260522",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                    big_trade_volume_baseline_ratio=2.0,
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=broker,
                    kafka_consumer=broker,
                    redis_client=RecordingRedis(),
                    market_data_client=FakeMarketDataClient(),
                ),
            )
            supervisor = BeastMarketRuntimeSupervisor(runtime)
            supervisor.start(["00700.HK"])
            runtime.ingest_worker.receive_callback(
                {
                    "code": "700",
                    "timestamp": "2026-05-22T09:30:00+08:00",
                    "price": 388.4,
                    "volume": 1000,
                    "turnover": 388400,
                }
            )

            result = asyncio.run(supervisor.tick_once(now="2026-05-22T09:30:01+08:00"))
            supervisor.stop()

        self.assertEqual(result["ingested_events"], 1)
        self.assertEqual(result["processed_events"], 1)
        self.assertEqual(result["terminal_messages"][0]["type"], "tick_realtime")
        self.assertEqual(runtime.raw_consumer_worker.stats.failed, 1)
        self.assertIn(
            "runtime_owned_raw_commit_failed",
            runtime.raw_consumer_worker.stats.dead_letters[0].reason,
        )

    def test_supervisor_start_fails_fast_when_initial_realtime_subscribe_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_minimal_silver(root)
            runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260522",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=FakeKafkaProducer(),
                    kafka_consumer=FakeKafkaConsumer(),
                    redis_client=RecordingRedis(),
                    market_data_client=FakeMarketDataClient(fail_on_subscribe=True),
                ),
            )
            supervisor = BeastMarketRuntimeSupervisor(runtime)

            with self.assertRaisesRegex(RuntimeError, "subscribe failed"):
                supervisor.start(["00700.HK"])

        self.assertEqual(supervisor.stats.runtime_state, "DEGRADED")
        self.assertFalse(supervisor.running)

    def test_optional_instruments_reader_errors_fall_back_to_symbol_name(self) -> None:
        mammoth = MammothAPI(reader=InstrumentKeyErrorReader())
        octopus = OctopusComputeV2(mammoth, InMemoryEventBus(), InMemoryRedisSnapshotCache())

        snapshot = octopus.preload_bod("00700.HK", "20260522")

        self.assertEqual(mammoth.get_instruments(), [])
        self.assertEqual(snapshot["snapshot"]["name"], "00700.HK")

    def test_subscription_manager_stop_clears_internal_subscription_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_minimal_silver(root)
            market_data_client = FakeMarketDataClient()
            runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260522",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=FakeKafkaProducer(),
                    kafka_consumer=FakeKafkaConsumer(),
                    redis_client=RecordingRedis(),
                    market_data_client=market_data_client,
                ),
            )
            runtime.subscription_manager.start()
            runtime.subscription_manager.subscribe("00700.HK")

            runtime.subscription_manager.stop()

        self.assertEqual(runtime.subscription_manager.subscribed_symbols, set())
        self.assertEqual(market_data_client.subscribed_symbols, ["00700.HK"])

    def test_runtime_health_snapshot_captures_lag_freshness_workers_and_subscription_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_minimal_silver(root)
            broker = FakeKafkaBroker()
            runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260522",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                    big_trade_volume_baseline_ratio=2.0,
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=broker,
                    kafka_consumer=broker,
                    redis_client=RecordingRedis(),
                    market_data_client=FakeMarketDataClient(),
                ),
            )
            supervisor = BeastMarketRuntimeSupervisor(runtime)
            supervisor.start(["00700.HK"])
            runtime.session_manager.connect("client")
            runtime.session_manager.flush("client")
            runtime.session_manager.handle_message(
                "client",
                {
                    "schema_version": 1,
                    "protocol": "terminal-message-v1",
                    "action": "subscribe",
                    "symbol": "00700.HK",
                },
            )
            runtime.session_manager.flush("client")
            runtime.ingest_worker.receive_callback(
                {
                    "code": "700",
                    "timestamp": "2026-05-22T09:30:00+08:00",
                    "price": 388.4,
                    "volume": 1000,
                    "turnover": 388400,
                }
            )
            asyncio.run(supervisor.tick_once(now="2026-05-22T09:30:01+08:00"))
            queued = [json.loads(item) for item in runtime.session_manager.flush("client")]

            path = root / "artifacts" / "runtime-health.json"
            snapshot = write_runtime_health_snapshot(
                supervisor,
                path,
                generated_at="2026-05-22T09:30:02+08:00",
            )
            persisted = json.loads(path.read_text(encoding="utf-8"))

        self.assertEqual(snapshot["schema_version"], 1)
        self.assertEqual(persisted["generated_at"], "2026-05-22T09:30:02+08:00")
        self.assertIsInstance(persisted["supervisor"]["started_at"], str)
        self.assertEqual(persisted["supervisor"]["last_tick_at"], "2026-05-22T09:30:01+08:00")
        self.assertIsNone(persisted["supervisor"]["stopped_at"])
        self.assertEqual(persisted["topics"]["raw_market_events_v1"]["lag"], 0)
        self.assertEqual(persisted["topics"]["processed_market_events_v1"]["lag"], 0)
        self.assertEqual(persisted["queues"]["raw_callback_backlog"], 0)
        self.assertTrue(persisted["queues"]["raw_callback_rejection_path"].endswith("20260522/callback-rejections.jsonl"))
        self.assertTrue(persisted["queues"]["raw_consumer_dead_letter_path"].endswith("20260522/raw-consumer-dead-letters.jsonl"))
        self.assertEqual(persisted["workers"]["ingest"]["processed"], 1)
        self.assertEqual(persisted["workers"]["raw_consumer"]["processed"], 1)
        self.assertEqual(persisted["producer"]["spooled_records"], 0)
        self.assertTrue(persisted["producer"]["spool_path"].endswith("kafka-spool/publish-failures.jsonl"))
        self.assertEqual(persisted["producer"]["quarantined_spool_records"], 0)
        self.assertTrue(persisted["producer"]["spool_quarantine_path"].endswith("kafka-spool/publish-failures.jsonl.quarantine"))
        self.assertEqual(persisted["redis"]["write_stats"]["failures"], 0)
        self.assertGreaterEqual(persisted["redis"]["write_stats"]["writes"], 1)
        self.assertGreaterEqual(persisted["redis"]["write_stats"]["max_latency_ms"], 0)
        self.assertEqual(len(persisted["performance_samples"]["subscribe_snapshot_ms"]), 1)
        self.assertGreaterEqual(persisted["performance_samples"]["subscribe_snapshot_ms"][0], 0)
        self.assertEqual(queued[0]["type"], "tick_realtime")
        self.assertEqual(queued[0]["symbol"], "00700.HK")
        self.assertEqual(persisted["subscription"]["subscribed_symbols"], ["00700.HK"])
        self.assertEqual(persisted["symbol_runtime_manager"]["runtime_count"], 1)
        self.assertEqual(persisted["symbol_runtime_manager"]["total_ref_count"], 1)
        self.assertEqual(persisted["symbol_runtime_manager"]["active_hydrations"], 0)
        self.assertEqual(persisted["symbol_runtime_manager"]["hydrating_symbols"], [])
        self.assertEqual(persisted["symbol_runtime_manager"]["max_concurrent_hydrations"], 8)
        self.assertEqual(persisted["symbol_runtime_manager"]["capacity_rejections"], 0)
        self.assertEqual(persisted["symbol_runtime_manager"]["state_sink_failures"], 0)
        self.assertEqual(persisted["symbol_runtime_manager"]["last_state_sink_error"], "")
        self.assertEqual(persisted["symbol_runtime_manager"]["state_sink_failure_symbols"], [])
        self.assertEqual(persisted["symbol_runtime_manager"]["state_counts"]["LIVE"], 1)
        self.assertEqual(persisted["symbol_runtime_manager"]["realtime_attached_symbols"], ["00700.HK"])
        self.assertEqual(persisted["symbol_runtime"]["00700.HK"]["state"], "LIVE")
        self.assertEqual(persisted["symbol_runtime"]["00700.HK"]["ref_count"], 1)
        self.assertEqual(persisted["symbol_runtime"]["00700.HK"]["hydrate_count"], 0)
        self.assertEqual(persisted["symbol_runtime"]["00700.HK"]["hydration_failures"], 0)
        self.assertGreaterEqual(persisted["symbol_runtime"]["00700.HK"]["last_hydration_latency_ms"], 0)
        self.assertGreaterEqual(persisted["symbol_runtime"]["00700.HK"]["max_hydration_latency_ms"], 0)
        self.assertTrue(persisted["symbol_runtime"]["00700.HK"]["realtime_attached"])
        self.assertEqual(persisted["redis_snapshot"]["checked_symbols"], ["00700.HK"])
        self.assertEqual(persisted["redis_snapshot"]["present_symbols"], ["00700.HK"])
        self.assertEqual(persisted["redis_snapshot"]["missing_symbols"], [])
        self.assertEqual(
            persisted["redis_snapshot"]["required_key_families"],
            [
                "terminal_snapshot",
                "terminal_minute",
                "terminal_alerts",
                "terminal_queue",
                "terminal_state",
                "ccass_holding",
                "ccass_history",
            ],
        )
        for family in persisted["redis_snapshot"]["required_key_families"]:
            self.assertEqual(
                persisted["redis_snapshot"]["key_family_coverage"][family]["present_symbols"],
                ["00700.HK"],
            )
            self.assertEqual(
                persisted["redis_snapshot"]["key_family_coverage"][family]["missing_symbols"],
                [],
            )
            self.assertEqual(
                persisted["redis_snapshot"]["key_family_coverage"][family]["missing_updated_at_symbols"],
                [],
            )
            self.assertIsInstance(
                persisted["redis_snapshot"]["key_family_coverage"][family]["updated_at_by_symbol"]["00700.HK"],
                str,
            )
            self.assertEqual(
                persisted["redis_snapshot"]["key_family_coverage"][family]["missing_ttl_symbols"],
                [],
            )
            self.assertGreater(
                persisted["redis_snapshot"]["key_family_coverage"][family]["ttl_seconds_by_symbol"]["00700.HK"],
                0,
            )
            self.assertEqual(
                persisted["redis_snapshot"]["key_family_coverage"][family]["contract_missing_by_symbol"],
                {},
            )
        self.assertEqual(
            persisted["redis_snapshot"]["key_family_coverage"]["ccass_history"]["participants_by_symbol"],
            {"00700.HK": ["C00010"]},
        )
        self.assertEqual(persisted["gateway_websocket"]["path"], "/ws")
        self.assertEqual(persisted["gateway_websocket"]["request_schema_version"], 1)
        self.assertEqual(persisted["gateway_websocket"]["accepted_protocol"], "terminal-message-v1")
        self.assertTrue(persisted["gateway_websocket"]["running"])
        self.assertEqual(persisted["gateway_websocket"]["connected_clients"], 0)
        self.assertEqual(persisted["gateway_websocket"]["failed_client_sends"], 0)
        self.assertGreater(persisted["gateway_websocket"]["send_timeout_seconds"], 0)
        self.assertEqual(persisted["gateway_activity"]["processed_records_consumed"], 0)
        self.assertEqual(persisted["gateway_activity"]["shadow_processed_records_drained"], 1)
        self.assertEqual(persisted["gateway_activity"]["direct_runtime_messages_emitted"], 1)
        self.assertEqual(persisted["gateway_activity"]["terminal_messages_emitted"], 1)
        self.assertEqual(persisted["gateway_activity"]["terminal_messages_delivered"], 0)
        self.assertEqual(persisted["gateway_activity"]["delivered_terminal_symbols"], [])
        self.assertIsNone(persisted["gateway_activity"]["last_terminal_message_delivered_at"])
        self.assertEqual(
            persisted["gateway_activity"]["client_queue"]["client_queue_max_size"],
            runtime.session_manager.client_queue_size,
        )
        self.assertEqual(persisted["gateway_activity"]["client_queue"]["alert_dropped"], 0)
        self.assertIn("00700.HK", persisted["health"]["collector"]["symbol_freshness"])
        freshness = persisted["health"]["collector"]["symbol_freshness"]["00700.HK"]
        self.assertTrue(freshness["subscribed"])
        self.assertEqual(freshness["latest_event_at"], "2026-05-22T09:30:00+08:00")

    def test_historical_readiness_blocks_live_without_minimum_daily_ccass_and_mapping(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_table(
                root / "silver_daily_bars_v1.csv",
                [
                    {
                        "schema_version": 1,
                        "symbol": "00700.HK",
                        "trade_date": "20260522",
                        "open": 386,
                        "high": 389,
                        "low": 385,
                        "close": 386.2,
                        "volume": 1000,
                        "turnover": 386200,
                        "source": "fixture",
                        "ingest_ts": "2026-05-22T00:00:00Z",
                        "row_hash": "daily",
                    }
                ],
            )
            write_ccass(root)
            runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260522",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=FakeKafkaProducer(),
                    kafka_consumer=FakeKafkaConsumer(),
                    redis_client=RecordingRedis(),
                    market_data_client=FakeMarketDataClient(),
                ),
            )

            readiness = evaluate_monitoring_historical_readiness(
                runtime.mammoth,
                symbols=["00700.HK"],
                trade_date="20260522",
            )

            self.assertFalse(readiness["passed"])
            self.assertIn("missing_daily_bars", readiness["blockers"])
            self.assertIn("missing_minute_bars", readiness["blockers"])
            self.assertIn("missing_ccass_holdings", readiness["blockers"])
            self.assertIn("missing_broker_mapping", readiness["blockers"])
            with self.assertRaisesRegex(RuntimeError, "historical data not ready"):
                BeastMarketRuntimeSupervisor(runtime).start(["00700.HK"])

    def test_historical_readiness_accepts_closed_day_minute_bars_from_effective_trade_date(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_minimal_silver(root)
            runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260525",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=FakeKafkaProducer(),
                    kafka_consumer=FakeKafkaConsumer(),
                    redis_client=RecordingRedis(),
                    market_data_client=FakeMarketDataClient(),
                ),
            )

            readiness = evaluate_monitoring_historical_readiness(
                runtime.mammoth,
                symbols=["00700.HK"],
                trade_date="20260525",
            )

        self.assertTrue(readiness["passed"])
        self.assertEqual(readiness["evidence"]["effective_trade_date_by_symbol"]["00700.HK"], "20260522")
        self.assertEqual(readiness["evidence"]["minute_bars"]["00700.HK"]["effective_trade_date"], "20260522")

    def test_historical_readiness_does_not_select_tick_only_day_for_minute_chart(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_minimal_silver(root)
            write_table(
                root / "silver_trade_ticks_v1.csv",
                [
                    {
                        "schema_version": 1,
                        "symbol": "00700.HK",
                        "trade_date": "20260525",
                        "tick_ts": "2026-05-25T09:30:00+08:00",
                        "price": 390.0,
                        "volume": 1000,
                        "turnover": 390000,
                        "side": "buy",
                        "source": "fixture",
                        "ingest_ts": "2026-05-25T09:30:01+08:00",
                        "row_hash": "tick-only-20260525",
                    }
                ],
            )
            runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260525",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=FakeKafkaProducer(),
                    kafka_consumer=FakeKafkaConsumer(),
                    redis_client=RecordingRedis(),
                    market_data_client=FakeMarketDataClient(),
                ),
            )

            readiness = evaluate_monitoring_historical_readiness(
                runtime.mammoth,
                symbols=["00700.HK"],
                trade_date="20260525",
            )
            snapshot = hydrate_symbol_snapshot(runtime, "00700.HK")

        self.assertTrue(readiness["passed"])
        self.assertEqual(readiness["evidence"]["effective_trade_date_by_symbol"]["00700.HK"], "20260522")
        self.assertEqual(readiness["evidence"]["minute_bars"]["00700.HK"]["effective_trade_date"], "20260522")
        self.assertEqual(snapshot["snapshot"]["tradeDate"], "20260522")
        self.assertEqual(snapshot["minute_bars"][-1]["timestamp"], "2026-05-22T09:31:00+08:00")

    def test_start_recovers_from_local_jsonl_and_backfills_trade_ticks_without_duplicate_alerts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_minimal_silver(root)
            write_table(
                root / "silver_trade_ticks_v1.csv",
                [
                    {
                        "schema_version": 1,
                        "symbol": "00700.HK",
                        "trade_date": "20260522",
                        "tick_ts": "2026-05-22T09:30:00+08:00",
                        "price": 388.4,
                        "volume": 1000,
                        "turnover": 388400,
                        "side": "buy",
                        "source": "fixture",
                        "ingest_ts": "2026-05-22T09:30:01+08:00",
                        "row_hash": "tick-duplicate",
                    },
                    {
                        "schema_version": 1,
                        "symbol": "00700.HK",
                        "trade_date": "20260522",
                        "tick_ts": "2026-05-22T09:31:00+08:00",
                        "price": 389.0,
                        "volume": 2000,
                        "turnover": 778000,
                        "side": "buy",
                        "source": "fixture",
                        "ingest_ts": "2026-05-22T09:31:01+08:00",
                        "row_hash": "tick-new",
                    },
                ],
            )
            runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260522",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=FakeKafkaBroker(),
                    kafka_consumer=FakeKafkaBroker(),
                    redis_client=RecordingRedis(),
                    market_data_client=FakeMarketDataClient(),
                ),
            )
            runtime.runtime_state.append_raw_event(
                "20260522",
                "00700.HK",
                make_raw_market_event(
                    kind="tick",
                    symbol="00700.HK",
                    source="xtquant",
                    seq=1,
                    source_ts="2026-05-22T09:30:00+08:00",
                    payload={"price": 388.4, "volume": 1000, "turnover": 388400, "side": "buy"},
                ),
            )

            supervisor = BeastMarketRuntimeSupervisor(runtime)
            supervisor.start(["00700.HK"])
            snapshot = runtime.cache.get_terminal_snapshot("20260522", "00700.HK")
            supervisor.stop()

        self.assertEqual([bar["timestamp"] for bar in snapshot["minute_bars"]], [
            "2026-05-22T09:30:00+08:00",
            "2026-05-22T09:31:00+08:00",
        ])
        self.assertEqual(snapshot["snapshot"]["volume"], 3000)
        self.assertEqual(len(snapshot["alerts"]), 2)
        self.assertEqual(supervisor.stats.runtime_state, "CLOSING")

    def test_recovery_applies_processed_events_to_seeded_symbol_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_minimal_silver(root)
            write_table(
                root / "silver_trade_ticks_v1.csv",
                [
                    {
                        "schema_version": 1,
                        "symbol": "00700.HK",
                        "trade_date": "20260522",
                        "tick_ts": "2026-05-22T09:31:00+08:00",
                        "price": 389.0,
                        "volume": 2000,
                        "turnover": 778000,
                        "side": "buy",
                        "source": "fixture",
                        "ingest_ts": "2026-05-22T09:31:01+08:00",
                        "row_hash": "runtime-recovery-tick",
                    },
                ],
            )
            runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260522",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=FakeKafkaBroker(),
                    kafka_consumer=FakeKafkaBroker(),
                    redis_client=RecordingRedis(),
                    market_data_client=FakeMarketDataClient(),
                ),
            )
            runtime.octopus.preload_bod("00700.HK", "20260522")

            result = recover_symbol_intraday(runtime, "00700.HK", data_trade_date="20260522")

        symbol_runtime = runtime.symbol_runtime_manager.runtimes["00700.HK"]
        self.assertEqual(result["backfilled_ticks"], 1)
        self.assertEqual(result["recovered_processed_events"], 1)
        self.assertEqual(result["runtime_processed_events_applied"], 1)
        self.assertEqual(symbol_runtime.snapshot_payload["snapshot"]["price"], 388.8)
        self.assertEqual(symbol_runtime.snapshot_payload["alerts"][0]["price"], 389.0)

    def test_redis_clear_then_restart_rebuilds_symbol_snapshot_from_1m_bars_and_runtime_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            redis = RecordingRedis()
            write_minimal_silver(root)
            write_table(
                root / "silver_trade_ticks_v1.csv",
                [
                    {
                        "schema_version": 1,
                        "symbol": "00700.HK",
                        "trade_date": "20260522",
                        "tick_ts": "2026-05-22T09:30:00+08:00",
                        "price": 388.4,
                        "volume": 1000,
                        "turnover": 388400,
                        "side": "buy",
                        "source": "fixture",
                        "ingest_ts": "2026-05-22T09:30:01+08:00",
                        "row_hash": "restart-tick-1",
                    },
                    {
                        "schema_version": 1,
                        "symbol": "00700.HK",
                        "trade_date": "20260522",
                        "tick_ts": "2026-05-22T09:31:00+08:00",
                        "price": 389.0,
                        "volume": 2000,
                        "turnover": 778000,
                        "side": "buy",
                        "source": "fixture",
                        "ingest_ts": "2026-05-22T09:31:01+08:00",
                        "row_hash": "restart-tick-2",
                    },
                ],
            )

            first_runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260522",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=FakeKafkaBroker(),
                    kafka_consumer=FakeKafkaBroker(),
                    redis_client=redis,
                    market_data_client=FakeMarketDataClient(),
                ),
            )
            first_supervisor = BeastMarketRuntimeSupervisor(first_runtime)
            first_supervisor.start(["00700.HK"])
            first_snapshot = first_runtime.cache.get_terminal_snapshot("20260522", "00700.HK")
            first_supervisor.stop()

            clear_result = clear_runtime_cache(
                redis_client=redis,
                trade_date="20260522",
                symbols=["00700.HK"],
                dry_run=False,
                confirm=True,
            )
            raw_events_path = root / "artifacts" / "runtime-state" / "20260522" / "00700.HK" / "raw-events.jsonl"
            raw_events_preserved = raw_events_path.exists()

            second_runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260522",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=FakeKafkaBroker(),
                    kafka_consumer=FakeKafkaBroker(),
                    redis_client=redis,
                    market_data_client=FakeMarketDataClient(),
                ),
            )
            second_supervisor = BeastMarketRuntimeSupervisor(second_runtime)
            second_supervisor.start(["00700.HK"])
            rebuilt = second_runtime.cache.get_terminal_snapshot("20260522", "00700.HK")
            second_supervisor.stop()

        self.assertEqual(first_snapshot["snapshot"]["volume"], 3000)
        self.assertEqual(len(first_snapshot["alerts"]), 2)
        self.assertTrue(raw_events_preserved)
        self.assertIn("terminal:20260522:snapshot:00700.HK", clear_result.deleted_keys)
        self.assertEqual(rebuilt["snapshot"]["volume"], 3000)
        self.assertEqual([bar["volume"] for bar in rebuilt["minute_bars"]], [1000, 2000])
        self.assertEqual(len(rebuilt["alerts"]), 2)
        self.assertIn("terminal:20260522:snapshot:00700.HK", redis.values)

    def test_closed_day_start_displays_latest_effective_trade_date_data_under_requested_date(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_minimal_silver(root)
            write_table(
                root / "silver_trade_ticks_v1.csv",
                [
                    {
                        "schema_version": 1,
                        "symbol": "00700.HK",
                        "trade_date": "20260522",
                        "tick_ts": "2026-05-22T09:30:00+08:00",
                        "price": 388.4,
                        "volume": 1000,
                        "turnover": 388400,
                        "side": "buy",
                        "source": "fixture",
                        "ingest_ts": "2026-05-22T09:30:01+08:00",
                        "row_hash": "closed-day-tick-1",
                    },
                    {
                        "schema_version": 1,
                        "symbol": "00700.HK",
                        "trade_date": "20260522",
                        "tick_ts": "2026-05-22T09:31:00+08:00",
                        "price": 389.0,
                        "volume": 2000,
                        "turnover": 778000,
                        "side": "buy",
                        "source": "fixture",
                        "ingest_ts": "2026-05-22T09:31:01+08:00",
                        "row_hash": "closed-day-tick-2",
                    },
                ],
            )
            runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260525",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=FakeKafkaProducer(),
                    kafka_consumer=FakeKafkaConsumer(),
                    redis_client=RecordingRedis(),
                    market_data_client=FakeMarketDataClient(),
                ),
            )
            supervisor = BeastMarketRuntimeSupervisor(runtime)

            supervisor.start(["00700.HK"])
            snapshot = runtime.cache.get_terminal_snapshot("20260525", "00700.HK")
            supervisor.stop()

        self.assertEqual(runtime.effective_trade_date_by_symbol, {"00700.HK": "20260522"})
        self.assertEqual(runtime.subscription_manager.subscribed_symbols, set())
        self.assertEqual(snapshot["snapshot"]["tradeDate"], "20260522")
        self.assertEqual(snapshot["snapshot"]["requestedTradeDate"], "20260525")
        self.assertTrue(snapshot["snapshot"]["isHistoricalSession"])
        self.assertEqual(snapshot["snapshot"]["previousClose"], 382.0)
        self.assertEqual(snapshot["snapshot"]["price"], 388.8)
        self.assertEqual([bar["timestamp"] for bar in snapshot["minute_bars"]], [
            "2026-05-22T09:30:00+08:00",
            "2026-05-22T09:31:00+08:00",
        ])

    def test_pre_open_trading_day_subscribes_realtime_even_before_today_minute_bars(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_minimal_silver(root)
            write_table(
                root / "silver_trading_calendar_v1.csv",
                [
                    {
                        "schema_version": 1,
                        "market": "HK",
                        "trade_date": "20260525",
                        "is_trading_day": True,
                        "source": "fixture",
                        "ingest_ts": "2026-05-25T00:00:00+08:00",
                        "row_hash": "calendar-open",
                    }
                ],
            )
            market_data_client = FakeMarketDataClient()
            runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260525",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=FakeKafkaProducer(),
                    kafka_consumer=FakeKafkaConsumer(),
                    redis_client=RecordingRedis(),
                    market_data_client=market_data_client,
                ),
            )
            supervisor = BeastMarketRuntimeSupervisor(runtime)

            supervisor.start(["00700.HK"])
            snapshot = runtime.cache.get_terminal_snapshot("20260525", "00700.HK")
            supervisor.stop()

        self.assertEqual(runtime.effective_trade_date_by_symbol, {"00700.HK": "20260522"})
        self.assertEqual(market_data_client.subscribed_symbols, ["00700.HK"])
        self.assertEqual(snapshot["snapshot"]["tradeDate"], "20260525")
        self.assertEqual(snapshot["snapshot"]["requestedTradeDate"], "20260525")
        self.assertFalse(snapshot["snapshot"]["isHistoricalSession"])
        self.assertEqual(snapshot["minute_bars"], [])
        self.assertEqual(snapshot["freshness"]["runtime_state"], "LIVE")
        self.assertIn("intraday_gap_before_attach", snapshot["freshness"]["degraded_reasons"])

    def test_cold_hydration_rolls_previous_session_to_today_before_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_minimal_silver(root)
            write_table(
                root / "silver_instruments_v1.csv",
                [
                    {
                        "schema_version": 1,
                        "symbol": "00700.HK",
                        "name": "腾讯控股",
                        "source": "fixture",
                        "ingest_ts": "2026-05-25T00:00:00+08:00",
                        "row_hash": "instrument-00700",
                    }
                ],
            )
            write_table(
                root / "silver_trading_calendar_v1.csv",
                [
                    {
                        "schema_version": 1,
                        "market": "HK",
                        "trade_date": "20260525",
                        "is_trading_day": True,
                        "source": "fixture",
                        "ingest_ts": "2026-05-25T00:00:00+08:00",
                        "row_hash": "calendar-open",
                    }
                ],
            )
            runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260525",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=FakeKafkaProducer(),
                    kafka_consumer=FakeKafkaConsumer(),
                    redis_client=RecordingRedis(),
                    market_data_client=FakeMarketDataClient(),
                ),
            )

            snapshot = hydrate_symbol_snapshot(runtime, "00700.HK")

        self.assertEqual(snapshot["snapshot"]["name"], "腾讯控股")
        self.assertEqual(snapshot["snapshot"]["tradeDate"], "20260525")
        self.assertFalse(snapshot["snapshot"]["isHistoricalSession"])
        self.assertEqual(snapshot["minute_bars"], [])
        self.assertEqual(snapshot["freshness"]["source_dates"]["minute_bars"], "")
        self.assertIn("intraday_gap_before_attach", snapshot["freshness"]["degraded_reasons"])

    def test_cold_hydration_seeds_realtime_minute_bar_from_full_tick(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_minimal_silver(root)
            write_table(
                root / "silver_trading_calendar_v1.csv",
                [
                    {
                        "schema_version": 1,
                        "market": "HK",
                        "trade_date": "20260525",
                        "is_trading_day": True,
                        "source": "fixture",
                        "ingest_ts": "2026-05-25T00:00:00+08:00",
                        "row_hash": "calendar-open",
                    }
                ],
            )
            runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260525",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=FakeKafkaProducer(),
                    kafka_consumer=FakeKafkaConsumer(),
                    redis_client=RecordingRedis(),
                    market_data_client=FakeMarketDataClient(
                        full_ticks={
                            "00700.HK": {
                                "timetag": "20260525 09:31:33.653",
                                "lastPrice": 390.0,
                                "volume": 12345,
                                "amount": 4814550,
                            }
                        }
                    ),
                ),
            )

            snapshot = hydrate_symbol_snapshot(runtime, "00700.HK")

        self.assertEqual(snapshot["snapshot"]["tradeDate"], "20260525")
        self.assertEqual(snapshot["minute_bars"][0]["timestamp"], "2026-05-25T09:31:00+08:00")
        self.assertEqual(snapshot["minute_bars"][0]["close"], 390.0)
        self.assertEqual(snapshot["freshness"]["source_dates"]["minute_bars"], "20260525")
        self.assertNotIn("intraday_gap_before_attach", snapshot["freshness"]["degraded_reasons"])

    def test_attach_realtime_accepts_truthy_non_builtin_trading_day_value(self) -> None:
        class TruthyTradingDay:
            def __bool__(self) -> bool:
                return True

        class Mammoth:
            def is_trading_day(self, trade_date: str, *, market: str = "HK") -> TruthyTradingDay:
                self.args = (trade_date, market)
                return TruthyTradingDay()

        runtime = type(
            "Runtime",
            (),
            {
                "config": type("Config", (), {"trade_date": "20260527"})(),
                "effective_trade_date_by_symbol": {"00700.HK": "20260526"},
                "mammoth": Mammoth(),
            },
        )()

        self.assertTrue(should_attach_realtime_for_symbol(runtime, "00700.HK"))
        self.assertEqual(runtime.mammoth.args, ("20260527", "HK"))

    def test_full_tick_seed_populates_today_snapshot_and_minute_bar(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_minimal_silver(root)
            market_data_client = FakeMarketDataClient(
                full_ticks={
                    "00700.HK": {
                        "timetag": "20260525 09:31:33.653",
                        "lastPrice": 390.0,
                        "open": 389.0,
                        "high": 391.0,
                        "low": 388.0,
                        "volume": 12345,
                        "amount": 4814550,
                    }
                }
            )
            runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260525",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=FakeKafkaProducer(),
                    kafka_consumer=FakeKafkaConsumer(),
                    redis_client=RecordingRedis(),
                    market_data_client=market_data_client,
                ),
            )
            snapshot = runtime.octopus.preload_bod(
                "00700.HK",
                "20260522",
                cache_trade_date="20260525",
                requested_trade_date="20260525",
            )
            promote_snapshot_to_realtime_session(runtime, "00700.HK", snapshot)

            processed = seed_symbol_from_market_full_tick(runtime, "00700.HK")
            cached = runtime.cache.get_terminal_snapshot("20260525", "00700.HK")

        self.assertEqual(processed[0]["result_type"], "snapshot")
        self.assertEqual(cached["snapshot"]["tradeDate"], "20260525")
        self.assertEqual(cached["snapshot"]["price"], 390.0)
        self.assertEqual(cached["snapshot"]["volume"], 12345)
        self.assertEqual(cached["minute_bars"][0]["timestamp"], "2026-05-25T09:31:00+08:00")
        self.assertEqual(cached["minute_bars"][0]["close"], 390.0)
        self.assertEqual(cached["freshness"]["runtime_state"], "LIVE")

    def test_running_runtime_subscribes_new_symbol_not_in_startup_watchlist(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_minimal_silver(root)
            append_second_symbol_silver(root)
            market_data_client = FakeMarketDataClient()
            redis = RecordingRedis()
            runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260522",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=FakeKafkaProducer(),
                    kafka_consumer=FakeKafkaConsumer(),
                    redis_client=redis,
                    market_data_client=market_data_client,
                ),
            )
            supervisor = BeastMarketRuntimeSupervisor(runtime)

            supervisor.start(["00700.HK"])
            runtime.session_manager.connect("client")
            runtime.session_manager.flush("client")
            runtime.session_manager.handle_message(
                "client",
                {
                    "schema_version": 1,
                    "protocol": "terminal-message-v1",
                    "action": "subscribe",
                    "symbol": "939",
                },
            )
            messages = [json.loads(item) for item in runtime.session_manager.flush("client")]
            supervisor.stop()

        self.assertEqual(messages[0]["type"], "snapshot")
        self.assertEqual(messages[0]["symbol"], "00939.HK")
        self.assertEqual(messages[0]["payload"]["snapshot"]["tradeDate"], "20260522")
        self.assertEqual(messages[0]["payload"]["minute_bars"][0]["timestamp"], "2026-05-22T09:30:00+08:00")
        self.assertEqual(runtime.effective_trade_date_by_symbol["00939.HK"], "20260522")
        self.assertEqual(runtime.symbol_runtime_manager.runtimes["00939.HK"].hydrate_count, 1)
        self.assertIn("00939.HK", market_data_client.subscribed_symbols)
        self.assertIn("terminal:20260522:snapshot:00939.HK", redis.values)

    def test_subscribe_to_active_intraday_gap_runtime_retries_full_tick_seed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_minimal_silver(root)
            write_table(
                root / "silver_trading_calendar_v1.csv",
                [
                    {
                        "schema_version": 1,
                        "market": "HK",
                        "trade_date": "20260525",
                        "is_trading_day": True,
                        "source": "fixture",
                        "ingest_ts": "2026-05-25T00:00:00+08:00",
                        "row_hash": "calendar-open",
                    }
                ],
            )
            market_data_client = FakeMarketDataClient()
            runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260525",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=FakeKafkaProducer(),
                    kafka_consumer=FakeKafkaConsumer(),
                    redis_client=RecordingRedis(),
                    market_data_client=market_data_client,
                ),
            )
            supervisor = BeastMarketRuntimeSupervisor(runtime)

            supervisor.start(["00700.HK"])
            market_data_client.full_ticks["00700.HK"] = {
                "timetag": "20260525 09:32:01.000",
                "lastPrice": 391.0,
                "volume": 1000,
                "amount": 391000,
            }
            runtime.session_manager.connect("client")
            runtime.session_manager.flush("client")
            runtime.session_manager.handle_message(
                "client",
                {
                    "schema_version": 1,
                    "protocol": "terminal-message-v1",
                    "action": "subscribe",
                    "symbol": "00700.HK",
                },
            )
            messages = [json.loads(item) for item in runtime.session_manager.flush("client")]
            supervisor.stop()

        self.assertEqual(messages[0]["type"], "snapshot")
        self.assertEqual(messages[0]["payload"]["minute_bars"][0]["timestamp"], "2026-05-25T09:32:00+08:00")
        self.assertEqual(messages[0]["payload"]["freshness"]["source_dates"]["minute_bars"], "20260525")
        self.assertNotIn("intraday_gap_before_attach", messages[0]["payload"]["freshness"]["degraded_reasons"])

    def test_run_supervised_runtime_owns_lifecycle_websocket_and_health_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_minimal_silver(root)
            broker = FakeKafkaBroker()
            market_data_client = FakeMarketDataClient()
            runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260522",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                    gateway_host="0.0.0.0",
                    gateway_port=9021,
                    gateway_path="/ws",
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=broker,
                    kafka_consumer=broker,
                    redis_client=RecordingRedis(),
                    market_data_client=market_data_client,
                ),
            )
            supervisor = BeastMarketRuntimeSupervisor(runtime)
            runtime.ingest_worker.receive_callback(
                {
                    "code": "700",
                    "timestamp": "2026-05-22T09:30:00+08:00",
                    "price": 388.4,
                    "volume": 1000,
                    "turnover": 388400,
                }
            )
            serve_factory = RecordingServeFactory()
            health_path = root / "artifacts" / "runtime-health.json"
            shadow_recorder = FileBackedShadowRunRecorder(
                directory=root / "shadow-run-streams",
                session_id="session-1",
                trading_date="20260522",
                started_at="2026-05-22T09:30:00+08:00",
                reset=True,
            )

            result = asyncio.run(
                run_supervised_runtime(
                    supervisor,
                    symbols=["00700.HK"],
                    tick_interval_seconds=0,
                    health_snapshot_path=health_path,
                    health_snapshot_interval_seconds=1,
                    serve_factory=serve_factory,
                    shadow_recorder=shadow_recorder,
                    max_ticks=1,
                )
            )
            persisted = json.loads(health_path.read_text(encoding="utf-8"))
            shadow_files = shadow_run_file_paths(root / "shadow-run-streams", trading_date="20260522", session_id="session-1")
            shadow = load_shadow_run_files(shadow_files)

        self.assertEqual(result.ticks, 1)
        self.assertEqual(result.stop_reason, "max_ticks")
        self.assertEqual(result.health_snapshot_path, health_path)
        self.assertEqual(result.final_health_snapshot["supervisor"]["ticks"], 1)
        self.assertEqual(result.final_health_snapshot["supervisor"]["stop_reason"], "max_ticks")
        self.assertEqual(serve_factory.host, "0.0.0.0")
        self.assertEqual(serve_factory.port, 9021)
        self.assertFalse(supervisor.running)
        self.assertTrue(market_data_client.started)
        self.assertTrue(market_data_client.stopped)
        self.assertEqual(market_data_client.subscribed_symbols[0], "00700.HK")
        self.assertEqual(persisted["supervisor"]["ingested_events"], 1)
        self.assertEqual(persisted["gateway_websocket"]["host"], "0.0.0.0")
        self.assertEqual(persisted["gateway_websocket"]["port"], 9021)
        self.assertEqual(persisted["gateway_websocket"]["path"], "/ws")
        self.assertEqual(persisted["gateway_websocket"]["accepted_protocol"], "terminal-message-v1")
        self.assertEqual(shadow["v2_events"][0]["symbol"], "00700.HK")
        self.assertEqual(shadow["v2_events"][0]["source_ts"], "2026-05-22T09:30:00+08:00")
        self.assertTrue(shadow["performance_samples"]["collector_to_kafka_ms"])
        self.assertTrue(shadow["performance_samples"]["processed_to_gateway_ms"])
        self.assertTrue(shadow["performance_samples"]["gateway_to_frontend_ms"])
        self.assertIsNone(runtime.websocket_service.shadow_recorder)

    def test_runtime_signal_handlers_request_graceful_stop_and_cleanup(self) -> None:
        async def exercise() -> tuple[str | None, bool, list[signal.Signals]]:
            stop = asyncio.Event()
            loop = FakeSignalLoop()
            controller = install_runtime_signal_handlers(
                stop,
                loop=loop,
                signals=(signal.SIGTERM,),
            )

            self.assertFalse(stop.is_set())
            loop.handlers[signal.SIGTERM]()
            controller.cleanup()
            return controller.received_signal, stop.is_set(), loop.removed

        received_signal, stopped, removed = asyncio.run(exercise())

        self.assertEqual(received_signal, "SIGTERM")
        self.assertTrue(stopped)
        self.assertEqual(removed, [signal.SIGTERM])

    def test_run_supervised_runtime_reports_external_stop_event(self) -> None:
        async def exercise(root: Path) -> tuple[int, str, bool, str | None]:
            write_minimal_silver(root)
            runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260522",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=FakeKafkaProducer(),
                    kafka_consumer=FakeKafkaConsumer(),
                    redis_client=RecordingRedis(),
                    market_data_client=FakeMarketDataClient(),
                ),
            )
            supervisor = BeastMarketRuntimeSupervisor(runtime)
            stop = asyncio.Event()
            stop.set()
            result = await run_supervised_runtime(
                supervisor,
                symbols=["00700.HK"],
                tick_interval_seconds=0,
                stop=stop,
                serve_factory=RecordingServeFactory(),
            )
            return result.ticks, result.stop_reason, supervisor.running, supervisor.stats.stop_reason

        with tempfile.TemporaryDirectory() as directory:
            ticks, stop_reason, running, supervisor_stop_reason = asyncio.run(exercise(Path(directory)))

        self.assertEqual(ticks, 0)
        self.assertEqual(stop_reason, "stop_event")
        self.assertFalse(running)
        self.assertEqual(supervisor_stop_reason, "stop_event")


class FakeKafkaProducer:
    def produce(self, topic: str, key: bytes, value: bytes) -> None:
        pass

    def flush(self) -> None:
        pass


class FakeSignalLoop:
    def __init__(self) -> None:
        self.handlers: dict[signal.Signals, object] = {}
        self.removed: list[signal.Signals] = []

    def add_signal_handler(self, signal_value: signal.Signals, handler) -> None:
        self.handlers[signal_value] = handler

    def remove_signal_handler(self, signal_value: signal.Signals) -> bool:
        self.removed.append(signal_value)
        return True


class FakeKafkaConsumer:
    def poll(self, topic: str, offset: int, timeout_ms: int) -> list[dict]:
        return []

    def committed(self, topic: str) -> int:
        return 0

    def high_watermark(self, topic: str) -> int:
        return 0


class FakeConfluentConsumer:
    def __init__(self, messages: list["FakeConfluentMessage"] | None = None) -> None:
        self.subscriptions: list[list[str]] = []
        self.messages = list(messages or [])
        self.committed_offsets: list[FakeTopicPartition] = []

    def subscribe(self, topics: list[str]) -> None:
        self.subscriptions.append(list(topics))

    def poll(self, timeout: float):
        if not self.messages:
            return None
        return self.messages.pop(0)

    def commit(self, offsets: list["FakeTopicPartition"] | None = None, asynchronous: bool = False) -> None:
        self.committed_offsets.extend(offsets or [])

    def get_watermark_offsets(self, partition, timeout: float, cached: bool) -> tuple[int, int]:
        return (0, 0)


class FakeTopicPartition:
    def __init__(self, topic: str, partition: int, offset: int = 0) -> None:
        self.topic = topic
        self.partition = partition
        self.offset = offset


class FakeConfluentMessage:
    def __init__(self, *, topic: str, key: bytes, value: bytes, offset: int) -> None:
        self._topic = topic
        self._key = key
        self._value = value
        self._offset = offset

    def topic(self) -> str:
        return self._topic

    def key(self) -> bytes:
        return self._key

    def value(self) -> bytes:
        return self._value

    def offset(self) -> int:
        return self._offset

    def error(self):
        return None


class FakeRedis:
    def set(self, key: str, value: str, ex: int) -> None:
        pass

    def get(self, key: str):
        return None


class FakeDuckDBConnection:
    pass


class FakeMarketDataClient:
    def __init__(self, full_ticks: dict[str, dict] | None = None, *, fail_on_subscribe: bool = False) -> None:
        self.started = False
        self.stopped = False
        self.subscribed_symbols: list[str] = []
        self.unsubscribed_symbols: list[str] = []
        self.full_ticks = full_ticks or {}
        self.fail_on_subscribe = fail_on_subscribe

    def start(self) -> None:
        self.started = True

    def stop(self) -> None:
        self.stopped = True

    def subscribe(self, symbol: str) -> None:
        if self.fail_on_subscribe:
            raise RuntimeError("subscribe failed")
        self.subscribed_symbols.append(symbol)

    def unsubscribe(self, symbol: str) -> None:
        self.unsubscribed_symbols.append(symbol)

    def get_full_ticks(self, symbols: list[str]) -> dict[str, dict]:
        return {symbol: self.full_ticks[symbol] for symbol in symbols if symbol in self.full_ticks}


class InstrumentKeyErrorReader:
    def read_table(self, data_type: str) -> list[dict]:
        rows = {
            "daily_bars": [
                {
                    "schema_version": 1,
                    "symbol": "00700.HK",
                    "trade_date": "20260521",
                    "open": 382,
                    "high": 384,
                    "low": 380,
                    "close": 382,
                    "volume": 2000,
                    "turnover": 764000,
                    "source": "fixture",
                    "ingest_ts": "2026-05-21T00:00:00Z",
                    "row_hash": "daily-previous",
                },
                {
                    "schema_version": 1,
                    "symbol": "00700.HK",
                    "trade_date": "20260522",
                    "open": 386,
                    "high": 389,
                    "low": 385,
                    "close": 386.2,
                    "volume": 1000,
                    "turnover": 386200,
                    "source": "fixture",
                    "ingest_ts": "2026-05-22T00:00:00Z",
                    "row_hash": "daily",
                },
            ],
            "minute_bars": [
                {
                    "schema_version": 1,
                    "symbol": "00700.HK",
                    "trade_date": "20260522",
                    "bar_ts": "2026-05-22T09:30:00+08:00",
                    "open": 388.0,
                    "high": 388.6,
                    "low": 387.8,
                    "close": 388.4,
                    "volume": 1000,
                    "turnover": 388400,
                    "source": "fixture",
                    "ingest_ts": "2026-05-22T09:31:00+08:00",
                    "row_hash": "minute-1",
                }
            ],
            "ccass_holdings": [
                {
                    "schema_version": 1,
                    "symbol": "00700.HK",
                    "trade_date": "20260521",
                    "participant_id": "C00010",
                    "participant_name": "JPMorgan",
                    "shares": 900,
                    "percent": 1.0,
                    "change": 0,
                    "source": "fixture",
                    "ingest_ts": "2026-05-21T00:00:00Z",
                    "row_hash": "holding-previous",
                },
                {
                    "schema_version": 1,
                    "symbol": "00700.HK",
                    "trade_date": "20260522",
                    "participant_id": "C00010",
                    "participant_name": "JPMorgan",
                    "shares": 1000,
                    "percent": 1.1,
                    "change": 10,
                    "source": "fixture",
                    "ingest_ts": "2026-05-22T00:00:00Z",
                    "row_hash": "holding-current",
                },
            ],
            "broker_queue": [],
            "broker_mapping": [
                {
                    "schema_version": 1,
                    "broker_code": "JPM",
                    "participant_id": "C00010",
                    "participant_name": "JPMorgan",
                    "source": "fixture",
                    "ingest_ts": "2026-05-22T00:00:00Z",
                    "row_hash": "mapping",
                }
            ],
        }
        return list(rows[data_type])


class FakeKafkaBroker:
    def __init__(self) -> None:
        self.records: dict[str, list[dict]] = {}
        self.committed_offsets: dict[str, int] = {}

    def produce(self, topic: str, key: bytes, value: bytes) -> None:
        records = self.records.setdefault(topic, [])
        records.append({"key": key, "value": value, "offset": len(records)})

    def flush(self) -> None:
        pass

    def poll(self, topic: str, offset: int, timeout_ms: int) -> list[dict]:
        return list(self.records.get(topic, [])[offset:])

    def commit(self, topic: str, offset: int) -> None:
        self.committed_offsets[topic] = offset

    def committed(self, topic: str) -> int:
        return self.committed_offsets.get(topic, 0)

    def high_watermark(self, topic: str) -> int:
        return len(self.records.get(topic, []))


class RecordingRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.ttls: dict[str, int] = {}

    def set(self, key: str, value: str, ex: int) -> None:
        self.values[key] = value
        self.ttls[key] = ex

    def get(self, key: str):
        return self.values.get(key)

    def delete(self, key: str) -> None:
        self.values.pop(key, None)
        self.ttls.pop(key, None)


class RecordingServeFactory:
    def __init__(self) -> None:
        self.host = ""
        self.port = 0

    def __call__(self, handler, host: str, port: int):
        self.host = host
        self.port = port
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False


def write_ccass(root: Path) -> None:
    rows = [
        {
            "schema_version": 1,
            "symbol": "00700.HK",
            "trade_date": "20260522",
            "participant_id": "C00010",
            "participant_name": "JPMorgan",
            "shares": 1000,
            "percent": 1.1,
            "change": 10,
            "source": "fixture",
            "ingest_ts": "2026-05-22T00:00:00Z",
            "row_hash": "holding",
        }
    ]
    path = root / "silver_ccass_holdings_v1.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_minimal_silver(root: Path) -> None:
    write_table(
        root / "silver_daily_bars_v1.csv",
        [
            {
                "schema_version": 1,
                "symbol": "00700.HK",
                "trade_date": "20260521",
                "open": 382,
                "high": 384,
                "low": 380,
                "close": 382,
                "volume": 2000,
                "turnover": 764000,
                "source": "fixture",
                "ingest_ts": "2026-05-21T00:00:00Z",
                "row_hash": "daily-previous",
            },
            {
                "schema_version": 1,
                "symbol": "00700.HK",
                "trade_date": "20260522",
                "open": 386,
                "high": 389,
                "low": 385,
                "close": 386.2,
                "volume": 1000,
                "turnover": 386200,
                "source": "fixture",
                "ingest_ts": "2026-05-22T00:00:00Z",
                "row_hash": "daily",
            }
        ],
    )
    write_table(
        root / "silver_minute_bars_v1.csv",
        [
            {
                "schema_version": 1,
                "symbol": "00700.HK",
                "trade_date": "20260522",
                "bar_ts": "2026-05-22T09:30:00+08:00",
                "open": 388.0,
                "high": 388.6,
                "low": 387.8,
                "close": 388.4,
                "volume": 1000,
                "turnover": 388400,
                "source": "fixture",
                "ingest_ts": "2026-05-22T09:31:00+08:00",
                "row_hash": "minute-1",
            },
            {
                "schema_version": 1,
                "symbol": "00700.HK",
                "trade_date": "20260522",
                "bar_ts": "2026-05-22T09:31:00+08:00",
                "open": 388.4,
                "high": 389.0,
                "low": 388.2,
                "close": 388.8,
                "volume": 2000,
                "turnover": 777600,
                "source": "fixture",
                "ingest_ts": "2026-05-22T09:32:00+08:00",
                "row_hash": "minute-2",
            },
        ],
    )
    write_table(
        root / "silver_ccass_holdings_v1.csv",
        [
            {
                "schema_version": 1,
                "symbol": "00700.HK",
                "trade_date": "20260521",
                "participant_id": "C00010",
                "participant_name": "JPMorgan",
                "shares": 900,
                "percent": 1.0,
                "change": 0,
                "source": "fixture",
                "ingest_ts": "2026-05-21T00:00:00Z",
                "row_hash": "holding-previous",
            },
            {
                "schema_version": 1,
                "symbol": "00700.HK",
                "trade_date": "20260522",
                "participant_id": "C00010",
                "participant_name": "JPMorgan",
                "shares": 1000,
                "percent": 1.1,
                "change": 10,
                "source": "fixture",
                "ingest_ts": "2026-05-22T00:00:00Z",
                "row_hash": "holding-current",
            },
        ],
    )
    write_table(
        root / "silver_broker_queue_v1.csv",
        [
            {
                "schema_version": 1,
                "symbol": "00700.HK",
                "trade_date": "20260522",
                "queue_ts": "2026-05-22T09:30:00+08:00",
                "side": "ask",
                "position": 1,
                "broker_code": "JPM",
                "source": "fixture",
                "ingest_ts": "2026-05-22T09:30:01+08:00",
                "row_hash": "queue",
            }
        ],
    )
    write_table(
        root / "silver_broker_mapping_v1.csv",
        [
            {
                "schema_version": 1,
                "broker_code": "JPM",
                "broker_name": "JPMorgan",
                "participant_id": "C00010",
                "participant_name": "JPMorgan",
                "effective_from": "20260101",
                "effective_to": "",
                "source": "fixture",
                "ingest_ts": "2026-05-22T00:00:00Z",
                "row_hash": "mapping",
            }
        ],
    )


def append_second_symbol_silver(root: Path) -> None:
    append_rows(
        root / "silver_daily_bars_v1.csv",
        [
            {
                "schema_version": 1,
                "symbol": "00939.HK",
                "trade_date": "20260521",
                "open": 6.7,
                "high": 6.8,
                "low": 6.6,
                "close": 6.7,
                "volume": 5000,
                "turnover": 33500,
                "source": "fixture",
                "ingest_ts": "2026-05-21T00:00:00Z",
                "row_hash": "daily-00939-previous",
            },
            {
                "schema_version": 1,
                "symbol": "00939.HK",
                "trade_date": "20260522",
                "open": 6.8,
                "high": 7.0,
                "low": 6.7,
                "close": 6.9,
                "volume": 6000,
                "turnover": 41400,
                "source": "fixture",
                "ingest_ts": "2026-05-22T00:00:00Z",
                "row_hash": "daily-00939",
            },
        ],
    )
    append_rows(
        root / "silver_minute_bars_v1.csv",
        [
            {
                "schema_version": 1,
                "symbol": "00939.HK",
                "trade_date": "20260522",
                "bar_ts": "2026-05-22T09:30:00+08:00",
                "open": 6.9,
                "high": 6.95,
                "low": 6.88,
                "close": 6.94,
                "volume": 3000,
                "turnover": 20820,
                "source": "fixture",
                "ingest_ts": "2026-05-22T09:31:00+08:00",
                "row_hash": "minute-00939-1",
            }
        ],
    )
    append_rows(
        root / "silver_ccass_holdings_v1.csv",
        [
            {
                "schema_version": 1,
                "symbol": "00939.HK",
                "trade_date": "20260521",
                "participant_id": "C00010",
                "participant_name": "JPMorgan",
                "shares": 1900,
                "percent": 1.0,
                "change": 0,
                "source": "fixture",
                "ingest_ts": "2026-05-21T00:00:00Z",
                "row_hash": "holding-00939-previous",
            },
            {
                "schema_version": 1,
                "symbol": "00939.HK",
                "trade_date": "20260522",
                "participant_id": "C00010",
                "participant_name": "JPMorgan",
                "shares": 2000,
                "percent": 1.1,
                "change": 100,
                "source": "fixture",
                "ingest_ts": "2026-05-22T00:00:00Z",
                "row_hash": "holding-00939-current",
            },
        ],
    )


def write_table(path: Path, rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def append_rows(path: Path, rows: list[dict]) -> None:
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writerows(rows)


if __name__ == "__main__":
    unittest.main()
