import copy
import csv
import tempfile
import unittest
from pathlib import Path

from beast_market import (
    BoundedRawEventQueue,
    FailingEventBus,
    FreshnessPolicy,
    GatewayClientQueue,
    InMemoryEventBus,
    InMemoryRedisSnapshotCache,
    LocalSpool,
    MammothAPI,
    OctopusComputeV2,
    RAW_TOPIC,
    RawEventConsumerWorker,
    RealtimeCollectorV2,
    RealtimeIngestWorker,
    ReliableEventBus,
    SymbolRuntimeManager,
    HistoricalHydrationService,
    HydrationKey,
    make_processed_market_event,
    XtQuantSubscriptionManager,
    make_raw_market_event,
    normalize_xtquant_callback,
    normalize_xtquant_tick,
)


class RuntimeGatewayTest(unittest.TestCase):
    def test_realtime_ingest_worker_normalizes_callbacks_and_dead_letters_bad_payloads(self) -> None:
        bus = InMemoryEventBus()
        rejected_callbacks: list[tuple[dict, str]] = []
        worker = RealtimeIngestWorker(
            BoundedRawEventQueue(max_size=3),
            RealtimeCollectorV2(bus),
            normalizer=normalize_xtquant_tick,
            reject_sink=lambda payload, reason: rejected_callbacks.append((payload, reason)),
        )

        self.assertTrue(worker.receive_callback({"code": "700", "price": 388.4, "volume": 1000}))
        self.assertTrue(worker.receive_callback({"code": "700", "price": 388.5}))
        events = worker.drain_all()

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["symbol"], "00700.HK")
        self.assertEqual(len(bus.read(RAW_TOPIC)), 1)
        self.assertEqual(worker.stats.failed, 1)
        self.assertEqual(worker.stats.dead_letters[0].reason, "missing price or volume")
        self.assertEqual(rejected_callbacks[0][1], "normalization_failed: missing price or volume")

    def test_realtime_ingest_worker_quarantines_callbacks_rejected_by_backpressure(self) -> None:
        rejected_callbacks: list[tuple[dict, str]] = []
        worker = RealtimeIngestWorker(
            BoundedRawEventQueue(max_size=1),
            RealtimeCollectorV2(InMemoryEventBus()),
            normalizer=normalize_xtquant_tick,
            reject_sink=lambda payload, reason: rejected_callbacks.append((payload, reason)),
        )

        self.assertTrue(worker.receive_callback({"code": "700", "price": 388.4, "volume": 1000}))
        self.assertFalse(worker.receive_callback({"code": "700", "price": 388.5, "volume": 1000}))

        self.assertEqual(worker.stats.rejected, 1)
        self.assertEqual(rejected_callbacks, [({"code": "700", "price": 388.5, "volume": 1000}, "raw_callback_queue_full")])

    def test_realtime_ingest_worker_marks_kafka_publish_failure_without_misclassifying_payload(self) -> None:
        spool = LocalSpool()
        bus = ReliableEventBus(FailingEventBus(fail_for_attempts=10), retries=0, spool=spool)
        collector = RealtimeCollectorV2(bus)
        rejected_callbacks: list[tuple[dict, str]] = []
        worker = RealtimeIngestWorker(
            BoundedRawEventQueue(max_size=3),
            collector,
            normalizer=normalize_xtquant_tick,
            reject_sink=lambda payload, reason: rejected_callbacks.append((payload, reason)),
        )

        worker.receive_callback({"code": "700", "price": 388.4, "volume": 1000})
        events = worker.drain_all()

        self.assertEqual(events, [])
        self.assertEqual(worker.stats.failed, 1)
        self.assertEqual(len(spool.records), 1)
        self.assertEqual(spool.records[0].key, "00700.HK")
        self.assertEqual(collector.health.process, "degraded")
        self.assertEqual(collector.health.kafka, "degraded")
        self.assertIn("failed to publish raw_market_events_v1/00700.HK", worker.stats.dead_letters[0].reason)
        self.assertTrue(rejected_callbacks[0][1].startswith("raw_publish_failed:"))
        self.assertNotIn("normalization_failed", rejected_callbacks[0][1])

    def test_realtime_ingest_worker_accepts_legacy_xtquant_periods(self) -> None:
        bus = InMemoryEventBus()
        worker = RealtimeIngestWorker(
            BoundedRawEventQueue(max_size=10),
            RealtimeCollectorV2(bus),
            normalizer=normalize_xtquant_callback,
        )

        worker.receive_callback(
            {
                "symbol": "00700.HK",
                "period": "hktransaction",
                "data": {
                    "Time": 1779413400000,
                    "Price": 388.4,
                    "Volume": 1000,
                    "Turnover": 388400,
                    "Side": "B",
                    "BrokerID": "JPM",
                },
            }
        )
        worker.receive_callback(
            {
                "symbol": "00700.HK",
                "period": "hkbrokerqueueex",
                "data": {
                    "Time": 1779413401000,
                    "BidQueues": [{"Price": 388.2, "Brokers": ["UBS"], "Volumes": [2000]}],
                },
            }
        )
        worker.receive_callback(
            {
                "symbol": "00700.HK",
                "period": "l2thousand",
                "data": {
                    "00700.HK": [
                        {
                            "Time": 1779413402000,
                            "AskPrice": [388.6],
                            "AskVolume": [3000],
                            "BidPrice": [388.2],
                            "BidVolume": [4000],
                        }
                    ]
                },
            }
        )

        events = worker.drain_all()

        self.assertEqual([event["kind"] for event in events], ["tick", "broker_queue", "l2_order_book"])
        self.assertEqual([record["value"]["kind"] for record in bus.read(RAW_TOPIC)], ["tick", "broker_queue", "l2_order_book"])
        self.assertEqual(events[1]["payload"]["entries"][0]["broker_code"], "UBS")
        self.assertEqual(events[2]["payload"]["ask"][0]["price"], 388.6)

    def test_collector_freshness_tracks_backlog_stale_symbols_and_resubscribe_requests(self) -> None:
        bus = InMemoryEventBus()
        collector = RealtimeCollectorV2(
            bus,
            freshness_policy=FreshnessPolicy(max_event_age_seconds=30, max_queue_backlog=1),
        )
        worker = RealtimeIngestWorker(
            BoundedRawEventQueue(max_size=3),
            collector,
            normalizer=normalize_xtquant_tick,
        )

        collector.subscribe_symbol("00700.HK")
        initial = collector.evaluate_freshness(now="2026-05-22T09:30:00+08:00")
        self.assertEqual(initial["resubscribe_symbols"], ["00700.HK"])
        self.assertEqual(
            collector.health.symbol_freshness["00700.HK"]["degraded_reason"],
            "no_events_after_subscribe",
        )

        worker.receive_callback(
            {"code": "700", "price": 388.4, "volume": 1000, "timestamp": "2026-05-22T09:30:00+08:00"}
        )
        worker.receive_callback(
            {"code": "700", "price": 388.5, "volume": 1000, "timestamp": "2026-05-22T09:30:01+08:00"}
        )
        backlog = collector.evaluate_freshness(now="2026-05-22T09:30:02+08:00")
        self.assertEqual(backlog["resubscribe_symbols"], ["00700.HK"])
        self.assertEqual(
            collector.health.symbol_freshness["00700.HK"]["degraded_reason"],
            "queue_backlog_exceeded",
        )

        worker.drain_all()
        fresh = collector.evaluate_freshness(now="2026-05-22T09:30:10+08:00")
        self.assertEqual(fresh["resubscribe_symbols"], [])
        self.assertFalse(collector.health.symbol_freshness["00700.HK"]["degraded"])

        stale = collector.evaluate_freshness(now="2026-05-22T09:31:00+08:00")
        self.assertEqual(stale["resubscribe_symbols"], ["00700.HK"])
        self.assertEqual(
            collector.health.symbol_freshness["00700.HK"]["degraded_reason"],
            "stale_event_stream",
        )

    def test_xtquant_subscription_manager_controls_lifecycle_and_resubscribes_stale_symbols(self) -> None:
        client = FakeMarketDataClient()
        collector = RealtimeCollectorV2(
            InMemoryEventBus(),
            freshness_policy=FreshnessPolicy(max_event_age_seconds=30),
        )
        manager = XtQuantSubscriptionManager(client, collector)

        manager.start()
        manager.subscribe("700")
        collector.ingest_tick(
            "00700.HK",
            {"timestamp": "2026-05-22T09:30:00+08:00", "price": 388.4, "volume": 1000},
        )
        result = manager.check_freshness_and_resubscribe(now="2026-05-22T09:31:00+08:00")
        manager.unsubscribe("00700.HK")
        manager.stop()

        self.assertEqual(result["resubscribe_symbols"], ["00700.HK"])
        self.assertEqual(
            client.calls,
            [
                ("start", ""),
                ("subscribe", "00700.HK"),
                ("unsubscribe", "00700.HK"),
                ("subscribe", "00700.HK"),
                ("unsubscribe", "00700.HK"),
                ("stop", ""),
            ],
        )
        self.assertEqual(manager.stats.resubscribes, 1)
        self.assertFalse(manager.running)

    def test_xtquant_subscription_manager_marks_collector_degraded_on_client_failure(self) -> None:
        client = FakeMarketDataClient(fail_on="subscribe")
        collector = RealtimeCollectorV2(InMemoryEventBus())
        manager = XtQuantSubscriptionManager(client, collector)

        with self.assertRaises(RuntimeError):
            manager.subscribe("700")

        self.assertEqual(collector.health.process, "degraded")
        self.assertEqual(manager.stats.failed, 1)
        self.assertEqual(manager.stats.errors, ["subscribe failed"])

    def test_raw_event_consumer_commits_only_after_successful_processing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_minimal_silver(root)
            mammoth = MammothAPI(root)
            bus = InMemoryEventBus()
            cache = InMemoryRedisSnapshotCache()
            octopus = OctopusComputeV2(mammoth, bus, cache, big_trade_volume_baseline_ratio=2.0)
            octopus.preload_bod("00700.HK", "20260522")
            good = make_raw_market_event(
                kind="tick",
                symbol="00700.HK",
                source="xtquant",
                seq=1,
                payload={"price": 388.4, "volume": 1000, "turnover": 388400},
            )
            bad = make_raw_market_event(
                kind="tick",
                symbol="00700.HK",
                source="xtquant",
                seq=2,
                payload={"price": 388.5, "volume": 1000, "turnover": 388500},
            )
            del bad["payload"]["turnover"]
            bus.publish(RAW_TOPIC, "00700.HK", good)
            bus.publish(RAW_TOPIC, "00700.HK", bad)

            worker = RawEventConsumerWorker(bus, octopus)
            processed = worker.poll_and_process("20260522")

            self.assertEqual(len(processed), 1)
            self.assertEqual(bus.committed_offset(RAW_TOPIC), 2)
            self.assertEqual(worker.stats.failed, 1)
            self.assertEqual(bus.lag(RAW_TOPIC, bus.committed_offset(RAW_TOPIC)), 0)

    def test_raw_event_consumer_dead_letters_kafka_key_symbol_mismatch_before_processing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_minimal_silver(root)
            mammoth = MammothAPI(root)
            bus = InMemoryEventBus()
            cache = InMemoryRedisSnapshotCache()
            octopus = OctopusComputeV2(mammoth, bus, cache)
            event = make_raw_market_event(
                kind="tick",
                symbol="00700.HK",
                source="xtquant",
                seq=1,
                payload={"price": 388.4, "volume": 1000, "turnover": 388400},
            )
            bus.records[RAW_TOPIC].append({"key": "00939.HK", "value": event})

            worker = RawEventConsumerWorker(bus, octopus)
            processed = worker.poll_and_process("20260522")

            self.assertEqual(processed, [])
            self.assertEqual(bus.committed_offset(RAW_TOPIC), 1)
            self.assertEqual(worker.stats.failed, 1)
            self.assertEqual(worker.stats.dead_letters[0].key, "00939.HK")
            self.assertEqual(worker.stats.dead_letters[0].reason, "Kafka event key must match event symbol")

    def test_raw_event_consumer_writes_bad_records_to_dead_letter_sink(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_minimal_silver(root)
            mammoth = MammothAPI(root)
            bus = InMemoryEventBus()
            octopus = OctopusComputeV2(mammoth, bus, InMemoryRedisSnapshotCache())
            event = make_raw_market_event(
                kind="tick",
                symbol="00700.HK",
                source="xtquant",
                seq=1,
                payload={"price": 388.4, "volume": 1000, "turnover": 388400},
            )
            bus.records[RAW_TOPIC].append({"key": "00939.HK", "value": event})
            dead_letters = []
            worker = RawEventConsumerWorker(bus, octopus, dead_letter_sink=dead_letters.append)

            worker.poll_and_process("20260522")

            self.assertEqual(len(dead_letters), 1)
            self.assertEqual(dead_letters[0].topic, RAW_TOPIC)
            self.assertEqual(dead_letters[0].key, "00939.HK")
            self.assertEqual(dead_letters[0].reason, "Kafka event key must match event symbol")

    def test_raw_event_consumer_uses_state_provider_before_octopus_internal_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_minimal_silver(root)
            mammoth = MammothAPI(root)
            bus = InMemoryEventBus()
            cache = InMemoryRedisSnapshotCache()
            octopus = OctopusComputeV2(mammoth, bus, cache, big_trade_volume_baseline_ratio=2.0)
            octopus.preload_bod("00700.HK", "20260522")
            external_state = copy.deepcopy(octopus.get_state("00700.HK"))
            octopus.state_by_symbol.clear()
            event = make_raw_market_event(
                kind="tick",
                symbol="00700.HK",
                source="xtquant",
                seq=1,
                payload={"price": 388.4, "volume": 1000, "turnover": 388400},
            )
            bus.publish(RAW_TOPIC, "00700.HK", event)

            worker = RawEventConsumerWorker(
                bus,
                octopus,
                state_provider=lambda symbol: external_state if symbol == "00700.HK" else None,
            )
            processed = worker.poll_and_process("20260522")

            self.assertTrue(processed)
            self.assertEqual(bus.committed_offset(RAW_TOPIC), 1)
            self.assertFalse(octopus.has_state("00700.HK"))
            self.assertEqual(external_state["snapshot"]["price"], 388.4)
            self.assertEqual(cache.get_terminal_snapshot("20260522", "00700.HK")["snapshot"]["price"], 388.4)

    def test_gateway_client_queue_coalesces_tick_and_queue_but_keeps_alerts(self) -> None:
        queue = GatewayClientQueue("client-1", max_size=2)
        queue.enqueue(message("tick_realtime", "00700.HK", price=388.1))
        queue.enqueue(message("tick_realtime", "00700.HK", price=388.4))
        queue.enqueue(message("queue_realtime", "00700.HK"))
        queue.enqueue(message("queue_realtime", "00700.HK"))
        queue.enqueue(message("tick_realtime", "00939.HK", price=7.4))
        queue.enqueue(message("alert_realtime", "00700.HK"))

        drained = queue.drain()

        self.assertEqual(queue.stats.coalesced, 2)
        self.assertEqual(queue.stats.dropped, 2)
        self.assertEqual(queue.stats.alerts_enqueued, 1)
        self.assertEqual(queue.stats.alert_dropped, 0)
        self.assertEqual(queue.stats.alert_overflow, 1)
        self.assertEqual([item["type"] for item in drained], ["queue_realtime", "alert_realtime"])
        self.assertEqual(len(drained), 2)

    def test_gateway_client_queue_preserves_critical_responses_when_full(self) -> None:
        queue = GatewayClientQueue("client-1", max_size=2)
        queue.enqueue(message("tick_realtime", "00700.HK", price=388.1))
        queue.enqueue(message("queue_realtime", "00700.HK"))
        queue.enqueue(message("snapshot", "00700.HK", price=388.4))

        drained = queue.drain()

        self.assertEqual(queue.stats.critical_overflow, 1)
        self.assertEqual(queue.stats.dropped, 1)
        self.assertEqual([item["type"] for item in drained], ["queue_realtime", "snapshot"])

    def test_symbol_runtime_applies_realtime_ticks_to_current_minute_bar(self) -> None:
        payload = snapshot_payload()
        manager = SymbolRuntimeManager(
            FakeSymbolGateway(),
            trade_date="20260522",
            hydrate_symbol=lambda symbol: payload,
        )
        manager.attach("00700.HK", "client-1")

        manager.apply_terminal_message(
            terminal_tick_message(
                "00700.HK",
                timestamp="2026-05-22T09:30:05+08:00",
                price=388.8,
                volume=100,
                turnover=38880,
            )
        )
        manager.apply_terminal_message(
            terminal_tick_message(
                "00700.HK",
                timestamp="2026-05-22T09:30:45+08:00",
                price=388.2,
                volume=200,
                turnover=77640,
            )
        )
        manager.apply_terminal_message(
            terminal_tick_message(
                "00700.HK",
                timestamp="2026-05-22T09:31:01+08:00",
                price=388.5,
                volume=300,
                turnover=116550,
            )
        )

        minute_bars = manager.runtimes["00700.HK"].snapshot_payload["minute_bars"]

        self.assertEqual(len(minute_bars), 2)
        self.assertEqual(minute_bars[0]["timestamp"], "2026-05-22T09:30:00+08:00")
        self.assertEqual(minute_bars[0]["price"], 388.2)
        self.assertEqual(minute_bars[0]["high"], 388.8)
        self.assertEqual(minute_bars[0]["low"], 388.0)
        self.assertEqual(minute_bars[0]["volume"], 1300)
        self.assertEqual(minute_bars[1]["timestamp"], "2026-05-22T09:31:00+08:00")
        self.assertEqual(minute_bars[1]["volume"], 300)

    def test_symbol_runtime_applies_processed_snapshot_without_gateway_terminal_conversion(self) -> None:
        payload = snapshot_payload()
        manager = SymbolRuntimeManager(
            FakeSymbolGateway(),
            trade_date="20260522",
            hydrate_symbol=lambda symbol: payload,
        )
        manager.attach("00700.HK", "client-1")
        processed_payload = {
            **snapshot_payload(),
            "snapshot": {"symbol": "00700.HK", "price": 389.4},
            "last_tick": {
                "timestamp": "2026-05-22T09:31:00+08:00",
                "price": 389.4,
                "volume": 1000,
                "turnover": 389400,
                "direction": "up",
            },
            "freshness": {
                "updated_at": "2026-05-22T09:31:00+08:00",
                "runtime_state": "LIVE",
                "degraded_reasons": [],
            },
        }
        event = make_processed_market_event(
            result_type="snapshot",
            symbol="00700.HK",
            source="octopus",
            seq=1,
            source_ts="2026-05-22T09:31:00+08:00",
            payload=processed_payload,
        )

        applied = manager.apply_processed_events([event])

        self.assertEqual(applied, 1)
        self.assertEqual(manager.runtimes["00700.HK"].snapshot_payload["snapshot"]["price"], 389.4)
        self.assertEqual(manager.runtimes["00700.HK"].snapshot_payload["last_tick"]["price"], 389.4)

    def test_symbol_runtime_applies_processed_alert_and_queue_delta(self) -> None:
        payload = snapshot_payload()
        manager = SymbolRuntimeManager(
            FakeSymbolGateway(),
            trade_date="20260522",
            hydrate_symbol=lambda symbol: payload,
        )
        manager.attach("00700.HK", "client-1")
        freshness = {"updated_at": "2026-05-22T09:31:00+08:00", "runtime_state": "LIVE"}
        alert_event = make_processed_market_event(
            result_type="big_trade_alert",
            symbol="00700.HK",
            source="octopus",
            seq=1,
            source_ts="2026-05-22T09:31:00+08:00",
            payload={"alert": {"id": "big-1", "symbol": "00700.HK"}, "freshness": freshness},
        )
        queue_event = make_processed_market_event(
            result_type="broker_queue",
            symbol="00700.HK",
            source="octopus",
            seq=2,
            source_ts="2026-05-22T09:31:00+08:00",
            payload={"broker_queue": {"ask": [{"broker": "001"}], "bid": []}, "freshness": freshness},
        )

        applied = manager.apply_processed_events([alert_event, queue_event])

        runtime_payload = manager.runtimes["00700.HK"].snapshot_payload
        self.assertEqual(applied, 2)
        self.assertEqual(runtime_payload["alerts"][0]["id"], "big-1")
        self.assertEqual(runtime_payload["broker_queue"]["ask"], [{"broker": "001"}])
        self.assertEqual(runtime_payload["freshness"]["runtime_state"], "LIVE")

    def test_symbol_runtime_ignores_malformed_realtime_queue_without_crashing(self) -> None:
        payload = snapshot_payload()
        manager = SymbolRuntimeManager(
            FakeSymbolGateway(),
            trade_date="20260522",
            hydrate_symbol=lambda symbol: payload,
        )
        manager.attach("00700.HK", "client-1")
        existing_queue = manager.runtimes["00700.HK"].snapshot_payload["broker_queue"]

        applied = manager.apply_terminal_message(
            {
                "schema_version": 1,
                "type": "queue_realtime",
                "event_id": "queue-bad-1",
                "symbol": "00700.HK",
                "source": "gateway",
                "source_ts": "2026-05-22T09:31:00+08:00",
                "payload": {
                    "broker_queue": None,
                    "freshness": {
                        "updated_at": "2026-05-22T09:31:00+08:00",
                        "runtime_state": "LIVE",
                    },
                },
            }
        )

        runtime_payload = manager.runtimes["00700.HK"].snapshot_payload
        self.assertTrue(applied)
        self.assertEqual(runtime_payload["broker_queue"], existing_queue)
        self.assertEqual(runtime_payload["freshness"]["runtime_state"], "LIVE")

    def test_symbol_runtime_generates_terminal_delta_from_raw_event_without_gateway_conversion(self) -> None:
        payload = snapshot_payload()

        def process_raw(raw_event: dict, trade_date: str, state: dict) -> list[dict]:
            state["snapshot"] = {"symbol": raw_event["symbol"], "price": raw_event["payload"]["price"]}
            state["last_tick"] = {
                "timestamp": raw_event["source_ts"],
                "price": raw_event["payload"]["price"],
                "volume": raw_event["payload"]["volume"],
                "turnover": raw_event["payload"]["turnover"],
            }
            state["freshness"] = {"updated_at": raw_event["source_ts"], "runtime_state": "LIVE"}
            return [
                make_processed_market_event(
                    result_type="snapshot",
                    symbol=raw_event["symbol"],
                    source="symbol-runtime-test",
                    seq=1,
                    source_ts=raw_event["source_ts"],
                    payload=state,
                )
            ]

        manager = SymbolRuntimeManager(
            FakeSymbolGateway(),
            trade_date="20260522",
            hydrate_symbol=lambda symbol: payload,
            raw_event_processor=process_raw,
        )
        manager.attach("00700.HK", "client-1")
        raw_event = make_raw_market_event(
            kind="tick",
            symbol="00700.HK",
            source="xtquant",
            seq=1,
            source_ts="2026-05-22T09:31:00+08:00",
            payload={"price": 389.0, "volume": 100, "turnover": 38900},
        )

        processed, terminal_messages = manager.apply_raw_event(raw_event, "20260522")

        self.assertEqual(len(processed), 1)
        self.assertEqual(terminal_messages[0]["type"], "tick_realtime")
        self.assertEqual(terminal_messages[0]["source"], "symbol-runtime")
        self.assertEqual(terminal_messages[0]["payload"]["tick"]["price"], 389.0)
        self.assertEqual(manager.snapshot()["00700.HK"]["delta_emitted"], 1)
        self.assertEqual(manager.manager_snapshot()["runtime_delta_emitted"], 1)

    def test_historical_hydration_singleflight_key_includes_data_type_and_effective_date(self) -> None:
        service = HistoricalHydrationService()
        first_key = HydrationKey("00700.HK", "minute_bars", "20260522")
        second_key = HydrationKey("00700.HK", "ccass_holdings", "20260522")
        calls: list[str] = []

        self.assertEqual(service.hydrate(first_key, lambda: calls.append("minute") or {"ok": "minute"}), {"ok": "minute"})
        self.assertEqual(service.hydrate(second_key, lambda: calls.append("ccass") or {"ok": "ccass"}), {"ok": "ccass"})

        self.assertEqual(calls, ["minute", "ccass"])


def message(message_type: str, symbol: str, *, price: float = 0) -> dict:
    payload = {"tick": {"price": price}} if message_type == "tick_realtime" else {"broker_queue": {}}
    if message_type == "alert_realtime":
        payload = {"alert": {"id": "alert-1"}}
    if message_type == "snapshot":
        payload = {
            "snapshot": {"symbol": symbol, "price": price},
            "minute_bars": [],
            "alerts": [],
            "broker_queue": {"ask": [], "bid": []},
            "ccass_holdings": [],
            "freshness": {"updated_at": "2026-05-22T09:30:00.000+08:00"},
        }
    return {
        "schema_version": 1,
        "type": message_type,
        "event_id": f"{message_type}-{symbol}-{price}",
        "symbol": symbol,
        "source": "gateway",
        "source_ts": "2026-05-22T09:30:00.000+08:00",
        "ingest_ts": "2026-05-22T09:30:00.010+08:00",
        "seq": 1,
        "payload": payload,
    }


def snapshot_payload() -> dict:
    return {
        "snapshot": {"symbol": "00700.HK", "price": 388.4},
        "minute_bars": [
            {
                "timestamp": "2026-05-22T09:30:00+08:00",
                "price": 388.4,
                "open": 388.0,
                "high": 388.6,
                "low": 388.0,
                "close": 388.4,
                "volume": 1000,
                "turnover": 388400,
                "direction": "up",
            }
        ],
        "alerts": [],
        "broker_queue": {"ask": [], "bid": []},
        "ccass_holdings": [],
        "freshness": {"updated_at": "2026-05-22T09:30:00+08:00"},
    }


def terminal_tick_message(
    symbol: str,
    *,
    timestamp: str,
    price: float,
    volume: int,
    turnover: float,
) -> dict:
    return {
        "type": "tick_realtime",
        "symbol": symbol,
        "payload": {
            "tick": {
                "timestamp": timestamp,
                "price": price,
                "volume": volume,
                "turnover": turnover,
                "direction": "flat",
            },
            "snapshot": {"symbol": symbol, "price": price},
            "freshness": {
                "updated_at": timestamp,
                "runtime_state": "LIVE",
                "degraded_reasons": [],
            },
        },
    }


class FakeSymbolGateway:
    def subscribe(self, symbol: str, trade_date: str) -> dict:
        return {"type": "snapshot", "symbol": symbol, "payload": snapshot_payload()}

    def snapshot_message(self, symbol: str, payload: dict) -> dict:
        return {"type": "snapshot", "symbol": symbol, "payload": payload}


def write_minimal_silver(root: Path) -> None:
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
    write_table(
        root / "silver_ccass_holdings_v1.csv",
        [
            {
                "schema_version": 1,
                "symbol": "00700.HK",
                "trade_date": "20260522",
                "participant_id": "C00010",
                "participant_name": "JPMorgan",
                "shares": 1000,
                "percent": 1.1,
                "source": "fixture",
                "ingest_ts": "2026-05-22T00:00:00Z",
                "row_hash": "holding",
            }
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


def write_table(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


class FakeMarketDataClient:
    def __init__(self, fail_on: str | None = None) -> None:
        self.fail_on = fail_on
        self.calls: list[tuple[str, str]] = []

    def start(self) -> None:
        self._call("start", "")

    def stop(self) -> None:
        self._call("stop", "")

    def subscribe(self, symbol: str) -> None:
        self._call("subscribe", symbol)

    def unsubscribe(self, symbol: str) -> None:
        self._call("unsubscribe", symbol)

    def _call(self, action: str, symbol: str) -> None:
        if self.fail_on == action:
            raise RuntimeError(f"{action} failed")
        self.calls.append((action, symbol))


if __name__ == "__main__":
    unittest.main()
