import copy
import csv
import tempfile
import unittest
from pathlib import Path

from beast_market import (
    PROCESSED_TOPIC,
    RAW_TOPIC,
    GatewayV2,
    InMemoryEventBus,
    InMemoryRedisSnapshotCache,
    MammothAPI,
    OctopusComputeV2,
    RealtimeCollectorV2,
    make_raw_market_event,
    validate_terminal_message,
)
from beast_market.pipeline import to_broker_queue


class BackendV2PipelineTest(unittest.TestCase):
    def test_mammoth_api_manifest_and_vertical_v2_flow(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_silver_tables(root)

            mammoth = MammothAPI(root)
            manifest = mammoth.build_manifest(
                data_type="daily_bars",
                start_date="20260522",
                end_date="20260522",
                symbols=["00700.HK"],
                code_version="test",
            )

            self.assertTrue(manifest["quality_checks"]["passed"])
            self.assertEqual(manifest["row_count"], 1)
            self.assertEqual(manifest["symbol_count"], 1)
            self.assertEqual(manifest["symbols"], ["00700.HK"])

            bus = InMemoryEventBus()
            cache = InMemoryRedisSnapshotCache()
            octopus = OctopusComputeV2(mammoth, bus, cache, big_trade_volume_baseline_ratio=2.0)
            snapshot = octopus.preload_bod("00700.HK", "20260522")

            self.assertEqual(snapshot["snapshot"]["previousClose"], 382.0)
            self.assertTrue(octopus.has_state("00700.HK"))
            self.assertIs(octopus.get_state("00700.HK"), snapshot)
            self.assertEqual([bar["timestamp"] for bar in snapshot["minute_bars"]], [
                "2026-05-22T09:30:00+08:00",
                "2026-05-22T09:31:00+08:00",
            ])
            self.assertEqual(snapshot["snapshot"]["price"], 388.8)
            self.assertEqual(snapshot["snapshot"]["volume"], 3000)
            self.assertEqual(snapshot["freshness"]["runtime_state"], "WARM")
            self.assertEqual(snapshot["ccass_evidence"]["current_date"], "20260522")
            self.assertEqual(snapshot["ccass_evidence"]["previous_date"], "20260521")
            self.assertEqual(len(snapshot["ccass_holdings"]), 2)
            queue_record = cache.values["terminal:20260522:queue:00700.HK"]
            self.assertEqual(queue_record["data"]["ask"][0]["brokerCode"], "JPM")
            self.assertEqual(queue_record["schema_version"], 1)
            self.assertEqual(queue_record["requested_trade_date"], "20260522")
            self.assertEqual(queue_record["effective_trade_date"], "20260522")
            self.assertEqual(queue_record["source_dates"]["minute_bars"], "20260522")
            self.assertEqual(queue_record["freshness"]["runtime_state"], "WARM")
            self.assertEqual(cache.values["terminal:20260522:state:00700.HK"]["data"]["effective_trade_date"], "20260522")
            self.assertIn("updated_at", queue_record)
            self.assertIn("ccass:history:00700.HK:C00010", cache.values)
            self.assertEqual(octopus.bod_by_symbol["00700.HK"].volume_baseline, 2_000_000)
            self.assertIn("JPM", octopus.bod_by_symbol["00700.HK"].broker_mapping_by_code)
            self.assertIn("C00010", octopus.bod_by_symbol["00700.HK"].participant_history_by_id)

            raw_event = make_raw_tick(bus)
            processed_events = octopus.process_raw_event(raw_event, "20260522")

            self.assertEqual(len(bus.read(RAW_TOPIC)), 1)
            self.assertEqual(len(bus.read(PROCESSED_TOPIC)), 1)
            self.assertEqual(processed_events[0]["result_type"], "snapshot")
            self.assertEqual(cache.get_terminal_snapshot("20260522", "00700.HK")["snapshot"]["price"], 388.4)

            gateway = GatewayV2(bus, cache)
            snapshot_message = gateway.subscribe("00700.HK", "20260522")
            validate_terminal_message(snapshot_message)
            self.assertEqual(snapshot_message["type"], "snapshot")
            self.assertIn("freshness", snapshot_message["payload"])

            realtime_messages = gateway.to_terminal_messages()
            self.assertEqual(realtime_messages[0]["type"], "tick_realtime")
            validate_terminal_message(realtime_messages[0])
            self.assertEqual(realtime_messages[0]["payload"]["tick"]["price"], 388.4)

            self.assertEqual(octopus.health.redis, "connected")
            self.assertIn("00700.HK", gateway.health.latest_event_at_by_symbol)

    def test_octopus_outputs_alert_and_queue_terminal_messages(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_silver_tables(root)

            bus = InMemoryEventBus()
            cache = InMemoryRedisSnapshotCache()
            octopus = OctopusComputeV2(
                MammothAPI(root),
                bus,
                cache,
            )
            octopus.preload_bod("00700.HK", "20260522")

            big_trade = make_raw_market_event(
                kind="tick",
                symbol="00700.HK",
                source="xtquant",
                seq=1,
                source_ts="2026-05-22T09:30:00+08:00",
                payload={
                    "price": 388.4,
                    "volume": 100000,
                    "turnover": 38840000,
                    "side": "buy",
                    "broker_code": "JPM",
                },
            )
            queue_update = make_raw_broker_queue(bus)

            processed = [
                *octopus.process_raw_event(big_trade, "20260522"),
                *octopus.process_raw_event(queue_update, "20260522"),
            ]

            self.assertEqual([event["result_type"] for event in processed], ["snapshot", "big_trade_alert", "broker_queue"])
            cached = cache.get_terminal_snapshot("20260522", "00700.HK")
            self.assertEqual(cached["alerts"][0]["side"], "buy")
            self.assertEqual(cached["broker_queue"]["bid"][0]["brokerCode"], "UBS")
            self.assertEqual(cached["broker_queue"]["bid"][0]["participantName"], "UBS Securities Hong Kong Limited")

            terminal = GatewayV2(bus, cache).to_terminal_messages()
            self.assertEqual([message["type"] for message in terminal], ["tick_realtime", "alert_realtime", "queue_realtime"])
            for message in terminal:
                validate_terminal_message(message)
            self.assertEqual(terminal[1]["payload"]["alert"]["turnover"], 38840000)
            self.assertEqual(terminal[1]["payload"]["alert"]["participantName"], "JPMorgan Chase Bank, N.A.")
            self.assertTrue(terminal[1]["payload"]["alert"]["isHighlighted"])
            self.assertEqual(terminal[2]["payload"]["broker_queue"]["bid"][0]["brokerCode"], "UBS")

    def test_hktransaction_below_big_trade_threshold_does_not_write_redis_or_broadcast(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_silver_tables(root)

            bus = InMemoryEventBus()
            cache = InMemoryRedisSnapshotCache()
            octopus = OctopusComputeV2(
                MammothAPI(root),
                bus,
                cache,
                big_trade_volume_baseline_ratio=0.0005,
            )
            octopus.preload_bod("00700.HK", "20260522")
            before = copy.deepcopy(cache.get_terminal_snapshot("20260522", "00700.HK"))

            small_trade = make_raw_market_event(
                kind="tick",
                symbol="00700.HK",
                source="xtquant",
                period="hktransaction",
                seq=1,
                source_ts="2026-05-22T09:30:05+08:00",
                payload={
                    "price": 389.0,
                    "volume": 999,
                    "turnover": 388611,
                    "side": "buy",
                    "broker_code": "JPM",
                },
            )

            processed = octopus.process_raw_event(small_trade, "20260522")

            self.assertEqual(processed, [])
            self.assertEqual(bus.read(PROCESSED_TOPIC), [])
            self.assertEqual(cache.get_terminal_snapshot("20260522", "00700.HK"), before)
            self.assertEqual(octopus.health.latest_event_at_by_symbol["00700.HK"], "2026-05-22T09:30:05+08:00")

    def test_hktransaction_big_trade_writes_only_alert_read_model(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_silver_tables(root)

            bus = InMemoryEventBus()
            cache = InMemoryRedisSnapshotCache()
            octopus = OctopusComputeV2(
                MammothAPI(root),
                bus,
                cache,
                big_trade_volume_baseline_ratio=0.0005,
            )
            octopus.preload_bod("00700.HK", "20260522")
            initial_price = cache.get_terminal_snapshot("20260522", "00700.HK")["snapshot"]["price"]

            big_trade = make_raw_market_event(
                kind="tick",
                symbol="00700.HK",
                source="xtquant",
                period="hktransaction",
                seq=1,
                source_ts="2026-05-22T09:30:05+08:00",
                payload={
                    "price": 389.0,
                    "volume": 100000,
                    "turnover": 38900000,
                    "side": "buy",
                    "broker_code": "JPM",
                },
            )

            processed = octopus.process_raw_event(big_trade, "20260522")

            self.assertEqual([event["result_type"] for event in processed], ["big_trade_alert"])
            cached = cache.get_terminal_snapshot("20260522", "00700.HK")
            self.assertEqual(cached["snapshot"]["price"], initial_price)
            self.assertEqual(cached["alerts"][0]["price"], 389.0)
            self.assertEqual(cached["alerts"][0]["participantName"], "JPMorgan Chase Bank, N.A.")
            terminal = GatewayV2(bus, cache).to_terminal_messages()
            self.assertEqual([message["type"] for message in terminal], ["alert_realtime"])
            validate_terminal_message(terminal[0])

    def test_first_same_day_tick_rolls_preopen_fallback_snapshot_to_live_session(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_silver_tables(root)

            bus = InMemoryEventBus()
            cache = InMemoryRedisSnapshotCache()
            octopus = OctopusComputeV2(MammothAPI(root), bus, cache, big_trade_volume_baseline_ratio=2.0)
            snapshot = octopus.preload_bod(
                "00700.HK",
                "20260522",
                cache_trade_date="20260525",
                requested_trade_date="20260525",
            )
            initial_is_historical_session = snapshot["snapshot"]["isHistoricalSession"]
            initial_snapshot_trade_date = snapshot["snapshot"]["tradeDate"]
            initial_minute_bar_count = len(snapshot["minute_bars"])
            processed = octopus.process_raw_event(
                make_raw_market_event(
                    kind="tick",
                    symbol="00700.HK",
                    source="xtquant",
                    seq=1,
                    source_ts="2026-05-25T09:30:01+08:00",
                    payload={"price": 390.0, "volume": 500, "turnover": 195000, "side": "buy"},
                ),
                "20260525",
            )
            cached = cache.get_terminal_snapshot("20260525", "00700.HK")

        self.assertTrue(initial_is_historical_session)
        self.assertEqual(initial_snapshot_trade_date, "20260522")
        self.assertEqual(initial_minute_bar_count, 2)
        self.assertEqual(cached["snapshot"]["tradeDate"], "20260525")
        self.assertEqual(cached["snapshot"]["requestedTradeDate"], "20260525")
        self.assertFalse(cached["snapshot"]["isHistoricalSession"])
        self.assertEqual(cached["snapshot"]["previousClose"], 386.2)
        self.assertEqual(cached["snapshot"]["volume"], 500)
        self.assertEqual(len(cached["minute_bars"]), 1)
        self.assertEqual(cached["minute_bars"][0]["timestamp"], "2026-05-25T09:30:00+08:00")
        self.assertEqual(cached["freshness"]["runtime_state"], "LIVE")
        self.assertEqual(cached["freshness"]["requested_trade_date"], "20260525")
        self.assertEqual(cached["freshness"]["effective_trade_date"], "20260525")
        self.assertEqual(cached["freshness"]["source_dates"]["minute_bars"], "20260525")
        self.assertEqual(processed[0]["payload"]["freshness"]["runtime_state"], "LIVE")

    def test_preopen_ticks_do_not_pollute_minute_bars(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_silver_tables(root)

            octopus = OctopusComputeV2(
                MammothAPI(root),
                InMemoryEventBus(),
                InMemoryRedisSnapshotCache(),
            )
            snapshot = octopus.preload_bod("00700.HK", "20260522")

            octopus.process_raw_event(
                make_raw_market_event(
                    kind="tick",
                    symbol="00700.HK",
                    source="xtquant",
                    seq=1,
                    source_ts="2026-05-22T09:20:00+08:00",
                    payload={"price": 388.9, "volume": 1000, "turnover": 388900, "side": "buy"},
                ),
                "20260522",
            )

        self.assertEqual(
            [bar["timestamp"] for bar in snapshot["minute_bars"]],
            [
                "2026-05-22T09:30:00+08:00",
                "2026-05-22T09:31:00+08:00",
            ],
        )
        self.assertEqual(snapshot["snapshot"]["price"], 388.9)
        self.assertEqual(snapshot["freshness"]["source_dates"]["minute_bars"], "20260522")

    def test_octopus_marks_redis_degraded_without_blocking_snapshot_or_realtime_processing(self) -> None:
        class FailingTerminalCache(InMemoryRedisSnapshotCache):
            def set_terminal_snapshot(self, trade_date: str, symbol: str, snapshot: dict) -> None:
                raise RuntimeError("redis unavailable")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_silver_tables(root)

            bus = InMemoryEventBus()
            octopus = OctopusComputeV2(MammothAPI(root), bus, FailingTerminalCache(), big_trade_volume_baseline_ratio=2.0)

            snapshot = octopus.preload_bod("00700.HK", "20260522")
            self.assertEqual(snapshot["snapshot"]["price"], 388.8)
            self.assertEqual(octopus.health.process, "degraded")
            self.assertEqual(octopus.health.redis, "degraded")
            self.assertTrue(snapshot["freshness"]["degraded"])
            self.assertIn("redis_terminal_snapshot_write_failed", snapshot["freshness"]["degraded_reasons"][0])

            processed = octopus.process_raw_event(make_raw_tick(bus), "20260522")

            self.assertEqual(len(processed), 1)
            self.assertEqual(processed[0]["result_type"], "snapshot")
            self.assertTrue(processed[0]["payload"]["freshness"]["degraded"])
            self.assertIn("redis_terminal_snapshot_write_failed", processed[0]["payload"]["freshness"]["degraded_reasons"][-1])

    def test_big_trade_alert_uses_broker_mapping_name_when_participant_name_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_silver_tables(root)
            write_table(
                root / "silver_broker_mapping_v1.csv",
                [
                    {
                        "schema_version": 1,
                        "broker_code": "101",
                        "broker_name": "Kingston Securities",
                        "effective_from": "20260101",
                        "source": "fixture",
                        "ingest_ts": "2026-05-22T00:00:00Z",
                        "row_hash": "mapping-without-participant",
                    }
                ],
            )

            bus = InMemoryEventBus()
            cache = InMemoryRedisSnapshotCache()
            octopus = OctopusComputeV2(
                MammothAPI(root),
                bus,
                cache,
            )
            octopus.preload_bod("00700.HK", "20260522")

            big_trade = make_raw_market_event(
                kind="tick",
                symbol="00700.HK",
                source="xtquant",
                seq=1,
                source_ts="2026-05-22T09:30:00+08:00",
                payload={
                    "price": 388.4,
                    "volume": 100000,
                    "turnover": 38840000,
                    "side": "buy",
                    "broker_code": "101",
                },
            )
            octopus.process_raw_event(big_trade, "20260522")

            alert = cache.get_terminal_snapshot("20260522", "00700.HK")["alerts"][0]
            self.assertEqual(alert["participantName"], "Kingston Securities")
            self.assertEqual(alert["brokerName"], "Kingston Securities")

    def test_big_trade_alert_uses_previous_day_volume_ratio_not_fixed_turnover(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_silver_tables(root)

            bus = InMemoryEventBus()
            cache = InMemoryRedisSnapshotCache()
            octopus = OctopusComputeV2(
                MammothAPI(root),
                bus,
                cache,
                big_trade_volume_baseline_ratio=0.0005,
            )
            octopus.preload_bod("00700.HK", "20260522")

            high_turnover_small_volume = make_raw_market_event(
                kind="tick",
                symbol="00700.HK",
                source="xtquant",
                period="hktransaction",
                seq=1,
                source_ts="2026-05-22T09:30:00+08:00",
                payload={
                    "price": 1_000_000.0,
                    "volume": 10,
                    "turnover": 10_000_000,
                    "side": "buy",
                    "broker_code": "JPM",
                },
            )
            processed = octopus.process_raw_event(high_turnover_small_volume, "20260522")

            self.assertEqual(processed, [])
            self.assertEqual(cache.get_terminal_snapshot("20260522", "00700.HK")["alerts"], [])

    def test_big_trade_alert_marks_source_undisclosed_broker_codes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_silver_tables(root)

            bus = InMemoryEventBus()
            cache = InMemoryRedisSnapshotCache()
            octopus = OctopusComputeV2(
                MammothAPI(root),
                bus,
                cache,
            )
            octopus.preload_bod("00700.HK", "20260522")

            big_trade = make_raw_market_event(
                kind="tick",
                symbol="00700.HK",
                source="xtquant",
                seq=1,
                source_ts="2026-05-22T09:30:00+08:00",
                payload={
                    "price": 388.4,
                    "volume": 100000,
                    "turnover": 38840000,
                    "side": "buy",
                    "broker_code": "0",
                    "active_broker_code": "0",
                    "trade_type": "101",
                },
            )
            octopus.process_raw_event(big_trade, "20260522")

            alert = cache.get_terminal_snapshot("20260522", "00700.HK")["alerts"][0]
            self.assertEqual(alert["participantName"], "集合竞价")
            self.assertEqual(alert["brokerName"], "集合竞价")
            self.assertNotIn("remark", alert)

    def test_big_trade_alert_notes_active_broker_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_silver_tables(root)

            bus = InMemoryEventBus()
            cache = InMemoryRedisSnapshotCache()
            octopus = OctopusComputeV2(
                MammothAPI(root),
                bus,
                cache,
            )
            octopus.preload_bod("00700.HK", "20260522")

            big_trade = make_raw_market_event(
                kind="tick",
                symbol="00700.HK",
                source="xtquant",
                seq=1,
                source_ts="2026-05-22T09:30:00+08:00",
                payload={
                    "price": 388.4,
                    "volume": 100000,
                    "turnover": 38840000,
                    "side": "buy",
                    "broker_code": "-1",
                    "participant_name": "中国投资",
                    "broker_name": "中国投资",
                    "active_broker_code": "6998",
                    "broker_code_source": "activeBrokerNo",
                },
            )
            octopus.process_raw_event(big_trade, "20260522")

            alert = cache.get_terminal_snapshot("20260522", "00700.HK")["alerts"][0]
            self.assertEqual(alert["participantName"], "中国投资")
            self.assertEqual(alert["brokerName"], "中国投资")
            self.assertNotIn("remark", alert)

    def test_octopus_aggregates_ticks_into_minute_bars(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_silver_tables(root)

            bus = InMemoryEventBus()
            cache = InMemoryRedisSnapshotCache()
            octopus = OctopusComputeV2(MammothAPI(root), bus, cache)
            octopus.preload_bod("00700.HK", "20260522")

            for index, raw_event in enumerate(
                [
                    raw_tick(seq=1, source_ts="2026-05-22T09:30:05+08:00", price=388.4, volume=100),
                    raw_tick(seq=2, source_ts="2026-05-22T09:30:45+08:00", price=388.8, volume=200),
                    raw_tick(seq=3, source_ts="2026-05-22T09:31:01+08:00", price=388.6, volume=300),
                ],
                start=1,
            ):
                octopus.process_raw_event(raw_event, "20260522")
                self.assertEqual(octopus.seq_by_symbol["00700.HK"], index)

            cached = cache.get_terminal_snapshot("20260522", "00700.HK")
            self.assertEqual(len(cached["minute_bars"]), 2)
            self.assertEqual(cached["minute_bars"][0]["timestamp"], "2026-05-22T09:30:00+08:00")
            self.assertEqual(cached["minute_bars"][0]["price"], 388.8)
            self.assertEqual(cached["minute_bars"][0]["volume"], 1300)
            self.assertEqual(cached["minute_bars"][0]["turnover"], 505000.0)
            self.assertEqual(cached["minute_bars"][0]["direction"], "up")
            self.assertEqual(cached["minute_bars"][1]["timestamp"], "2026-05-22T09:31:00+08:00")
            self.assertEqual(cached["minute_bars"][1]["volume"], 2300)

    def test_octopus_applies_l2_order_book_enhancement_to_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_silver_tables(root)

            bus = InMemoryEventBus()
            cache = InMemoryRedisSnapshotCache()
            octopus = OctopusComputeV2(MammothAPI(root), bus, cache)
            octopus.preload_bod("00700.HK", "20260522")

            from beast_market.pipeline import RealtimeCollectorV2

            raw_event = RealtimeCollectorV2(bus).ingest_l2_order_book(
                "00700.HK",
                {
                    "timestamp": "2026-05-22T09:30:02+08:00",
                    "ask": [
                        {"price": 388.8, "volume": 3000, "order_count": 2},
                        {"price": 388.6, "volume": 1000, "order_count": 1},
                    ],
                    "bid": [
                        {"price": 388.1, "volume": 2000, "order_count": 3},
                        {"price": 388.3, "volume": 4000, "order_count": 4},
                    ],
                },
            )

            self.assertEqual(raw_event["kind"], "l2_order_book")
            processed = octopus.process_raw_event(raw_event, "20260522")

            self.assertEqual(processed[0]["result_type"], "l2_order_book")
            cached = cache.get_terminal_snapshot("20260522", "00700.HK")
            self.assertEqual(cached["l2_order_book"]["best_ask"], 388.6)
            self.assertEqual(cached["l2_order_book"]["best_bid"], 388.3)
            self.assertAlmostEqual(cached["l2_order_book"]["spread"], 0.3)
            self.assertEqual(cached["l2_order_book"]["ask"][0]["volume"], 1000)
            self.assertEqual(cached["l2_order_book"]["bid"][0]["volume"], 4000)

            terminal = GatewayV2(bus, cache).to_terminal_messages()
            self.assertEqual(terminal[0]["type"], "snapshot")
            validate_terminal_message(terminal[0])
            self.assertEqual(terminal[0]["payload"]["l2_order_book"]["best_bid"], 388.3)

    def test_octopus_requires_bod_preload_before_realtime_processing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_silver_tables(root)

            octopus = OctopusComputeV2(MammothAPI(root), InMemoryEventBus(), InMemoryRedisSnapshotCache())

            with self.assertRaisesRegex(RuntimeError, "BOD state must be preloaded"):
                octopus.process_raw_event(raw_tick(seq=1, source_ts="2026-05-22T09:30:05+08:00", price=388.4, volume=100), "20260522")

    def test_octopus_process_raw_event_can_use_external_state_without_internal_owner(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_silver_tables(root)

            bus = InMemoryEventBus()
            cache = InMemoryRedisSnapshotCache()
            octopus = OctopusComputeV2(
                MammothAPI(root),
                bus,
                cache,
                big_trade_volume_baseline_ratio=0.00001,
            )
            external_state = copy.deepcopy(octopus.preload_bod("00700.HK", "20260522"))
            octopus.state_by_symbol.clear()
            raw_event = raw_tick(seq=1, source_ts="2026-05-22T09:32:05+08:00", price=389.4, volume=100)

            processed = octopus.process_raw_event_with_state(raw_event, "20260522", external_state)

            self.assertFalse(octopus.has_state("00700.HK"))
            self.assertEqual([event["result_type"] for event in processed], ["snapshot", "big_trade_alert"])
            self.assertEqual(external_state["snapshot"]["price"], 389.4)
            self.assertEqual(external_state["alerts"][0]["price"], 389.4)
            self.assertEqual(cache.get_terminal_snapshot("20260522", "00700.HK")["snapshot"]["price"], 389.4)

    def test_octopus_apply_tick_to_state_returns_explicit_state_update(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_silver_tables(root)

            octopus = OctopusComputeV2(
                MammothAPI(root),
                InMemoryEventBus(),
                InMemoryRedisSnapshotCache(),
                big_trade_volume_baseline_ratio=0.00001,
            )
            state = octopus.preload_bod("00700.HK", "20260522")
            raw_event = raw_tick(seq=1, source_ts="2026-05-22T09:32:05+08:00", price=389.4, volume=100)

            update = octopus.apply_tick_to_state(state, raw_event, "20260522")

            self.assertIs(update.state, state)
            self.assertEqual(update.tick["direction"], "up")
            self.assertEqual(update.state["snapshot"]["price"], 389.4)
            self.assertEqual(update.state["last_tick"]["price"], 389.4)
            self.assertEqual(update.state["minute_bars"][-1]["timestamp"], "2026-05-22T09:32:00+08:00")
            self.assertIsNotNone(update.alert)
            self.assertEqual(update.state["alerts"][0]["id"], update.alert["id"])

    def test_full_tick_seed_updates_snapshot_without_big_trade_alert(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_silver_tables(root)

            octopus = OctopusComputeV2(
                MammothAPI(root),
                InMemoryEventBus(),
                InMemoryRedisSnapshotCache(),
                big_trade_volume_baseline_ratio=0.00001,
            )
            state = octopus.preload_bod("00700.HK", "20260522")
            initial_alert_count = len(state["alerts"])
            raw_event = make_raw_market_event(
                kind="tick",
                symbol="00700.HK",
                source="xtquant_full_tick",
                seq=1,
                source_ts="2026-05-22T09:32:05+08:00",
                payload={
                    "price": 389.4,
                    "volume": 5_000_000,
                    "turnover": 1_947_000_000,
                    "side": "",
                    "broker_code": "",
                },
                period="full_tick",
            )

            update = octopus.apply_tick_to_state(state, raw_event, "20260522")

            self.assertEqual(update.state["snapshot"]["price"], 389.4)
            self.assertEqual(update.state["snapshot"]["volume"], 5_000_000)
            self.assertIsNone(update.alert)
            self.assertEqual(len(update.state["alerts"]), initial_alert_count)

    def test_octopus_apply_broker_queue_to_state_returns_explicit_update(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_silver_tables(root)

            bus = InMemoryEventBus()
            octopus = OctopusComputeV2(MammothAPI(root), bus, InMemoryRedisSnapshotCache())
            state = octopus.preload_bod("00700.HK", "20260522")
            raw_event = make_raw_broker_queue(bus)

            update = octopus.apply_broker_queue_to_state(state, raw_event, "20260522")

            self.assertIs(update.state, state)
            self.assertEqual(update.broker_queue["bid"][0]["brokerCode"], "UBS")
            self.assertEqual(update.broker_queue["bid"][0]["participantName"], "UBS Securities Hong Kong Limited")
            self.assertEqual(update.state["broker_queue"]["bid"][0]["brokerCode"], "UBS")
            self.assertEqual(update.state["freshness"]["source_dates"]["broker_queue"], "20260522")

    def test_broker_queue_rolls_historical_snapshot_to_realtime_session(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_silver_tables(root)

            bus = InMemoryEventBus()
            octopus = OctopusComputeV2(MammothAPI(root), bus, InMemoryRedisSnapshotCache())
            state = octopus.preload_bod(
                "00700.HK",
                "20260522",
                cache_trade_date="20260525",
                requested_trade_date="20260525",
            )
            raw_event = make_raw_broker_queue(bus)

            update = octopus.apply_broker_queue_to_state(state, raw_event, "20260525")

        self.assertEqual(update.state["snapshot"]["tradeDate"], "20260525")
        self.assertEqual(update.state["snapshot"]["requestedTradeDate"], "20260525")
        self.assertFalse(update.state["snapshot"]["isHistoricalSession"])
        self.assertEqual(update.state["snapshot"]["volume"], 0)
        self.assertEqual(update.state["minute_bars"], [])
        self.assertEqual(update.state["freshness"]["effective_trade_date"], "20260525")

    def test_broker_queue_participant_display_uses_three_bucket_taxonomy(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_silver_tables(root)

            bus = InMemoryEventBus()
            octopus = OctopusComputeV2(MammothAPI(root), bus, InMemoryRedisSnapshotCache())
            state = octopus.preload_bod("00700.HK", "20260522")
            raw_event = RealtimeCollectorV2(bus).ingest_broker_queue(
                "00700.HK",
                {
                    "timestamp": "2026-05-22T09:30:00+08:00",
                    "entries": [
                        {"side": "ask", "position": 1, "broker_code": "UBS", "price": 388.6},
                        {"side": "ask", "position": 2, "broker_code": "0", "price": 388.7},
                        {"side": "bid", "position": 1, "broker_code": "101", "price": 388.2},
                        {"side": "bid", "position": 2, "broker_code": "NOPE", "participant_name": "--", "price": 388.1},
                    ],
                },
            )

            update = octopus.apply_broker_queue_to_state(state, raw_event, "20260522")

        self.assertEqual(update.broker_queue["ask"][0]["participantName"], "UBS Securities Hong Kong Limited")
        self.assertEqual(update.broker_queue["ask"][1]["participantName"], "未披露")
        self.assertEqual(update.broker_queue["bid"][0]["participantName"], "集合竞价")
        self.assertEqual(update.broker_queue["bid"][1]["participantName"], "未披露")

    def test_broker_queue_reindexes_duplicate_positions_with_stable_unique_ids(self) -> None:
        rows = [
            {
                "symbol": "02726.HK",
                "side": "ask",
                "position": 1,
                "queue_ts": "2026-05-26T09:30:00+08:00",
                "broker_code": "JPM",
                "participant_name": "JPMorgan",
                "price": 12.34,
                "volume": 1000,
            },
            {
                "symbol": "02726.HK",
                "side": "ask",
                "position": 1,
                "queue_ts": "2026-05-26T09:30:01+08:00",
                "broker_code": "UBS",
                "participant_name": "UBS",
                "price": 12.34,
                "volume": 2000,
            },
            {
                "symbol": "02726.HK",
                "side": "bid",
                "position": 1,
                "queue_ts": "2026-05-26T09:30:00+08:00",
                "broker_code": "CITI",
                "participant_name": "Citibank",
                "price": 12.32,
                "volume": 3000,
            },
        ]

        queue = to_broker_queue(rows)

        self.assertEqual([row["position"] for row in queue["ask"]], [1, 2])
        self.assertEqual(len({row["id"] for row in queue["ask"]}), 2)
        self.assertEqual(queue["ask"][1]["brokerCode"], "UBS")
        self.assertEqual(queue["bid"][0]["position"], 1)

    def test_octopus_apply_l2_order_book_to_state_returns_explicit_update(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_silver_tables(root)

            bus = InMemoryEventBus()
            octopus = OctopusComputeV2(MammothAPI(root), bus, InMemoryRedisSnapshotCache())
            state = octopus.preload_bod("00700.HK", "20260522")
            raw_event = make_raw_l2_order_book(bus)

            update = octopus.apply_l2_order_book_to_state(state, raw_event, "20260522")

            self.assertIs(update.state, state)
            self.assertEqual(update.order_book["best_ask"], 388.6)
            self.assertEqual(update.order_book["best_bid"], 388.3)
            self.assertAlmostEqual(update.order_book["spread"], 0.3)
            self.assertEqual(update.state["l2_order_book"]["ask"][0]["volume"], 1000)
            self.assertEqual(update.state["freshness"]["source_dates"]["l2_order_book"], "20260522")

    def test_l2_order_book_rolls_historical_snapshot_to_realtime_session(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_silver_tables(root)

            bus = InMemoryEventBus()
            octopus = OctopusComputeV2(MammothAPI(root), bus, InMemoryRedisSnapshotCache())
            state = octopus.preload_bod(
                "00700.HK",
                "20260522",
                cache_trade_date="20260525",
                requested_trade_date="20260525",
            )
            raw_event = make_raw_l2_order_book(bus)

            update = octopus.apply_l2_order_book_to_state(state, raw_event, "20260525")

        self.assertEqual(update.state["snapshot"]["tradeDate"], "20260525")
        self.assertFalse(update.state["snapshot"]["isHistoricalSession"])
        self.assertEqual(update.state["snapshot"]["turnover"], 0.0)
        self.assertEqual(update.state["freshness"]["source_dates"]["l2_order_book"], "20260525")

    def test_mammoth_quality_checks_reject_bad_silver_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_table(
                root / "silver_daily_bars_v1.csv",
                [
                    {
                        "schema_version": 1,
                        "symbol": "700",
                        "trade_date": "2026-05-22",
                        "open": 1,
                        "high": 1,
                        "low": 1,
                        "close": 1,
                        "volume": -1,
                        "turnover": 1,
                        "source": "fixture",
                        "ingest_ts": "2026-05-22T00:00:00Z",
                        "row_hash": "bad",
                    }
                ],
            )
            mammoth = MammothAPI(root)
            checks = mammoth.run_quality_checks("daily_bars", mammoth._read_table("daily_bars"))

            self.assertFalse(checks["passed"])
            self.assertIn("invalid_symbol_format", checks["failed_items"])
            self.assertEqual(checks["invalid_symbol_rows"], ["700"])
            self.assertIn("invalid_date_format", checks["failed_items"])
            self.assertIn("negative_values", checks["failed_items"])

    def test_mammoth_quality_checks_require_canonical_terminal_symbols(self) -> None:
        api = MammothAPI(
            reader=StaticReader(
                {
                    "daily_bars": [
                        {
                            "schema_version": 1,
                            "symbol": "0700.HK",
                            "trade_date": "20260522",
                            "open": 1,
                            "high": 1,
                            "low": 1,
                            "close": 1,
                            "volume": 1,
                            "turnover": 1,
                            "source": "fixture",
                            "ingest_ts": "2026-05-22T00:00:00Z",
                            "row_hash": "bad-symbol",
                        }
                    ]
                }
            )
        )

        checks = api.run_quality_checks("daily_bars", api._read_table("daily_bars"))

        self.assertFalse(checks["passed"])
        self.assertEqual(checks["invalid_symbol_rows"], ["0700.HK"])


def make_raw_tick(bus: InMemoryEventBus) -> dict:
    from beast_market.pipeline import RealtimeCollectorV2

    collector = RealtimeCollectorV2(bus)
    return collector.ingest_event(
        kind="tick",
        symbol="00700.HK",
        period="full_tick",
        source_ts="2026-05-22T09:30:00.000+08:00",
        payload={
            "price": 388.4,
            "volume": 100000,
            "turnover": 38840000,
            "side": "buy",
            "broker_code": "JPM",
        },
    )


def make_raw_broker_queue(bus: InMemoryEventBus) -> dict:
    from beast_market.pipeline import RealtimeCollectorV2

    collector = RealtimeCollectorV2(bus)
    return collector.ingest_broker_queue(
        "00700.HK",
        {
            "timestamp": "2026-05-22T09:30:01+08:00",
            "side": "bid",
            "entries": [
                {
                    "id": "bid-new-1",
                    "position": 1,
                    "side": "bid",
                    "broker_code": "UBS",
                    "price": 388.2,
                    "volume": 200000,
                }
            ],
        },
    )


def make_raw_l2_order_book(bus: InMemoryEventBus) -> dict:
    from beast_market.pipeline import RealtimeCollectorV2

    collector = RealtimeCollectorV2(bus)
    return collector.ingest_l2_order_book(
        "00700.HK",
        {
            "timestamp": "2026-05-22T09:30:02+08:00",
            "ask": [
                {"price": 388.8, "volume": 3000, "order_count": 2},
                {"price": 388.6, "volume": 1000, "order_count": 1},
            ],
            "bid": [
                {"price": 388.1, "volume": 2000, "order_count": 3},
                {"price": 388.3, "volume": 4000, "order_count": 4},
            ],
        },
    )


def raw_tick(seq: int, source_ts: str, price: float, volume: int) -> dict:
    return make_raw_market_event(
        kind="tick",
        symbol="00700.HK",
        source="xtquant",
        seq=seq,
        source_ts=source_ts,
        payload={
            "price": price,
            "volume": volume,
            "turnover": price * volume,
            "side": "buy",
            "broker_code": "JPM",
        },
    )


def write_silver_tables(root: Path) -> None:
    write_table(
        root / "silver_daily_bars_v1.csv",
        [
            {
                "schema_version": 1,
                "symbol": "00700.HK",
                "trade_date": "20260521",
                "open": 380.0,
                "high": 384.0,
                "low": 379.0,
                "close": 382.0,
                "volume": 2000000,
                "turnover": 764000000,
                "source": "fixture",
                "ingest_ts": "2026-05-21T00:00:00Z",
                "row_hash": "daily-0",
            },
            {
                "schema_version": 1,
                "symbol": "00700.HK",
                "trade_date": "20260522",
                "open": 386.8,
                "high": 389.0,
                "low": 385.4,
                "close": 386.2,
                "volume": 1000000,
                "turnover": 386200000,
                "source": "fixture",
                "ingest_ts": "2026-05-22T00:00:00Z",
                "row_hash": "daily-1",
            }
        ],
    )
    write_table(
        root / "silver_trade_ticks_v1.csv",
        [
            {
                "schema_version": 1,
                "symbol": "00700.HK",
                "trade_date": "20260522",
                "tick_ts": "2026-05-22T09:30:00+08:00",
                "price": 388.4,
                "volume": 100000,
                "turnover": 38840000,
                "side": "buy",
                "source": "fixture",
                "ingest_ts": "2026-05-22T09:30:01+08:00",
                "row_hash": "tick-1",
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
                "participant_name": "JPMorgan Chase Bank, N.A.",
                "shares": 990000,
                "percent": 1.0,
                "change": 5000,
                "is_highlighted": "true",
                "source": "fixture",
                "ingest_ts": "2026-05-21T00:00:00Z",
                "row_hash": "holding-0",
            },
            {
                "schema_version": 1,
                "symbol": "00700.HK",
                "trade_date": "20260522",
                "participant_id": "C00010",
                "participant_name": "JPMorgan Chase Bank, N.A.",
                "shares": 1000000,
                "percent": 1.1,
                "change": 10000,
                "is_highlighted": "true",
                "source": "fixture",
                "ingest_ts": "2026-05-22T00:00:00Z",
                "row_hash": "holding-1",
            },
            {
                "schema_version": 1,
                "symbol": "00700.HK",
                "trade_date": "20260522",
                "participant_id": "C00039",
                "participant_name": "Citibank N.A.",
                "shares": 900000,
                "percent": 1.0,
                "change": -1000,
                "is_highlighted": "true",
                "source": "fixture",
                "ingest_ts": "2026-05-22T00:00:00Z",
                "row_hash": "holding-2",
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
                "broker_name": "JPMorgan",
                "participant_id": "C00010",
                "participant_name": "JPMorgan Chase Bank, N.A.",
                "price": 388.6,
                "volume": 100000,
                "source": "fixture",
                "ingest_ts": "2026-05-22T09:30:01+08:00",
                "row_hash": "queue-1",
            },
            {
                "schema_version": 1,
                "symbol": "00700.HK",
                "trade_date": "20260522",
                "queue_ts": "2026-05-22T09:30:00+08:00",
                "side": "bid",
                "position": 1,
                "broker_code": "CITI",
                "broker_name": "Citibank",
                "participant_id": "C00039",
                "participant_name": "Citibank N.A.",
                "price": 388.2,
                "volume": 90000,
                "source": "fixture",
                "ingest_ts": "2026-05-22T09:30:01+08:00",
                "row_hash": "queue-2",
            },
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
                "participant_name": "JPMorgan Chase Bank, N.A.",
                "effective_from": "20260101",
                "effective_to": "",
                "source": "fixture",
                "ingest_ts": "2026-05-22T00:00:00Z",
                "row_hash": "mapping-1",
            },
            {
                "schema_version": 1,
                "broker_code": "UBS",
                "broker_name": "UBS Securities Hong Kong Limited",
                "participant_id": "C00085",
                "participant_name": "UBS Securities Hong Kong Limited",
                "effective_from": "20260101",
                "effective_to": "",
                "source": "fixture",
                "ingest_ts": "2026-05-22T00:00:00Z",
                "row_hash": "mapping-2",
            }
        ],
    )


def write_table(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


class StaticReader:
    def __init__(self, rows_by_type: dict[str, list[dict]]) -> None:
        self.rows_by_type = rows_by_type

    def read_table(self, data_type: str) -> list[dict]:
        return self.rows_by_type.get(data_type, [])


if __name__ == "__main__":
    unittest.main()
