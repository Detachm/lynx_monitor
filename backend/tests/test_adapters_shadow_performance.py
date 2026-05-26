import json
import unittest
from tempfile import TemporaryDirectory

from beast_market import (
    BoundedRawEventQueue,
    EventPublishError,
    FailingEventBus,
    FileBackedSpool,
    FileBackedShadowRunRecorder,
    LegacyShadowTelemetryAdapter,
    KafkaAdapterConfig,
    KafkaEventBusAdapter,
    LocalSpool,
    ReliableEventBus,
    ShadowRunRecorder,
    ShadowRunThresholds,
    build_shadow_run_report,
    compare_event_streams,
    evaluate_performance,
    load_shadow_run_files,
    make_raw_market_event,
    normalize_legacy_terminal_event,
    record_v2_runtime_tick,
    shadow_run_file_paths,
)
from beast_market.shadow_run import thresholds_from_dict


class AdapterShadowPerformanceTest(unittest.TestCase):
    def test_reliable_event_bus_retries_and_acks_successful_publish(self) -> None:
        inner = FailingEventBus(fail_for_attempts=2)
        bus = ReliableEventBus(inner, retries=2)
        event = make_raw_market_event(
            kind="tick",
            symbol="00700.HK",
            source="xtquant",
            seq=1,
            payload={"price": 388.4, "volume": 1000, "turnover": 388400},
        )

        bus.publish("raw_market_events_v1", "00700.HK", event)

        self.assertEqual(inner.attempts, 3)
        self.assertEqual(len(bus.read("raw_market_events_v1")), 1)
        self.assertTrue(bus.results[-1].acknowledged)
        self.assertEqual(bus.results[-1].attempts, 3)

    def test_reliable_event_bus_spools_and_dlqs_after_retry_budget(self) -> None:
        spool = LocalSpool()
        bus = ReliableEventBus(FailingEventBus(fail_for_attempts=10), retries=1, spool=spool)
        event = make_raw_market_event(
            kind="tick",
            symbol="00700.HK",
            source="xtquant",
            seq=1,
            payload={"price": 388.4, "volume": 1000, "turnover": 388400},
        )

        with self.assertRaises(EventPublishError):
            bus.publish("raw_market_events_v1", "00700.HK", event)

        self.assertEqual(len(spool.records), 1)
        self.assertEqual(len(bus.dead_letters), 1)
        self.assertFalse(bus.results[-1].acknowledged)

    def test_reliable_event_bus_spools_kafka_delivery_callback_failures(self) -> None:
        spool = LocalSpool()
        adapter = KafkaEventBusAdapter(
            CallbackFailureKafkaProducer(),
            config=KafkaAdapterConfig(delivery_timeout_seconds=0.1),
        )
        bus = ReliableEventBus(adapter, retries=0, spool=spool)
        event = make_raw_market_event(
            kind="tick",
            symbol="00700.HK",
            source="xtquant",
            seq=1,
            payload={"price": 388.4, "volume": 1000, "turnover": 388400},
        )

        with self.assertRaises(EventPublishError):
            bus.publish("raw_market_events_v1", "00700.HK", event)

        self.assertEqual(len(spool.records), 1)
        self.assertEqual(spool.records[0].topic, "raw_market_events_v1")
        self.assertEqual(spool.records[0].key, "00700.HK")
        self.assertIn("delivery failed", spool.records[0].reason)

    def test_file_backed_spool_persists_publish_failures_across_instances(self) -> None:
        with TemporaryDirectory() as directory:
            spool_path = f"{directory}/publish-failures.jsonl"
            spool = FileBackedSpool(spool_path)
            bus = ReliableEventBus(FailingEventBus(fail_for_attempts=10), retries=0, spool=spool)
            event = make_raw_market_event(
                kind="tick",
                symbol="00700.HK",
                source="xtquant",
                seq=1,
                payload={"price": 388.4, "volume": 1000, "turnover": 388400},
            )

            with self.assertRaises(EventPublishError):
                bus.publish("raw_market_events_v1", "00700.HK", event)

            reloaded = FileBackedSpool(spool_path)
            self.assertEqual(len(reloaded.records), 1)
            self.assertEqual(reloaded.records[0].topic, "raw_market_events_v1")
            self.assertEqual(reloaded.records[0].key, "00700.HK")
            self.assertEqual(reloaded.records[0].value["event_id"], event["event_id"])
            self.assertIn("simulated publish failure", reloaded.records[0].reason)

            drained = reloaded.drain()

            self.assertEqual(len(drained), 1)
            self.assertEqual(FileBackedSpool(spool_path).records, [])

    def test_file_backed_spool_quarantines_corrupt_persisted_records(self) -> None:
        with TemporaryDirectory() as directory:
            spool_path = f"{directory}/publish-failures.jsonl"
            quarantine_path = f"{directory}/publish-failures.quarantine.jsonl"
            valid_record = {
                "schema_version": 1,
                "topic": "raw_market_events_v1",
                "key": "00700.HK",
                "value": {"event_id": "event-1", "symbol": "00700.HK"},
                "reason": "broker unavailable",
            }
            with open(spool_path, "w", encoding="utf-8") as handle:
                handle.write("{bad json\n")
                handle.write(json.dumps(["not", "object"]) + "\n")
                handle.write(json.dumps({"topic": "raw_market_events_v1", "key": "00700.HK"}) + "\n")
                handle.write(json.dumps(valid_record) + "\n")

            spool = FileBackedSpool(spool_path, quarantine_path=quarantine_path)

            self.assertEqual(len(spool.records), 1)
            self.assertEqual(spool.records[0].value["event_id"], "event-1")
            self.assertEqual(spool.quarantined_records, 3)
            with open(quarantine_path, encoding="utf-8") as handle:
                quarantined = [json.loads(line) for line in handle]
            self.assertEqual(
                [record["reason"] for record in quarantined],
                ["invalid_json", "record_not_object", "record_shape_invalid"],
            )
            self.assertEqual([record["line_number"] for record in quarantined], [1, 2, 3])
            self.assertEqual(quarantined[0]["source_path"], spool_path)

    def test_bounded_raw_event_queue_reports_backpressure_without_silent_drop(self) -> None:
        queue = BoundedRawEventQueue(max_size=1)
        first = {"event_id": "first"}
        second = {"event_id": "second"}

        self.assertTrue(queue.push(first))
        self.assertFalse(queue.push(second))
        self.assertEqual(queue.backlog, 1)
        self.assertEqual(queue.dropped, [second])
        self.assertEqual(queue.pop(), first)

    def test_shadow_run_comparison_accepts_matching_stream_and_rejects_bad_stream(self) -> None:
        legacy = [
            event("legacy-1", "00700.HK", 1),
            event("legacy-2", "00700.HK", 2),
            event("legacy-3", "00939.HK", 1),
        ]
        matching_v2 = [
            event("v2-1", "00700.HK", 1),
            event("v2-2", "00700.HK", 2),
            event("v2-3", "00939.HK", 1),
        ]
        bad_v2 = [
            event("v2-1", "00700.HK", 2),
            event("v2-1", "00700.HK", 1),
        ]

        self.assertTrue(compare_event_streams(legacy, matching_v2)["passed"])
        result = compare_event_streams(legacy, bad_v2)
        self.assertFalse(result["passed"])
        self.assertIn("00700.HK", result["failed_symbols"])
        self.assertIn("00939.HK", result["failed_symbols"])

    def test_shadow_run_report_combines_stream_freshness_latency_and_performance_gates(self) -> None:
        legacy = [
            event("legacy-1", "00700.HK", 1, "2026-05-22T09:30:00+08:00", "2026-05-22T09:30:00.010+08:00"),
            event("legacy-2", "00700.HK", 2, "2026-05-22T09:30:10+08:00", "2026-05-22T09:30:10.010+08:00"),
        ]
        v2 = [
            event("v2-1", "00700.HK", 1, "2026-05-22T09:30:00+08:00", "2026-05-22T09:30:00.010+08:00"),
            event("v2-2", "00700.HK", 2, "2026-05-22T09:30:10+08:00", "2026-05-22T09:30:10.010+08:00"),
        ]

        report = build_shadow_run_report(
            session_id="session-1",
            trading_date="20260522",
            started_at="2026-05-22T09:30:00+08:00",
            finished_at="2026-05-22T09:31:00+08:00",
            legacy_events=legacy,
            v2_events=v2,
            performance_samples=passing_performance_samples(),
        )

        self.assertTrue(report["passed"])
        self.assertEqual(report["duration_seconds"], 60)
        self.assertEqual(report["legacy_source_coverage_seconds"], 10)
        self.assertEqual(report["v2_source_coverage_seconds"], 10)
        self.assertEqual(report["comparison"]["symbols"]["00700.HK"]["legacy_source_coverage_seconds"], 10)
        self.assertEqual(report["comparison"]["symbols"]["00700.HK"]["v2_source_coverage_seconds"], 10)
        self.assertEqual(report["comparison"]["symbols"]["00700.HK"]["max_stale_gap_seconds"], 10)
        self.assertAlmostEqual(report["comparison"]["symbols"]["00700.HK"]["max_latency_delta_ms"], 10, places=3)

        stale_report = build_shadow_run_report(
            session_id="session-2",
            trading_date="20260522",
            started_at="2026-05-22T09:30:00+08:00",
            finished_at="2026-05-22T09:33:00+08:00",
            legacy_events=legacy,
            v2_events=[
                event("v2-1", "00700.HK", 1, "2026-05-22T09:30:00+08:00", "2026-05-22T09:30:00.010+08:00"),
                event("v2-2", "00700.HK", 2, "2026-05-22T09:32:00+08:00", "2026-05-22T09:32:00.500+08:00"),
            ],
            performance_samples=passing_performance_samples(),
            thresholds=ShadowRunThresholds(max_stale_gap_seconds=30, max_latency_delta_ms=250),
        )

        self.assertFalse(stale_report["passed"])
        self.assertFalse(stale_report["comparison"]["symbols"]["00700.HK"]["passed"])

    def test_shadow_run_recorder_collects_parallel_events_and_performance_samples(self) -> None:
        recorder = ShadowRunRecorder(
            session_id="session-1",
            trading_date="20260522",
            started_at="2026-05-22T09:30:00+08:00",
        )

        recorder.record_legacy_event(
            {
                "event_id": "legacy-1",
                "symbol": "00700.HK",
                "seq": 1,
                "source_ts": "2026-05-22T09:30:00+08:00",
                "ingest_ts": "2026-05-22T09:30:00.010+08:00",
            }
        )
        recorder.record_v2_event(
            {
                "event_id": "v2-1",
                "symbol": "00700.HK",
                "seq": 1,
                "source_ts": "2026-05-22T09:30:00+08:00",
                "ingest_ts": "2026-05-22T09:30:00.010+08:00",
            }
        )
        for key, value in passing_performance_samples().items():
            for sample in value:
                recorder.record_performance_sample(key, sample)

        report = recorder.build_report(finished_at="2026-05-22T09:31:00+08:00")

        self.assertTrue(report["passed"])
        self.assertEqual(report["legacy_event_count"], 1)
        self.assertEqual(report["v2_event_count"], 1)
        self.assertEqual(report["performance"]["metrics"]["collector_to_kafka"]["p95_ms"], 19)

    def test_record_v2_runtime_tick_extracts_events_and_pipeline_samples(self) -> None:
        recorder = ShadowRunRecorder(
            session_id="session-1",
            trading_date="20260522",
            started_at="2026-05-22T09:30:00+08:00",
        )

        record_v2_runtime_tick(
            recorder,
            {
                "raw_events": [
                    event("raw-1", "00700.HK", 1, "2026-05-22T09:30:00+08:00", "2026-05-22T09:30:00.010+08:00")
                ],
                "processed_event_payloads": [
                    event("processed-1", "00700.HK", 1, "2026-05-22T09:30:00+08:00", "2026-05-22T09:30:00.020+08:00")
                ],
                "terminal_messages": [
                    event("terminal-1", "00700.HK", 1, "2026-05-22T09:30:00+08:00", "2026-05-22T09:30:00.030+08:00")
                ],
            },
        )

        self.assertEqual(recorder.v2_events[0]["event_id"], "terminal-1")
        self.assertAlmostEqual(recorder.performance_samples["collector_to_kafka_ms"][0], 10, places=3)
        self.assertAlmostEqual(recorder.performance_samples["processed_to_gateway_ms"][0], 20, places=3)
        self.assertAlmostEqual(recorder.performance_samples["gateway_to_frontend_ms"][0], 30, places=3)

    def test_legacy_shadow_adapter_normalizes_common_legacy_terminal_messages(self) -> None:
        recorder = ShadowRunRecorder(
            session_id="session-1",
            trading_date="20260522",
            started_at="2026-05-22T09:30:00+08:00",
        )
        adapter = LegacyShadowTelemetryAdapter(recorder, source="legacy-ws")

        first = adapter.record(
            {
                "type": "tick",
                "code": "700",
                "timestamp": "2026-05-22T09:30:00+08:00",
                "receivedAt": "2026-05-22T09:30:00.010+08:00",
            }
        )
        second = adapter.record(
            {
                "eventId": "legacy-event-2",
                "payload": {
                    "snapshot": {
                        "symbol": "939.hk",
                        "updatedAt": "2026-05-22T09:30:01+08:00",
                    }
                },
            }
        )

        self.assertEqual(first["symbol"], "00700.HK")
        self.assertEqual(first["seq"], 1)
        self.assertTrue(first["event_id"].startswith("legacy-legacy-ws-00700.HK-1-"))
        self.assertEqual(second["symbol"], "00939.HK")
        self.assertEqual(second["event_id"], "legacy-event-2")
        self.assertEqual(recorder.legacy_events[0]["symbol"], "00700.HK")
        self.assertEqual(recorder.legacy_events[1]["source_ts"], "2026-05-22T09:30:01+08:00")

    def test_legacy_shadow_normalizer_rejects_missing_symbol_or_timestamp(self) -> None:
        with self.assertRaisesRegex(ValueError, "missing symbol"):
            normalize_legacy_terminal_event({"timestamp": "2026-05-22T09:30:00+08:00"})
        with self.assertRaisesRegex(ValueError, "missing source timestamp"):
            normalize_legacy_terminal_event({"symbol": "700"})

    def test_file_backed_shadow_run_recorder_persists_parallel_session_evidence(self) -> None:
        with TemporaryDirectory() as temp_dir:
            recorder = FileBackedShadowRunRecorder(
                directory=temp_dir,
                session_id="session-1",
                trading_date="20260522",
                started_at="2026-05-22T09:30:00+08:00",
                reset=True,
            )
            legacy = LegacyShadowTelemetryAdapter(recorder)
            legacy.record(
                {
                    "event_id": "legacy-1",
                    "symbol": "700",
                    "timestamp": "2026-05-22T09:30:00+08:00",
                    "received_ts": "2026-05-22T09:30:00.010+08:00",
                }
            )
            recorder.record_v2_event(
                {
                    "event_id": "v2-1",
                    "symbol": "00700.HK",
                    "seq": 1,
                    "source_ts": "2026-05-22T09:30:00+08:00",
                    "ingest_ts": "2026-05-22T09:30:00.010+08:00",
                }
            )
            for key, value in passing_performance_samples().items():
                for sample in value:
                    recorder.record_performance_sample(key, sample)

            files = shadow_run_file_paths(temp_dir, trading_date="20260522", session_id="session-1")
            loaded = load_shadow_run_files(files)
            report = recorder.build_report(finished_at="2026-05-22T13:30:00+08:00")

        self.assertEqual(files.legacy_events_path.name, "20260522.session-1.legacy.ndjson")
        self.assertEqual(loaded["metadata"]["session_id"], "session-1")
        self.assertEqual(loaded["legacy_events"][0]["event_id"], "legacy-1")
        self.assertEqual(loaded["v2_events"][0]["event_id"], "v2-1")
        self.assertEqual(loaded["performance_samples"]["subscribe_snapshot_ms"], [50.0, 80.0, 100.0])
        self.assertTrue(report["passed"])
        self.assertEqual(report["duration_seconds"], 14400)
        self.assertEqual(report["evidence_source"]["kind"], "file_backed_shadow_run")
        self.assertEqual(report["evidence_source"]["legacy_event_count"], 1)
        self.assertEqual(report["evidence_source"]["v2_event_count"], 1)
        self.assertEqual(
            report["evidence_source"]["performance_sample_counts"]["subscribe_snapshot_ms"],
            3,
        )

    def test_shadow_run_file_loader_rejects_invalid_performance_samples_and_thresholds(self) -> None:
        with TemporaryDirectory() as temp_dir:
            recorder = FileBackedShadowRunRecorder(
                directory=temp_dir,
                session_id="session-1",
                trading_date="20260522",
                started_at="2026-05-22T09:30:00+08:00",
                reset=True,
            )
            files = shadow_run_file_paths(temp_dir, trading_date="20260522", session_id="session-1")
            files.performance_samples_path.write_text(
                json.dumps({"key": "collector_to_kafka_ms", "value_ms": -1}) + "\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "value_ms must be a non-negative finite number"):
                load_shadow_run_files(files)

            with self.assertRaisesRegex(ValueError, "value_ms must be a non-negative finite number"):
                recorder.record_performance_sample("collector_to_kafka_ms", float("nan"))

            with self.assertRaisesRegex(ValueError, "max_duplicate_ratio must be between 0 and 1"):
                thresholds_from_dict({"max_duplicate_ratio": 2})

            with self.assertRaisesRegex(ValueError, "max_missing_symbol_count must be a non-negative integer"):
                thresholds_from_dict({"max_missing_symbol_count": 0.5})

    def test_shadow_run_file_loader_rejects_invalid_event_stream_rows(self) -> None:
        with TemporaryDirectory() as temp_dir:
            FileBackedShadowRunRecorder(
                directory=temp_dir,
                session_id="session-1",
                trading_date="20260522",
                started_at="2026-05-22T09:30:00+08:00",
                reset=True,
            )
            files = shadow_run_file_paths(temp_dir, trading_date="20260522", session_id="session-1")
            files.legacy_events_path.write_text(
                json.dumps(
                    {
                        "event_id": "legacy-1",
                        "symbol": "700",
                        "seq": 1,
                        "source_ts": "2026-05-22T09:30:00+08:00",
                        "ingest_ts": "2026-05-22T09:30:00.010+08:00",
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "legacy event line 1 symbol must use canonical format"):
                load_shadow_run_files(files)

            files.legacy_events_path.write_text("", encoding="utf-8")
            files.v2_events_path.write_text(
                json.dumps(
                    {
                        "event_id": "v2-1",
                        "symbol": "00700.HK",
                        "seq": "1",
                        "source_ts": "2026-05-22T09:30:00+08:00",
                        "ingest_ts": "2026-05-22T09:30:00.010+08:00",
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "v2 event line 1 seq must be a positive integer"):
                load_shadow_run_files(files)

    def test_performance_probe_evaluates_plan_sla(self) -> None:
        passing = evaluate_performance(
            {
                "collector_to_kafka_ms": [5, 10, 20, 25, 28],
                "processed_to_gateway_ms": [10, 20, 30],
                "gateway_to_frontend_ms": [10, 20, 30],
                "subscribe_snapshot_ms": [50, 80, 100],
                "frontend_store_update_ms": [80, 120, 180],
            }
        )
        failing = evaluate_performance(
            {
                "collector_to_kafka_ms": [5, 10, 200],
                "processed_to_gateway_ms": [10],
                "gateway_to_frontend_ms": [10],
                "subscribe_snapshot_ms": [50],
                "frontend_store_update_ms": [300],
            }
        )

        self.assertTrue(passing["passed"])
        self.assertEqual(passing["insufficient_sample_keys"], [])
        self.assertEqual(passing["sample_counts"]["frontend_store_update_ms"], 3)
        self.assertFalse(failing["passed"])
        self.assertIn("processed_to_gateway_ms", failing["insufficient_sample_keys"])
        self.assertFalse(evaluate_performance({"collector_to_kafka_ms": [1]})["passed"])


def event(
    event_id: str,
    symbol: str,
    seq: int,
    source_ts: str = "2026-05-22T09:30:00.000+08:00",
    ingest_ts: str = "2026-05-22T09:30:00.010+08:00",
) -> dict:
    return {
        "event_id": event_id,
        "symbol": symbol,
        "seq": seq,
        "source_ts": source_ts,
        "ingest_ts": ingest_ts,
    }


def passing_performance_samples() -> dict[str, list[float]]:
    return {
        "collector_to_kafka_ms": [5, 10, 20],
        "processed_to_gateway_ms": [10, 20, 30],
        "gateway_to_frontend_ms": [10, 20, 30],
        "subscribe_snapshot_ms": [50, 80, 100],
        "frontend_store_update_ms": [80, 120, 180],
    }


class CallbackFailureKafkaProducer:
    def __init__(self) -> None:
        self.callback = None

    def produce(self, topic: str, key: bytes, value: bytes, on_delivery=None) -> None:
        self.callback = on_delivery

    def poll(self, timeout: float) -> None:
        if self.callback is not None:
            callback = self.callback
            self.callback = None
            callback(RuntimeError("delivery failed"), {"offset": 0})


if __name__ == "__main__":
    unittest.main()
