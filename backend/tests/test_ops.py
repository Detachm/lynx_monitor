import csv
import hashlib
import json
import os
import shlex
import shutil
import socket
import subprocess
import unittest
import zipfile
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import beast_market.ops as ops_module
from beast_market import (
    FailingEventBus,
    FileBackedSpool,
    FileBackedShadowRunRecorder,
    InMemoryEventBus,
    LegacyRetirementEvidence,
    build_multi_trader_smoke_observation,
    build_multi_trader_smoke_workflows_template,
    clear_runtime_cache,
    clear_runtime_state,
    evaluate_runtime_config_artifact,
    finalize_multi_trader_smoke,
    finalize_shadow_run_cutover,
    import_frontend_performance_samples,
    inspect_multi_trader_smoke_readiness,
    import_multi_trader_smoke_artifact,
    import_multi_trader_smoke_artifacts,
    load_shadow_run_files,
    make_raw_market_event,
    package_multi_trader_smoke,
    prepare_multi_trader_smoke,
    record_multi_trader_smoke_workflow,
    replay_kafka_spool,
    runtime_config_from_artifact,
    save_shadow_run_report,
    shadow_run_file_paths,
    verify_multi_trader_smoke_services,
    write_legacy_retirement_evidence,
)
from beast_market.ops_cli import main as ops_main


class OpsTest(unittest.TestCase):
    def test_finalize_shadow_run_cutover_writes_report_readiness_and_frontend_env(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_passing_shadow_stream(root / "streams")

            result = finalize_shadow_run_cutover(
                stream_directory=root / "streams",
                session_id="session-1",
                trading_date="20260522",
                finished_at="2026-05-22T13:30:00+08:00",
                reports_directory=root / "reports",
                readiness_path=root / "cutover-readiness.json",
                frontend_env_path=root / ".env.cutover",
                live_url="ws://gateway.internal:9020/ws",
            )

            readiness = json.loads(result.readiness_path.read_text(encoding="utf-8"))
            env = result.frontend_env_path.read_text(encoding="utf-8")

        self.assertTrue(result.report["passed"])
        self.assertTrue(readiness["frontend_default_v2_allowed"])
        self.assertEqual(result.report_path.name, "20260522.session-1.shadow-run.json")
        self.assertIn("VITE_MARKET_DATA_MODE=auto", env)
        self.assertIn("VITE_MARKET_WS_URL=ws://gateway.internal:9020/ws", env)

    def test_ops_cli_finalizes_file_backed_shadow_run(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_passing_shadow_stream(root / "streams")
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "finalize-shadow-run",
                        "--stream-directory",
                        str(root / "streams"),
                        "--session-id",
                        "session-1",
                        "--trading-date",
                        "20260522",
                        "--finished-at",
                        "2026-05-22T13:30:00+08:00",
                        "--reports-directory",
                        str(root / "reports"),
                        "--readiness-path",
                        str(root / "cutover-readiness.json"),
                        "--frontend-env-path",
                        str(root / ".env.cutover"),
                        "--live-url",
                        "ws://gateway.internal:9020/ws",
                    ]
                )
            summary = json.loads(output.getvalue())

        self.assertEqual(exit_code, 0)
        self.assertTrue(summary["passed"])
        self.assertEqual(summary["session_id"], "session-1")

    def test_ops_cli_imports_legacy_telemetry_into_shadow_stream(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_path = root / "legacy.ndjson"
            input_path.write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "type": "tick",
                                "code": "700",
                                "timestamp": "2026-05-22T09:30:00+08:00",
                                "receivedAt": "2026-05-22T09:30:00.010+08:00",
                            }
                        ),
                        json.dumps(
                            {
                                "eventId": "legacy-2",
                                "payload": {
                                    "snapshot": {
                                        "symbol": "939.hk",
                                        "updatedAt": "2026-05-22T09:30:01+08:00",
                                    }
                                },
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "import-legacy-telemetry",
                        "--input-path",
                        str(input_path),
                        "--stream-directory",
                        str(root / "streams"),
                        "--session-id",
                        "session-1",
                        "--trading-date",
                        "20260522",
                        "--started-at",
                        "2026-05-22T09:30:00+08:00",
                        "--source",
                        "legacy-ws",
                        "--reset",
                    ]
                )
            summary = json.loads(output.getvalue())
            files = shadow_run_file_paths(root / "streams", trading_date="20260522", session_id="session-1")
            loaded = load_shadow_run_files(files)

        self.assertEqual(exit_code, 0)
        self.assertEqual(summary["imported_count"], 2)
        self.assertEqual(loaded["legacy_events"][0]["symbol"], "00700.HK")
        self.assertEqual(loaded["legacy_events"][0]["seq"], 1)
        self.assertEqual(loaded["legacy_events"][1]["symbol"], "00939.HK")
        self.assertEqual(loaded["legacy_events"][1]["event_id"], "legacy-2")

    def test_legacy_telemetry_import_reset_preserves_v2_and_performance_streams(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            streams = root / "streams"
            recorder = FileBackedShadowRunRecorder(
                directory=streams,
                session_id="session-1",
                trading_date="20260522",
                started_at="2026-05-22T09:30:00+08:00",
                reset=True,
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
            recorder.record_performance_sample("gateway_to_frontend_ms", 30)
            recorder.record_legacy_event(
                {
                    "event_id": "stale-legacy",
                    "symbol": "00700.HK",
                    "seq": 1,
                    "source_ts": "2026-05-22T09:29:00+08:00",
                    "ingest_ts": "2026-05-22T09:29:00.010+08:00",
                }
            )
            input_path = root / "legacy.ndjson"
            input_path.write_text(
                json.dumps(
                    {
                        "eventId": "legacy-1",
                        "code": "700",
                        "timestamp": "2026-05-22T09:30:00+08:00",
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            with redirect_stdout(StringIO()):
                exit_code = ops_main(
                    [
                        "import-legacy-telemetry",
                        "--input-path",
                        str(input_path),
                        "--stream-directory",
                        str(streams),
                        "--session-id",
                        "session-1",
                        "--trading-date",
                        "20260522",
                        "--started-at",
                        "2026-05-22T09:30:00+08:00",
                        "--reset",
                    ]
                )
            loaded = load_shadow_run_files(shadow_run_file_paths(streams, trading_date="20260522", session_id="session-1"))

        self.assertEqual(exit_code, 0)
        self.assertEqual([event["event_id"] for event in loaded["legacy_events"]], ["legacy-1"])
        self.assertEqual(loaded["v2_events"][0]["event_id"], "v2-1")
        self.assertEqual(loaded["performance_samples"]["gateway_to_frontend_ms"], [30.0])

    def test_imports_frontend_performance_samples_into_shadow_stream(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_path = root / "frontend-performance.ndjson"
            input_path.write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "key": "frontend_store_update_ms",
                                "valueMs": 42.5,
                                "symbol": "00700.HK",
                                "messageType": "tick_realtime",
                            }
                        ),
                        json.dumps({"key": "frontend_store_update_ms", "value_ms": 90}),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            result = import_frontend_performance_samples(
                input_path=input_path,
                stream_directory=root / "streams",
                session_id="session-1",
                trading_date="20260522",
                started_at="2026-05-22T09:30:00+08:00",
            )
            files = shadow_run_file_paths(root / "streams", trading_date="20260522", session_id="session-1")
            loaded = load_shadow_run_files(files)

        self.assertEqual(result.imported_count, 2)
        self.assertEqual(result.performance_samples_path, files.performance_samples_path)
        self.assertEqual(loaded["performance_samples"]["frontend_store_update_ms"], [42.5, 90.0])

    def test_ops_cli_imports_frontend_performance_samples(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_path = root / "frontend-performance.ndjson"
            input_path.write_text(
                json.dumps({"key": "frontend_store_update_ms", "valueMs": 120}) + "\n",
                encoding="utf-8",
            )
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "import-frontend-performance",
                        "--input-path",
                        str(input_path),
                        "--stream-directory",
                        str(root / "streams"),
                        "--session-id",
                        "session-1",
                        "--trading-date",
                        "20260522",
                        "--started-at",
                        "2026-05-22T09:30:00+08:00",
                    ]
                )
            summary = json.loads(output.getvalue())
            loaded = load_shadow_run_files(
                shadow_run_file_paths(root / "streams", trading_date="20260522", session_id="session-1")
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(summary["imported_count"], 1)
        self.assertEqual(loaded["performance_samples"]["frontend_store_update_ms"], [120.0])

    def test_replays_kafka_spool_with_dry_run_success_and_failure_paths(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            spool_path = root / "publish-failures.jsonl"
            event = raw_event("raw-1")
            spool = FileBackedSpool(spool_path)
            spool.append("raw_market_events_v1", "00700.HK", event, "simulated failure")

            dry_run = replay_kafka_spool(
                spool_path=spool_path,
                event_bus=InMemoryEventBus(),
                dry_run=True,
            )

            self.assertTrue(dry_run.dry_run)
            self.assertEqual(dry_run.remaining_count, 1)
            self.assertEqual(dry_run.quarantined_count, 0)
            self.assertTrue(str(dry_run.quarantine_path).endswith("publish-failures.jsonl.quarantine"))
            self.assertTrue(spool_path.exists())

            failing = replay_kafka_spool(
                spool_path=spool_path,
                event_bus=FailingEventBus(fail_for_attempts=10),
                dry_run=False,
                confirm=True,
            )

            self.assertEqual(failing.replayed_count, 0)
            self.assertEqual(failing.failed_count, 1)
            self.assertEqual(failing.remaining_count, 1)
            self.assertEqual(failing.quarantined_count, 0)
            self.assertTrue(spool_path.exists())

            bus = InMemoryEventBus()
            replayed = replay_kafka_spool(
                spool_path=spool_path,
                event_bus=bus,
                dry_run=False,
                confirm=True,
            )

            self.assertEqual(replayed.replayed_count, 1)
            self.assertEqual(replayed.failed_count, 0)
            self.assertEqual(replayed.remaining_count, 0)
            self.assertEqual(replayed.quarantined_count, 0)
            self.assertTrue(replayed.deleted_spool)
            self.assertFalse(spool_path.exists())
            self.assertEqual(bus.records["raw_market_events_v1"][0]["value"]["event_id"], "raw-1")

    def test_replay_kafka_spool_reports_quarantined_corrupt_lines_on_dry_run(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            spool_path = root / "publish-failures.jsonl"
            spool_path.write_text(
                "\n".join(
                    [
                        "{bad json",
                        json.dumps(
                            {
                                "schema_version": 1,
                                "topic": "raw_market_events_v1",
                                "key": "00700.HK",
                                "value": raw_event("raw-1"),
                                "reason": "simulated failure",
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            result = replay_kafka_spool(
                spool_path=spool_path,
                event_bus=InMemoryEventBus(),
                dry_run=True,
            )

            self.assertEqual(result.remaining_count, 1)
            self.assertEqual(result.quarantined_count, 1)
            self.assertTrue(result.quarantine_path.exists())

    def test_replay_kafka_spool_preserves_order_and_rewrites_remaining_after_partial_failure(self) -> None:
        class FailOnSecondPublish(InMemoryEventBus):
            def __init__(self) -> None:
                super().__init__()
                self.attempts = 0

            def publish(self, topic: str, key: str, value: dict) -> None:
                self.attempts += 1
                if self.attempts == 2:
                    raise RuntimeError("second publish failed")
                super().publish(topic, key, value)

        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            spool_path = root / "publish-failures.jsonl"
            spool = FileBackedSpool(spool_path)
            spool.append("raw_market_events_v1", "00700.HK", raw_event("raw-1"), "simulated failure")
            spool.append("processed_market_events_v1", "00700.HK", raw_event("raw-2"), "simulated failure")
            spool.append("raw_market_events_v1", "00939.HK", raw_event("raw-3"), "simulated failure")

            result = replay_kafka_spool(
                spool_path=spool_path,
                event_bus=FailOnSecondPublish(),
                dry_run=False,
                confirm=True,
            )

            remaining = FileBackedSpool(spool_path).records
            self.assertEqual(result.replayed_count, 1)
            self.assertEqual(result.failed_count, 1)
            self.assertEqual(result.remaining_count, 2)
            self.assertFalse(result.deleted_spool)
            self.assertEqual([record.value["event_id"] for record in remaining], ["raw-2", "raw-3"])
            self.assertEqual([record.topic for record in remaining], ["processed_market_events_v1", "raw_market_events_v1"])

    def test_ops_cli_dry_runs_kafka_spool_replay_without_kafka_dependency(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            spool_path = root / "publish-failures.jsonl"
            FileBackedSpool(spool_path).append(
                "raw_market_events_v1",
                "00700.HK",
                raw_event("raw-1"),
                "simulated failure",
            )
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "replay-kafka-spool",
                        "--spool-path",
                        str(spool_path),
                        "--kafka-bootstrap-servers",
                        "localhost:9092",
                        "--dry-run",
                    ]
                )
            summary = json.loads(output.getvalue())

        self.assertEqual(exit_code, 0)
        self.assertTrue(summary["dry_run"])
        self.assertEqual(summary["remaining_count"], 1)
        self.assertEqual(summary["quarantined_count"], 0)
        self.assertTrue(summary["quarantine_path"].endswith("publish-failures.jsonl.quarantine"))
        self.assertFalse(summary["deleted_spool"])

    def test_ops_cli_dry_run_kafka_spool_replay_reports_quarantine(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            spool_path = root / "publish-failures.jsonl"
            spool_path.write_text("{bad json\n", encoding="utf-8")
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "replay-kafka-spool",
                        "--spool-path",
                        str(spool_path),
                        "--kafka-bootstrap-servers",
                        "localhost:9092",
                        "--dry-run",
                    ]
            )
            summary = json.loads(output.getvalue())
            quarantine_exists = Path(summary["quarantine_path"]).exists()

        self.assertEqual(exit_code, 0)
        self.assertTrue(summary["dry_run"])
        self.assertEqual(summary["remaining_count"], 0)
        self.assertEqual(summary["quarantined_count"], 1)
        self.assertTrue(quarantine_exists)

    def test_frontend_performance_import_rejects_wrong_key_or_invalid_value_without_partial_write(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_path = root / "frontend-performance.ndjson"
            input_path.write_text(
                "\n".join(
                    [
                        json.dumps({"key": "frontend_store_update_ms", "valueMs": 120}),
                        json.dumps({"key": "gateway_to_frontend_ms", "valueMs": 10}),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "must use key frontend_store_update_ms"):
                import_frontend_performance_samples(
                    input_path=input_path,
                    stream_directory=root / "streams",
                    session_id="session-1",
                    trading_date="20260522",
                    started_at="2026-05-22T09:30:00+08:00",
                )
            files = shadow_run_file_paths(root / "streams", trading_date="20260522", session_id="session-1")
            input_path.write_text(
                json.dumps({"key": "frontend_store_update_ms", "valueMs": -1}) + "\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "non-negative numeric valueMs"):
                import_frontend_performance_samples(
                    input_path=input_path,
                    stream_directory=root / "streams",
                    session_id="session-1",
                    trading_date="20260522",
                    started_at="2026-05-22T09:30:00+08:00",
                )

        self.assertFalse(files.performance_samples_path.exists())

    def test_ops_cli_generates_required_historical_manifests(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            silver_root = root / "silver"
            manifest_root = root / "manifests"
            silver_root.mkdir()
            write_silver_tables(silver_root)
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "generate-historical-manifests",
                        "--silver-root",
                        str(silver_root),
                        "--manifest-directory",
                        str(manifest_root),
                        "--start-date",
                        "20260522",
                        "--end-date",
                        "20260522",
                        "--symbols",
                        "00700.HK",
                        "--code-version",
                        "v2-prod",
                    ]
                )
            summary = json.loads(output.getvalue())
            manifests = sorted(manifest_root.glob("*.manifest.json"))
            broker_mapping = json.loads(
                (manifest_root / "broker_mapping.20260522-20260522.v2-prod.manifest.json").read_text(
                    encoding="utf-8"
                )
            )
            participant_history = json.loads(
                (manifest_root / "participant_history.20260522-20260522.v2-prod.manifest.json").read_text(
                    encoding="utf-8"
                )
            )

        self.assertEqual(exit_code, 0)
        self.assertTrue(summary["passed"])
        self.assertEqual(len(manifests), 7)
        self.assertEqual(summary["failed_data_types"], [])
        self.assertIn("participant_history", summary["data_types"])
        self.assertEqual(participant_history["source_data_type"], "ccass_holdings")
        self.assertEqual(broker_mapping["row_count"], 1)

    def test_clear_runtime_cache_is_scoped_and_requires_confirmation(self) -> None:
        redis = DeletableRedis(
            {
                "terminal:20260522:snapshot:00700.HK": "{}",
                "terminal:20260522:minute:00700.HK": "{}",
                "terminal:20260522:alerts:00700.HK": "{}",
                "terminal:20260522:queue:00700.HK": "{}",
                "terminal:20260522:state:00700.HK": "{}",
                "ccass:holding:00700.HK": "{}",
                "ccass:history:00700.HK:C00010": "{}",
                "terminal:20260522:snapshot:00939.HK": "{}",
            }
        )

        dry_run = clear_runtime_cache(
            redis_client=redis,
            trade_date="20260522",
            symbols=["00700.HK"],
            dry_run=True,
        )

        self.assertIn("ccass:history:00700.HK:C00010", dry_run.keys)
        self.assertIn("terminal:20260522:snapshot:00700.HK", dry_run.keys)
        self.assertIn("terminal:20260522:state:00700.HK", dry_run.keys)
        self.assertNotIn("terminal:20260522:snapshot:00939.HK", dry_run.keys)
        self.assertEqual(redis.deleted, [])
        with self.assertRaisesRegex(ValueError, "requires --confirm"):
            clear_runtime_cache(
                redis_client=redis,
                trade_date="20260522",
                symbols=["00700.HK"],
                dry_run=False,
            )

        deleted = clear_runtime_cache(
            redis_client=redis,
            trade_date="20260522",
            symbols=["00700.HK"],
            dry_run=False,
            confirm=True,
        )

        self.assertEqual(sorted(redis.deleted), deleted.deleted_keys)
        self.assertNotIn("ccass:history:00700.HK:*", deleted.deleted_keys)
        self.assertIn("terminal:20260522:snapshot:00939.HK", redis.values)

    def test_clear_runtime_cache_does_not_delete_unexpanded_history_wildcard(self) -> None:
        redis = DeletableRedis(
            {
                "terminal:20260522:snapshot:00700.HK": "{}",
                "ccass:holding:00700.HK": "{}",
            }
        )

        dry_run = clear_runtime_cache(
            redis_client=redis,
            trade_date="20260522",
            symbols=["00700.HK"],
            dry_run=True,
        )
        deleted = clear_runtime_cache(
            redis_client=redis,
            trade_date="20260522",
            symbols=["00700.HK"],
            dry_run=False,
            confirm=True,
        )

        self.assertIn("ccass:history:00700.HK:*", dry_run.keys)
        self.assertNotIn("ccass:history:00700.HK:*", deleted.deleted_keys)

    def test_clear_runtime_state_only_deletes_local_jsonl_when_explicit(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            state_dir = root / "20260522" / "00700.HK"
            state_dir.mkdir(parents=True)
            raw_path = state_dir / "raw-events.jsonl"
            callback_path = root / "20260522" / "callback-rejections.jsonl"
            dead_letter_path = root / "20260522" / "raw-consumer-dead-letters.jsonl"
            raw_path.write_text("{}\n", encoding="utf-8")
            callback_path.write_text("{}\n", encoding="utf-8")
            dead_letter_path.write_text("{}\n", encoding="utf-8")

            dry_run = clear_runtime_state(
                runtime_state_root=root,
                trade_date="20260522",
                symbols=["00700.HK"],
                dry_run=True,
            )
            self.assertTrue(raw_path.exists())
            self.assertIn(str(raw_path), dry_run.paths)
            self.assertNotIn(str(callback_path), dry_run.paths)
            self.assertNotIn(str(dead_letter_path), dry_run.paths)

            deleted = clear_runtime_state(
                runtime_state_root=root,
                trade_date="20260522",
                symbols=["00700.HK"],
                dry_run=False,
                confirm=True,
            )
            self.assertTrue(callback_path.exists())
            self.assertTrue(dead_letter_path.exists())

        self.assertIn(str(raw_path), deleted.deleted_paths)
        self.assertNotIn(str(callback_path), deleted.deleted_paths)
        self.assertNotIn(str(dead_letter_path), deleted.deleted_paths)

    def test_clear_runtime_state_requires_explicit_callback_rejection_scope(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            callback_path = root / "20260522" / "callback-rejections.jsonl"
            callback_path.parent.mkdir(parents=True)
            callback_path.write_text("{}\n", encoding="utf-8")

            dry_run = clear_runtime_state(
                runtime_state_root=root,
                trade_date="20260522",
                symbols=["00700.HK"],
                dry_run=True,
                include_callback_rejections=True,
            )
            deleted = clear_runtime_state(
                runtime_state_root=root,
                trade_date="20260522",
                symbols=["00700.HK"],
                dry_run=False,
                confirm=True,
                include_callback_rejections=True,
            )

        self.assertIn(str(callback_path), dry_run.paths)
        self.assertIn(str(callback_path), deleted.deleted_paths)

    def test_clear_runtime_state_requires_explicit_dead_letter_scope(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            dead_letter_path = root / "20260522" / "raw-consumer-dead-letters.jsonl"
            dead_letter_path.parent.mkdir(parents=True)
            dead_letter_path.write_text("{}\n", encoding="utf-8")

            dry_run = clear_runtime_state(
                runtime_state_root=root,
                trade_date="20260522",
                symbols=["00700.HK"],
                dry_run=True,
                include_dead_letters=True,
            )
            deleted = clear_runtime_state(
                runtime_state_root=root,
                trade_date="20260522",
                symbols=["00700.HK"],
                dry_run=False,
                confirm=True,
                include_dead_letters=True,
            )

        self.assertIn(str(dead_letter_path), dry_run.paths)
        self.assertIn(str(dead_letter_path), deleted.deleted_paths)

    def test_ops_cli_clear_runtime_state_can_include_dead_letters(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            dead_letter_path = root / "20260522" / "raw-consumer-dead-letters.jsonl"
            dead_letter_path.parent.mkdir(parents=True)
            dead_letter_path.write_text("{}\n", encoding="utf-8")
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "clear-runtime-state",
                        "--runtime-state-root",
                        str(root),
                        "--trade-date",
                        "20260522",
                        "--symbols",
                        "00700.HK",
                        "--include-dead-letters",
                        "--confirm",
                    ]
                )
            summary = json.loads(output.getvalue())

        self.assertEqual(exit_code, 0)
        self.assertFalse(dead_letter_path.exists())
        self.assertIn(str(dead_letter_path), summary["deleted_paths"])

    def test_ops_cli_writes_frontend_deployment_evidence(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            env_path = root / ".env.cutover"
            readiness = {
                "schema_version": 1,
                "passed": True,
                "frontend_default_v2_allowed": True,
                "legacy_retirement_allowed": False,
                "blockers": [],
                "legacy_retirement_blockers": ["legacy_retirement_requires_operator_approval"],
                "report_count": 1,
                "accepted_report_ids": ["session-1"],
                "rejected_reports": [],
                "policy": {
                    "min_parallel_session_count": 1,
                    "min_session_duration_seconds": 14400,
                    "min_stream_coverage_ratio": 0.9,
                    "require_non_empty_streams": True,
                    "require_no_failed_symbols": True,
                    "allow_legacy_retirement": False,
                },
            }
            env_path.write_text(
                "\n".join(
                    [
                        "VITE_MARKET_DATA_MODE=auto",
                        "VITE_MARKET_WS_URL=ws://gateway.internal:9020/ws",
                        "VITE_MARKET_PROTOCOL=terminal-message-v1",
                        f"VITE_MARKET_CUTOVER_READINESS={json.dumps(readiness)}",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "verify-frontend-deployment",
                        "--env-path",
                        str(env_path),
                        "--expected-live-url",
                        "ws://gateway.internal:9020/ws",
                        "--output-path",
                        str(root / "frontend-deployment.json"),
                    ]
                )
            summary = json.loads(output.getvalue())

        self.assertEqual(exit_code, 0)
        self.assertTrue(summary["frontend_default_v2_deployed"])
        self.assertEqual(summary["blockers"], [])

    def test_ops_cli_verifies_runtime_health_evidence(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_json(root / "runtime-health.json", runtime_health_payload())
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "verify-runtime-health",
                        "--runtime-health-path",
                        str(root / "runtime-health.json"),
                        "--output-path",
                        str(root / "runtime-health-verification.json"),
                    ]
                )
            summary = json.loads(output.getvalue())
            persisted = json.loads((root / "runtime-health-verification.json").read_text(encoding="utf-8"))

        self.assertEqual(exit_code, 0)
        self.assertTrue(summary["passed"])
        self.assertTrue(persisted["passed"])
        self.assertEqual(summary["blockers"], [])

    def test_ops_cli_returns_nonzero_for_failed_runtime_health_verification(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            payload = runtime_health_payload()
            payload["topics"]["raw_market_events_v1"]["lag"] = 1
            write_json(root / "runtime-health.json", payload)
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "verify-runtime-health",
                        "--runtime-health-path",
                        str(root / "runtime-health.json"),
                        "--output-path",
                        str(root / "runtime-health-verification.json"),
                    ]
                )
            summary = json.loads(output.getvalue())
            persisted = json.loads((root / "runtime-health-verification.json").read_text(encoding="utf-8"))

        self.assertEqual(exit_code, 1)
        self.assertFalse(summary["passed"])
        self.assertFalse(persisted["passed"])
        self.assertIn("runtime_health_kafka_lag_present", summary["blockers"])

    def test_ops_cli_rejects_runtime_health_with_quarantined_kafka_spool_records(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            payload = runtime_health_payload()
            payload["producer"]["quarantined_spool_records"] = 1
            write_json(root / "runtime-health.json", payload)
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "verify-runtime-health",
                        "--runtime-health-path",
                        str(root / "runtime-health.json"),
                        "--output-path",
                        str(root / "runtime-health-verification.json"),
                    ]
                )
            summary = json.loads(output.getvalue())

        self.assertEqual(exit_code, 1)
        self.assertFalse(summary["passed"])
        self.assertIn("runtime_health_producer_quarantined_spool_records_present", summary["blockers"])

    def test_ops_cli_rejects_runtime_health_without_kafka_spool_artifact_paths(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            payload = runtime_health_payload()
            payload["producer"]["spool_path"] = ""
            payload["producer"]["spool_quarantine_path"] = "artifacts/runtime-state/kafka-spool/not-a-quarantine.jsonl"
            write_json(root / "runtime-health.json", payload)
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "verify-runtime-health",
                        "--runtime-health-path",
                        str(root / "runtime-health.json"),
                        "--output-path",
                        str(root / "runtime-health-verification.json"),
                    ]
                )
            summary = json.loads(output.getvalue())

        self.assertEqual(exit_code, 1)
        self.assertFalse(summary["passed"])
        self.assertIn("runtime_health_producer_spool_path_invalid", summary["blockers"])
        self.assertIn("runtime_health_producer_spool_quarantine_path_invalid", summary["blockers"])

    def test_ops_cli_verifies_runtime_config_artifact(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            silver_root = root / "silver"
            silver_root.mkdir()
            config = runtime_config_payload(silver_root)
            write_json(root / "runtime-config.json", config)
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "verify-runtime-config",
                        "--config-path",
                        str(root / "runtime-config.json"),
                        "--output-path",
                        str(root / "runtime-config-verification.json"),
                    ]
                )
            summary = json.loads(output.getvalue())
            persisted = json.loads((root / "runtime-config-verification.json").read_text(encoding="utf-8"))
            runtime_config = runtime_config_from_artifact(config)

        self.assertEqual(exit_code, 0)
        self.assertTrue(summary["passed"])
        self.assertTrue(persisted["passed"])
        self.assertEqual(summary["blockers"], [])
        self.assertEqual(runtime_config.trade_date, "20260522")
        self.assertEqual(runtime_config.kafka.raw_topic, "raw_market_events_v1")
        self.assertEqual(runtime_config.redis.terminal_ttl_seconds, 28800)
        self.assertEqual(runtime_config.gateway_host, "0.0.0.0")
        self.assertEqual(runtime_config.kafka_spool_dir, "artifacts/runtime-state/kafka-spool")
        self.assertEqual(runtime_config.big_trade_volume_baseline_ratio, 0.0005)

    def test_runtime_config_loader_rejects_unverified_artifacts(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            silver_root = root / "silver"
            silver_root.mkdir()
            config = runtime_config_payload(silver_root)
            config["runtime"]["big_trade_turnover_threshold"] = 50_000_000

            with self.assertRaisesRegex(ValueError, "runtime_config_big_trade_turnover_threshold_deprecated"):
                runtime_config_from_artifact(config)

    def test_runtime_config_verification_treats_processed_topic_as_optional_shadow_path(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            silver_root = root / "silver"
            silver_root.mkdir()
            config = runtime_config_payload(silver_root)
            config["kafka"].pop("processed_topic")
            missing_result = evaluate_runtime_config_artifact(config)
            config["kafka"]["processed_topic"] = "legacy_processed"
            mismatched_result = evaluate_runtime_config_artifact(config)

        self.assertTrue(missing_result["passed"])
        self.assertNotIn("runtime_config_processed_topic_mismatch", missing_result["blockers"])
        self.assertFalse(mismatched_result["passed"])
        self.assertIn("runtime_config_processed_topic_mismatch", mismatched_result["blockers"])

    def test_runtime_config_verification_rejects_unsafe_or_incomplete_config(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            missing_silver = root / "missing-silver"
            config = runtime_config_payload(missing_silver)
            missing_silver.rmdir()
            config["kafka"]["raw_topic"] = "legacy_ticks"
            config["runtime"]["install_signal_handlers"] = False
            config["runtime"]["big_trade_volume_baseline_ratio"] = 0
            config["runtime"]["big_trade_turnover_threshold"] = 50_000_000
            config["gateway"]["host"] = "127.0.0.1"
            config["production_clients"]["kafka_consumer"] = False
            config["redis"]["password"] = "do-not-store-secrets"

            result = evaluate_runtime_config_artifact(config)

        self.assertFalse(result["passed"])
        self.assertIn("runtime_config_silver_root_missing_on_disk", result["blockers"])
        self.assertIn("runtime_config_raw_topic_mismatch", result["blockers"])
        self.assertIn("runtime_config_signal_handlers_not_enabled", result["blockers"])
        self.assertIn("runtime_config_big_trade_volume_ratio_invalid", result["blockers"])
        self.assertIn("runtime_config_big_trade_turnover_threshold_deprecated", result["blockers"])
        self.assertIn("runtime_config_gateway_host_loopback", result["blockers"])
        self.assertIn("runtime_config_production_clients_missing", result["blockers"])
        self.assertIn("runtime_config_contains_secret_like_keys", result["blockers"])

    def test_ops_cli_returns_nonzero_for_failed_runtime_config_verification(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            silver_root = root / "silver"
            silver_root.mkdir()
            config = runtime_config_payload(silver_root)
            config["runtime"]["big_trade_turnover_threshold"] = 50_000_000
            write_json(root / "runtime-config.json", config)
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "verify-runtime-config",
                        "--config-path",
                        str(root / "runtime-config.json"),
                        "--output-path",
                        str(root / "runtime-config-verification.json"),
                    ]
                )
            summary = json.loads(output.getvalue())
            persisted = json.loads((root / "runtime-config-verification.json").read_text(encoding="utf-8"))

        self.assertEqual(exit_code, 1)
        self.assertFalse(summary["passed"])
        self.assertFalse(persisted["passed"])
        self.assertIn("runtime_config_big_trade_turnover_threshold_deprecated", summary["blockers"])

    def test_ops_cli_writes_legacy_decommission_evidence(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            observation_path = root / "legacy-observation.json"
            observation_path.write_text(
                json.dumps(
                    {
                        "observed_at": "2026-05-22T16:15:00+08:00",
                        "legacy_websocket_enabled": False,
                        "old_topic_consumers": {"legacy_ticks": 0, "legacy_broker_queue": 0},
                        "old_topic_lag": {"legacy_ticks": 0, "legacy_broker_queue": 0},
                    }
                ),
                encoding="utf-8",
            )
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "verify-legacy-decommission",
                        "--observation-path",
                        str(observation_path),
                        "--output-path",
                        str(root / "legacy-decommission.json"),
                        "--expected-old-topics",
                        "legacy_ticks,legacy_broker_queue",
                    ]
                )
            summary = json.loads(output.getvalue())

        self.assertEqual(exit_code, 0)
        self.assertTrue(summary["legacy_websocket_disabled"])
        self.assertTrue(summary["old_topic_consumers_disabled"])
        self.assertTrue(summary["no_legacy_consumers_observed"])

    def test_ops_cli_writes_multi_trader_smoke_evidence(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            observation_path = root / "multi-trader-smoke-observation.json"
            output_path = root / "multi-trader-smoke-evidence.json"
            observation_path.write_text(json.dumps(multi_trader_smoke_payload()), encoding="utf-8")
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "verify-multi-trader-smoke",
                        "--observation-path",
                        str(observation_path),
                        "--output-path",
                        str(output_path),
                    ]
                )
            summary = json.loads(output.getvalue())
            persisted = json.loads(output_path.read_text(encoding="utf-8"))

        self.assertEqual(exit_code, 0)
        self.assertTrue(summary["passed"])
        self.assertTrue(persisted["passed"])
        self.assertEqual(persisted["watchlist_overlap"], ["00700.HK"])

    def test_ops_cli_returns_nonzero_for_failed_multi_trader_smoke(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            payload = multi_trader_smoke_payload()
            payload["runtime_health"] = {"passed": False}
            observation_path = root / "multi-trader-smoke-observation.json"
            output_path = root / "multi-trader-smoke-evidence.json"
            observation_path.write_text(json.dumps(payload), encoding="utf-8")
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "verify-multi-trader-smoke",
                        "--observation-path",
                        str(observation_path),
                        "--output-path",
                        str(output_path),
                    ]
                )
            summary = json.loads(output.getvalue())
            persisted = json.loads(output_path.read_text(encoding="utf-8"))

        self.assertEqual(exit_code, 1)
        self.assertFalse(summary["passed"])
        self.assertFalse(persisted["passed"])
        self.assertIn("multi_trader_smoke_runtime_health_evidence_missing", summary["blockers"])

    def test_builds_multi_trader_smoke_observation_from_collected_artifacts(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            payload = multi_trader_smoke_payload()
            clients_dir = root / "clients"
            workflows_path = root / "workflows.json"
            runtime_health_path = root / "runtime-health-verification.json"
            performance_dir = root / "performance"
            output_path = root / "multi-trader-smoke-observation.json"
            clients_dir.mkdir()
            performance_dir.mkdir()
            for index, client in enumerate(payload["clients"], start=1):
                (clients_dir / f"desk-{index}.json").write_text(
                    json.dumps(client_smoke_artifact(client)),
                    encoding="utf-8",
                )
            workflows_path.write_text(json.dumps({"workflows": payload["workflows"]}), encoding="utf-8")
            runtime_health_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "passed": True,
                        "blockers": [],
                        "evidence": {
                            "symbol_runtime": {
                                "00700.HK": {"hydrate_count": 1},
                                "00939.HK": {"hydrate_count": 1},
                                "00005.HK": {"hydrate_count": 1},
                            },
                            "symbol_runtime_manager": {
                                "active_hydrations": 0,
                                "max_concurrent_hydrations": 8,
                                "capacity_rejections": 0,
                                "hydrating_symbols": [],
                            },
                            "gateway_websocket": gateway_smoke_evidence(),
                            "gateway_activity": {"client_queue": gateway_client_queue_payload()},
                        },
                    }
                ),
                encoding="utf-8",
            )
            (performance_dir / "desk-a.json").write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "exported_at": "2026-05-25T11:00:00+08:00",
                        "machine_id": "desk-a",
                        "performance_samples": {"subscribe_snapshot_ms": [120.0]},
                        "raw_samples": [
                            {"key": "frontend_store_update_ms", "valueMs": 2.0},
                            {"key": "subscribe_snapshot_ms", "valueMs": 120.0},
                        ],
                    }
                ),
                encoding="utf-8",
            )
            (performance_dir / "desk-b.json").write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "exported_at": "2026-05-25T11:00:01+08:00",
                        "machine_id": "desk-b",
                        "performance_samples": {"subscribe_snapshot_ms": [80.0]},
                    }
                ),
                encoding="utf-8",
            )

            result = build_multi_trader_smoke_observation(
                clients_path=clients_dir,
                workflows_path=workflows_path,
                runtime_health_path=runtime_health_path,
                performance_samples_path=performance_dir,
                observed_at="2026-05-25T11:00:00+08:00",
                output_path=output_path,
            )
            persisted = json.loads(output_path.read_text(encoding="utf-8"))

        self.assertEqual(result.output_path, output_path)
        self.assertEqual(persisted["clients"], payload["clients"])
        self.assertEqual(persisted["workflows"], payload["workflows"])
        self.assertTrue(persisted["runtime_health"]["passed"])
        self.assertEqual(persisted["runtime_health"]["path"], str(runtime_health_path))
        self.assertEqual(persisted["runtime_health"]["symbol_runtime"]["00700.HK"]["hydrate_count"], 1)
        self.assertEqual(persisted["runtime_health"]["symbol_runtime_manager"]["active_hydrations"], 0)
        self.assertEqual(persisted["runtime_health"]["gateway_activity"]["client_queue"]["observed_client_count"], 2)
        self.assertEqual(persisted["performance_samples"]["subscribe_snapshot_ms"], [120.0, 80.0])
        self.assertEqual(
            persisted["performance_artifacts"],
            [
                {
                    "path": str(performance_dir / "desk-a.json"),
                    "machine_id": "desk-a",
                    "exported_at": "2026-05-25T11:00:00+08:00",
                    "subscribe_snapshot_count": 1,
                },
                {
                    "path": str(performance_dir / "desk-b.json"),
                    "machine_id": "desk-b",
                    "exported_at": "2026-05-25T11:00:01+08:00",
                    "subscribe_snapshot_count": 1,
                },
            ],
        )

    def test_build_multi_trader_smoke_observation_rejects_raw_performance_samples_file(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            payload = multi_trader_smoke_payload()
            clients_dir = root / "clients"
            workflows_path = root / "workflows.json"
            runtime_health_path = root / "runtime-health-verification.json"
            performance_path = root / "performance.json"
            output_path = root / "multi-trader-smoke-observation.json"
            clients_dir.mkdir()
            for index, client in enumerate(payload["clients"], start=1):
                write_json(clients_dir / f"desk-{index}.json", client_smoke_artifact(client))
            write_json(workflows_path, {"workflows": payload["workflows"]})
            write_json(runtime_health_path, real_data_runner_smoke_health_payload())
            performance_path.write_text(
                json.dumps([{"key": "gateway_subscribe_snapshot_ms", "value_ms": 80.0}]),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "performance smoke artifact must be a JSON object"):
                build_multi_trader_smoke_observation(
                    clients_path=clients_dir,
                    workflows_path=workflows_path,
                    runtime_health_path=runtime_health_path,
                    performance_samples_path=performance_path,
                    observed_at="2026-05-25T11:00:00+08:00",
                    output_path=output_path,
                )

    def test_build_multi_trader_smoke_observation_rejects_invalid_observed_at_before_writing(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            payload = multi_trader_smoke_payload()
            clients_path = root / "clients.json"
            workflows_path = root / "workflows.json"
            runtime_health_path = root / "runtime-health.json"
            output_path = root / "multi-trader-smoke-observation.json"
            write_json(clients_path, payload["clients"])
            write_json(workflows_path, {"workflows": payload["workflows"]})
            write_json(runtime_health_path, runtime_health_payload())

            with self.assertRaisesRegex(ValueError, "observed_at must be an ISO datetime"):
                build_multi_trader_smoke_observation(
                    clients_path=clients_path,
                    workflows_path=workflows_path,
                    runtime_health_path=runtime_health_path,
                    observed_at="20260525 110000",
                    output_path=output_path,
                )

            self.assertFalse(output_path.exists())

    def test_builds_multi_trader_smoke_observation_from_runtime_health_performance_samples(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            payload = multi_trader_smoke_payload()
            clients_path = root / "clients.json"
            workflows_path = root / "workflows.json"
            runtime_health_path = root / "runtime-health.json"
            output_path = root / "multi-trader-smoke-observation.json"
            clients_path.write_text(json.dumps(payload["clients"]), encoding="utf-8")
            workflows_path.write_text(json.dumps(payload["workflows"]), encoding="utf-8")
            runtime_health = runtime_health_payload()
            runtime_health["performance_samples"] = {"subscribe_snapshot_ms": [70.0, 90.0, 110.0]}
            runtime_health_path.write_text(json.dumps(runtime_health), encoding="utf-8")

            result = build_multi_trader_smoke_observation(
                clients_path=clients_path,
                workflows_path=workflows_path,
                runtime_health_path=runtime_health_path,
                observed_at="2026-05-25T11:00:00+08:00",
                output_path=output_path,
            )
            persisted = json.loads(output_path.read_text(encoding="utf-8"))

        self.assertEqual(result.output_path, output_path)
        self.assertEqual(persisted["performance_samples"]["subscribe_snapshot_ms"], [70.0, 90.0, 110.0])
        self.assertEqual(persisted["runtime_health"]["performance_samples"]["subscribe_snapshot_ms"], [70.0, 90.0, 110.0])
        self.assertTrue(persisted["runtime_health"]["passed"])

    def test_builds_multi_trader_smoke_observation_from_runtime_health_verification_p95(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            payload = multi_trader_smoke_payload()
            clients_path = root / "clients.json"
            workflows_path = root / "workflows.json"
            runtime_health_path = root / "runtime-health-verification.json"
            output_path = root / "multi-trader-smoke-observation.json"
            clients_path.write_text(json.dumps(payload["clients"]), encoding="utf-8")
            workflows_path.write_text(json.dumps(payload["workflows"]), encoding="utf-8")
            runtime_health_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "passed": True,
                        "blockers": [],
                        "evidence": {
                            "symbol_runtime": {
                                "00700.HK": {"hydrate_count": 1},
                                "00939.HK": {"hydrate_count": 1},
                                "00005.HK": {"hydrate_count": 1},
                            },
                            "symbol_runtime_manager": {
                                "active_hydrations": 0,
                                "max_concurrent_hydrations": 8,
                                "capacity_rejections": 0,
                                "hydrating_symbols": [],
                            },
                            "gateway_websocket": gateway_smoke_evidence(),
                            "performance_samples": {
                                "sample_counts": {"subscribe_snapshot_ms": 3},
                                "subscribe_snapshot_p95_ms": 96.0,
                            },
                        },
                    }
                ),
                encoding="utf-8",
            )

            build_multi_trader_smoke_observation(
                clients_path=clients_path,
                workflows_path=workflows_path,
                runtime_health_path=runtime_health_path,
                observed_at="2026-05-25T11:00:00+08:00",
                output_path=output_path,
            )
            persisted = json.loads(output_path.read_text(encoding="utf-8"))

        self.assertEqual(persisted["metrics"]["warm_snapshot_p95_ms"], 96.0)
        self.assertEqual(persisted["runtime_health"]["performance_metrics"]["warm_snapshot_p95_ms"], 96.0)

    def test_ops_cli_builds_multi_trader_smoke_observation(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            payload = multi_trader_smoke_payload()
            clients_path = root / "clients.json"
            second_clients_path = root / "clients-desk-b.json"
            workflows_path = root / "workflows.json"
            runtime_health_path = root / "runtime-health.json"
            output_path = root / "multi-trader-smoke-observation.json"
            clients_path.write_text(json.dumps([payload["clients"][0]]), encoding="utf-8")
            second_clients_path.write_text(json.dumps(client_smoke_artifact(payload["clients"][1])), encoding="utf-8")
            workflows_path.write_text(json.dumps(payload["workflows"]), encoding="utf-8")
            runtime_health_path.write_text(
                json.dumps(
                    {
                        "passed": True,
                        "symbol_runtime": {"00700.HK": {"hydrate_count": 1}},
                        "symbol_runtime_manager": {
                            "active_hydrations": 0,
                            "max_concurrent_hydrations": 8,
                            "capacity_rejections": 0,
                            "hydrating_symbols": [],
                        },
                        "gateway_websocket": gateway_smoke_evidence(),
                    }
                ),
                encoding="utf-8",
            )
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "build-multi-trader-smoke-observation",
                        "--clients-path",
                        f"{clients_path},{second_clients_path}",
                        "--workflows-path",
                        str(workflows_path),
                        "--runtime-health-path",
                        str(runtime_health_path),
                        "--observed-at",
                        "2026-05-25T11:00:00+08:00",
                        "--output-path",
                        str(output_path),
                    ]
                )
            summary = json.loads(output.getvalue())
            persisted = json.loads(output_path.read_text(encoding="utf-8"))

        self.assertEqual(exit_code, 0)
        self.assertEqual(summary["client_count"], 2)
        self.assertEqual(summary["workflow_count"], 6)
        self.assertTrue(summary["runtime_health_passed"])
        self.assertEqual(persisted["observed_at"], "2026-05-25T11:00:00+08:00")

    def test_ops_cli_build_multi_trader_smoke_observation_rejects_invalid_observed_at(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            payload = multi_trader_smoke_payload()
            clients_path = root / "clients.json"
            workflows_path = root / "workflows.json"
            runtime_health_path = root / "runtime-health.json"
            output_path = root / "multi-trader-smoke-observation.json"
            write_json(clients_path, payload["clients"])
            write_json(workflows_path, {"workflows": payload["workflows"]})
            write_json(runtime_health_path, runtime_health_payload())
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "build-multi-trader-smoke-observation",
                        "--clients-path",
                        str(clients_path),
                        "--workflows-path",
                        str(workflows_path),
                        "--runtime-health-path",
                        str(runtime_health_path),
                        "--observed-at",
                        "20260525 110000",
                        "--output-path",
                        str(output_path),
                    ]
                )
            summary = json.loads(output.getvalue())

        self.assertEqual(exit_code, 1)
        self.assertFalse(summary["passed"])
        self.assertEqual(summary["error"], "observed_at must be an ISO datetime")
        self.assertFalse(output_path.exists())

    def test_ops_cli_builds_and_verifies_smoke_observation_from_real_data_runner_health(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            payload = multi_trader_smoke_payload()
            clients_dir = root / "clients"
            clients_dir.mkdir()
            for index, client in enumerate(payload["clients"], start=1):
                (clients_dir / f"desk-{index}.json").write_text(
                    json.dumps(client_smoke_artifact(client)),
                    encoding="utf-8",
                )
            workflows_path = root / "workflows.json"
            runtime_health_path = root / "runtime-health-verification.json"
            preflight_path = root / "lan-preflight.json"
            observation_path = root / "multi-trader-smoke-observation.json"
            evidence_path = root / "multi-trader-smoke-evidence.json"
            workflows_path.write_text(json.dumps({"workflows": payload["workflows"]}), encoding="utf-8")
            runtime_health_path.write_text(json.dumps(real_data_runner_smoke_health_payload()), encoding="utf-8")
            preflight_path.write_text(json.dumps(real_data_runner_smoke_preflight_payload()), encoding="utf-8")

            build_output = StringIO()
            with redirect_stdout(build_output):
                build_exit_code = ops_main(
                    [
                        "build-multi-trader-smoke-observation",
                        "--clients-path",
                        str(clients_dir),
                        "--workflows-path",
                        str(workflows_path),
                        "--runtime-health-path",
                        str(runtime_health_path),
                        "--preflight-path",
                        str(preflight_path),
                        "--observed-at",
                        "2026-05-25T11:00:00+08:00",
                        "--output-path",
                        str(observation_path),
                    ]
                )
            verify_output = StringIO()
            with redirect_stdout(verify_output):
                verify_exit_code = ops_main(
                    [
                        "verify-multi-trader-smoke",
                        "--observation-path",
                        str(observation_path),
                        "--output-path",
                        str(evidence_path),
                    ]
                )
            observation = json.loads(observation_path.read_text(encoding="utf-8"))
            evidence = json.loads(evidence_path.read_text(encoding="utf-8"))

        self.assertEqual(build_exit_code, 0)
        self.assertEqual(verify_exit_code, 0)
        self.assertEqual(observation["runtime_health"]["path"], str(runtime_health_path))
        self.assertEqual(observation["runtime_health"]["generated_at"], "2026-05-25T11:00:00+08:00")
        self.assertEqual(observation["runtime_health"]["gateway_activity"]["client_queue"]["observed_client_count"], 2)
        self.assertEqual(observation["performance_samples"]["subscribe_snapshot_ms"], [80.0, 100.0, 120.0])
        self.assertTrue(evidence["passed"])
        self.assertEqual(evidence["gateway_client_activity"]["observed_client_count"], 2)
        self.assertEqual(evidence["metrics"]["warm_snapshot_p95_ms"], 118.0)
        self.assertEqual(evidence["metrics"]["duplicate_hydrations"], 0)

    def test_finalizes_multi_trader_smoke_from_prepared_directory(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            payload = multi_trader_smoke_payload()
            clients_dir = root / "clients"
            clients_dir.mkdir()
            for index, client in enumerate(payload["clients"], start=1):
                (clients_dir / f"desk-{index}.json").write_text(
                    json.dumps(client_smoke_artifact(client)),
                    encoding="utf-8",
                )
            (root / "workflows.json").write_text(json.dumps({"workflows": payload["workflows"]}), encoding="utf-8")
            write_real_data_runner_smoke_preflight_artifacts(root)
            (root / "runtime-health-verification.json").write_text(
                json.dumps(real_data_runner_smoke_health_payload()),
                encoding="utf-8",
            )

            result = finalize_multi_trader_smoke(
                root_path=root,
                observed_at="2026-05-25T11:00:00+08:00",
            )
            observation = json.loads(result.observation_path.read_text(encoding="utf-8"))
            evidence = json.loads(result.evidence_path.read_text(encoding="utf-8"))
            manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))

        self.assertTrue(result.evidence["passed"])
        self.assertEqual(observation["observed_at"], "2026-05-25T11:00:00+08:00")
        self.assertEqual(observation["preflight"]["gateway_url"], "ws://192.168.1.10:9020/ws")
        self.assertEqual(observation["performance_samples"]["subscribe_snapshot_ms"], [80.0, 100.0, 120.0])
        self.assertTrue(evidence["passed"])
        self.assertTrue(evidence["preflight"]["present"])
        self.assertEqual(manifest["schema_version"], 1)
        self.assertIn("multi-trader-smoke-evidence.json", {item["path"] for item in manifest["files"]})
        self.assertNotIn("smoke-run-manifest.json", {item["path"] for item in manifest["files"]})
        self.assertTrue(all(len(item["sha256"]) == 64 for item in manifest["files"]))

    def test_finalize_multi_trader_smoke_rejects_invalid_observed_at_before_observation(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            payload = multi_trader_smoke_payload()
            clients_dir = root / "clients"
            clients_dir.mkdir()
            for index, client in enumerate(payload["clients"], start=1):
                write_json(clients_dir / f"desk-{index}.json", client_smoke_artifact(client))
            write_json(root / "workflows.json", {"workflows": payload["workflows"]})
            write_real_data_runner_smoke_preflight_artifacts(root)
            write_json(root / "runtime-health-verification.json", real_data_runner_smoke_health_payload())

            result = finalize_multi_trader_smoke(
                root_path=root,
                observed_at="20260525 110000",
            )
            evidence = json.loads(result.evidence_path.read_text(encoding="utf-8"))

        self.assertFalse(result.evidence["passed"])
        self.assertEqual(evidence["blockers"], ["multi_trader_smoke_observed_at_invalid"])
        self.assertFalse(result.observation_path.exists())

    def test_finalizes_multi_trader_smoke_uses_client_embedded_performance_when_directory_empty(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            payload = multi_trader_smoke_payload()
            clients_dir = root / "clients"
            clients_dir.mkdir()
            (root / "performance").mkdir()
            for index, client in enumerate(payload["clients"], start=1):
                (clients_dir / f"desk-{index}.json").write_text(
                    json.dumps(
                        {
                            **client_smoke_artifact(client),
                            "performance_samples": {"subscribe_snapshot_ms": [40.0 + index]},
                        }
                    ),
                    encoding="utf-8",
                )
            (root / "workflows.json").write_text(json.dumps({"workflows": payload["workflows"]}), encoding="utf-8")
            write_real_data_runner_smoke_preflight_artifacts(root)
            runtime_health = real_data_runner_smoke_health_payload()
            runtime_health["performance_samples"] = {"subscribe_snapshot_ms": [80.0]}
            (root / "runtime-health-verification.json").write_text(json.dumps(runtime_health), encoding="utf-8")

            result = finalize_multi_trader_smoke(
                root_path=root,
                observed_at="2026-05-25T11:00:00+08:00",
            )
            observation = json.loads(result.observation_path.read_text(encoding="utf-8"))

        self.assertTrue(result.evidence["passed"])
        self.assertEqual(observation["performance_samples"]["subscribe_snapshot_ms"], [41.0, 42.0, 80.0])
        self.assertEqual(
            observation["performance_artifacts"],
            [
                {
                    "path": str(clients_dir / "desk-1.json"),
                    "machine_id": "desk-a",
                    "exported_at": "2026-05-25T10:59:00+08:00",
                    "subscribe_snapshot_count": 1,
                },
                {
                    "path": str(clients_dir / "desk-2.json"),
                    "machine_id": "desk-b",
                    "exported_at": "2026-05-25T10:59:00+08:00",
                    "subscribe_snapshot_count": 1,
                },
            ],
        )

    def test_ops_cli_finalizes_multi_trader_smoke(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            payload = multi_trader_smoke_payload()
            clients_dir = root / "clients"
            clients_dir.mkdir()
            for index, client in enumerate(payload["clients"], start=1):
                (clients_dir / f"desk-{index}.json").write_text(
                    json.dumps(client_smoke_artifact(client)),
                    encoding="utf-8",
                )
            (root / "workflows.json").write_text(json.dumps({"workflows": payload["workflows"]}), encoding="utf-8")
            write_real_data_runner_smoke_preflight_artifacts(root)
            (root / "runtime-health-verification.json").write_text(
                json.dumps(real_data_runner_smoke_health_payload()),
                encoding="utf-8",
            )
            write_json(
                root / "smoke-import-manifest.json",
                smoke_import_manifest_payload("clients/desk-1.json", "clients/desk-2.json"),
            )
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "finalize-multi-trader-smoke",
                        "--root-path",
                        str(root),
                        "--observed-at",
                        "2026-05-25T11:00:00+08:00",
                    ]
                )
            summary = json.loads(output.getvalue())
            observation_exists = (root / "multi-trader-smoke-observation.json").exists()
            evidence_exists = (root / "multi-trader-smoke-evidence.json").exists()
            manifest_exists = (root / "smoke-run-manifest.json").exists()

        self.assertEqual(exit_code, 0)
        self.assertTrue(summary["passed"])
        self.assertEqual(summary["manifest_path"], str(root / "smoke-run-manifest.json"))
        self.assertTrue(observation_exists)
        self.assertTrue(evidence_exists)
        self.assertTrue(manifest_exists)

    def test_ops_cli_finalizes_and_packages_multi_trader_smoke(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            payload = multi_trader_smoke_payload()
            clients_dir = root / "clients"
            clients_dir.mkdir()
            for index, client in enumerate(payload["clients"], start=1):
                (clients_dir / f"desk-{index}.json").write_text(
                    json.dumps(client_smoke_artifact(client)),
                    encoding="utf-8",
                )
            (root / "workflows.json").write_text(json.dumps({"workflows": payload["workflows"]}), encoding="utf-8")
            write_real_data_runner_smoke_preflight_artifacts(root)
            (root / "runtime-health-verification.json").write_text(
                json.dumps(real_data_runner_smoke_health_payload()),
                encoding="utf-8",
            )
            write_json(
                root / "smoke-import-manifest.json",
                smoke_import_manifest_payload("clients/desk-1.json", "clients/desk-2.json"),
            )
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "finalize-multi-trader-smoke",
                        "--root-path",
                        str(root),
                        "--observed-at",
                        "2026-05-25T11:00:00+08:00",
                        "--package",
                    ]
                )
            summary = json.loads(output.getvalue())
            metadata = json.loads((root / "smoke-run-package.json").read_text(encoding="utf-8"))
            with zipfile.ZipFile(root / "multi-trader-smoke-evidence.zip") as archive:
                names = set(archive.namelist())

        self.assertEqual(exit_code, 0)
        self.assertTrue(summary["passed"])
        self.assertEqual(summary["package_path"], str(root / "multi-trader-smoke-evidence.zip"))
        self.assertEqual(summary["package_metadata_path"], str(root / "smoke-run-package.json"))
        self.assertEqual(summary["package_sha256"], metadata["sha256"])
        self.assertIn("multi-trader-smoke-evidence.json", names)
        self.assertIn("multi-trader-smoke-observation.json", names)
        self.assertIn("smoke-run-manifest.json", names)
        self.assertIn("smoke-import-manifest.json", names)

    def test_ops_cli_finalize_package_is_rerunnable_after_package_metadata_exists(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            payload = multi_trader_smoke_payload()
            clients_dir = root / "clients"
            clients_dir.mkdir()
            for index, client in enumerate(payload["clients"], start=1):
                write_json(clients_dir / f"desk-{index}.json", client_smoke_artifact(client))
            write_json(root / "workflows.json", {"workflows": payload["workflows"]})
            write_real_data_runner_smoke_preflight_artifacts(root)
            write_json(root / "runtime-health-verification.json", real_data_runner_smoke_health_payload())
            write_json(
                root / "smoke-import-manifest.json",
                smoke_import_manifest_payload("clients/desk-1.json", "clients/desk-2.json"),
            )
            first_output = StringIO()

            with redirect_stdout(first_output):
                first_exit_code = ops_main(
                    [
                        "finalize-multi-trader-smoke",
                        "--root-path",
                        str(root),
                        "--observed-at",
                        "2026-05-25T11:00:00+08:00",
                        "--package",
                    ]
                )
            first_summary = json.loads(first_output.getvalue())
            second_output = StringIO()
            with redirect_stdout(second_output):
                second_exit_code = ops_main(
                    [
                        "finalize-multi-trader-smoke",
                        "--root-path",
                        str(root),
                        "--observed-at",
                        "2026-05-25T11:01:00+08:00",
                        "--package",
                    ]
                )
            second_summary = json.loads(second_output.getvalue())
            manifest = json.loads((root / "smoke-run-manifest.json").read_text(encoding="utf-8"))

        self.assertEqual(first_exit_code, 0)
        self.assertTrue(first_summary["passed"])
        self.assertEqual(second_exit_code, 0)
        self.assertTrue(second_summary["passed"])
        self.assertNotIn("smoke-run-package.json", {item["path"] for item in manifest["files"]})

    def test_ops_cli_finalize_package_reports_package_readiness_on_failure(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            payload = multi_trader_smoke_payload()
            clients_dir = root / "clients"
            clients_dir.mkdir()
            for index, client in enumerate(payload["clients"], start=1):
                write_json(clients_dir / f"desk-{index}.json", client_smoke_artifact(client))
            write_json(root / "workflows.json", {"workflows": payload["workflows"]})
            write_real_data_runner_smoke_preflight_artifacts(root)
            write_json(root / "runtime-health-verification.json", real_data_runner_smoke_health_payload())
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "finalize-multi-trader-smoke",
                        "--root-path",
                        str(root),
                        "--observed-at",
                        "2026-05-25T11:00:00+08:00",
                        "--package",
                    ]
                )
            summary = json.loads(output.getvalue())

        self.assertEqual(exit_code, 1)
        self.assertFalse(summary["passed"])
        self.assertEqual(summary["blockers"], [])
        self.assertEqual(summary["package_error"], "package-multi-trader-smoke requires smoke-import-manifest.json")
        self.assertIn("package_readiness", summary)
        self.assertTrue(summary["package_readiness"]["evidence"]["passed"])
        self.assertIn(
            "multi_trader_smoke_import_manifest_missing_for_package",
            summary["package_readiness"]["blockers"],
        )

    def test_finalize_multi_trader_smoke_writes_failed_evidence_for_missing_artifacts(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)

            result = finalize_multi_trader_smoke(
                root_path=root,
                observed_at="2026-05-25T11:00:00+08:00",
            )
            evidence = json.loads(result.evidence_path.read_text(encoding="utf-8"))
            manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))

        self.assertFalse(result.evidence["passed"])
        self.assertFalse(evidence["passed"])
        self.assertIn("multi_trader_smoke_clients_missing", evidence["blockers"])
        self.assertIn("multi_trader_smoke_workflows_missing", evidence["blockers"])
        self.assertIn("multi_trader_smoke_runtime_health_missing", evidence["blockers"])
        self.assertIn("multi_trader_smoke_preflight_missing", evidence["blockers"])
        self.assertIn("multi_trader_smoke_service_preflight_missing", evidence["blockers"])
        self.assertTrue(evidence["missing_paths"])
        self.assertIn("multi-trader-smoke-evidence.json", {item["path"] for item in manifest["files"]})

    def test_finalize_multi_trader_smoke_requires_matching_service_preflight_artifact(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            payload = multi_trader_smoke_payload()
            clients_dir = root / "clients"
            clients_dir.mkdir()
            for index, client in enumerate(payload["clients"], start=1):
                write_json(clients_dir / f"desk-{index}.json", client_smoke_artifact(client))
            write_json(root / "workflows.json", {"workflows": payload["workflows"]})
            write_json(root / "lan-preflight.json", real_data_runner_smoke_preflight_payload())
            write_json(root / "runtime-health-verification.json", real_data_runner_smoke_health_payload())

            missing_service = finalize_multi_trader_smoke(
                root_path=root,
                observed_at="2026-05-25T11:00:00+08:00",
            )

            mismatched_service = real_data_runner_smoke_service_checks_payload()
            mismatched_service["checked_at"] = "2026-05-25T11:05:00+08:00"
            write_json(root / "service-preflight.json", mismatched_service)
            mismatched = finalize_multi_trader_smoke(
                root_path=root,
                observed_at="2026-05-25T11:01:00+08:00",
            )

        self.assertFalse(missing_service.evidence["passed"])
        self.assertIn("multi_trader_smoke_service_preflight_missing", missing_service.evidence["blockers"])
        self.assertFalse(mismatched.evidence["passed"])
        self.assertIn("multi_trader_smoke_service_preflight_mismatch", mismatched.evidence["blockers"])

    def test_ops_cli_finalize_multi_trader_smoke_returns_nonzero_for_missing_artifacts(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "finalize-multi-trader-smoke",
                        "--root-path",
                        str(root),
                        "--observed-at",
                        "2026-05-25T11:00:00+08:00",
                    ]
                )
            summary = json.loads(output.getvalue())
            evidence = json.loads((root / "multi-trader-smoke-evidence.json").read_text(encoding="utf-8"))

        self.assertEqual(exit_code, 1)
        self.assertFalse(summary["passed"])
        self.assertIn("multi_trader_smoke_clients_missing", summary["blockers"])
        self.assertEqual(summary["blockers"], evidence["blockers"])

    def test_finalize_multi_trader_smoke_writes_failed_evidence_for_invalid_json(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            clients_dir = root / "clients"
            clients_dir.mkdir()
            (clients_dir / "desk-a.json").write_text("{bad json", encoding="utf-8")
            (root / "workflows.json").write_text("{bad json", encoding="utf-8")
            write_real_data_runner_smoke_preflight_artifacts(root)
            (root / "runtime-health-verification.json").write_text(
                json.dumps(real_data_runner_smoke_health_payload()),
                encoding="utf-8",
            )

            result = finalize_multi_trader_smoke(
                root_path=root,
                observed_at="2026-05-25T11:00:00+08:00",
            )
            evidence = json.loads(result.evidence_path.read_text(encoding="utf-8"))

        self.assertFalse(result.evidence["passed"])
        self.assertIn("multi_trader_smoke_clients_invalid", evidence["blockers"])
        self.assertIn("multi_trader_smoke_workflows_invalid", evidence["blockers"])
        self.assertTrue(any(path.endswith("desk-a.json") for path in evidence["invalid_paths"]))
        self.assertTrue(any(path.endswith("workflows.json") for path in evidence["invalid_paths"]))

    def test_finalize_multi_trader_smoke_writes_failed_evidence_for_invalid_client_artifact_semantics(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            payload = multi_trader_smoke_payload()
            clients_dir = root / "clients"
            clients_dir.mkdir()
            bad_client = json.loads(json.dumps(payload["clients"][0]))
            bad_client["data_source_mode"] = "mock"
            write_json(clients_dir / "desk-a.json", client_smoke_artifact(bad_client))
            write_json(clients_dir / "desk-b.json", client_smoke_artifact(payload["clients"][1]))
            write_json(root / "workflows.json", {"workflows": payload["workflows"]})
            write_real_data_runner_smoke_preflight_artifacts(root)
            write_json(root / "runtime-health-verification.json", real_data_runner_smoke_health_payload())

            result = finalize_multi_trader_smoke(
                root_path=root,
                observed_at="2026-05-25T11:00:00+08:00",
            )
            evidence = json.loads(result.evidence_path.read_text(encoding="utf-8"))

        self.assertFalse(result.evidence["passed"])
        self.assertIn("multi_trader_smoke_clients_invalid", evidence["blockers"])
        self.assertIn(str(clients_dir / "desk-a.json"), evidence["invalid_paths"])

    def test_packages_multi_trader_smoke_json_evidence(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "clients").mkdir()
            write_json(root / "clients" / "client.json", {"clients": []})
            write_json(root / "clients" / "desk-a.json", {"client_id": "desk-a"})
            write_json(root / "multi-trader-smoke-evidence.json", {"passed": True})
            write_json(root / "workflows.json", {"workflows": []})
            write_real_data_runner_smoke_preflight_artifacts(root)
            write_json(root / "smoke-import-manifest.json", smoke_import_manifest_payload())
            write_smoke_run_manifest(
                root,
                root / "clients" / "client.json",
                root / "clients" / "desk-a.json",
                root / "lan-preflight.json",
                root / "multi-trader-smoke-evidence.json",
                root / "service-preflight.json",
                root / "workflows.json",
                root / "smoke-import-manifest.json",
            )
            write_json(root / "smoke-run-package.json", {"stale": True})

            result = package_multi_trader_smoke(root_path=root)
            metadata = json.loads(result.metadata_path.read_text(encoding="utf-8"))
            with zipfile.ZipFile(result.package_path) as archive:
                names = sorted(archive.namelist())
            package_hash = hashlib.sha256(result.package_path.read_bytes()).hexdigest()

        self.assertEqual(result.sha256, package_hash)
        self.assertEqual(metadata["sha256"], package_hash)
        self.assertEqual(result.file_count, 8)
        self.assertEqual(
            names,
            [
                "clients/client.json",
                "clients/desk-a.json",
                "lan-preflight.json",
                "multi-trader-smoke-evidence.json",
                "service-preflight.json",
                "smoke-import-manifest.json",
                "smoke-run-manifest.json",
                "workflows.json",
            ],
        )
        self.assertNotIn("smoke-run-package.json", names)
        self.assertEqual(metadata["files"], names)

    def test_ops_cli_packages_multi_trader_smoke(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_json(root / "multi-trader-smoke-evidence.json", {"passed": True})
            (root / "clients").mkdir()
            write_json(root / "clients" / "client.json", {"clients": []})
            write_real_data_runner_smoke_preflight_artifacts(root)
            write_json(root / "smoke-import-manifest.json", smoke_import_manifest_payload())
            write_smoke_run_manifest(
                root,
                root / "multi-trader-smoke-evidence.json",
                root / "clients" / "client.json",
                root / "lan-preflight.json",
                root / "service-preflight.json",
                root / "smoke-import-manifest.json",
            )
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "package-multi-trader-smoke",
                        "--root-path",
                        str(root),
                    ]
                )
            summary = json.loads(output.getvalue())

        self.assertEqual(exit_code, 0)
        self.assertEqual(summary["package_path"], str(root / "multi-trader-smoke-evidence.zip"))
        self.assertEqual(summary["metadata_path"], str(root / "smoke-run-package.json"))
        self.assertEqual(len(summary["sha256"]), 64)
        self.assertEqual(
            summary["files"],
            [
                "clients/client.json",
                "lan-preflight.json",
                "multi-trader-smoke-evidence.json",
                "service-preflight.json",
                "smoke-import-manifest.json",
                "smoke-run-manifest.json",
            ],
        )

    def test_ops_cli_package_multi_trader_smoke_requires_import_manifest(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_json(root / "multi-trader-smoke-evidence.json", {"passed": True})
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "package-multi-trader-smoke",
                        "--root-path",
                        str(root),
                    ]
            )
            summary = json.loads(output.getvalue())
            write_json(root / "smoke-import-manifest.json", {"schema_version": 1, "runs": []})
            invalid_output = StringIO()
            with redirect_stdout(invalid_output):
                invalid_exit_code = ops_main(
                    [
                        "package-multi-trader-smoke",
                        "--root-path",
                        str(root),
                    ]
                )
            invalid_summary = json.loads(invalid_output.getvalue())

        self.assertEqual(exit_code, 1)
        self.assertFalse(summary["passed"])
        self.assertEqual(summary["error"], "package-multi-trader-smoke requires smoke-import-manifest.json")
        self.assertEqual(invalid_exit_code, 1)
        self.assertFalse(invalid_summary["passed"])
        self.assertEqual(
            invalid_summary["error"],
            "package-multi-trader-smoke import manifest imported invalid",
        )

    def test_ops_cli_package_multi_trader_smoke_rejects_missing_import_output(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_json(root / "multi-trader-smoke-evidence.json", {"passed": True})
            manifest = smoke_import_manifest_payload()
            manifest["imported"][0]["output_path"] = "clients/missing.json"
            manifest["runs"][0]["imported"][0]["output_path"] = "clients/missing.json"
            write_json(root / "smoke-import-manifest.json", manifest)
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "package-multi-trader-smoke",
                        "--root-path",
                        str(root),
                    ]
                )
            summary = json.loads(output.getvalue())

        self.assertEqual(exit_code, 1)
        self.assertFalse(summary["passed"])
        self.assertEqual(summary["error"], "package-multi-trader-smoke import manifest output missing")

    def test_ops_cli_package_multi_trader_smoke_requires_import_manifest_coverage(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            payload = multi_trader_smoke_payload()
            clients_dir = root / "clients"
            clients_dir.mkdir()
            write_json(clients_dir / "desk-1.json", client_smoke_artifact(payload["clients"][0]))
            write_json(clients_dir / "desk-2.json", client_smoke_artifact(payload["clients"][1]))
            write_json(root / "multi-trader-smoke-evidence.json", {"passed": True})
            write_real_data_runner_smoke_preflight_artifacts(root)
            write_json(root / "smoke-import-manifest.json", smoke_import_manifest_payload("clients/desk-1.json"))
            write_smoke_run_manifest(
                root,
                root / "clients" / "desk-1.json",
                root / "clients" / "desk-2.json",
                root / "lan-preflight.json",
                root / "service-preflight.json",
                root / "multi-trader-smoke-evidence.json",
                root / "smoke-import-manifest.json",
            )
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "package-multi-trader-smoke",
                        "--root-path",
                        str(root),
                    ]
                )
            summary = json.loads(output.getvalue())

        self.assertEqual(exit_code, 1)
        self.assertFalse(summary["passed"])
        self.assertEqual(summary["error"], "package-multi-trader-smoke import manifest coverage missing")

    def test_validate_smoke_import_manifest_rejects_unsafe_provenance(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "clients").mkdir()
            (root / "performance").mkdir()
            write_json(root / "clients" / "desk-1.json", {"clients": []})
            write_json(root / "performance" / "perf-1.json", {"performance_samples": {"subscribe_snapshot_ms": [80]}})

            def invalid_manifest(error: str, mutate) -> None:
                manifest = smoke_import_manifest_payload("clients/desk-1.json")
                mutate(manifest)
                with self.assertRaisesRegex(ValueError, error):
                    ops_module.validate_smoke_import_manifest(manifest, root_path=root)

            invalid_manifest(
                "kind invalid",
                lambda manifest: manifest["imported"][0].update({"kind": "queue"}),
            )
            invalid_manifest(
                "input_path invalid",
                lambda manifest: manifest["imported"][0].update({"input_path": "   "}),
            )
            invalid_manifest(
                "output_path invalid",
                lambda manifest: manifest["imported"][0].update({"output_path": "../desk-1.json"}),
            )
            invalid_manifest(
                "kind output mismatch",
                lambda manifest: manifest["imported"][0].update({"kind": "client", "output_path": "performance/perf-1.json"}),
            )

            duplicate = smoke_import_manifest_payload("clients/desk-1.json", "clients/desk-1.json")
            with self.assertRaisesRegex(ValueError, "duplicate output"):
                ops_module.validate_smoke_import_manifest(duplicate, root_path=root)

            run_count_mismatch = smoke_import_manifest_payload("clients/desk-1.json")
            run_count_mismatch["runs"][0]["imported_count"] = 99
            with self.assertRaisesRegex(ValueError, "run imported_count mismatch"):
                ops_module.validate_smoke_import_manifest(run_count_mismatch, root_path=root)

            run_payload_mismatch = smoke_import_manifest_payload("clients/desk-1.json")
            run_payload_mismatch["runs"][0]["imported"] = [
                {
                    "kind": "performance",
                    "input_path": "/downloads/perf-1.json",
                    "output_path": "performance/perf-1.json",
                }
            ]
            with self.assertRaisesRegex(ValueError, "imported run mismatch"):
                ops_module.validate_smoke_import_manifest(run_payload_mismatch, root_path=root)

    def test_ops_cli_package_multi_trader_smoke_requires_passed_evidence(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "clients").mkdir()
            write_json(root / "clients" / "client.json", {"clients": []})
            write_json(root / "smoke-import-manifest.json", smoke_import_manifest_payload())
            missing_output = StringIO()

            with redirect_stdout(missing_output):
                missing_exit_code = ops_main(
                    [
                        "package-multi-trader-smoke",
                        "--root-path",
                        str(root),
                    ]
                )
            missing_summary = json.loads(missing_output.getvalue())

            write_json(root / "multi-trader-smoke-evidence.json", {"passed": False})
            failing_output = StringIO()
            with redirect_stdout(failing_output):
                failing_exit_code = ops_main(
                    [
                        "package-multi-trader-smoke",
                        "--root-path",
                        str(root),
                    ]
                )
            failing_summary = json.loads(failing_output.getvalue())

        self.assertEqual(missing_exit_code, 1)
        self.assertFalse(missing_summary["passed"])
        self.assertEqual(
            missing_summary["error"],
            "package-multi-trader-smoke requires passed multi-trader-smoke-evidence.json",
        )
        self.assertEqual(failing_exit_code, 1)
        self.assertFalse(failing_summary["passed"])
        self.assertEqual(
            failing_summary["error"],
            "package-multi-trader-smoke requires passed multi-trader-smoke-evidence.json",
        )

    def test_ops_cli_package_multi_trader_smoke_requires_fresh_run_manifest(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_json(root / "multi-trader-smoke-evidence.json", {"passed": True})
            (root / "clients").mkdir()
            write_json(root / "clients" / "client.json", {"clients": []})
            write_real_data_runner_smoke_preflight_artifacts(root)
            write_json(root / "smoke-import-manifest.json", smoke_import_manifest_payload())
            missing_output = StringIO()

            with redirect_stdout(missing_output):
                missing_exit_code = ops_main(
                    [
                        "package-multi-trader-smoke",
                        "--root-path",
                        str(root),
                    ]
                )
            missing_summary = json.loads(missing_output.getvalue())

            write_smoke_run_manifest(
                root,
                root / "multi-trader-smoke-evidence.json",
                root / "lan-preflight.json",
                root / "service-preflight.json",
                root / "smoke-import-manifest.json",
            )
            stale_output = StringIO()
            with redirect_stdout(stale_output):
                stale_exit_code = ops_main(
                    [
                        "package-multi-trader-smoke",
                        "--root-path",
                        str(root),
                    ]
                )
            stale_summary = json.loads(stale_output.getvalue())

        self.assertEqual(missing_exit_code, 1)
        self.assertEqual(missing_summary["error"], "package-multi-trader-smoke requires smoke-run-manifest.json")
        self.assertEqual(stale_exit_code, 1)
        self.assertEqual(stale_summary["error"], "package-multi-trader-smoke smoke-run-manifest stale")

    def test_imports_multi_trader_smoke_artifact_to_detected_directory(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "downloads"
            input_dir.mkdir()
            client_path = input_dir / "client-export.json"
            performance_path = input_dir / "performance export.json"
            write_json(
                client_path,
                {
                    "schema_version": 1,
                    "exported_at": "2026-05-25T11:00:00+08:00",
                    "clients": [multi_trader_smoke_payload()["clients"][0]],
                },
            )
            write_json(
                performance_path,
                {
                    "schema_version": 1,
                    "exported_at": "2026-05-25T11:00:00+08:00",
                    "machine_id": "desk-a",
                    "performance_samples": {"subscribe_snapshot_ms": [120]},
                },
            )

            client_result = import_multi_trader_smoke_artifact(root_path=root / "smoke", input_path=client_path)
            performance_result = import_multi_trader_smoke_artifact(root_path=root / "smoke", input_path=performance_path)
            duplicate_result = import_multi_trader_smoke_artifact(root_path=root / "smoke", input_path=client_path)
            invalid_client_path = input_dir / "invalid-client.json"
            write_json(invalid_client_path, {"schema_version": 1, "clients": [multi_trader_smoke_payload()["clients"][0]]})
            manifest = json.loads((root / "smoke" / "smoke-import-manifest.json").read_text(encoding="utf-8"))
            with self.assertRaisesRegex(ValueError, "exported_at must be an ISO datetime"):
                import_multi_trader_smoke_artifact(root_path=root / "smoke", input_path=invalid_client_path)

        self.assertEqual(client_result.kind, "client")
        self.assertEqual(performance_result.kind, "performance")
        self.assertEqual(client_result.manifest_path, root / "smoke" / "smoke-import-manifest.json")
        self.assertEqual(client_result.output_path.parent.name, "clients")
        self.assertEqual(performance_result.output_path.parent.name, "performance")
        self.assertEqual(performance_result.output_path.name, "performance-export.json")
        self.assertEqual(duplicate_result.output_path.name, "client-export-2.json")
        self.assertEqual(manifest["imported_count"], 3)
        self.assertEqual(len(manifest["runs"]), 3)
        self.assertEqual(manifest["imported"][0]["output_path"], "clients/client-export.json")
        self.assertEqual(manifest["imported"][1]["output_path"], "performance/performance-export.json")

    def test_import_rejects_semantically_invalid_client_smoke_artifact(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_path = root / "client.json"
            client = json.loads(json.dumps(multi_trader_smoke_payload()["clients"][0]))
            client["machine_id"] = " desk-a "
            client["data_source_mode"] = "mock"
            client["page_url"] = "http://127.0.0.1:5173/"
            client["gateway_url"] = "ws://127.0.0.1:9020/ws"
            client["symbol_statuses"].pop("00700.HK")
            write_json(
                input_path,
                {
                    "schema_version": 1,
                    "exported_at": "2026-05-25T11:00:00+08:00",
                    "clients": [client],
                },
            )

            with self.assertRaisesRegex(ValueError, "semantic validation failed") as raised:
                import_multi_trader_smoke_artifact(root_path=root / "smoke", input_path=input_path)

        message = str(raised.exception)
        self.assertIn("multi_trader_smoke_client_machine_invalid", message)
        self.assertIn("multi_trader_smoke_client_not_live", message)
        self.assertIn("multi_trader_smoke_client_page_url_loopback", message)
        self.assertIn("multi_trader_smoke_client_gateway_url_loopback", message)
        self.assertIn("multi_trader_smoke_symbol_statuses_missing_watchlist_symbols", message)

    def test_import_rejects_semantically_invalid_performance_smoke_artifact(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_path = root / "performance.json"
            missing_machine_path = root / "performance-missing-machine.json"
            write_json(
                input_path,
                {
                    "schema_version": 1,
                    "exported_at": "2026-05-25T11:00:00+08:00",
                    "machine_id": " desk-a ",
                    "performance_samples": {"subscribe_snapshot_ms": [120]},
                },
            )
            write_json(
                missing_machine_path,
                {
                    "schema_version": 1,
                    "exported_at": "2026-05-25T11:00:00+08:00",
                    "performance_samples": {"subscribe_snapshot_ms": [120]},
                },
            )

            with self.assertRaisesRegex(ValueError, "machine_id must not require trimming"):
                import_multi_trader_smoke_artifact(root_path=root / "smoke", input_path=input_path)
            with self.assertRaisesRegex(ValueError, "machine_id is required"):
                import_multi_trader_smoke_artifact(root_path=root / "smoke", input_path=missing_machine_path)

    def test_ops_cli_imports_multi_trader_smoke_artifact(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_path = root / "client.json"
            write_json(
                input_path,
                {
                    "schema_version": 1,
                    "exported_at": "2026-05-25T11:00:00+08:00",
                    "clients": [multi_trader_smoke_payload()["clients"][0]],
                },
            )
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "import-multi-trader-smoke-artifact",
                        "--root-path",
                        str(root / "smoke"),
                        "--input-path",
                        str(input_path),
                    ]
                )
            summary = json.loads(output.getvalue())

        self.assertEqual(exit_code, 0)
        self.assertEqual(summary["kind"], "client")
        self.assertEqual(summary["manifest_path"], str(root / "smoke" / "smoke-import-manifest.json"))
        self.assertTrue(summary["output_path"].endswith("clients/client.json"))

    def test_imports_multi_trader_smoke_artifacts_from_directory(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            downloads = root / "downloads"
            downloads.mkdir()
            write_json(
                downloads / "client-a.json",
                {
                    "schema_version": 1,
                    "exported_at": "2026-05-25T11:00:00+08:00",
                    "clients": [multi_trader_smoke_payload()["clients"][0]],
                },
            )
            write_json(
                downloads / "perf-a.json",
                {
                    "schema_version": 1,
                    "exported_at": "2026-05-25T11:00:00+08:00",
                    "machine_id": "desk-a",
                    "performance_samples": {"subscribe_snapshot_ms": [80]},
                },
            )
            write_json(downloads / "unrelated.json", {"hello": "world"})

            result = import_multi_trader_smoke_artifacts(root_path=root / "smoke", input_path=downloads)
            second = root / "second-downloads"
            second.mkdir()
            write_json(
                second / "client-b.json",
                {
                    "schema_version": 1,
                    "exported_at": "2026-05-25T11:01:00+08:00",
                    "clients": [multi_trader_smoke_payload()["clients"][1]],
                },
            )
            second_result = import_multi_trader_smoke_artifacts(root_path=root / "smoke", input_path=second)
            manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))

        self.assertEqual([item.kind for item in result.imported], ["client", "performance"])
        self.assertEqual([item.kind for item in second_result.imported], ["client"])
        self.assertEqual(len(result.skipped), 1)
        self.assertTrue(result.skipped[0]["path"].endswith("unrelated.json"))
        self.assertEqual(manifest["imported_count"], 3)
        self.assertEqual(manifest["skipped_count"], 1)
        self.assertEqual(len(manifest["runs"]), 2)
        self.assertTrue(manifest["imported"][0]["output_path"].endswith("clients/client-a.json"))
        self.assertTrue(manifest["runs"][1]["imported"][0]["output_path"].endswith("clients/client-b.json"))

    def test_import_normalizes_existing_absolute_manifest_output_paths(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            smoke_root = root / "smoke"
            clients_dir = smoke_root / "clients"
            clients_dir.mkdir(parents=True)
            write_json(clients_dir / "old-client.json", client_smoke_artifact(multi_trader_smoke_payload()["clients"][0]))
            write_json(
                smoke_root / "smoke-import-manifest.json",
                {
                    "schema_version": 1,
                    "source_path": "/downloads",
                    "imported_count": 1,
                    "skipped_count": 0,
                    "imported": [
                        {
                            "kind": "client",
                            "input_path": "/downloads/old-client.json",
                            "output_path": str(clients_dir / "old-client.json"),
                        }
                    ],
                    "skipped": [],
                    "runs": [
                        {
                            "source_path": "/downloads",
                            "imported_count": 1,
                            "skipped_count": 0,
                            "imported": [
                                {
                                    "kind": "client",
                                    "input_path": "/downloads/old-client.json",
                                    "output_path": str(clients_dir / "old-client.json"),
                                }
                            ],
                            "skipped": [],
                        }
                    ],
                },
            )
            downloads = root / "downloads"
            downloads.mkdir()
            write_json(downloads / "client-b.json", client_smoke_artifact(multi_trader_smoke_payload()["clients"][1]))

            import_multi_trader_smoke_artifacts(root_path=smoke_root, input_path=downloads)
            manifest = json.loads((smoke_root / "smoke-import-manifest.json").read_text(encoding="utf-8"))

        self.assertEqual(manifest["imported"][0]["output_path"], "clients/old-client.json")
        self.assertEqual(manifest["runs"][0]["imported"][0]["output_path"], "clients/old-client.json")

    def test_ops_cli_imports_multi_trader_smoke_artifacts_from_directory(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            downloads = root / "downloads"
            downloads.mkdir()
            write_json(
                downloads / "client-a.json",
                {
                    "schema_version": 1,
                    "exported_at": "2026-05-25T11:00:00+08:00",
                    "clients": [multi_trader_smoke_payload()["clients"][0]],
                },
            )
            write_json(
                downloads / "perf-a.json",
                {
                    "schema_version": 1,
                    "exported_at": "2026-05-25T11:00:00+08:00",
                    "machine_id": "desk-a",
                    "performance_samples": {"subscribe_snapshot_ms": [80]},
                },
            )
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "import-multi-trader-smoke-artifacts",
                        "--root-path",
                        str(root / "smoke"),
                        "--input-path",
                        str(downloads),
                    ]
                )
            summary = json.loads(output.getvalue())

        self.assertEqual(exit_code, 0)
        self.assertEqual(summary["imported_count"], 2)
        self.assertEqual(summary["skipped_count"], 0)
        self.assertEqual(summary["manifest_path"], str(root / "smoke" / "smoke-import-manifest.json"))
        self.assertEqual([item["kind"] for item in summary["imported"]], ["client", "performance"])

    def test_builds_multi_trader_smoke_workflow_template(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            output_path = root / "workflows.json"

            result = build_multi_trader_smoke_workflows_template(
                output_path=output_path,
                cold_query_symbol="00005.HK",
                redis_clear_symbol="00700.HK",
                add_to_watchlist_symbol="00939.HK",
                requested_trade_date="20260525",
                effective_trade_date="20260522",
            )
            persisted = json.loads(output_path.read_text(encoding="utf-8"))

        self.assertEqual(result.output_path, output_path)
        self.assertEqual(set(result.workflows), {
            "cold_query",
            "add_to_watchlist",
            "refresh_recovery",
            "redis_clear_recovery",
            "process_restart_recovery",
            "closed_market_effective_date",
        })
        self.assertEqual(persisted["schema_version"], 1)
        self.assertEqual(persisted["workflows"]["cold_query"]["symbol"], "00005.HK")
        self.assertFalse(persisted["workflows"]["cold_query"]["passed"])
        self.assertFalse(persisted["workflows"]["refresh_recovery"]["browser_refreshed"])
        self.assertFalse(persisted["workflows"]["redis_clear_recovery"]["cache_cleared"])
        self.assertFalse(persisted["workflows"]["redis_clear_recovery"]["snapshot_rebuilt"])
        self.assertFalse(persisted["workflows"]["process_restart_recovery"]["backend_restarted"])
        self.assertEqual(persisted["workflows"]["closed_market_effective_date"]["requested_trade_date"], "20260525")
        self.assertEqual(persisted["workflows"]["closed_market_effective_date"]["effective_trade_date"], "20260522")

    def test_workflow_readiness_flags_same_date_closed_market_before_pass(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            output_path = root / "workflows.json"

            build_multi_trader_smoke_workflows_template(
                output_path=output_path,
                requested_trade_date="20260526",
                effective_trade_date="20260526",
            )
            readiness = ops_module.multi_trader_smoke_workflow_readiness(output_path)

        self.assertIn("multi_trader_smoke_closed_market_dates_not_distinct", readiness["blockers"])
        self.assertIn(
            "multi_trader_smoke_closed_market_dates_not_distinct",
            readiness["evidence_blockers_by_workflow"]["closed_market_effective_date"],
        )

    def test_ops_cli_builds_multi_trader_smoke_workflow_template(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            output_path = root / "workflows.json"
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "build-multi-trader-smoke-workflows-template",
                        "--output-path",
                        str(output_path),
                        "--cold-query-symbol",
                        "00005.HK",
                        "--redis-clear-symbol",
                        "00700.HK",
                        "--add-to-watchlist-symbol",
                        "00939.HK",
                        "--requested-trade-date",
                        "20260525",
                        "--effective-trade-date",
                        "20260522",
                    ]
                )
            summary = json.loads(output.getvalue())
            persisted = json.loads(output_path.read_text(encoding="utf-8"))

        self.assertEqual(exit_code, 0)
        self.assertEqual(summary["workflow_count"], 6)
        self.assertEqual(summary["output_path"], str(output_path))
        self.assertEqual(persisted["workflows"]["add_to_watchlist"]["symbol"], "00939.HK")

    def test_records_multi_trader_smoke_workflow_without_hand_editing_json(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            workflows_path = root / "workflows.json"
            build_multi_trader_smoke_workflows_template(
                output_path=workflows_path,
                cold_query_symbol="00005.HK",
                requested_trade_date="20260525",
                effective_trade_date="20260522",
            )

            result = record_multi_trader_smoke_workflow(
                workflows_path=workflows_path,
                workflow="cold_query",
                observed_at="2026-05-25T11:00:00+08:00",
                notes="client A observed loading then snapshot",
            )
            persisted = json.loads(workflows_path.read_text(encoding="utf-8"))

        self.assertEqual(result.workflow, "cold_query")
        self.assertTrue(persisted["workflows"]["cold_query"]["passed"])
        self.assertTrue(persisted["workflows"]["cold_query"]["loading_observed"])
        self.assertTrue(persisted["workflows"]["cold_query"]["snapshot_visible"])
        self.assertEqual(persisted["workflows"]["cold_query"]["observed_at"], "2026-05-25T11:00:00+08:00")
        self.assertFalse(persisted["workflows"]["refresh_recovery"]["passed"])

    def test_records_multi_trader_smoke_workflow_normalizes_and_rejects_symbols(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            workflows_path = root / "workflows.json"
            build_multi_trader_smoke_workflows_template(output_path=workflows_path)

            result = record_multi_trader_smoke_workflow(
                workflows_path=workflows_path,
                workflow="cold_query",
                symbol="700",
                observed_at="2026-05-25T11:00:00+08:00",
            )
            persisted = json.loads(workflows_path.read_text(encoding="utf-8"))

        self.assertEqual(result.workflow, "cold_query")
        self.assertEqual(persisted["workflows"]["cold_query"]["symbol"], "00700.HK")

        with TemporaryDirectory() as temp_dir:
            workflows_path = Path(temp_dir) / "workflows.json"
            build_multi_trader_smoke_workflows_template(output_path=workflows_path)
            with self.assertRaisesRegex(ValueError, "smoke symbol must use canonical HK format"):
                record_multi_trader_smoke_workflow(
                    workflows_path=workflows_path,
                    workflow="redis_clear_recovery",
                    symbol="ABC",
                    observed_at="2026-05-25T11:00:00+08:00",
                )

    def test_record_multi_trader_smoke_workflow_rejects_incomplete_evidence(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            workflows_path = root / "workflows.json"
            build_multi_trader_smoke_workflows_template(output_path=workflows_path)
            before = workflows_path.read_text(encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "multi_trader_smoke_cold_query_symbol_invalid"):
                record_multi_trader_smoke_workflow(
                    workflows_path=workflows_path,
                    workflow="cold_query",
                    observed_at="2026-05-25T11:00:00+08:00",
                )
            with self.assertRaisesRegex(ValueError, "multi_trader_smoke_requested_date_missing"):
                record_multi_trader_smoke_workflow(
                    workflows_path=workflows_path,
                    workflow="closed_market_effective_date",
                    observed_at="2026-05-25T11:00:00+08:00",
                )

            after = workflows_path.read_text(encoding="utf-8")

        self.assertEqual(after, before)

    def test_record_multi_trader_smoke_workflow_rejects_invalid_observed_at(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            workflows_path = root / "workflows.json"
            build_multi_trader_smoke_workflows_template(
                output_path=workflows_path,
                cold_query_symbol="00005.HK",
            )
            before = workflows_path.read_text(encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "observed_at must be an ISO datetime"):
                record_multi_trader_smoke_workflow(
                    workflows_path=workflows_path,
                    workflow="cold_query",
                    observed_at="20260525 110000",
                )
            after = workflows_path.read_text(encoding="utf-8")

        self.assertEqual(after, before)

    def test_ops_cli_records_multi_trader_smoke_workflow(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            workflows_path = root / "workflows.json"
            build_multi_trader_smoke_workflows_template(output_path=workflows_path)
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "record-multi-trader-smoke-workflow",
                        "--workflows-path",
                        str(workflows_path),
                        "--workflow",
                        "closed_market_effective_date",
                        "--requested-trade-date",
                        "20260525",
                        "--effective-trade-date",
                        "20260522",
                        "--observed-at",
                        "2026-05-25T11:00:00+08:00",
                    ]
                )
            summary = json.loads(output.getvalue())
            persisted = json.loads(workflows_path.read_text(encoding="utf-8"))

        self.assertEqual(exit_code, 0)
        self.assertTrue(summary["passed"])
        self.assertTrue(persisted["workflows"]["closed_market_effective_date"]["passed"])
        self.assertTrue(persisted["workflows"]["closed_market_effective_date"]["source_dates_visible"])
        self.assertEqual(persisted["workflows"]["closed_market_effective_date"]["effective_trade_date"], "20260522")

    def test_ops_cli_record_multi_trader_smoke_workflow_reports_incomplete_evidence(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            workflows_path = root / "workflows.json"
            build_multi_trader_smoke_workflows_template(output_path=workflows_path)
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "record-multi-trader-smoke-workflow",
                        "--workflows-path",
                        str(workflows_path),
                        "--workflow",
                        "closed_market_effective_date",
                        "--observed-at",
                        "2026-05-25T11:00:00+08:00",
                    ]
                )
            summary = json.loads(output.getvalue())
            persisted = json.loads(workflows_path.read_text(encoding="utf-8"))

        self.assertEqual(exit_code, 1)
        self.assertFalse(summary["passed"])
        self.assertEqual(summary["workflow"], "closed_market_effective_date")
        self.assertIn("multi_trader_smoke_requested_date_missing", summary["error"])
        self.assertFalse(persisted["workflows"]["closed_market_effective_date"]["passed"])

    def test_inspects_multi_trader_smoke_readiness_before_finalize(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)

            missing = inspect_multi_trader_smoke_readiness(root_path=root)

            payload = multi_trader_smoke_payload()
            clients_dir = root / "clients"
            clients_dir.mkdir()
            for index, client in enumerate(payload["clients"], start=1):
                write_json(clients_dir / f"desk-{index}.json", client_smoke_artifact(client))
            write_json(root / "workflows.json", {"schema_version": 1, "workflows": payload["workflows"]})
            write_real_data_runner_smoke_preflight_artifacts(root)
            write_json(root / "runtime-health-verification.json", real_data_runner_smoke_health_payload())

            ready = inspect_multi_trader_smoke_readiness(root_path=root)
            write_json(root / "multi-trader-smoke-evidence.json", {"passed": True})
            write_json(
                root / "smoke-import-manifest.json",
                {
                    "schema_version": 1,
                    "source_path": "/downloads",
                    "imported_count": 2,
                    "skipped_count": 0,
                    "imported": [
                        {"kind": "client", "input_path": "/downloads/desk-1.json", "output_path": "clients/desk-1.json"},
                        {"kind": "client", "input_path": "/downloads/desk-2.json", "output_path": "clients/desk-2.json"},
                    ],
                    "skipped": [],
                    "runs": [
                        {
                            "source_path": "/downloads",
                            "imported_count": 2,
                            "skipped_count": 0,
                            "imported": [
                                {
                                    "kind": "client",
                                    "input_path": "/downloads/desk-1.json",
                                    "output_path": "clients/desk-1.json",
                                },
                                {
                                    "kind": "client",
                                    "input_path": "/downloads/desk-2.json",
                                    "output_path": "clients/desk-2.json",
                                },
                            ],
                            "skipped": [],
                        }
                    ],
                },
            )
            write_smoke_run_manifest(
                root,
                root / "clients" / "desk-1.json",
                root / "clients" / "desk-2.json",
                root / "workflows.json",
                root / "lan-preflight.json",
                root / "service-preflight.json",
                root / "runtime-health-verification.json",
                root / "multi-trader-smoke-evidence.json",
                root / "smoke-import-manifest.json",
            )
            package_ready = inspect_multi_trader_smoke_readiness(root_path=root)

            bad_workflows = dict(payload["workflows"])
            bad_workflows["cold_query"] = {"passed": True, "symbol": "00005.HK"}
            write_json(root / "workflows.json", {"schema_version": 1, "workflows": bad_workflows})
            bad = inspect_multi_trader_smoke_readiness(root_path=root)

            write_json(root / "workflows.json", {"schema_version": 1, "workflows": payload["workflows"]})
            mock_client_payload = json.loads(json.dumps(payload["clients"][0]))
            mock_client_payload["data_source_mode"] = "mock"
            write_json(clients_dir / "desk-1.json", client_smoke_artifact(mock_client_payload))
            mock_client = inspect_multi_trader_smoke_readiness(root_path=root)
            duplicate_client_payload = json.loads(json.dumps(payload["clients"][1]))
            duplicate_client_payload["machine_id"] = "desk-a"
            write_json(clients_dir / "desk-1.json", client_smoke_artifact(payload["clients"][0]))
            write_json(clients_dir / "desk-2.json", client_smoke_artifact(duplicate_client_payload))
            duplicate_client = inspect_multi_trader_smoke_readiness(root_path=root)

            write_json(clients_dir / "desk-2.json", client_smoke_artifact(payload["clients"][1]))
            runtime_health_missing_declared = real_data_runner_smoke_health_payload()
            runtime_health_missing_declared["evidence"]["gateway_activity"]["client_queue"][
                "observed_declared_client_ids"
            ] = ["desk-a", "desk-x"]
            write_json(root / "runtime-health-verification.json", runtime_health_missing_declared)
            runtime_declared_mismatch = inspect_multi_trader_smoke_readiness(root_path=root)

        self.assertFalse(missing.ready)
        self.assertIn("multi_trader_smoke_clients_missing", missing.summary["blockers"])
        self.assertIn("multi_trader_smoke_workflows_missing", missing.summary["blockers"])
        self.assertEqual(missing.summary["next_actions"][0]["stage"], "prepare")
        self.assertIn("--root-path auto", missing.summary["next_actions"][0]["command"])
        self.assertIn("--lan-host auto", missing.summary["next_actions"][0]["command"])
        self.assertTrue(ready.ready)
        self.assertTrue(ready.summary["smoke_preview"]["passed"])
        self.assertFalse(ready.summary["package_readiness"]["ready"])
        self.assertEqual(ready.summary["next_actions"][0]["stage"], "finalize")
        self.assertIn("finalize-package.sh", ready.summary["next_actions"][0]["command"])
        self.assertIn(
            "multi_trader_smoke_import_manifest_missing_for_package",
            ready.summary["package_readiness"]["blockers"],
        )
        self.assertIn(
            "multi_trader_smoke_evidence_missing_for_package",
            ready.summary["package_readiness"]["blockers"],
        )
        self.assertTrue(package_ready.ready)
        self.assertTrue(package_ready.summary["package_readiness"]["ready"])
        self.assertEqual(package_ready.summary["next_actions"][0]["stage"], "handoff")
        self.assertIn("verify-handoff.sh", package_ready.summary["next_actions"][0]["command"])
        self.assertTrue(ready.summary["preflight_readiness"]["service_checks_present"])
        self.assertTrue(ready.summary["preflight_readiness"]["service_checks_passed"])
        self.assertTrue(ready.summary["runtime_health_readiness"]["passed"])
        self.assertEqual(ready.summary["runtime_health_readiness"]["observed_client_count"], 2)
        self.assertEqual(ready.summary["runtime_health_readiness"]["observed_declared_client_count"], 2)
        self.assertEqual(ready.summary["runtime_health_readiness"]["subscribe_snapshot_sample_count"], 3)
        self.assertEqual(ready.summary["artifact_counts"]["clients"], 2)
        self.assertEqual(ready.summary["workflows"]["incomplete"], [])
        self.assertEqual(set(ready.summary["workflows"]["passed"]), set(payload["workflows"]))
        self.assertFalse(bad.ready)
        self.assertEqual(bad.summary["next_actions"][0]["stage"], "workflows")
        self.assertIn("multi_trader_smoke_cold_query_loading_missing", bad.summary["blockers"])
        self.assertIn(
            "multi_trader_smoke_cold_query_snapshot_missing",
            bad.summary["workflows"]["evidence_blockers"],
        )
        self.assertEqual(
            bad.summary["workflows"]["evidence_blockers_by_workflow"]["cold_query"],
            [
                "multi_trader_smoke_cold_query_loading_missing",
                "multi_trader_smoke_cold_query_snapshot_missing",
            ],
        )
        self.assertFalse(mock_client.ready)
        self.assertIn("multi_trader_smoke_clients_invalid", mock_client.summary["blockers"])
        self.assertIn(str(clients_dir / "desk-1.json"), mock_client.summary["invalid_paths"])
        self.assertNotIn("smoke_preview", mock_client.summary)
        self.assertFalse(duplicate_client.ready)
        self.assertIn("multi_trader_smoke_client_machine_duplicate", duplicate_client.summary["blockers"])
        self.assertIn(
            "multi_trader_smoke_client_machine_duplicate",
            duplicate_client.summary["smoke_preview"]["blockers"],
        )
        self.assertFalse(runtime_declared_mismatch.ready)
        self.assertIn(
            "multi_trader_smoke_gateway_declared_client_coverage_missing",
            runtime_declared_mismatch.summary["blockers"],
        )
        self.assertEqual(
            runtime_declared_mismatch.summary["runtime_health_readiness"]["missing_declared_client_machines"],
            ["desk-b"],
        )
        self.assertEqual(
            runtime_declared_mismatch.summary["runtime_health_readiness"]["observed_declared_client_ids"],
            ["desk-a", "desk-x"],
        )
        self.assertEqual(runtime_declared_mismatch.summary["next_actions"][0]["stage"], "client_activity")

        with TemporaryDirectory() as missing_temp_dir:
            missing_root = Path(missing_temp_dir)
            write_json(
                missing_root / "lan-preflight.json",
                {**real_data_runner_smoke_preflight_payload(), "service_checks": None},
            )
            missing_service = inspect_multi_trader_smoke_readiness(root_path=missing_root)

        self.assertFalse(missing_service.ready)
        self.assertFalse(missing.summary["runtime_health_readiness"]["present"])
        self.assertIn("multi_trader_smoke_runtime_health_missing", missing.summary["runtime_health_readiness"]["blockers"])
        self.assertFalse(missing_service.summary["preflight_readiness"]["service_checks_present"])
        self.assertIn(
            "multi_trader_smoke_service_preflight_missing",
            missing_service.summary["preflight_readiness"]["blockers"],
        )
        self.assertEqual(missing_service.summary["next_actions"][0]["stage"], "runtime_health")
        self.assertEqual(missing_service.summary["next_actions"][1]["stage"], "frontend")
        self.assertEqual(missing_service.summary["next_actions"][2]["stage"], "service_preflight")
        self.assertIn("start-backend.sh", missing_service.summary["next_actions"][0]["command"])
        self.assertIn("start-frontend.sh", missing_service.summary["next_actions"][1]["command"])
        self.assertIn("verify-services.sh", missing_service.summary["next_actions"][2]["command"])

    def test_inspect_multi_trader_smoke_next_actions_use_preflight_commands(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            prepare_multi_trader_smoke(
                root_path=root,
                lan_host="192.168.1.10",
                cold_query_symbol="00005.HK",
                redis_clear_symbol="00700.HK",
                add_to_watchlist_symbol="00939.HK",
                requested_trade_date="20260525",
                effective_trade_date="20260522",
            )

            readiness = inspect_multi_trader_smoke_readiness(root_path=root)

        self.assertEqual(
            [action["stage"] for action in readiness.summary["next_actions"][:3]],
            ["runtime_health", "frontend", "service_preflight"],
        )
        actions_by_stage = {action["stage"]: action for action in readiness.summary["next_actions"]}
        self.assertIn("service_preflight", actions_by_stage)
        self.assertIn("verify-services.sh", actions_by_stage["service_preflight"]["command"])
        self.assertIn("runtime_health", actions_by_stage)
        self.assertIn("start-backend.sh", actions_by_stage["runtime_health"]["command"])
        self.assertIn("start-frontend.sh", actions_by_stage["frontend"]["command"])
        self.assertTrue(Path(actions_by_stage["runtime_health"]["command"]).is_absolute())
        self.assertTrue(Path(actions_by_stage["frontend"]["command"]).is_absolute())
        self.assertTrue(Path(actions_by_stage["service_preflight"]["command"]).is_absolute())
        self.assertIn("client_artifacts", actions_by_stage)
        self.assertEqual(actions_by_stage["client_artifacts"]["url"], "http://192.168.1.10:5173/")
        self.assertIn("import-artifacts.sh <downloads-file-or-dir>", actions_by_stage["client_artifacts"]["command"])
        self.assertIn("client_activity", actions_by_stage)
        self.assertEqual(actions_by_stage["client_activity"]["url"], "http://192.168.1.10:5173/")
        self.assertEqual(actions_by_stage["client_activity"]["instructions_path"], str(root / "CLIENT_INSTRUCTIONS.md"))
        self.assertIn("workflows", actions_by_stage)
        self.assertIn("record-workflow.sh <workflow> [workflow args...]", actions_by_stage["workflows"]["command"])

    def test_inspect_multi_trader_smoke_next_actions_report_runtime_client_activity_gap(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            payload = multi_trader_smoke_payload()
            clients_dir = root / "clients"
            clients_dir.mkdir()
            for index, client in enumerate(payload["clients"], start=1):
                write_json(clients_dir / f"desk-{index}.json", client_smoke_artifact(client))
            write_json(root / "workflows.json", {"schema_version": 1, "workflows": payload["workflows"]})
            write_real_data_runner_smoke_preflight_artifacts(root)
            runtime_health = real_data_runner_smoke_health_payload()
            runtime_health["evidence"]["gateway_activity"]["client_queue"] = {
                **gateway_client_queue_payload(),
                "observed_client_count": 1,
                "observed_client_ids": ["desk-a"],
                "observed_declared_client_count": 1,
                "observed_declared_client_ids": ["desk-a"],
                "max_connected_clients": 1,
            }
            write_json(root / "runtime-health-verification.json", runtime_health)

            readiness = inspect_multi_trader_smoke_readiness(root_path=root)

        actions_by_stage = {action["stage"]: action for action in readiness.summary["next_actions"]}
        self.assertFalse(readiness.ready)
        self.assertIn("multi_trader_smoke_gateway_observed_clients_insufficient", readiness.summary["blockers"])
        self.assertFalse(readiness.summary["runtime_health_readiness"]["passed"])
        self.assertEqual(readiness.summary["runtime_health_readiness"]["observed_client_count"], 1)
        self.assertEqual(readiness.summary["runtime_health_readiness"]["observed_declared_client_count"], 1)
        self.assertIn(
            "real_data_runner_insufficient_observed_clients",
            readiness.summary["runtime_health_readiness"]["blockers"],
        )
        self.assertIn("client_activity", actions_by_stage)
        self.assertIn("multi_trader_smoke_gateway_observed_clients_insufficient", actions_by_stage["client_activity"]["reason"])
        self.assertIn("http://192.168.1.10:5173/", actions_by_stage["client_activity"]["url"])
        self.assertIn("CLIENT_INSTRUCTIONS.md", actions_by_stage["client_activity"]["instructions_path"])

    def test_inspect_multi_trader_smoke_next_action_reports_reachable_gateway_without_runtime_health(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            gateway_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            gateway_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            gateway_socket.bind(("0.0.0.0", 0))
            gateway_socket.listen(1)
            gateway_port = gateway_socket.getsockname()[1]
            try:
                preflight = real_data_runner_smoke_preflight_payload()
                preflight["gateway_port"] = gateway_port
                preflight["gateway_url"] = f"ws://192.168.1.10:{gateway_port}/ws"
                preflight["commands"]["backend_script"] = str(root / "scripts" / "start-backend.sh")
                service_checks = real_data_runner_smoke_service_checks_payload()
                service_checks["checks"]["gateway"]["port"] = gateway_port
                service_checks["checks"]["gateway"]["url"] = f"ws://192.168.1.10:{gateway_port}/ws"
                preflight["service_checks"] = service_checks
                write_json(root / "lan-preflight.json", preflight)
                write_json(root / "service-preflight.json", service_checks)

                readiness = inspect_multi_trader_smoke_readiness(root_path=root)
            finally:
                gateway_socket.close()

        runtime_action = readiness.summary["next_actions"][0]
        gateway_port_readiness = readiness.summary["preflight_readiness"]["ports"]["gateway"]
        self.assertEqual(runtime_action["stage"], "runtime_health")
        self.assertFalse(gateway_port_readiness["bind_available"])
        self.assertIn(os.getpid(), [listener["pid"] for listener in gateway_port_readiness["listeners"]])
        self.assertIn("prepared Gateway service is reachable", runtime_action["command"])
        self.assertIn("runtime health file is missing", runtime_action["command"])
        self.assertIn(f"pid={os.getpid()}", runtime_action["command"])
        self.assertIn(str(gateway_port), runtime_action["command"])

    def test_inspect_multi_trader_smoke_reports_invalid_performance_artifacts(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            payload = multi_trader_smoke_payload()
            clients_dir = root / "clients"
            performance_dir = root / "performance"
            clients_dir.mkdir()
            performance_dir.mkdir()
            for index, client in enumerate(payload["clients"], start=1):
                write_json(clients_dir / f"desk-{index}.json", client_smoke_artifact(client))
            write_json(root / "workflows.json", {"schema_version": 1, "workflows": payload["workflows"]})
            write_real_data_runner_smoke_preflight_artifacts(root)
            write_json(root / "runtime-health-verification.json", real_data_runner_smoke_health_payload())
            invalid_performance = performance_dir / "desk-a.json"
            write_json(
                invalid_performance,
                {
                    "schema_version": 1,
                    "machine_id": "desk-a",
                    "performance_samples": {"subscribe_snapshot_ms": [120.0, None]},
                    "raw_samples": [{"key": "subscribe_snapshot_ms", "valueMs": -1}],
                },
            )

            readiness = inspect_multi_trader_smoke_readiness(root_path=root)
            finalized = finalize_multi_trader_smoke(root_path=root, observed_at="2026-05-25T11:00:00+08:00")

        self.assertFalse(readiness.ready)
        self.assertIn("multi_trader_smoke_performance_invalid", readiness.summary["blockers"])
        self.assertIn(str(invalid_performance), readiness.summary["invalid_paths"])
        self.assertFalse(finalized.evidence["passed"])
        self.assertIn("multi_trader_smoke_performance_invalid", finalized.evidence["blockers"])
        self.assertIn(str(invalid_performance), finalized.evidence["invalid_paths"])

    def test_inspect_multi_trader_smoke_blocks_client_activity_when_runtime_minute_bars_missing(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            payload = multi_trader_smoke_payload()
            write_json(root / "workflows.json", {"schema_version": 1, "workflows": payload["workflows"]})
            write_real_data_runner_smoke_preflight_artifacts(root)
            runtime_health = real_data_runner_smoke_health_payload()
            runtime_health["evidence"]["symbol_runtime"]["00700.HK"] = {
                "hydrate_count": 1,
                "degraded_reasons": ["missing_minute_bars"],
                "freshness": {"source_dates": {"minute_bars": ""}},
            }
            write_json(root / "runtime-health-verification.json", runtime_health)

            readiness = inspect_multi_trader_smoke_readiness(root_path=root)

        self.assertFalse(readiness.ready)
        self.assertIn("multi_trader_smoke_minute_bars_missing", readiness.summary["blockers"])
        self.assertEqual(readiness.summary["runtime_health_readiness"]["missing_minute_bar_symbols"], ["00700.HK"])
        self.assertEqual(readiness.summary["next_actions"][0]["stage"], "data_quality")
        self.assertIn("silver_minute_bars_v1", readiness.summary["next_actions"][0]["command"])

    def test_ops_cli_inspects_multi_trader_smoke_readiness(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            build_multi_trader_smoke_workflows_template(output_path=root / "workflows.json")
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "inspect-multi-trader-smoke",
                        "--root-path",
                        str(root),
                    ]
                )
            summary = json.loads(output.getvalue())

        self.assertEqual(exit_code, 1)
        self.assertFalse(summary["ready"])
        self.assertIn("multi_trader_smoke_workflows_incomplete", summary["blockers"])
        self.assertEqual(len(summary["workflows"]["incomplete"]), 6)

    def test_ops_cli_inspect_multi_trader_smoke_can_print_next_action_only(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            prepare_multi_trader_smoke(
                root_path=root,
                lan_host="192.168.1.10",
                cold_query_symbol="00005.HK",
                redis_clear_symbol="00700.HK",
                add_to_watchlist_symbol="00939.HK",
                requested_trade_date="20260525",
                effective_trade_date="20260522",
            )
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "inspect-multi-trader-smoke",
                        "--root-path",
                        str(root),
                        "--next-action",
                    ]
                )
            summary = json.loads(output.getvalue())

        self.assertEqual(exit_code, 1)
        self.assertFalse(summary["ready"])
        self.assertFalse(summary["package_ready"])
        self.assertEqual(summary["next_action"]["stage"], "runtime_health")
        self.assertIn("start-backend.sh", summary["next_action"]["command"])
        self.assertFalse(summary["runtime_health_readiness"]["present"])
        self.assertIn("multi_trader_smoke_runtime_health_missing", summary["runtime_health_readiness"]["blockers"])
        self.assertTrue(summary["preflight_readiness"]["present"])

    def test_ops_cli_inspect_multi_trader_smoke_can_require_package_readiness(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            payload = multi_trader_smoke_payload()
            clients_dir = root / "clients"
            clients_dir.mkdir()
            for index, client in enumerate(payload["clients"], start=1):
                write_json(clients_dir / f"desk-{index}.json", client_smoke_artifact(client))
            write_json(root / "workflows.json", {"schema_version": 1, "workflows": payload["workflows"]})
            write_real_data_runner_smoke_preflight_artifacts(root)
            write_json(root / "runtime-health-verification.json", real_data_runner_smoke_health_payload())
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "inspect-multi-trader-smoke",
                        "--root-path",
                        str(root),
                    ]
                )
            summary = json.loads(output.getvalue())
            package_output = StringIO()
            with redirect_stdout(package_output):
                package_exit_code = ops_main(
                    [
                        "inspect-multi-trader-smoke",
                        "--root-path",
                        str(root),
                        "--require-package-ready",
                        "--next-action",
                    ]
                )
            package_summary = json.loads(package_output.getvalue())

            write_json(root / "multi-trader-smoke-evidence.json", {"passed": True})
            write_json(
                root / "smoke-import-manifest.json",
                {
                    "schema_version": 1,
                    "source_path": "/downloads",
                    "imported_count": 2,
                    "skipped_count": 0,
                    "imported": [
                        {"kind": "client", "input_path": "/downloads/desk-1.json", "output_path": "clients/desk-1.json"},
                        {"kind": "client", "input_path": "/downloads/desk-2.json", "output_path": "clients/desk-2.json"},
                    ],
                    "skipped": [],
                    "runs": [
                        {
                            "source_path": "/downloads",
                            "imported_count": 2,
                            "skipped_count": 0,
                            "imported": [
                                {
                                    "kind": "client",
                                    "input_path": "/downloads/desk-1.json",
                                    "output_path": "clients/desk-1.json",
                                },
                                {
                                    "kind": "client",
                                    "input_path": "/downloads/desk-2.json",
                                    "output_path": "clients/desk-2.json",
                                },
                            ],
                            "skipped": [],
                        }
                    ],
                },
            )
            write_smoke_run_manifest(
                root,
                root / "clients" / "desk-1.json",
                root / "clients" / "desk-2.json",
                root / "workflows.json",
                root / "lan-preflight.json",
                root / "service-preflight.json",
                root / "runtime-health-verification.json",
                root / "multi-trader-smoke-evidence.json",
                root / "smoke-import-manifest.json",
            )
            ready_output = StringIO()
            with redirect_stdout(ready_output):
                ready_exit_code = ops_main(
                    [
                        "inspect-multi-trader-smoke",
                        "--root-path",
                        str(root),
                        "--require-package-ready",
                    ]
                )
            ready_summary = json.loads(ready_output.getvalue())

        self.assertEqual(exit_code, 0)
        self.assertTrue(summary["ready"])
        self.assertFalse(summary["package_readiness"]["ready"])
        self.assertEqual(package_exit_code, 1)
        self.assertTrue(package_summary["ready"])
        self.assertFalse(package_summary["package_ready"])
        self.assertEqual(package_summary["next_action"]["stage"], "finalize")
        self.assertEqual(ready_exit_code, 0)
        self.assertTrue(ready_summary["ready"])
        self.assertTrue(ready_summary["package_readiness"]["ready"])

    def test_inspect_multi_trader_smoke_package_readiness_rejects_stale_service_preflight_timing(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            payload = multi_trader_smoke_payload()
            clients_dir = root / "clients"
            clients_dir.mkdir()
            for index, client in enumerate(payload["clients"], start=1):
                write_json(clients_dir / f"desk-{index}.json", client_smoke_artifact(client))
            write_json(root / "workflows.json", {"schema_version": 1, "workflows": payload["workflows"]})
            write_real_data_runner_smoke_preflight_artifacts(root)
            preflight = real_data_runner_smoke_preflight_payload()
            service_preflight = real_data_runner_smoke_service_checks_payload()
            service_preflight["checked_at"] = "2026-05-25T11:05:00+08:00"
            preflight["service_checks"] = service_preflight
            write_json(root / "lan-preflight.json", preflight)
            write_json(root / "service-preflight.json", service_preflight)
            write_json(root / "runtime-health-verification.json", real_data_runner_smoke_health_payload())
            write_json(root / "multi-trader-smoke-evidence.json", {"schema_version": 1, "passed": True, "observed_at": "2026-05-25T11:00:00+08:00"})
            write_json(
                root / "smoke-import-manifest.json",
                smoke_import_manifest_payload("clients/desk-1.json", "clients/desk-2.json"),
            )
            write_smoke_run_manifest(
                root,
                root / "clients" / "desk-1.json",
                root / "clients" / "desk-2.json",
                root / "workflows.json",
                root / "lan-preflight.json",
                root / "service-preflight.json",
                root / "runtime-health-verification.json",
                root / "multi-trader-smoke-evidence.json",
                root / "smoke-import-manifest.json",
            )
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "inspect-multi-trader-smoke",
                        "--root-path",
                        str(root),
                        "--require-package-ready",
                    ]
                )
            summary = json.loads(output.getvalue())

        self.assertEqual(exit_code, 1)
        self.assertFalse(summary["package_readiness"]["ready"])
        self.assertIn(
            "multi_trader_smoke_service_preflight_after_observed_for_package",
            summary["package_readiness"]["blockers"],
        )
        self.assertEqual(
            summary["package_readiness"]["preflight"]["service_preflight_timing"]["checked_at"],
            "2026-05-25T11:05:00+08:00",
        )

    def test_prepares_multi_trader_smoke_directory_and_preflight(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "smoke"

            result = prepare_multi_trader_smoke(
                root_path=root,
                lan_host="192.168.1.10",
                local_addresses=["192.168.1.10"],
                cold_query_symbol="00005.HK",
                redis_clear_symbol="00700.HK",
                add_to_watchlist_symbol="00939.HK",
                requested_trade_date="20260525",
                effective_trade_date="20260522",
            )
            preflight = json.loads(result.preflight_path.read_text(encoding="utf-8"))
            workflows = json.loads(result.workflows_path.read_text(encoding="utf-8"))
            clients_dir_exists = (root / "clients").is_dir()
            performance_dir_exists = (root / "performance").is_dir()
            readme_path = root / "README.md"
            readme_exists = readme_path.is_file()
            readme_text = readme_path.read_text(encoding="utf-8")
            client_instructions_path = root / "CLIENT_INSTRUCTIONS.md"
            client_instructions_exists = client_instructions_path.is_file()
            client_instructions_text = client_instructions_path.read_text(encoding="utf-8")
            backend_script = root / "scripts" / "start-backend.sh"
            frontend_script = root / "scripts" / "start-frontend.sh"
            service_script = root / "scripts" / "verify-services.sh"
            inspect_script = root / "scripts" / "inspect-next-action.sh"
            verify_handoff_script = root / "scripts" / "verify-handoff.sh"
            finalize_script = root / "scripts" / "finalize-package.sh"
            import_script = root / "scripts" / "import-artifacts.sh"
            record_script = root / "scripts" / "record-workflow.sh"
            restart_backend_script = root / "scripts" / "restart-backend.sh"
            backend_script_exists = backend_script.is_file()
            backend_script_executable = bool(backend_script.stat().st_mode & 0o111)
            backend_script_text = backend_script.read_text(encoding="utf-8")
            restart_backend_script_exists = restart_backend_script.is_file()
            restart_backend_script_executable = bool(restart_backend_script.stat().st_mode & 0o111)
            restart_backend_script_text = restart_backend_script.read_text(encoding="utf-8")
            frontend_script_exists = frontend_script.is_file()
            frontend_script_text = frontend_script.read_text(encoding="utf-8")
            service_script_exists = service_script.is_file()
            service_script_text = service_script.read_text(encoding="utf-8")
            inspect_script_exists = inspect_script.is_file()
            inspect_script_text = inspect_script.read_text(encoding="utf-8")
            verify_handoff_script_exists = verify_handoff_script.is_file()
            verify_handoff_script_text = verify_handoff_script.read_text(encoding="utf-8")
            finalize_script_exists = finalize_script.is_file()
            finalize_script_text = finalize_script.read_text(encoding="utf-8")
            import_script_exists = import_script.is_file()
            import_script_text = import_script.read_text(encoding="utf-8")
            record_script_exists = record_script.is_file()
            record_script_text = record_script.read_text(encoding="utf-8")
            inspect_script_run = subprocess.run(
                [str(inspect_script)],
                check=False,
                capture_output=True,
                text=True,
            )

        self.assertTrue(result.preflight["passed"])
        self.assertTrue(clients_dir_exists)
        self.assertTrue(performance_dir_exists)
        self.assertTrue(readme_exists)
        self.assertIn("LAN Multi-Trader Smoke Run", readme_text)
        self.assertIn("http://192.168.1.10:5173/", readme_text)
        self.assertIn("record-workflow.sh", readme_text)
        self.assertIn("verify-handoff.sh", readme_text)
        self.assertIn("finalize-package.sh", readme_text)
        self.assertIn("CLIENT_INSTRUCTIONS.md", readme_text)
        self.assertIn("## Operator Flow", readme_text)
        self.assertIn("Run the verify services script", readme_text)
        self.assertIn("Run the inspect next action script repeatedly", readme_text)
        self.assertIn("Run the verify handoff gate script", readme_text)
        self.assertIn("## Workflow Recording", readme_text)
        self.assertIn("record-workflow.sh cold_query --symbol 00005.HK", readme_text)
        self.assertIn("record-workflow.sh add_to_watchlist --symbol 00939.HK", readme_text)
        self.assertIn("record-workflow.sh refresh_recovery", readme_text)
        self.assertIn("record-workflow.sh redis_clear_recovery --symbol 00700.HK", readme_text)
        self.assertIn("clear-runtime-cache --trade-date 20260525 --symbols 00700.HK", readme_text)
        self.assertIn("--dry-run", readme_text)
        self.assertIn("--confirm", readme_text)
        self.assertIn("## Process Restart Recovery", readme_text)
        self.assertIn("restart-backend.sh", readme_text)
        self.assertIn("record-workflow.sh process_restart_recovery", readme_text)
        self.assertIn("closed_market_effective_date --requested-trade-date 20260525 --effective-trade-date 20260522", readme_text)
        self.assertIn("service-preflight.json", readme_text)
        self.assertIn("smoke-import-manifest.json", readme_text)
        self.assertIn("smoke-run-manifest.json", readme_text)
        self.assertIn("multi-trader-smoke-evidence.zip", readme_text)
        self.assertIn("smoke-run-package.json", readme_text)
        self.assertTrue(client_instructions_exists)
        self.assertIn("http://192.168.1.10:5173/", client_instructions_text)
        self.assertIn("machine id", client_instructions_text)
        self.assertIn("client smoke JSON", client_instructions_text)
        self.assertIn("performance smoke JSON", client_instructions_text)
        self.assertTrue(backend_script_exists)
        self.assertTrue(backend_script_executable)
        self.assertIn("real_data_runner", backend_script_text)
        self.assertIn(f"cd {shlex.quote(str(ops_module.repository_root()))}", backend_script_text)
        self.assertIn("prepared Gateway port 9020 is already in use", backend_script_text)
        self.assertTrue(restart_backend_script_exists)
        self.assertTrue(restart_backend_script_executable)
        self.assertIn("real_data_runner", restart_backend_script_text)
        self.assertIn(str(root / "runtime-health-verification.json"), restart_backend_script_text)
        self.assertIn(str(backend_script.resolve()), restart_backend_script_text)
        self.assertTrue(frontend_script_exists)
        self.assertIn("npm run dev", frontend_script_text)
        self.assertIn("--port 5173", frontend_script_text)
        self.assertIn("--strictPort", frontend_script_text)
        self.assertIn("prepared frontend port 5173 is already in use", frontend_script_text)
        self.assertTrue(service_script_exists)
        self.assertIn("verify-multi-trader-smoke-services", service_script_text)
        self.assertTrue(inspect_script_exists)
        self.assertIn("inspect-multi-trader-smoke", inspect_script_text)
        self.assertIn("--next-action", inspect_script_text)
        self.assertEqual(inspect_script_run.returncode, 1)
        self.assertEqual(json.loads(inspect_script_run.stdout)["next_action"]["stage"], "runtime_health")
        self.assertEqual(inspect_script_run.stderr, "")
        self.assertTrue(verify_handoff_script_exists)
        self.assertIn("inspect-multi-trader-smoke", verify_handoff_script_text)
        self.assertIn("--require-package-ready", verify_handoff_script_text)
        self.assertTrue(finalize_script_exists)
        self.assertIn("finalize-multi-trader-smoke", finalize_script_text)
        self.assertIn('$(date --iso-8601=seconds)', finalize_script_text)
        self.assertTrue(import_script_exists)
        self.assertIn("import-multi-trader-smoke-artifacts", import_script_text)
        self.assertIn('--input-path "$1"', import_script_text)
        self.assertIn('kind="${2:-auto}"', import_script_text)
        self.assertTrue(record_script_exists)
        self.assertIn("record-multi-trader-smoke-workflow", record_script_text)
        self.assertIn("--workflows-path", record_script_text)
        self.assertIn('--workflow "$workflow"', record_script_text)
        self.assertIn('$(date --iso-8601=seconds)', record_script_text)
        self.assertEqual(preflight["page_url"], "http://192.168.1.10:5173/")
        self.assertEqual(preflight["gateway_url"], "ws://192.168.1.10:9020/ws")
        self.assertEqual(preflight["requested_frontend_port"], "5173")
        self.assertEqual(preflight["requested_gateway_port"], "9020")
        self.assertFalse(preflight["auto_selected_frontend_port"])
        self.assertFalse(preflight["auto_selected_gateway_port"])
        self.assertTrue(preflight["local_lan_host"]["matches_local_address"])
        self.assertEqual(preflight["runtime_symbols"], ["00005.HK", "00700.HK", "00939.HK"])
        self.assertEqual(preflight["frontend_initial_symbols"], ["00700.HK"])
        self.assertEqual(preflight["artifact_paths"]["runtime_health"], str(root / "runtime-health-verification.json"))
        self.assertEqual(preflight["artifact_paths"]["service_preflight"], str(root / "service-preflight.json"))
        self.assertEqual(preflight["artifact_paths"]["scripts"], str(root / "scripts"))
        self.assertEqual(preflight["artifact_paths"]["readme"], str(readme_path))
        self.assertEqual(preflight["artifact_paths"]["client_instructions"], str(client_instructions_path))
        self.assertEqual(preflight["artifact_paths"]["import_manifest"], str(root / "smoke-import-manifest.json"))
        self.assertEqual(preflight["artifact_paths"]["run_manifest"], str(root / "smoke-run-manifest.json"))
        self.assertEqual(preflight["artifact_paths"]["package"], str(root / "multi-trader-smoke-evidence.zip"))
        self.assertEqual(preflight["artifact_paths"]["package_metadata"], str(root / "smoke-run-package.json"))
        self.assertIn("--trade-date 20260525", preflight["commands"]["backend"])
        self.assertEqual(preflight["commands"]["backend_script"], str(backend_script.resolve()))
        self.assertEqual(preflight["commands"]["restart_backend_script"], str(restart_backend_script.resolve()))
        self.assertEqual(preflight["commands"]["frontend_script"], str(frontend_script.resolve()))
        self.assertEqual(preflight["commands"]["service_preflight_script"], str(service_script.resolve()))
        self.assertEqual(preflight["commands"]["inspect_next_action_script"], str(inspect_script.resolve()))
        self.assertEqual(preflight["commands"]["verify_handoff_script"], str(verify_handoff_script.resolve()))
        self.assertEqual(preflight["commands"]["finalize_package_script"], str(finalize_script.resolve()))
        self.assertEqual(preflight["commands"]["import_artifacts_script"], str(import_script.resolve()))
        self.assertEqual(preflight["commands"]["record_workflow_script"], str(record_script.resolve()))
        self.assertIn("--next-action", preflight["commands"]["inspect_next_action"])
        self.assertIn("--require-package-ready", preflight["commands"]["verify_handoff"])
        self.assertIn('$(date --iso-8601=seconds)', preflight["commands"]["finalize_package"])
        self.assertEqual(
            preflight["commands"]["redis_clear_dry_run"],
            "PYTHONPATH=backend python -m beast_market.ops_cli clear-runtime-cache --trade-date 20260525 "
            "--symbols 00700.HK --redis-url redis://127.0.0.1:6379/0 --dry-run",
        )
        self.assertEqual(
            preflight["commands"]["redis_clear_confirm"],
            "PYTHONPATH=backend python -m beast_market.ops_cli clear-runtime-cache --trade-date 20260525 "
            "--symbols 00700.HK --redis-url redis://127.0.0.1:6379/0 --confirm",
        )
        self.assertIn("import-multi-trader-smoke-artifacts", preflight["commands"]["import_artifacts"])
        self.assertIn("record-multi-trader-smoke-workflow", preflight["commands"]["record_workflow"])
        self.assertIn("--symbols 00005.HK,00700.HK,00939.HK", preflight["commands"]["backend"])
        self.assertIn(f"--runtime-health-path {root / 'runtime-health-verification.json'}", preflight["commands"]["backend"])
        self.assertEqual(
            preflight["commands"]["frontend"],
            "cd market-terminal && VITE_MARKET_DATA_MODE=live VITE_MARKET_WS_URL=ws://192.168.1.10:9020/ws "
            "VITE_MARKET_PROTOCOL=terminal-message-v1 VITE_MARKET_SYMBOLS=00700.HK "
            "npm run dev -- --port 5173 --strictPort",
        )
        self.assertEqual(
            preflight["commands"]["frontend_pnpm"],
            "cd market-terminal && VITE_MARKET_DATA_MODE=live VITE_MARKET_WS_URL=ws://192.168.1.10:9020/ws "
            "VITE_MARKET_PROTOCOL=terminal-message-v1 VITE_MARKET_SYMBOLS=00700.HK "
            "corepack pnpm dev -- --port 5173 --strictPort",
        )
        self.assertEqual(preflight["commands"]["frontend_npm"], preflight["commands"]["frontend"])
        self.assertIn("verify-multi-trader-smoke-services", preflight["commands"]["service_preflight"])
        self.assertIn(str(root), preflight["commands"]["service_preflight"])
        self.assertEqual(workflows["workflows"]["cold_query"]["symbol"], "00005.HK")
        self.assertIn("Use two different LAN client machines for the final smoke.", client_instructions_text)
        self.assertIn("Client A: start with `00700.HK`, then cold query `00005.HK`.", client_instructions_text)
        self.assertIn("Client B: start with `00700.HK`, then add `00939.HK` to watchlist.", client_instructions_text)
        self.assertIn("After the operator clears Redis for `00700.HK`", client_instructions_text)
        self.assertIn("Keep this browser tab open and connected until the backend operator confirms runtime health has observed this machine id.", client_instructions_text)
        self.assertNotIn("00700.HK, 00700.HK", client_instructions_text)

    def test_prepare_multi_trader_smoke_passes_silver_root_to_backend_command(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "smoke"
            silver_root = Path(temp_dir) / "xtquant silver"
            silver_root.mkdir()

            result = prepare_multi_trader_smoke(
                root_path=root,
                lan_host="192.168.1.10",
                local_addresses=["192.168.1.10"],
                gateway_port=9021,
                silver_root=silver_root,
                cold_query_symbol="00005.HK",
                redis_clear_symbol="00700.HK",
                add_to_watchlist_symbol="00939.HK",
                requested_trade_date="20260525",
            )
            preflight = json.loads(result.preflight_path.read_text(encoding="utf-8"))
            backend_script_text = (root / "scripts" / "start-backend.sh").read_text(encoding="utf-8")

        self.assertEqual(preflight["silver_root"], str(silver_root))
        self.assertIn("--silver-root", preflight["commands"]["backend"])
        self.assertIn(shlex.quote(str(silver_root)), preflight["commands"]["backend"])
        self.assertIn("--silver-root", backend_script_text)
        self.assertIn(shlex.quote(str(silver_root)), backend_script_text)

    def test_prepares_multi_trader_smoke_can_auto_select_free_ports(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "smoke"
            with patch.object(ops_module, "port_bind_available", side_effect=lambda port: port not in {5173, 9020}):
                result = prepare_multi_trader_smoke(
                    root_path=root,
                    lan_host="192.168.1.10",
                    local_addresses=["192.168.1.10"],
                    frontend_port="auto",
                    gateway_port="auto",
                )
            preflight = json.loads(result.preflight_path.read_text(encoding="utf-8"))
            frontend_script_text = (root / "scripts" / "start-frontend.sh").read_text(encoding="utf-8")
            backend_script_text = (root / "scripts" / "start-backend.sh").read_text(encoding="utf-8")

        self.assertTrue(result.preflight["passed"])
        self.assertEqual(preflight["requested_frontend_port"], "auto")
        self.assertEqual(preflight["requested_gateway_port"], "auto")
        self.assertTrue(preflight["auto_selected_frontend_port"])
        self.assertTrue(preflight["auto_selected_gateway_port"])
        self.assertEqual(preflight["frontend_port"], 5174)
        self.assertEqual(preflight["gateway_port"], 9021)
        self.assertEqual(preflight["page_url"], f"http://192.168.1.10:{preflight['frontend_port']}/")
        self.assertEqual(preflight["gateway_url"], f"ws://192.168.1.10:{preflight['gateway_port']}/ws")
        self.assertIn(f"--port {preflight['frontend_port']}", frontend_script_text)
        self.assertIn(f"VITE_MARKET_WS_URL=ws://192.168.1.10:{preflight['gateway_port']}/ws", frontend_script_text)
        self.assertIn("VITE_MARKET_DATA_MODE=live", frontend_script_text)
        self.assertIn(f"prepared Gateway port {preflight['gateway_port']} is already in use", backend_script_text)

    def test_prepare_multi_trader_smoke_scripts_pin_repository_root_not_cwd(self) -> None:
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            root = temp_path / "smoke"
            original_cwd = Path.cwd()
            os.chdir(temp_path)
            try:
                result = prepare_multi_trader_smoke(
                    root_path=root,
                    lan_host="192.168.1.10",
                    local_addresses=["192.168.1.10"],
                    cold_query_symbol="00005.HK",
                    redis_clear_symbol="00700.HK",
                    add_to_watchlist_symbol="00939.HK",
                    requested_trade_date="20260525",
                    effective_trade_date="20260522",
                )
            finally:
                os.chdir(original_cwd)

            backend_script_text = (root / "scripts" / "start-backend.sh").read_text(encoding="utf-8")
            frontend_script_text = (root / "scripts" / "start-frontend.sh").read_text(encoding="utf-8")

        repo_root = ops_module.repository_root()
        self.assertTrue(result.preflight["passed"])
        self.assertIn(f"cd {shlex.quote(str(repo_root))}", backend_script_text)
        self.assertIn(f"cd {shlex.quote(str(repo_root))}", frontend_script_text)
        self.assertNotIn(f"cd {shlex.quote(str(temp_path))}", backend_script_text)

    def test_prepare_multi_trader_smoke_quotes_service_script_root_path(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "smoke dir"

            result = prepare_multi_trader_smoke(
                root_path=root,
                lan_host="192.168.1.10",
                local_addresses=["192.168.1.10"],
            )
            preflight = json.loads(result.preflight_path.read_text(encoding="utf-8"))
            service_script_text = (root / "scripts" / "verify-services.sh").read_text(encoding="utf-8")

        quoted_root = shlex.quote(str(root))
        self.assertIn(f"--root-path {quoted_root}", preflight["commands"]["service_preflight"])
        self.assertIn(f"--root-path {quoted_root}", service_script_text)

    def test_prepare_multi_trader_smoke_start_scripts_fail_when_prepared_port_is_busy(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "smoke"
            frontend_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            frontend_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            frontend_socket.bind(("0.0.0.0", 0))
            frontend_socket.listen(1)
            frontend_port = frontend_socket.getsockname()[1]
            gateway_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            gateway_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            gateway_socket.bind(("0.0.0.0", 0))
            gateway_socket.listen(1)
            gateway_port = gateway_socket.getsockname()[1]
            try:
                prepare_multi_trader_smoke(
                    root_path=root,
                    lan_host="192.168.1.10",
                    local_addresses=["192.168.1.10"],
                    frontend_port=frontend_port,
                    gateway_port=gateway_port,
                )
                frontend_run = subprocess.run(
                    [str(root / "scripts" / "start-frontend.sh")],
                    check=False,
                    capture_output=True,
                    text=True,
                )
                backend_run = subprocess.run(
                    [str(root / "scripts" / "start-backend.sh")],
                    check=False,
                    capture_output=True,
                    text=True,
                )
            finally:
                frontend_socket.close()
                gateway_socket.close()

        self.assertEqual(frontend_run.returncode, 1)
        self.assertIn(f"prepared frontend port {frontend_port} is already in use", frontend_run.stderr)
        self.assertEqual(backend_run.returncode, 1)
        self.assertIn(f"prepared Gateway port {gateway_port} is already in use", backend_run.stderr)

    def test_prepares_multi_trader_smoke_can_auto_detect_lan_host(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "smoke"

            result = prepare_multi_trader_smoke(
                root_path=root,
                lan_host="auto",
                local_addresses=["192.168.1.10", "10.0.0.5"],
                require_local_lan_host=True,
            )
            preflight = json.loads(result.preflight_path.read_text(encoding="utf-8"))

        self.assertTrue(result.preflight["passed"])
        self.assertEqual(preflight["requested_lan_host"], "auto")
        self.assertTrue(preflight["auto_detected_lan_host"])
        self.assertEqual(preflight["lan_host"], "10.0.0.5")
        self.assertEqual(preflight["page_url"], "http://10.0.0.5:5173/")
        self.assertEqual(preflight["commands"]["client_url"], "http://10.0.0.5:5173/")

    def test_prepares_multi_trader_smoke_can_auto_create_root_path(self) -> None:
        with TemporaryDirectory() as temp_dir:
            base = Path(temp_dir) / "multi-trader-smoke"

            result = prepare_multi_trader_smoke(
                root_path="auto",
                auto_root_base_path=base,
                lan_host="192.168.1.10",
                local_addresses=["192.168.1.10"],
            )
            preflight = json.loads(result.preflight_path.read_text(encoding="utf-8"))

        self.assertTrue(result.preflight["passed"])
        self.assertEqual(result.root_path.parent, base)
        self.assertTrue(result.root_path.name)
        self.assertEqual(preflight["requested_root_path"], "auto")
        self.assertTrue(preflight["auto_created_root_path"])
        self.assertEqual(preflight["artifact_paths"]["root"], str(result.root_path))

    def test_prepares_multi_trader_smoke_reports_auto_detect_failure(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "smoke"

            result = prepare_multi_trader_smoke(
                root_path=root,
                lan_host="auto",
                local_addresses=[],
            )
            preflight = json.loads(result.preflight_path.read_text(encoding="utf-8"))

        self.assertFalse(result.preflight["passed"])
        self.assertFalse(preflight["auto_detected_lan_host"])
        self.assertIn("multi_trader_smoke_lan_host_auto_detect_failed", preflight["blockers"])

    def test_ops_cli_prepare_multi_trader_smoke_can_auto_create_root_path(self) -> None:
        output = StringIO()

        with redirect_stdout(output):
            exit_code = ops_main(
                [
                    "prepare-multi-trader-smoke",
                    "--root-path",
                    "auto",
                    "--lan-host",
                    "192.168.1.10",
                ]
        )
        summary = json.loads(output.getvalue())
        root = Path(summary["root_path"])
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        preflight = json.loads((root / "lan-preflight.json").read_text(encoding="utf-8"))

        self.assertEqual(exit_code, 0)
        self.assertNotEqual(summary["root_path"], "auto")
        self.assertEqual(root.parent, ops_module.repository_root() / "artifacts" / "multi-trader-smoke")
        self.assertEqual(preflight["requested_root_path"], "auto")
        self.assertTrue(preflight["auto_created_root_path"])

    def test_ops_cli_prepare_multi_trader_smoke_auto_root_is_repo_relative_not_cwd(self) -> None:
        with TemporaryDirectory() as temp_dir:
            original_cwd = Path.cwd()
            os.chdir(temp_dir)
            output = StringIO()
            try:
                with redirect_stdout(output):
                    exit_code = ops_main(
                        [
                            "prepare-multi-trader-smoke",
                            "--root-path",
                            "auto",
                            "--lan-host",
                            "192.168.1.10",
                        ]
                    )
            finally:
                os.chdir(original_cwd)
            summary = json.loads(output.getvalue())
            root = Path(summary["root_path"])
            self.addCleanup(shutil.rmtree, root, ignore_errors=True)

        self.assertEqual(exit_code, 0)
        self.assertTrue(root.is_absolute())
        self.assertEqual(root.parent, ops_module.repository_root() / "artifacts" / "multi-trader-smoke")
        self.assertFalse((Path(temp_dir) / "artifacts").exists())

    def test_ops_cli_prepare_multi_trader_smoke_can_print_shell_env(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "smoke dir"
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "prepare-multi-trader-smoke",
                        "--root-path",
                        str(root),
                        "--lan-host",
                        "192.168.1.10",
                        "--print-env",
                    ]
                )
            lines = output.getvalue().strip().splitlines()

        self.assertEqual(exit_code, 0)
        self.assertEqual(lines[0], f"export SMOKE_DIR={shlex.quote(str(root))}")
        self.assertEqual(lines[1], "export SMOKE_PAGE_URL=http://192.168.1.10:5173/")
        self.assertEqual(lines[2], "export SMOKE_GATEWAY_URL=ws://192.168.1.10:9020/ws")
        self.assertEqual(lines[3], f"export SMOKE_PREFLIGHT_PATH={shlex.quote(str(root / 'lan-preflight.json'))}")
        self.assertEqual(lines[4], f"export SMOKE_CLIENT_INSTRUCTIONS_PATH={shlex.quote(str(root / 'CLIENT_INSTRUCTIONS.md'))}")
        self.assertEqual(lines[5], f"export SMOKE_WORKFLOWS_PATH={shlex.quote(str(root / 'workflows.json'))}")
        self.assertEqual(lines[6], f"export SMOKE_SERVICE_PREFLIGHT_PATH={shlex.quote(str(root / 'service-preflight.json'))}")
        self.assertEqual(lines[7], f"export SMOKE_RUNTIME_HEALTH_PATH={shlex.quote(str(root / 'runtime-health-verification.json'))}")
        self.assertEqual(lines[8], f"export SMOKE_OBSERVATION_PATH={shlex.quote(str(root / 'multi-trader-smoke-observation.json'))}")
        self.assertEqual(lines[9], f"export SMOKE_EVIDENCE_PATH={shlex.quote(str(root / 'multi-trader-smoke-evidence.json'))}")
        self.assertEqual(lines[10], f"export SMOKE_IMPORT_MANIFEST_PATH={shlex.quote(str(root / 'smoke-import-manifest.json'))}")
        self.assertEqual(lines[11], f"export SMOKE_RUN_MANIFEST_PATH={shlex.quote(str(root / 'smoke-run-manifest.json'))}")
        self.assertEqual(lines[12], f"export SMOKE_PACKAGE_PATH={shlex.quote(str(root / 'multi-trader-smoke-evidence.zip'))}")
        self.assertEqual(lines[13], f"export SMOKE_PACKAGE_METADATA_PATH={shlex.quote(str(root / 'smoke-run-package.json'))}")
        self.assertIn("export SMOKE_BACKEND_COMMAND=", lines[14])
        self.assertIn("real_data_runner", lines[14])
        self.assertIn("export SMOKE_FRONTEND_COMMAND=", lines[15])
        self.assertIn("VITE_MARKET_DATA_MODE=live", lines[15])
        self.assertIn("VITE_MARKET_WS_URL=ws://192.168.1.10:9020/ws", lines[15])
        self.assertIn("VITE_MARKET_SYMBOLS=", lines[15])
        self.assertIn("export SMOKE_SERVICE_PREFLIGHT_COMMAND=", lines[16])
        self.assertIn("verify-multi-trader-smoke-services", lines[16])
        self.assertIn("export SMOKE_BACKEND_SCRIPT=", lines[17])
        self.assertIn("start-backend.sh", lines[17])
        self.assertIn(str(root.resolve()), lines[17])
        self.assertIn("export SMOKE_RESTART_BACKEND_SCRIPT=", lines[18])
        self.assertIn("restart-backend.sh", lines[18])
        self.assertIn(str(root.resolve()), lines[18])
        self.assertIn("export SMOKE_FRONTEND_SCRIPT=", lines[19])
        self.assertIn("start-frontend.sh", lines[19])
        self.assertIn(str(root.resolve()), lines[19])
        self.assertIn("export SMOKE_SERVICE_PREFLIGHT_SCRIPT=", lines[20])
        self.assertIn("verify-services.sh", lines[20])
        self.assertIn("export SMOKE_INSPECT_NEXT_ACTION_SCRIPT=", lines[21])
        self.assertIn("inspect-next-action.sh", lines[21])
        self.assertIn("export SMOKE_VERIFY_HANDOFF_SCRIPT=", lines[22])
        self.assertIn("verify-handoff.sh", lines[22])
        self.assertIn("export SMOKE_FINALIZE_PACKAGE_SCRIPT=", lines[23])
        self.assertIn("finalize-package.sh", lines[23])
        self.assertIn("export SMOKE_IMPORT_ARTIFACTS_SCRIPT=", lines[24])
        self.assertIn("import-artifacts.sh", lines[24])
        self.assertIn("export SMOKE_RECORD_WORKFLOW_SCRIPT=", lines[25])
        self.assertIn("record-workflow.sh", lines[25])

    def test_prepare_multi_trader_smoke_quotes_restart_command_in_readme(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "smoke dir"

            prepare_multi_trader_smoke(
                root_path=root,
                lan_host="192.168.1.10",
            )
            readme_text = (root / "README.md").read_text(encoding="utf-8")

        self.assertIn(f"{shlex.quote(str(root / 'scripts' / 'restart-backend.sh'))}", readme_text)
        self.assertIn(f"{shlex.quote(str(root / 'scripts' / 'start-backend.sh'))}", readme_text)
        self.assertIn(f"{shlex.quote(str(root / 'scripts' / 'start-frontend.sh'))}", readme_text)
        self.assertIn(f"{shlex.quote(str(root / 'scripts' / 'verify-services.sh'))}", readme_text)
        self.assertIn(f"{shlex.quote(str(root / 'scripts' / 'inspect-next-action.sh'))}", readme_text)
        self.assertIn(f"{shlex.quote(str(root / 'scripts' / 'verify-handoff.sh'))}", readme_text)
        self.assertIn(f"{shlex.quote(str(root / 'scripts' / 'finalize-package.sh'))}", readme_text)
        self.assertIn(f"{shlex.quote(str(root / 'scripts' / 'record-workflow.sh'))} cold_query", readme_text)
        self.assertIn(f"{shlex.quote(str(root / 'scripts' / 'record-workflow.sh'))} process_restart_recovery", readme_text)
        self.assertIn(f"{shlex.quote(str(root / 'scripts' / 'import-artifacts.sh'))} <downloads-file-or-dir>", readme_text)
        self.assertNotIn(f"{root / 'scripts' / 'record-workflow.sh'} cold_query", readme_text)

    def test_ops_cli_prepare_multi_trader_smoke_print_env_includes_silver_root_when_supplied(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "smoke"
            silver_root = Path(temp_dir) / "silver root"
            silver_root.mkdir()
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "prepare-multi-trader-smoke",
                        "--root-path",
                        str(root),
                        "--lan-host",
                        "192.168.1.10",
                        "--silver-root",
                        str(silver_root),
                        "--print-env",
                    ]
                )
            lines = output.getvalue().strip().splitlines()
            preflight = json.loads((root / "lan-preflight.json").read_text(encoding="utf-8"))

        self.assertEqual(exit_code, 0)
        self.assertIn(f"export SMOKE_SILVER_ROOT={shlex.quote(str(silver_root))}", lines)
        backend_line = next(line for line in lines if line.startswith("export SMOKE_BACKEND_COMMAND="))
        self.assertIn("--silver-root", backend_line)
        self.assertIn(shlex.quote(str(silver_root)), backend_line)
        self.assertEqual(preflight["silver_root"], str(silver_root))

    def test_ops_cli_prepare_multi_trader_smoke_print_env_returns_nonzero_on_failed_preflight(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "smoke"
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "prepare-multi-trader-smoke",
                        "--root-path",
                        str(root),
                        "--lan-host",
                        "127.0.0.1",
                        "--print-env",
                    ]
                )
            lines = output.getvalue().strip().splitlines()

        self.assertEqual(exit_code, 1)
        self.assertEqual(lines[0], f"export SMOKE_DIR={shlex.quote(str(root))}")
        self.assertIn("multi_trader_smoke_lan_host_not_client_routable", lines[-2])
        self.assertEqual(lines[-1], "false")

    def test_prepares_multi_trader_smoke_normalizes_and_rejects_symbols(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "smoke"

            result = prepare_multi_trader_smoke(
                root_path=root,
                lan_host="192.168.1.10",
                cold_query_symbol="700",
                redis_clear_symbol="00700.HK",
                add_to_watchlist_symbol="939",
            )
            preflight = json.loads(result.preflight_path.read_text(encoding="utf-8"))
            workflows = json.loads(result.workflows_path.read_text(encoding="utf-8"))

        self.assertEqual(preflight["runtime_symbols"], ["00700.HK", "00939.HK"])
        self.assertEqual(preflight["frontend_initial_symbols"], ["00700.HK"])
        self.assertIn("--symbols 00700.HK,00939.HK", preflight["commands"]["backend"])
        self.assertEqual(workflows["workflows"]["cold_query"]["symbol"], "00700.HK")
        self.assertEqual(workflows["workflows"]["add_to_watchlist"]["symbol"], "00939.HK")

        with TemporaryDirectory() as temp_dir:
            with self.assertRaisesRegex(ValueError, "smoke symbol must use canonical HK format"):
                prepare_multi_trader_smoke(
                    root_path=Path(temp_dir) / "smoke",
                    lan_host="192.168.1.10",
                    cold_query_symbol="ABC",
                )

    def test_preflight_rejects_loopback_lan_host(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "smoke"

            result = prepare_multi_trader_smoke(root_path=root, lan_host="127.0.0.1")

        self.assertFalse(result.preflight["passed"])
        self.assertIn("multi_trader_smoke_lan_host_not_client_routable", result.preflight["blockers"])
        self.assertIn("multi_trader_smoke_page_url_not_client_routable", result.preflight["blockers"])
        self.assertIn("multi_trader_smoke_gateway_url_not_client_routable", result.preflight["blockers"])

    def test_preflight_can_require_lan_host_to_match_local_address(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "smoke"

            result = prepare_multi_trader_smoke(
                root_path=root,
                lan_host="192.168.1.99",
                require_local_lan_host=True,
                local_addresses=["192.168.1.10"],
            )

        self.assertFalse(result.preflight["passed"])
        self.assertIn("multi_trader_smoke_lan_host_not_local", result.preflight["blockers"])
        self.assertIn("multi_trader_smoke_lan_host_not_detected_on_local_machine", result.preflight["warnings"])

    def test_verifies_multi_trader_smoke_services_and_updates_preflight(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "smoke"
            prepare_multi_trader_smoke(root_path=root, lan_host="192.168.1.10")

            with patch("beast_market.ops.probe_service_url") as probe:
                probe.side_effect = [
                    {
                        "url": "http://192.168.1.10:5173/",
                        "host": "192.168.1.10",
                        "port": 5173,
                        "probe_kind": "frontend_http",
                        "reachable": True,
                        "status_code": 200,
                        "error": "",
                    },
                    {
                        "url": "ws://192.168.1.10:9020/ws",
                        "host": "192.168.1.10",
                        "port": 9020,
                        "probe_kind": "gateway_websocket",
                        "reachable": True,
                        "websocket_handshake": True,
                        "status_code": 101,
                        "error": "",
                    },
                ]
                result = verify_multi_trader_smoke_services(root_path=root, timeout_seconds=0.2)

            preflight = json.loads((root / "lan-preflight.json").read_text(encoding="utf-8"))
            service_preflight = json.loads((root / "service-preflight.json").read_text(encoding="utf-8"))

        self.assertTrue(result.passed)
        self.assertTrue(service_preflight["passed"])
        self.assertTrue(preflight["passed"])
        self.assertEqual(preflight["service_checks"]["checks"]["frontend"]["port"], 5173)
        self.assertEqual(preflight["service_checks"]["checks"]["frontend"]["probe_kind"], "frontend_http")
        self.assertEqual(preflight["service_checks"]["checks"]["frontend"]["status_code"], 200)
        self.assertEqual(preflight["service_checks"]["checks"]["gateway"]["port"], 9020)
        self.assertEqual(preflight["service_checks"]["checks"]["gateway"]["probe_kind"], "gateway_websocket")
        self.assertTrue(preflight["service_checks"]["checks"]["gateway"]["websocket_handshake"])

    def test_verifies_multi_trader_smoke_services_records_unreachable_ports(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "smoke"
            prepare_multi_trader_smoke(root_path=root, lan_host="192.168.1.10")

            with patch("beast_market.ops.probe_service_url") as probe:
                probe.side_effect = [
                    {
                        "url": "http://192.168.1.10:5173/",
                        "host": "192.168.1.10",
                        "port": 5173,
                        "probe_kind": "frontend_http",
                        "reachable": False,
                        "status_code": None,
                        "error": "connection refused",
                    },
                    {
                        "url": "ws://192.168.1.10:9020/ws",
                        "host": "192.168.1.10",
                        "port": 9020,
                        "probe_kind": "gateway_websocket",
                        "reachable": True,
                        "websocket_handshake": True,
                        "status_code": 101,
                        "error": "",
                    },
                ]
                result = verify_multi_trader_smoke_services(root_path=root, timeout_seconds=0.2)

            preflight = json.loads((root / "lan-preflight.json").read_text(encoding="utf-8"))

        self.assertFalse(result.passed)
        self.assertFalse(preflight["passed"])
        self.assertIn("multi_trader_smoke_frontend_service_unreachable", preflight["blockers"])
        self.assertFalse(preflight["service_checks"]["checks"]["frontend"]["reachable"])

    def test_verifies_multi_trader_smoke_services_clears_previous_service_blockers(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "smoke"
            prepare_multi_trader_smoke(root_path=root, lan_host="192.168.1.10")

            with patch("beast_market.ops.probe_service_url") as probe:
                probe.side_effect = [
                    {
                        "url": "http://192.168.1.10:5173/",
                        "host": "192.168.1.10",
                        "port": 5173,
                        "reachable": False,
                        "error": "connection refused",
                    },
                    {
                        "url": "ws://192.168.1.10:9020/ws",
                        "host": "192.168.1.10",
                        "port": 9020,
                        "reachable": False,
                        "error": "connection refused",
                    },
                ]
                first = verify_multi_trader_smoke_services(root_path=root, timeout_seconds=0.2)

            with patch("beast_market.ops.probe_service_url") as probe:
                probe.side_effect = [
                    {
                        "url": "http://192.168.1.10:5173/",
                        "host": "192.168.1.10",
                        "port": 5173,
                        "reachable": True,
                        "error": "",
                    },
                    {
                        "url": "ws://192.168.1.10:9020/ws",
                        "host": "192.168.1.10",
                        "port": 9020,
                        "reachable": True,
                        "error": "",
                    },
                ]
                second = verify_multi_trader_smoke_services(root_path=root, timeout_seconds=0.2)

            preflight = json.loads((root / "lan-preflight.json").read_text(encoding="utf-8"))

        self.assertFalse(first.passed)
        self.assertTrue(second.passed)
        self.assertTrue(preflight["passed"])
        self.assertEqual(preflight["blockers"], [])
        self.assertTrue(preflight["service_checks"]["checks"]["frontend"]["reachable"])
        self.assertTrue(preflight["service_checks"]["checks"]["gateway"]["reachable"])

    def test_verifies_multi_trader_smoke_services_keeps_non_service_preflight_blockers(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "smoke"
            prepare_multi_trader_smoke(root_path=root, lan_host="127.0.0.1")

            with patch("beast_market.ops.probe_service_url") as probe:
                probe.side_effect = [
                    {
                        "url": "http://127.0.0.1:5173/",
                        "host": "127.0.0.1",
                        "port": 5173,
                        "reachable": True,
                        "error": "",
                    },
                    {
                        "url": "ws://127.0.0.1:9020/ws",
                        "host": "127.0.0.1",
                        "port": 9020,
                        "reachable": True,
                        "error": "",
                    },
                ]
                result = verify_multi_trader_smoke_services(root_path=root, timeout_seconds=0.2)

            preflight = json.loads((root / "lan-preflight.json").read_text(encoding="utf-8"))

        self.assertFalse(result.passed)
        self.assertFalse(preflight["passed"])
        self.assertEqual(result.service_checks["blockers"], [])
        self.assertIn("multi_trader_smoke_lan_host_not_client_routable", result.preflight["blockers"])
        self.assertIn("multi_trader_smoke_lan_host_not_client_routable", preflight["blockers"])

    def test_service_probe_requires_http_and_websocket_protocol_success(self) -> None:
        frontend_socket = FakeProbeSocket("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
        gateway_socket = FakeProbeSocket(
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n"
        )
        bad_gateway_socket = FakeProbeSocket("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")

        with patch("beast_market.ops.socket.create_connection", side_effect=[frontend_socket, gateway_socket, bad_gateway_socket]):
            frontend = ops_module.probe_service_url(
                "http://192.168.1.10:5173/",
                timeout_seconds=0.2,
                probe_kind="frontend_http",
            )
            gateway = ops_module.probe_service_url(
                "ws://192.168.1.10:9020/ws",
                timeout_seconds=0.2,
                probe_kind="gateway_websocket",
            )
            wrong_gateway = ops_module.probe_service_url(
                "ws://192.168.1.10:9020/ws",
                timeout_seconds=0.2,
                probe_kind="gateway_websocket",
            )

        self.assertTrue(frontend["reachable"])
        self.assertEqual(frontend["status_code"], 200)
        self.assertIn(b"GET / HTTP/1.1", frontend_socket.sent)
        self.assertTrue(gateway["reachable"])
        self.assertTrue(gateway["websocket_handshake"])
        self.assertIn(b"Upgrade: websocket", gateway_socket.sent)
        self.assertFalse(wrong_gateway["reachable"])
        self.assertEqual(wrong_gateway["error"], "websocket_handshake_failed")

    def test_ops_cli_prepares_multi_trader_smoke(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "smoke"
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "prepare-multi-trader-smoke",
                        "--root-path",
                        str(root),
                        "--lan-host",
                        "192.168.1.10",
                        "--cold-query-symbol",
                        "00005.HK",
                        "--redis-clear-symbol",
                        "00700.HK",
                        "--add-to-watchlist-symbol",
                        "00939.HK",
                        "--requested-trade-date",
                        "20260525",
                        "--effective-trade-date",
                        "20260522",
                    ]
            )
            summary = json.loads(output.getvalue())
            preflight_exists = (root / "lan-preflight.json").exists()
            workflows_exists = (root / "workflows.json").exists()

        self.assertEqual(exit_code, 0)
        self.assertTrue(summary["passed"])
        self.assertEqual(summary["page_url"], "http://192.168.1.10:5173/")
        self.assertEqual(summary["runtime_symbols"], ["00005.HK", "00700.HK", "00939.HK"])
        self.assertEqual(summary["frontend_initial_symbols"], ["00700.HK"])
        self.assertEqual(summary["artifact_paths"]["service_preflight"], str(root / "service-preflight.json"))
        self.assertIn("real_data_runner", summary["commands"]["backend"])
        self.assertIn("--trade-date 20260525", summary["commands"]["backend"])
        self.assertIn("--symbols 00005.HK,00700.HK,00939.HK", summary["commands"]["backend"])
        self.assertIn("VITE_MARKET_DATA_MODE=live", summary["commands"]["frontend"])
        self.assertIn("VITE_MARKET_WS_URL=ws://192.168.1.10:9020/ws", summary["commands"]["frontend"])
        self.assertIn("VITE_MARKET_SYMBOLS=00700.HK", summary["commands"]["frontend"])
        self.assertIn("npm run dev -- --port 5173 --strictPort", summary["commands"]["frontend"])
        self.assertIn("verify-multi-trader-smoke-services", summary["commands"]["service_preflight"])
        self.assertTrue(preflight_exists)
        self.assertTrue(workflows_exists)

    def test_ops_cli_verifies_multi_trader_smoke_services(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "smoke"
            output = StringIO()
            prepare_multi_trader_smoke(root_path=root, lan_host="192.168.1.10")

            with patch("beast_market.ops.probe_service_url") as probe:
                probe.side_effect = [
                    {
                        "url": "http://192.168.1.10:5173/",
                        "host": "192.168.1.10",
                        "port": 5173,
                        "reachable": True,
                        "error": "",
                    },
                    {
                        "url": "ws://192.168.1.10:9020/ws",
                        "host": "192.168.1.10",
                        "port": 9020,
                        "reachable": True,
                        "error": "",
                    },
                ]
                with redirect_stdout(output):
                    exit_code = ops_main(
                        [
                            "verify-multi-trader-smoke-services",
                            "--root-path",
                            str(root),
                            "--timeout-seconds",
                            "0.2",
                        ]
                    )

            summary = json.loads(output.getvalue())
            service_preflight_exists = (root / "service-preflight.json").exists()

        self.assertEqual(exit_code, 0)
        self.assertTrue(summary["passed"])
        self.assertEqual(summary["blockers"], [])
        self.assertEqual(summary["service_blockers"], [])
        self.assertTrue(service_preflight_exists)
        self.assertTrue(summary["checks"]["frontend"]["reachable"])

    def test_ops_cli_finalizes_legacy_retirement_from_artifacts(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            readiness_path = root / "cutover-readiness.json"
            frontend_path = root / "frontend-deployment.json"
            decommission_path = root / "legacy-decommission.json"
            readiness_path.write_text(
                json.dumps(
                    {
                        "legacy_retirement_allowed": True,
                        "frontend_default_v2_allowed": True,
                        "accepted_report_ids": ["session-1"],
                    }
                ),
                encoding="utf-8",
            )
            frontend_path.write_text(json.dumps({"frontend_default_v2_deployed": True}), encoding="utf-8")
            decommission_path.write_text(
                json.dumps(
                    {
                        "legacy_websocket_disabled": True,
                        "old_topic_consumers_disabled": True,
                        "no_legacy_consumers_observed": True,
                    }
                ),
                encoding="utf-8",
            )
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "finalize-legacy-retirement",
                        "--readiness-path",
                        str(readiness_path),
                        "--frontend-deployment-path",
                        str(frontend_path),
                        "--legacy-decommission-path",
                        str(decommission_path),
                        "--rollback-window-completed",
                        "--rollback-window-started-at",
                        "2026-05-22T14:00:00+08:00",
                        "--rollback-window-completed-at",
                        "2026-05-22T16:00:00+08:00",
                        "--operator-approved",
                        "--operator-approved-at",
                        "2026-05-22T16:05:00+08:00",
                        "--output-path",
                        str(root / "legacy-retirement.json"),
                        "--notes",
                        "operator approved",
                    ]
                )
            summary = json.loads(output.getvalue())

        self.assertEqual(exit_code, 0)
        self.assertTrue(summary["legacy_retired"])
        self.assertEqual(summary["blockers"], [])

    def test_ops_cli_verifies_full_evidence_bundle(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reports = root / "reports"
            manifests = root / "manifests"
            reports.mkdir()
            manifests.mkdir()
            report = passing_report()
            write_shadow_source_files(root / "shadow-sources", report)
            save_shadow_run_report(report, reports)
            write_required_manifests(manifests)
            write_json(
                root / "runtime-health.json",
                runtime_health_payload(for_completed_shadow_session=True),
            )
            write_json(root / "runtime-config.json", runtime_config_payload(root / "silver"))
            readiness = {
                "schema_version": 1,
                "passed": True,
                "frontend_default_v2_allowed": True,
                "legacy_retirement_allowed": True,
                "blockers": [],
                "legacy_retirement_blockers": [],
                "report_count": 1,
                "accepted_report_ids": ["session-1"],
                "rejected_reports": [],
                "policy": {
                    "min_parallel_session_count": 1,
                    "min_session_duration_seconds": 14400,
                    "min_stream_coverage_ratio": 0.9,
                    "require_non_empty_streams": True,
                    "require_no_failed_symbols": True,
                    "allow_legacy_retirement": True,
                },
            }
            write_json(root / "cutover-readiness.json", readiness)
            write_json(
                root / "frontend-deployment.json",
                {
                    "schema_version": 1,
                    "prepared_at": "2026-05-25T10:55:00+08:00",
                    "passed": True,
                    "expected_live_url": "ws://gateway.internal:9020/ws",
                    "frontend_default_v2_deployed": True,
                    "verified_at": "2026-05-22T13:30:02+08:00",
                    "deployed_env": {
                        "VITE_MARKET_DATA_MODE": "auto",
                        "VITE_MARKET_WS_URL": "ws://gateway.internal:9020/ws",
                        "VITE_MARKET_PROTOCOL": "terminal-message-v1",
                        "VITE_MARKET_CUTOVER_READINESS": readiness,
                    },
                },
            )
            write_json(
                root / "legacy-decommission.json",
                {
                    "schema_version": 1,
                    "prepared_at": "2026-05-25T10:55:00+08:00",
                    "passed": True,
                    "legacy_websocket_disabled": True,
                    "old_topic_consumers_disabled": True,
                    "no_legacy_consumers_observed": True,
                    "observation": {
                        "observed_at": "2026-05-22T16:15:00+08:00",
                        "expected_old_topics": ["legacy_ticks", "legacy_broker_queue"],
                        "legacy_websocket_enabled": False,
                        "old_topic_consumers": {"legacy_ticks": 0, "legacy_broker_queue": 0},
                        "old_topic_lag": {"legacy_ticks": 0, "legacy_broker_queue": 0},
                    },
                },
            )
            write_legacy_retirement_evidence(
                readiness=readiness,
                evidence=LegacyRetirementEvidence(
                    frontend_default_v2_deployed=True,
                    legacy_websocket_disabled=True,
                    old_topic_consumers_disabled=True,
                    no_legacy_consumers_observed=True,
                    rollback_window_completed=True,
                    rollback_window_started_at="2026-05-22T14:00:00+08:00",
                    rollback_window_completed_at="2026-05-22T16:00:00+08:00",
                    operator_approved=True,
                    operator_approved_at="2026-05-22T16:20:00+08:00",
                ),
                output_path=root / "legacy-retirement.json",
            )
            smoke_root = root / "smoke"
            smoke_root.mkdir()
            write_json(
                smoke_root / "multi-trader-smoke-evidence.json",
                {
                    "schema_version": 1,
                    "passed": True,
                    "blockers": [],
                    "client_count": 2,
                    "watchlist_overlap": ["00700.HK"],
                },
            )
            write_json(
                smoke_root / "lan-preflight.json",
                {
                    "schema_version": 1,
                    "prepared_at": "2026-05-25T10:55:00+08:00",
                    "passed": True,
                    "blockers": [],
                    "lan_host": "gateway.internal",
                    "frontend_port": 5173,
                    "gateway_port": 9020,
                    "page_url": "http://gateway.internal:5173/",
                    "gateway_url": "ws://gateway.internal:9020/ws",
                    "service_checks": {
                        "schema_version": 1,
                        "checked_at": "2026-05-25T10:55:00+08:00",
                        "passed": True,
                        "blockers": [],
                        "timeout_seconds": 1.0,
                        "checks": {
                            "frontend": {
                                "url": "http://gateway.internal:5173/",
                                "host": "gateway.internal",
                                "port": 5173,
                                "probe_kind": "frontend_http",
                                "reachable": True,
                                "status_code": 200,
                                "error": "",
                            },
                            "gateway": {
                                "url": "ws://gateway.internal:9020/ws",
                                "host": "gateway.internal",
                                "port": 9020,
                                "probe_kind": "gateway_websocket",
                                "reachable": True,
                                "websocket_handshake": True,
                                "status_code": 101,
                                "error": "",
                            },
                        },
                    },
                },
            )
            write_json(
                smoke_root / "service-preflight.json",
                json.loads((smoke_root / "lan-preflight.json").read_text(encoding="utf-8"))["service_checks"],
            )
            (smoke_root / "clients").mkdir()
            write_json(smoke_root / "clients" / "client.json", {"clients": []})
            write_json(smoke_root / "smoke-import-manifest.json", smoke_import_manifest_payload())
            write_smoke_run_manifest(
                smoke_root,
                smoke_root / "multi-trader-smoke-evidence.json",
                smoke_root / "lan-preflight.json",
                smoke_root / "service-preflight.json",
                smoke_root / "smoke-import-manifest.json",
                smoke_root / "clients" / "client.json",
            )
            package_multi_trader_smoke(root_path=smoke_root)
            output = StringIO()

            with redirect_stdout(output):
                exit_code = ops_main(
                    [
                        "verify-evidence-bundle",
                        "--shadow-reports-directory",
                        str(reports),
                        "--manifest-directory",
                        str(manifests),
                        "--runtime-config-path",
                        str(root / "runtime-config.json"),
                        "--runtime-health-path",
                        str(root / "runtime-health.json"),
                        "--readiness-path",
                        str(root / "cutover-readiness.json"),
                        "--frontend-deployment-path",
                        str(root / "frontend-deployment.json"),
                        "--legacy-decommission-path",
                        str(root / "legacy-decommission.json"),
                        "--legacy-retirement-path",
                        str(root / "legacy-retirement.json"),
                        "--multi-trader-smoke-path",
                        str(smoke_root / "multi-trader-smoke-evidence.json"),
                        "--multi-trader-smoke-preflight-path",
                        str(smoke_root / "lan-preflight.json"),
                        "--multi-trader-smoke-manifest-path",
                        str(smoke_root / "smoke-run-manifest.json"),
                        "--multi-trader-smoke-package-path",
                        str(smoke_root / "multi-trader-smoke-evidence.zip"),
                        "--multi-trader-smoke-package-metadata-path",
                        str(smoke_root / "smoke-run-package.json"),
                        "--output-path",
                        str(root / "evidence-bundle.json"),
                    ]
                )
            summary = json.loads(output.getvalue())
            failing_output = StringIO()
            with redirect_stdout(failing_output):
                failing_exit_code = ops_main(
                    [
                        "verify-evidence-bundle",
                        "--shadow-reports-directory",
                        str(reports),
                        "--manifest-directory",
                        str(manifests),
                        "--runtime-config-path",
                        str(root / "runtime-config.json"),
                        "--runtime-health-path",
                        str(root / "runtime-health.json"),
                        "--readiness-path",
                        str(root / "cutover-readiness.json"),
                        "--frontend-deployment-path",
                        str(root / "frontend-deployment.json"),
                        "--legacy-decommission-path",
                        str(root / "legacy-decommission.json"),
                        "--legacy-retirement-path",
                        str(root / "legacy-retirement.json"),
                        "--multi-trader-smoke-path",
                        str(root / "missing-multi-trader-smoke.json"),
                        "--output-path",
                        str(root / "failing-evidence-bundle.json"),
                    ]
                )
            failing_summary = json.loads(failing_output.getvalue())

        self.assertEqual(exit_code, 0)
        self.assertTrue(summary["passed"])
        self.assertEqual(summary["blockers"], [])
        self.assertEqual(failing_exit_code, 1)
        self.assertFalse(failing_summary["passed"])
        self.assertIn("missing_multi_trader_smoke", failing_summary["blockers"])


def write_passing_shadow_stream(directory: Path) -> None:
    recorder = FileBackedShadowRunRecorder(
        directory=directory,
        session_id="session-1",
        trading_date="20260522",
        started_at="2026-05-22T09:30:00+08:00",
        reset=True,
    )
    for index in range(241):
        minute = index if index < 240 else 240
        hour = 9 + (30 + minute) // 60
        minute_of_hour = (30 + minute) % 60
        timestamp = f"2026-05-22T{hour:02d}:{minute_of_hour:02d}:00+08:00"
        recorder.record_legacy_event(
            {
                "event_id": f"legacy-{index + 1}",
                "symbol": "00700.HK",
                "seq": index + 1,
                "source_ts": timestamp,
                "ingest_ts": timestamp,
            }
        )
        recorder.record_v2_event(
            {
                "event_id": f"v2-{index + 1}",
                "symbol": "00700.HK",
                "seq": index + 1,
                "source_ts": timestamp,
                "ingest_ts": timestamp,
            }
        )
    for key, samples in {
        "collector_to_kafka_ms": [5, 10, 20],
        "processed_to_gateway_ms": [10, 20, 30],
        "gateway_to_frontend_ms": [10, 20, 30],
        "subscribe_snapshot_ms": [50, 80, 100],
        "frontend_store_update_ms": [80, 120, 180],
    }.items():
        for sample in samples:
            recorder.record_performance_sample(key, sample)


def runtime_config_payload(silver_root: Path) -> dict:
    silver_root.mkdir(parents=True, exist_ok=True)
    return {
        "schema_version": 1,
        "trade_date": "20260522",
        "silver_root": str(silver_root),
        "gateway": {
            "host": "0.0.0.0",
            "port": 9020,
            "path": "/ws",
        },
        "kafka": {
            "raw_topic": "raw_market_events_v1",
            "processed_topic": "processed_market_events_v1",
            "consumer_group": "beast-terminal-v2",
            "poll_timeout_ms": 1000,
            "auto_offset_reset": "latest",
        },
        "redis": {
            "terminal_ttl_seconds": 28800,
            "history_ttl_seconds": 2592000,
        },
        "runtime": {
            "raw_queue_max_size": 10000,
            "client_queue_size": 100,
            "kafka_spool_dir": "artifacts/runtime-state/kafka-spool",
            "kafka_retries": 3,
            "symbol_eviction_grace_seconds": 300,
            "max_concurrent_hydrations": 8,
            "big_trade_volume_baseline_ratio": 0.0005,
            "install_signal_handlers": True,
        },
        "freshness": {
            "max_event_age_seconds": 60,
            "max_queue_backlog": 1000,
        },
        "production_clients": {
            "duckdb_connection": True,
            "kafka_producer": True,
            "kafka_consumer": True,
            "redis_client": True,
            "market_data_client": True,
        },
    }


def passing_report() -> dict:
    return {
        "schema_version": 1,
        "session_id": "session-1",
        "trading_date": "20260522",
        "started_at": "2026-05-22T09:30:00+08:00",
        "finished_at": "2026-05-22T13:30:00+08:00",
        "duration_seconds": 14400,
        "passed": True,
        "comparison": {
            "passed": True,
            "failed_symbols": [],
            "missing_symbol_count": 0,
            "symbols": {
                "00700.HK": {
                    "legacy_count": 1,
                    "v2_count": 1,
                    "count_delta_ratio": 0.0,
                    "duplicate_ratio": 0.0,
                    "out_of_order_ratio": 0.0,
                    "max_latency_delta_ms": 10,
                    "max_stale_gap_seconds": 10,
                    "legacy_source_coverage_seconds": 14400,
                    "v2_source_coverage_seconds": 14400,
                    "missing": False,
                    "passed": True,
                }
            },
            "thresholds": {
                "max_event_count_delta_ratio": 0.01,
                "max_duplicate_ratio": 0.001,
                "max_out_of_order_ratio": 0.001,
                "max_missing_symbol_count": 0,
                "max_latency_delta_ms": 250,
                "max_stale_gap_seconds": 60,
            },
        },
        "performance": {
            "passed": True,
            "missing_sample_keys": [],
            "insufficient_sample_keys": [],
            "min_samples_per_key": 3,
            "sample_counts": performance_sample_counts(),
            "metrics": performance_metrics(),
        },
        "legacy_event_count": 1,
        "v2_event_count": 1,
        "legacy_source_coverage_seconds": 14400,
        "v2_source_coverage_seconds": 14400,
        "evidence_source": {
            "schema_version": 1,
            "kind": "file_backed_shadow_run",
            "files": {
                "metadata": "artifacts/shadow-runs/20260522.session-1.metadata.json",
                "legacy_events": "artifacts/shadow-runs/20260522.session-1.legacy.ndjson",
                "v2_events": "artifacts/shadow-runs/20260522.session-1.v2.ndjson",
                "performance_samples": "artifacts/shadow-runs/20260522.session-1.performance.ndjson",
            },
            "legacy_event_count": 1,
            "v2_event_count": 1,
            "performance_sample_counts": performance_sample_counts(),
        },
    }


def performance_metrics() -> dict:
    return {
        "collector_to_kafka": {"p95_ms": 20, "p99_ms": 20, "p95_target_ms": 30, "p99_target_ms": 100},
        "processed_to_gateway": {"p95_ms": 30, "p95_target_ms": 50},
        "gateway_to_frontend": {"p95_ms": 30, "p95_target_ms": 50},
        "subscribe_snapshot": {"p95_ms": 100, "p95_target_ms": 200},
        "frontend_store_update": {"p95_ms": 180, "p95_target_ms": 250},
    }


def performance_sample_counts() -> dict:
    return {
        "collector_to_kafka_ms": 3,
        "processed_to_gateway_ms": 3,
        "gateway_to_frontend_ms": 3,
        "subscribe_snapshot_ms": 3,
        "frontend_store_update_ms": 3,
    }


def write_json(path: Path, value: dict) -> None:
    path.write_text(json.dumps(value), encoding="utf-8")


class FakeProbeSocket:
    def __init__(self, response: str) -> None:
        self._response = response.encode("iso-8859-1")
        self.sent = b""

    def __enter__(self) -> "FakeProbeSocket":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        return None

    def settimeout(self, timeout: float) -> None:
        self.timeout = timeout

    def sendall(self, data: bytes) -> None:
        self.sent += data

    def recv(self, size: int) -> bytes:
        return self._response[:size]


def write_real_data_runner_smoke_preflight_artifacts(root: Path) -> None:
    write_json(root / "lan-preflight.json", real_data_runner_smoke_preflight_payload())
    write_json(root / "service-preflight.json", real_data_runner_smoke_service_checks_payload())


def multi_trader_smoke_payload() -> dict:
    return {
        "schema_version": 1,
        "observed_at": "2026-05-25T11:00:00+08:00",
        "clients": [
            {
                "machine_id": "desk-a",
                "data_source_mode": "live",
                "page_url": "http://192.168.1.10:5173/",
                "gateway_url": "ws://192.168.1.10:9020/ws",
                "connected": True,
                "watchlist": ["00700.HK", "00939.HK"],
                "refresh_recovered": True,
                "symbol_statuses": {
                    "00700.HK": symbol_smoke_status("live", "20260522", "20260522"),
                    "00939.HK": symbol_smoke_status("closed", "20260525", "20260522"),
                },
            },
            {
                "machine_id": "desk-b",
                "data_source_mode": "live",
                "page_url": "http://192.168.1.10:5173/",
                "gateway_url": "ws://192.168.1.10:9020/ws",
                "connected": True,
                "watchlist": ["00700.HK", "00005.HK"],
                "refresh_recovered": True,
                "symbol_statuses": {
                    "00700.HK": symbol_smoke_status("live", "20260522", "20260522"),
                    "00005.HK": symbol_smoke_status("warm", "20260522", "20260522"),
                },
            },
        ],
        "workflows": {
            "cold_query": {"passed": True, "symbol": "00005.HK", "loading_observed": True, "snapshot_visible": True, "observed_at": "2026-05-25T10:58:00+08:00"},
            "add_to_watchlist": {"passed": True, "symbol": "00005.HK", "persisted": True, "observed_at": "2026-05-25T10:58:10+08:00"},
            "refresh_recovery": {"passed": True, "browser_refreshed": True, "watchlist_restored": True, "snapshots_visible": True, "observed_at": "2026-05-25T10:58:20+08:00"},
            "redis_clear_recovery": {"passed": True, "symbol": "00700.HK", "cache_cleared": True, "snapshot_rebuilt": True, "observed_at": "2026-05-25T10:58:30+08:00"},
            "process_restart_recovery": {"passed": True, "backend_restarted": True, "first_screen_restored": True, "observed_at": "2026-05-25T10:58:40+08:00"},
            "closed_market_effective_date": {
                "passed": True,
                "expected_closed_market": True,
                "requested_trade_date": "20260525",
                "effective_trade_date": "20260522",
                "source_dates_visible": True,
                "observed_at": "2026-05-25T10:58:50+08:00",
            },
        },
        "metrics": {"warm_snapshot_p95_ms": 150.0, "duplicate_hydrations": 0},
        "runtime_health": {
            "passed": True,
            "path": "artifacts/runtime-health.json",
            "generated_at": "2026-05-25T10:59:00+08:00",
            "gateway_websocket": gateway_smoke_evidence(),
            "gateway_activity": {"client_queue": gateway_client_queue_payload()},
        },
        "preflight": real_data_runner_smoke_preflight_payload(),
    }


def symbol_smoke_status(status: str, requested_date: str, effective_date: str) -> dict:
    return {
        "status": status,
        "snapshot_loaded": True,
        "requested_trade_date": requested_date,
        "effective_trade_date": effective_date,
        "source_dates": {"minute_bars": effective_date},
        "degraded_reasons": [],
    }


def gateway_smoke_evidence() -> dict:
    return {"host": "0.0.0.0", "port": 9020, "path": "/ws"}


def real_data_runner_smoke_health_payload() -> dict:
    return {
        "schema_version": 1,
        "prepared_at": "2026-05-25T10:55:00+08:00",
        "passed": True,
        "blockers": [],
        "generated_at": "2026-05-25T11:00:00+08:00",
        "evidence": {
            "symbol_runtime": {
                "00700.HK": {"hydrate_count": 1},
                "00939.HK": {"hydrate_count": 1},
                "00005.HK": {"hydrate_count": 1},
            },
            "symbol_runtime_manager": {
                "active_hydrations": 0,
                "max_concurrent_hydrations": 8,
                "capacity_rejections": 0,
                "hydrating_symbols": [],
            },
            "performance_samples": {"subscribe_snapshot_ms": [80.0, 100.0, 120.0]},
            "gateway_activity": {"client_queue": gateway_client_queue_payload()},
            "gateway_websocket": {"host": "0.0.0.0", "port": 9020, "path": "/ws"},
        },
    }


def real_data_runner_smoke_preflight_payload() -> dict:
    return {
        "schema_version": 1,
        "prepared_at": "2026-05-25T10:55:00+08:00",
        "passed": True,
        "blockers": [],
        "lan_host": "192.168.1.10",
        "frontend_port": 5173,
        "gateway_port": 9020,
        "page_url": "http://192.168.1.10:5173/",
        "gateway_url": "ws://192.168.1.10:9020/ws",
        "artifact_paths": {
            "client_instructions": "CLIENT_INSTRUCTIONS.md",
        },
        "commands": {
            "backend_script": "scripts/start-backend.sh",
            "frontend_script": "scripts/start-frontend.sh",
            "service_preflight_script": "scripts/verify-services.sh",
            "finalize_package_script": "scripts/finalize-package.sh",
            "verify_handoff_script": "scripts/verify-handoff.sh",
            "import_artifacts_script": "scripts/import-artifacts.sh",
        },
        "service_checks": real_data_runner_smoke_service_checks_payload(),
    }


def real_data_runner_smoke_service_checks_payload() -> dict:
    return {
        "schema_version": 1,
        "checked_at": "2026-05-25T10:55:00+08:00",
        "passed": True,
        "blockers": [],
        "timeout_seconds": 1.0,
        "checks": {
            "frontend": {
                "url": "http://192.168.1.10:5173/",
                "host": "192.168.1.10",
                "port": 5173,
                "probe_kind": "frontend_http",
                "reachable": True,
                "status_code": 200,
                "error": "",
            },
            "gateway": {
                "url": "ws://192.168.1.10:9020/ws",
                "host": "192.168.1.10",
                "port": 9020,
                "probe_kind": "gateway_websocket",
                "reachable": True,
                "websocket_handshake": True,
                "status_code": 101,
                "error": "",
            },
        },
    }


def runtime_health_payload(*, for_completed_shadow_session: bool = False) -> dict:
    generated_at = "2026-05-22T13:30:02+08:00" if for_completed_shadow_session else "2026-05-22T09:30:02+08:00"
    last_tick_at = "2026-05-22T13:30:01+08:00" if for_completed_shadow_session else "2026-05-22T09:30:01+08:00"
    last_delivery_at = "2026-05-22T13:30:01+08:00" if for_completed_shadow_session else "2026-05-22T09:30:01+08:00"
    latest_event_at = "2026-05-22T13:30:00+08:00" if for_completed_shadow_session else "2026-05-22T09:30:00+08:00"
    return {
        "schema_version": 1,
        "generated_at": generated_at,
        "trade_date": "20260522",
        "topics": {
            "raw_market_events_v1": {"committed_offset": 1, "lag": 0},
            "processed_market_events_v1": {"committed_offset": 1, "lag": 0},
        },
        "supervisor": {
            "starts": 1,
            "stops": 0,
            "ticks": 1,
            "ingested_events": 1,
            "processed_events": 1,
            "broadcast_messages": 0,
            "freshness_checks": 1,
            "started_at": "2026-05-22T09:30:00+08:00",
            "last_tick_at": last_tick_at,
            "stopped_at": None,
        },
        "queues": {
            "raw_callback_backlog": 0,
            "raw_callback_rejected": 0,
            "raw_callback_rejection_path": "artifacts/runtime-state/20260522/callback-rejections.jsonl",
            "raw_consumer_dead_letter_path": "artifacts/runtime-state/20260522/raw-consumer-dead-letters.jsonl",
        },
        "workers": {
            "ingest": {"processed": 1, "dead_letters": []},
            "raw_consumer": {"processed": 1, "dead_letters": []},
        },
        "producer": {
            "publish_attempts": 2,
            "dead_letters": 0,
            "spooled_records": 0,
            "quarantined_spool_records": 0,
            "spool_path": "artifacts/runtime-state/kafka-spool/publish-failures.jsonl",
            "spool_quarantine_path": "artifacts/runtime-state/kafka-spool/publish-failures.jsonl.quarantine",
        },
        "redis": {
            "write_stats": {
                "writes": 2,
                "failures": 0,
                "last_latency_ms": 1.0,
                "max_latency_ms": 1.0,
                "last_error": "",
            }
        },
        "subscription": {
            "running": True,
            "subscribed_symbols": ["00700.HK"],
            "stats": {"starts": 1, "stops": 0, "subscribe_calls": 1, "unsubscribe_calls": 0},
        },
        "symbol_runtime_manager": {
            "runtime_count": 1,
            "state_counts": {
                "COLD": 0,
                "HYDRATING": 0,
                "WARM": 0,
                "LIVE": 1,
                "DEGRADED": 0,
                "EVICTING": 0,
            },
            "total_ref_count": 1,
            "active_hydrations": 0,
            "hydrating_symbols": [],
            "max_concurrent_hydrations": 8,
            "capacity_rejections": 0,
            "state_sink_failures": 0,
            "last_state_sink_error": "",
            "state_sink_failure_symbols": [],
            "snapshot_sink_failures": 0,
            "last_snapshot_sink_error": "",
            "snapshot_sink_failure_symbols": [],
            "realtime_attached_symbols": ["00700.HK"],
            "eviction_grace_seconds": 300,
        },
        "symbol_runtime": {
            "00700.HK": {
                "symbol": "00700.HK",
                "state": "LIVE",
                "ref_count": 1,
                "subscribers": ["client-1"],
                "hydrate_count": 1,
                "hydration_failures": 0,
                "last_hydration_latency_ms": 10.0,
                "max_hydration_latency_ms": 10.0,
                "last_hydration_error": "",
                "degraded_reasons": [],
                "eviction_started_at": None,
                "eviction_grace_seconds": 300,
                "max_concurrent_hydrations": 8,
                "capacity_rejections": 0,
                "realtime_attached": True,
                "has_snapshot_payload": True,
                "freshness": {"runtime_state": "LIVE"},
            }
        },
        "redis_snapshot": {
            "trade_date": "20260522",
            "checked_symbols": ["00700.HK"],
            "present_symbols": ["00700.HK"],
            "missing_symbols": [],
            "required_key_families": [
                "terminal_snapshot",
                "terminal_minute",
                "terminal_alerts",
                "terminal_queue",
                "terminal_state",
                "ccass_holding",
                "ccass_history",
            ],
            "key_family_coverage": redis_key_family_coverage_payload(latest_event_at),
        },
        "gateway_websocket": {
            "host": "0.0.0.0",
            "port": 9020,
            "path": "/ws",
            "request_schema_version": 1,
            "accepted_protocol": "terminal-message-v1",
            "running": True,
            "connected_clients": 0,
        },
        "gateway_activity": {
            "processed_records_consumed": 0,
            "shadow_processed_records_drained": 1,
            "direct_runtime_messages_emitted": 1,
            "terminal_messages_emitted": 1,
            "terminal_messages_delivered": 1,
            "delivered_terminal_symbols": ["00700.HK"],
            "last_terminal_message_delivered_at": last_delivery_at,
            "client_queue": gateway_client_queue_payload(),
        },
        "performance_samples": {"subscribe_snapshot_ms": [80.0, 100.0, 120.0]},
        "health": {
            "collector": {
                "symbol_freshness": {
                    "00700.HK": {
                        "subscribed": True,
                        "degraded": False,
                        "latest_event_at": latest_event_at,
                    }
                }
            },
            "octopus": {"redis": "connected"},
            "gateway": {"redis": "connected"},
        },
    }


def redis_key_family_coverage_payload(updated_at: str) -> dict:
    coverage = {}
    for family in [
        "terminal_snapshot",
        "terminal_minute",
        "terminal_alerts",
        "terminal_queue",
        "terminal_state",
        "ccass_holding",
        "ccass_history",
    ]:
        coverage[family] = {
            "checked_symbols": ["00700.HK"],
            "present_symbols": ["00700.HK"],
            "missing_symbols": [],
            "updated_at_by_symbol": {"00700.HK": updated_at},
            "missing_updated_at_symbols": [],
            "ttl_seconds_by_symbol": {"00700.HK": 3600},
            "missing_ttl_symbols": [],
            "contract_missing_by_symbol": {},
        }
    coverage["ccass_history"]["participants_by_symbol"] = {"00700.HK": ["C00010"]}
    coverage["ccass_history"]["missing_keys"] = {}
    return coverage


def gateway_client_queue_payload() -> dict:
    return {
        "connected_clients": 0,
        "observed_client_count": 2,
        "observed_client_ids": ["desk-a", "desk-b"],
        "observed_declared_client_count": 2,
        "observed_declared_client_ids": ["desk-a", "desk-b"],
        "max_connected_clients": 2,
        "client_queue_max_size": 100,
        "current_backlog_by_client": {},
        "total_current_backlog": 0,
        "max_current_backlog": 0,
        "enqueued": 0,
        "coalesced": 0,
        "dropped": 0,
        "alerts_enqueued": 0,
        "alert_overflow": 0,
        "alert_dropped": 0,
        "critical_overflow": 0,
    }


def write_silver_tables(root: Path) -> None:
    write_csv(
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
    write_csv(
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
                "row_hash": "tick",
            }
        ],
    )
    write_csv(
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
                "row_hash": "minute",
            }
        ],
    )
    write_csv(
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
    write_csv(
        root / "silver_broker_queue_v1.csv",
        [
            {
                "schema_version": 1,
                "symbol": "00700.HK",
                "trade_date": "20260522",
                "queue_ts": "2026-05-22T09:30:00+08:00",
                "side": "bid",
                "position": 1,
                "broker_code": "JPM",
                "source": "fixture",
                "ingest_ts": "2026-05-22T09:30:01+08:00",
                "row_hash": "queue",
            }
        ],
    )
    write_csv(
        root / "silver_broker_mapping_v1.csv",
        [
            {
                "schema_version": 1,
                "broker_code": "JPM",
                "broker_name": "JPMorgan",
                "effective_from": "20260101",
                "source": "fixture",
                "ingest_ts": "2026-05-22T00:00:00Z",
                "row_hash": "mapping",
            }
        ],
    )


def write_csv(path: Path, rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_required_manifests(directory: Path) -> None:
    for data_type in ["daily_bars", "minute_bars", "trade_ticks", "ccass_holdings", "participant_history", "broker_queue", "broker_mapping"]:
        (directory / f"{data_type}.20260522-20260522.v2.manifest.json").write_text(
            json.dumps(manifest_payload(data_type)),
            encoding="utf-8",
        )


def write_shadow_source_files(directory: Path, report: dict) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    files = {
        "metadata": directory / "20260522.session-1.metadata.json",
        "legacy_events": directory / "20260522.session-1.legacy.ndjson",
        "v2_events": directory / "20260522.session-1.v2.ndjson",
        "performance_samples": directory / "20260522.session-1.performance.ndjson",
    }
    write_json(
        files["metadata"],
        {
            "schema_version": 1,
            "session_id": report["session_id"],
            "trading_date": report["trading_date"],
            "started_at": report["started_at"],
        },
    )
    files["legacy_events"].write_text(
        "".join(
            json.dumps(shadow_source_event(f"legacy-{index + 1}", seq=index + 1)) + "\n"
            for index in range(report["legacy_event_count"])
        ),
        encoding="utf-8",
    )
    files["v2_events"].write_text(
        "".join(
            json.dumps(shadow_source_event(f"v2-{index + 1}", seq=index + 1)) + "\n"
            for index in range(report["v2_event_count"])
        ),
        encoding="utf-8",
    )
    performance_lines = []
    for key, count in report["performance"]["sample_counts"].items():
        performance_lines.extend(json.dumps({"key": key, "value_ms": 1.0}) + "\n" for _ in range(count))
    files["performance_samples"].write_text("".join(performance_lines), encoding="utf-8")
    report["evidence_source"]["files"] = {key: str(path) for key, path in files.items()}


def shadow_source_event(event_id: str, *, seq: int) -> dict:
    return {
        "event_id": event_id,
        "symbol": "00700.HK",
        "seq": seq,
        "source_ts": "2026-05-22T09:30:00+08:00",
        "ingest_ts": "2026-05-22T09:30:00.010+08:00",
    }


def raw_event(event_id: str) -> dict:
    event = make_raw_market_event(
        kind="tick",
        symbol="00700.HK",
        source="xtquant",
        seq=1,
        payload={"price": 388.4, "volume": 1000, "turnover": 388400},
    )
    event["event_id"] = event_id
    return event


def manifest_payload(data_type: str) -> dict:
    source_data_type = "ccass_holdings" if data_type == "participant_history" else data_type
    scoped_symbols = [] if data_type == "broker_mapping" else ["00700.HK"]
    return {
        "schema_version": 1,
        "data_type": data_type,
        "source_data_type": source_data_type,
        "table": f"silver_{source_data_type}_v1",
        "date_range": {"start": "20260522", "end": "20260522"},
        "symbols": scoped_symbols,
        "symbol_count": len(scoped_symbols),
        "row_count": 1,
        "failed_items": [],
        "code_version": "v2",
        "started_at": "2026-05-22T09:00:00+08:00",
        "finished_at": "2026-05-22T09:01:00+08:00",
        "quality_checks": {
            "missing_required_columns": [],
            "duplicate_primary_keys": [],
            "invalid_symbol_rows": [],
            "invalid_date_rows": [],
            "negative_value_count": 0,
            "empty_output": False,
            "passed": True,
            "failed_items": [],
        },
    }


def write_smoke_run_manifest(root: Path, *paths: Path) -> None:
    files = []
    for path in paths:
        data = path.read_bytes()
        files.append(
            {
                "path": path.relative_to(root).as_posix(),
                "bytes": len(data),
                "sha256": hashlib.sha256(data).hexdigest(),
            }
        )
    write_json(root / "smoke-run-manifest.json", {"schema_version": 1, "file_count": len(files), "files": files})


def smoke_import_manifest_payload(*output_paths: str) -> dict:
    if not output_paths:
        output_paths = ("clients/client.json",)
    imported = [
        {
            "kind": "performance" if output_path.startswith("performance/") else "client",
            "input_path": f"/downloads/{Path(output_path).name}",
            "output_path": output_path,
        }
        for output_path in output_paths
    ]
    return {
        "schema_version": 1,
        "source_path": "/downloads",
        "imported_count": len(imported),
        "skipped_count": 0,
        "imported": imported,
        "skipped": [],
        "runs": [
            {
                "source_path": "/downloads",
                "imported_count": len(imported),
                "skipped_count": 0,
                "imported": imported,
                "skipped": [],
            }
        ],
    }


def client_smoke_artifact(client: dict, exported_at: str = "2026-05-25T10:59:00+08:00") -> dict:
    return {"schema_version": 1, "exported_at": exported_at, "clients": [client]}


class DeletableRedis:
    def __init__(self, values: dict[str, str]) -> None:
        self.values = dict(values)
        self.deleted: list[str] = []

    def delete(self, key: str) -> None:
        self.deleted.append(key)
        self.values.pop(key, None)


if __name__ == "__main__":
    unittest.main()
