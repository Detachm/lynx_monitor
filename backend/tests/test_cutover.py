import json
import hashlib
import tempfile
import unittest
import zipfile
from pathlib import Path

from beast_market import (
    CutoverPolicy,
    EvidenceBundlePaths,
    FrontendDeploymentEvidence,
    LegacyDecommissionObservation,
    LegacyRetirementEvidence,
    evaluate_cutover_readiness,
    evaluate_evidence_bundle,
    evaluate_frontend_deployment,
    evaluate_legacy_decommission,
    evaluate_legacy_retirement,
    evaluate_multi_trader_smoke,
    evaluate_runtime_health,
    frontend_cutover_env,
    legacy_retirement_evidence_from_artifacts,
    load_env_file,
    load_legacy_decommission_observation,
    load_cutover_reports,
    load_shadow_run_report_directory,
    save_shadow_run_report,
    write_cutover_artifacts,
    write_cutover_readiness,
    write_evidence_bundle_verification,
    write_frontend_cutover_env,
    write_frontend_deployment_evidence,
    write_legacy_decommission_evidence,
    write_legacy_retirement_from_artifacts,
    write_legacy_retirement_evidence,
    write_multi_trader_smoke_evidence,
    write_runtime_health_verification,
)
from beast_market.cutover import (
    historical_manifest_errors,
    shadow_run_report_errors,
    validate_multi_trader_smoke_import_manifest,
    validate_multi_trader_smoke_package_run_manifest,
)


class CutoverReadinessTest(unittest.TestCase):
    def test_allows_frontend_default_and_legacy_retirement_after_accepted_session(self) -> None:
        result = evaluate_cutover_readiness(
            [passing_report()],
            policy=CutoverPolicy(min_session_duration_seconds=60),
        )

        self.assertTrue(result["passed"])
        self.assertTrue(result["frontend_default_v2_allowed"])
        self.assertTrue(result["legacy_retirement_allowed"])
        self.assertEqual(result["accepted_report_ids"], ["session-1"])
        self.assertEqual(result["blockers"], [])

    def test_blocks_when_report_failed_even_if_session_is_long_enough(self) -> None:
        report = passing_report()
        report["passed"] = False
        report["comparison"]["passed"] = False
        report["comparison"]["failed_symbols"] = ["00700.HK"]

        result = evaluate_cutover_readiness(
            [report],
            policy=CutoverPolicy(min_session_duration_seconds=60),
        )

        self.assertFalse(result["frontend_default_v2_allowed"])
        self.assertFalse(result["legacy_retirement_allowed"])
        self.assertIn("insufficient_accepted_parallel_sessions", result["blockers"])
        self.assertIn("shadow_run_report_failed", result["rejected_reports"][0]["blockers"])
        self.assertIn("stream_comparison_failed", result["rejected_reports"][0]["blockers"])
        self.assertIn("failed_symbols_present", result["rejected_reports"][0]["blockers"])

    def test_blocks_short_or_empty_sessions(self) -> None:
        report = passing_report()
        report["duration_seconds"] = 10
        report["v2_event_count"] = 0

        result = evaluate_cutover_readiness(
            [report],
            policy=CutoverPolicy(min_session_duration_seconds=60),
        )

        self.assertFalse(result["passed"])
        self.assertIn("session_duration_below_minimum", result["rejected_reports"][0]["blockers"])
        self.assertIn("v2_stream_empty", result["rejected_reports"][0]["blockers"])

    def test_rejects_weak_historical_manifest_audit_fields(self) -> None:
        bad_started_at = manifest_payload("daily_bars")
        bad_started_at["started_at"] = "20260522 090000"
        self.assertIn("started_at_invalid", historical_manifest_errors(bad_started_at))

        reversed_window = manifest_payload("daily_bars")
        reversed_window["finished_at"] = "2026-05-22T08:59:00+08:00"
        self.assertIn("finished_at_before_started_at", historical_manifest_errors(reversed_window))

        weak_quality = manifest_payload("daily_bars")
        weak_quality["quality_checks"] = {"passed": "yes", "failed_items": []}
        weak_quality_errors = historical_manifest_errors(weak_quality)
        self.assertIn("quality_checks_required_fields_missing", weak_quality_errors)
        self.assertIn("quality_checks_passed_invalid", weak_quality_errors)

        inconsistent_failed_items = manifest_payload("daily_bars")
        inconsistent_failed_items["failed_items"] = ["duplicate_primary_keys"]
        inconsistent_failed_items["quality_checks"]["failed_items"] = ["duplicate_primary_keys"]
        inconsistent_failed_items["quality_checks"]["passed"] = True
        self.assertIn(
            "failed_items_present_when_quality_passed",
            historical_manifest_errors(inconsistent_failed_items),
        )

        malformed_symbol = manifest_payload("daily_bars")
        malformed_symbol["symbols"] = ["700"]
        malformed_symbol["symbol_count"] = 1
        self.assertIn("symbols_format_invalid", historical_manifest_errors(malformed_symbol))

        duplicate_symbol = manifest_payload("daily_bars")
        duplicate_symbol["symbols"] = ["00700.HK", "00700.HK"]
        duplicate_symbol["symbol_count"] = 1
        self.assertIn("symbols_duplicate", historical_manifest_errors(duplicate_symbol))

        missing_failed_item = manifest_payload("daily_bars")
        missing_failed_item["quality_checks"]["invalid_symbol_rows"] = ["700"]
        self.assertIn(
            "quality_checks_invalid_symbol_rows_failed_item_missing",
            historical_manifest_errors(missing_failed_item),
        )

        missing_negative_failed_item = manifest_payload("daily_bars")
        missing_negative_failed_item["quality_checks"]["negative_value_count"] = 1
        self.assertIn(
            "quality_checks_negative_values_failed_item_missing",
            historical_manifest_errors(missing_negative_failed_item),
        )

        missing_empty_failed_item = manifest_payload("daily_bars")
        missing_empty_failed_item["quality_checks"]["empty_output"] = True
        self.assertIn(
            "quality_checks_empty_output_failed_item_missing",
            historical_manifest_errors(missing_empty_failed_item),
        )

        spoofed_non_empty_manifest = manifest_payload("daily_bars")
        spoofed_non_empty_manifest["row_count"] = 0
        spoofed_non_empty_errors = historical_manifest_errors(spoofed_non_empty_manifest)
        self.assertIn("row_count_empty_output_mismatch", spoofed_non_empty_errors)
        self.assertIn("row_count_empty_when_quality_passed", spoofed_non_empty_errors)

        spoofed_empty_manifest = manifest_payload("daily_bars")
        spoofed_empty_manifest["quality_checks"]["empty_output"] = True
        self.assertIn("row_count_empty_output_mismatch", historical_manifest_errors(spoofed_empty_manifest))

    def test_packaged_smoke_import_manifest_rejects_unsafe_provenance(self) -> None:
        zip_names = ["clients/desk-1.json", "performance/perf-1.json", "smoke-import-manifest.json"]
        manifest = {
            "schema_version": 1,
            "imported_count": 2,
            "skipped_count": 0,
            "imported": [
                {"kind": "client", "input_path": "/downloads/desk-1.json", "output_path": "clients/desk-1.json"},
                {"kind": "performance", "input_path": "/downloads/perf-1.json", "output_path": "performance/perf-1.json"},
            ],
            "skipped": [],
            "runs": [
                {
                    "imported_count": 2,
                    "skipped_count": 0,
                    "imported": [
                        {"kind": "client", "input_path": "/downloads/desk-1.json", "output_path": "clients/desk-1.json"},
                        {"kind": "performance", "input_path": "/downloads/perf-1.json", "output_path": "performance/perf-1.json"},
                    ],
                    "skipped": [],
                }
            ],
        }
        evidence: dict[str, object] = {}
        self.assertEqual(validate_multi_trader_smoke_import_manifest(manifest, evidence, zip_names), [])

        invalid = json.loads(json.dumps(manifest))
        invalid["imported"][0]["kind"] = "queue"
        invalid["imported"][1]["input_path"] = " "
        invalid["imported"][1]["output_path"] = "/tmp/performance/perf-1.json"
        invalid["runs"][0]["imported_count"] = 99
        blockers = validate_multi_trader_smoke_import_manifest(invalid, {}, zip_names)

        self.assertIn("multi_trader_smoke_package_import_manifest_kind_invalid", blockers)
        self.assertIn("multi_trader_smoke_package_import_manifest_input_path_invalid", blockers)
        self.assertIn("multi_trader_smoke_package_import_manifest_output_path_invalid", blockers)
        self.assertIn("multi_trader_smoke_package_import_manifest_run_count_mismatch", blockers)

        duplicate = json.loads(json.dumps(manifest))
        duplicate["imported"][1]["output_path"] = "clients/desk-1.json"
        duplicate["runs"][0]["imported"][1]["output_path"] = "clients/desk-1.json"
        duplicate_blockers = validate_multi_trader_smoke_import_manifest(duplicate, {}, zip_names)

        self.assertIn("multi_trader_smoke_package_import_manifest_kind_output_mismatch", duplicate_blockers)
        self.assertIn("multi_trader_smoke_package_import_manifest_duplicate_output", duplicate_blockers)

    def test_packaged_smoke_run_manifest_rejects_schema_and_duplicate_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            artifact_path = root / "multi-trader-smoke-evidence.json"
            artifact_path.write_text(json.dumps({"passed": True}), encoding="utf-8")
            data = artifact_path.read_bytes()
            package_path = root / "smoke.zip"
            with zipfile.ZipFile(package_path, mode="w") as archive:
                archive.write(artifact_path, "multi-trader-smoke-evidence.json")
                archive.writestr("smoke-run-manifest.json", "{}")
            manifest = {
                "schema_version": 2,
                "file_count": 2,
                "files": [
                    {
                        "path": "multi-trader-smoke-evidence.json",
                        "bytes": len(data),
                        "sha256": hashlib.sha256(data).hexdigest(),
                    },
                    {
                        "path": "multi-trader-smoke-evidence.json",
                        "bytes": len(data),
                        "sha256": hashlib.sha256(data).hexdigest(),
                    },
                ],
            }
            evidence: dict[str, object] = {}

            blockers = validate_multi_trader_smoke_package_run_manifest(
                manifest,
                evidence,
                package_path,
                ["multi-trader-smoke-evidence.json", "smoke-run-manifest.json"],
            )

        self.assertIn("multi_trader_smoke_package_manifest_schema_invalid", blockers)
        self.assertIn("multi_trader_smoke_package_manifest_duplicate_files", blockers)
        self.assertEqual(evidence["multi_trader_smoke_package_manifest_duplicate_paths"], ["multi-trader-smoke-evidence.json"])

    def test_rejects_weak_shadow_run_report_timing_fields(self) -> None:
        invalid_started_at = passing_report()
        invalid_started_at["started_at"] = "20260522 093000"
        self.assertIn("started_at_invalid", shadow_run_report_errors(invalid_started_at))

        reversed_window = passing_report()
        reversed_window["finished_at"] = "2026-05-22T08:59:00+08:00"
        reversed_window["duration_seconds"] = 1860
        self.assertIn("finished_at_before_started_at", shadow_run_report_errors(reversed_window))

        mismatched_duration = passing_report()
        mismatched_duration["duration_seconds"] = 60
        duration_errors = shadow_run_report_errors(mismatched_duration)
        self.assertIn("duration_seconds_mismatch", duration_errors)

        missing_evidence_source = passing_report()
        del missing_evidence_source["evidence_source"]
        self.assertIn("evidence_source_missing", shadow_run_report_errors(missing_evidence_source))

        mismatched_evidence_source = passing_report()
        mismatched_evidence_source["evidence_source"]["v2_event_count"] = 1
        mismatched_evidence_source["evidence_source"]["performance_sample_counts"]["gateway_to_frontend_ms"] = 1
        evidence_source_errors = shadow_run_report_errors(mismatched_evidence_source)
        self.assertIn("evidence_source_v2_event_count_mismatch", evidence_source_errors)
        self.assertIn("evidence_source_performance_sample_counts_mismatch", evidence_source_errors)

        result = evaluate_cutover_readiness(
            [mismatched_duration],
            policy=CutoverPolicy(min_session_duration_seconds=60),
        )

        self.assertFalse(result["passed"])
        self.assertIn("shadow_run_duration_seconds_mismatch", result["rejected_reports"][0]["blockers"])

    def test_rejects_shadow_run_report_count_mismatches(self) -> None:
        mismatched_top_level_count = passing_report()
        mismatched_top_level_count["legacy_event_count"] = 3
        self.assertIn("legacy_event_count_mismatch", shadow_run_report_errors(mismatched_top_level_count))

        mismatched_failed_symbols = passing_report()
        mismatched_failed_symbols["comparison"]["symbols"]["00700.HK"]["passed"] = False
        self.assertIn(
            "comparison_failed_symbols_mismatch",
            shadow_run_report_errors(mismatched_failed_symbols),
        )

        mismatched_missing_symbols = passing_report()
        mismatched_missing_symbols["comparison"]["symbols"]["00700.HK"]["missing"] = True
        self.assertIn(
            "comparison_missing_symbol_count_mismatch",
            shadow_run_report_errors(mismatched_missing_symbols),
        )

        invalid_symbol_count = passing_report()
        invalid_symbol_count["comparison"]["symbols"]["00700.HK"]["v2_count"] = "2"
        self.assertIn(
            "comparison_symbol_v2_count_invalid:00700.HK",
            shadow_run_report_errors(invalid_symbol_count),
        )

    def test_rejects_shadow_run_report_weak_comparison_summary_shape(self) -> None:
        invalid_failed_symbol = passing_report()
        invalid_failed_symbol["comparison"]["failed_symbols"] = ["700"]
        self.assertIn(
            "comparison_failed_symbols_format_invalid",
            shadow_run_report_errors(invalid_failed_symbol),
        )

        duplicate_failed_symbol = passing_report()
        duplicate_failed_symbol["comparison"]["symbols"]["00700.HK"]["passed"] = False
        duplicate_failed_symbol["comparison"]["failed_symbols"] = ["00700.HK", "00700.HK"]
        self.assertIn(
            "comparison_failed_symbols_duplicate",
            shadow_run_report_errors(duplicate_failed_symbol),
        )

        bool_missing_count = passing_report()
        bool_missing_count["comparison"]["missing_symbol_count"] = False
        self.assertIn(
            "comparison_missing_symbol_count_invalid",
            shadow_run_report_errors(bool_missing_count),
        )

        missing_marked_passed = passing_report()
        missing_marked_passed["comparison"]["symbols"]["00700.HK"]["missing"] = True
        missing_marked_passed["comparison"]["missing_symbol_count"] = 1
        self.assertIn(
            "comparison_symbol_missing_marked_passed:00700.HK",
            shadow_run_report_errors(missing_marked_passed),
        )

    def test_rejects_shadow_run_report_weak_comparison_metrics(self) -> None:
        weak_symbol_metric = passing_report()
        weak_symbol_metric["comparison"]["symbols"]["00700.HK"]["duplicate_ratio"] = -0.1
        self.assertIn(
            "comparison_symbol_duplicate_ratio_invalid:00700.HK",
            shadow_run_report_errors(weak_symbol_metric),
        )

        missing_symbol_metric = passing_report()
        del missing_symbol_metric["comparison"]["symbols"]["00700.HK"]["max_latency_delta_ms"]
        self.assertIn(
            "comparison_symbol_field_missing:00700.HK:max_latency_delta_ms",
            shadow_run_report_errors(missing_symbol_metric),
        )

        weak_threshold = passing_report()
        weak_threshold["comparison"]["thresholds"]["max_duplicate_ratio"] = "0.001"
        self.assertIn(
            "comparison_threshold_invalid:max_duplicate_ratio",
            shadow_run_report_errors(weak_threshold),
        )

        weak_missing_threshold = passing_report()
        weak_missing_threshold["comparison"]["thresholds"]["max_missing_symbol_count"] = 0.5
        self.assertIn(
            "comparison_threshold_invalid:max_missing_symbol_count",
            shadow_run_report_errors(weak_missing_threshold),
        )

    def test_rejects_shadow_run_report_weak_performance_consistency(self) -> None:
        missing_sample_mismatch = passing_report()
        missing_sample_mismatch["performance"]["sample_counts"]["gateway_to_frontend_ms"] = 0
        self.assertIn(
            "performance_missing_sample_keys_mismatch",
            shadow_run_report_errors(missing_sample_mismatch),
        )

        insufficient_sample_mismatch = passing_report()
        insufficient_sample_mismatch["performance"]["sample_counts"]["frontend_store_update_ms"] = 1
        self.assertIn(
            "performance_insufficient_sample_keys_mismatch",
            shadow_run_report_errors(insufficient_sample_mismatch),
        )

        passed_with_metric_failure = passing_report()
        passed_with_metric_failure["performance"]["metrics"]["frontend_store_update"]["p95_ms"] = 300
        self.assertIn(
            "performance_passed_with_metric_failures",
            shadow_run_report_errors(passed_with_metric_failure),
        )

    def test_blocks_when_shadow_streams_do_not_cover_parallel_session(self) -> None:
        report = passing_report()
        report["duration_seconds"] = 3600
        report["legacy_source_coverage_seconds"] = 30
        report["v2_source_coverage_seconds"] = 30

        result = evaluate_cutover_readiness(
            [report],
            policy=CutoverPolicy(min_session_duration_seconds=60, min_stream_coverage_ratio=0.9),
        )

        self.assertFalse(result["passed"])
        self.assertIn("legacy_stream_coverage_below_minimum", result["rejected_reports"][0]["blockers"])
        self.assertIn("v2_stream_coverage_below_minimum", result["rejected_reports"][0]["blockers"])

    def test_blocks_when_any_symbol_does_not_cover_parallel_session(self) -> None:
        report = passing_report()
        report["duration_seconds"] = 3600
        report["legacy_source_coverage_seconds"] = 3600
        report["v2_source_coverage_seconds"] = 3600
        report["comparison"]["symbols"]["00700.HK"]["legacy_source_coverage_seconds"] = 30
        report["comparison"]["symbols"]["00700.HK"]["v2_source_coverage_seconds"] = 30

        result = evaluate_cutover_readiness(
            [report],
            policy=CutoverPolicy(min_session_duration_seconds=60, min_stream_coverage_ratio=0.9),
        )

        self.assertFalse(result["passed"])
        self.assertIn("legacy_symbol_coverage_below_minimum:00700.HK", result["rejected_reports"][0]["blockers"])
        self.assertIn("v2_symbol_coverage_below_minimum:00700.HK", result["rejected_reports"][0]["blockers"])

    def test_requires_at_least_one_report(self) -> None:
        result = evaluate_cutover_readiness(
            [],
            policy=CutoverPolicy(min_session_duration_seconds=60),
        )

        self.assertFalse(result["passed"])
        self.assertIn("no_shadow_run_reports", result["blockers"])
        self.assertIn("insufficient_accepted_parallel_sessions", result["blockers"])

    def test_can_hold_legacy_retirement_after_frontend_gate_passes(self) -> None:
        result = evaluate_cutover_readiness(
            [passing_report()],
            policy=CutoverPolicy(min_session_duration_seconds=60, allow_legacy_retirement=False),
        )

        self.assertTrue(result["frontend_default_v2_allowed"])
        self.assertFalse(result["legacy_retirement_allowed"])
        self.assertEqual(result["blockers"], [])
        self.assertEqual(
            result["legacy_retirement_blockers"],
            ["legacy_retirement_requires_operator_approval"],
        )

    def test_loads_json_report_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            report_path = Path(temp_dir) / "report.json"
            report_path.write_text(json.dumps(passing_report()), encoding="utf-8")

            reports = load_cutover_reports([report_path])

        self.assertEqual(len(reports), 1)
        self.assertEqual(reports[0]["session_id"], "session-1")

    def test_persists_shadow_run_reports_and_generates_cutover_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reports_dir = root / "reports"
            readiness_path = root / "cutover-readiness.json"
            env_path = root / ".env.cutover"

            report_path = save_shadow_run_report(passing_report(), reports_dir)
            reports = load_shadow_run_report_directory(reports_dir)
            readiness = write_cutover_readiness(
                reports_directory=reports_dir,
                output_path=readiness_path,
                policy=CutoverPolicy(min_session_duration_seconds=60),
            )
            env = write_frontend_cutover_env(
                readiness,
                env_path,
                live_url="ws://gateway.internal:9020/ws",
            )

            self.assertEqual(report_path.name, "20260522.session-1.shadow-run.json")
            self.assertEqual(reports[0]["session_id"], "session-1")
            self.assertTrue(json.loads(readiness_path.read_text(encoding="utf-8"))["frontend_default_v2_allowed"])
            self.assertEqual(env["VITE_MARKET_DATA_MODE"], "auto")
            self.assertEqual(env["VITE_MARKET_PROTOCOL"], "terminal-message-v1")
            self.assertIn("frontend_default_v2_allowed", env_path.read_text(encoding="utf-8"))

    def test_write_cutover_artifacts_writes_report_readiness_and_frontend_env(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)

            paths = write_cutover_artifacts(
                report=passing_report(),
                reports_directory=root / "reports",
                readiness_path=root / "cutover-readiness.json",
                frontend_env_path=root / ".env.cutover",
                live_url="ws://gateway.internal:9020/ws",
                policy=CutoverPolicy(min_session_duration_seconds=60),
            )

            self.assertTrue(paths.report_path.exists())
            self.assertTrue(paths.readiness_path.exists())
            self.assertTrue(paths.frontend_env_path.exists())

            env = frontend_cutover_env(
                json.loads(paths.readiness_path.read_text(encoding="utf-8")),
                live_url="ws://gateway.internal:9020/ws",
            )
            self.assertEqual(env["VITE_MARKET_WS_URL"], "ws://gateway.internal:9020/ws")
            self.assertEqual(env["VITE_MARKET_PROTOCOL"], "terminal-message-v1")

    def test_frontend_deployment_evidence_requires_live_url_and_cutover_readiness(self) -> None:
        readiness = evaluate_cutover_readiness(
            [passing_report()],
        )
        result = evaluate_frontend_deployment(
            FrontendDeploymentEvidence(
                expected_live_url="ws://gateway.internal:9020/ws",
                deployed_env=frontend_cutover_env(readiness, live_url="ws://gateway.internal:9020/ws"),
                verified_at="2026-05-22T13:30:02+08:00",
            )
        )
        blocked = evaluate_frontend_deployment(
            FrontendDeploymentEvidence(
                expected_live_url="ws://gateway.internal:9020/ws",
                deployed_env={
                    "VITE_MARKET_DATA_MODE": "auto",
                    "VITE_MARKET_WS_URL": "ws://legacy.internal:9020/ws",
                    "VITE_MARKET_CUTOVER_READINESS": json.dumps({"frontend_default_v2_allowed": False}),
                },
                verified_at="2026-05-22T13:30:02+08:00",
            )
        )
        invalid_url = evaluate_frontend_deployment(
            FrontendDeploymentEvidence(
                expected_live_url="https://gateway.internal:9020/ws",
                deployed_env=frontend_cutover_env(readiness, live_url="https://gateway.internal:9020/ws"),
                verified_at="2026-05-22T13:30:02+08:00",
            )
        )
        wrong_path = evaluate_frontend_deployment(
            FrontendDeploymentEvidence(
                expected_live_url="ws://gateway.internal:9020/legacy",
                deployed_env=frontend_cutover_env(readiness, live_url="ws://gateway.internal:9020/legacy"),
                verified_at="2026-05-22T13:30:02+08:00",
            )
        )
        missing_port = evaluate_frontend_deployment(
            FrontendDeploymentEvidence(
                expected_live_url="ws://gateway.internal/ws",
                deployed_env=frontend_cutover_env(readiness, live_url="ws://gateway.internal/ws"),
                verified_at="2026-05-22T13:30:02+08:00",
            )
        )
        loopback_url = evaluate_frontend_deployment(
            FrontendDeploymentEvidence(
                expected_live_url="ws://127.0.0.1:9020/ws",
                deployed_env=frontend_cutover_env(readiness, live_url="ws://127.0.0.1:9020/ws"),
                verified_at="2026-05-22T13:30:02+08:00",
            )
        )

        self.assertTrue(result["frontend_default_v2_deployed"])
        self.assertFalse(blocked["frontend_default_v2_deployed"])
        self.assertIn("frontend_live_url_mismatch", blocked["blockers"])
        self.assertIn("frontend_protocol_not_terminal_message_v1", blocked["blockers"])
        self.assertIn("frontend_cutover_readiness_artifact_invalid", blocked["blockers"])
        self.assertIn("frontend_cutover_readiness_schema_invalid", blocked["blockers"])
        self.assertIn("frontend_cutover_readiness_not_passed", blocked["blockers"])
        self.assertIn("frontend_cutover_readiness_does_not_allow_v2", blocked["blockers"])
        self.assertIn("frontend_cutover_readiness_accepted_report_ids_invalid", blocked["blockers"])
        self.assertIn("frontend_cutover_readiness_policy_missing", blocked["blockers"])
        self.assertIn("frontend_auto_mode_would_select_mock", blocked["blockers"])
        self.assertFalse(invalid_url["frontend_default_v2_deployed"])
        self.assertIn("frontend_live_url_invalid", invalid_url["blockers"])
        self.assertIn("frontend_live_url_gateway_path_mismatch", wrong_path["blockers"])
        self.assertIn("frontend_live_url_gateway_port_missing", missing_port["blockers"])
        self.assertIn("frontend_live_url_loopback_host", loopback_url["blockers"])

    def test_writes_frontend_deployment_evidence_from_env_file(self) -> None:
        readiness = evaluate_cutover_readiness(
            [passing_report()],
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            env_path = root / ".env.cutover"
            output_path = root / "frontend-deployment.json"
            write_frontend_cutover_env(readiness, env_path, live_url="ws://gateway.internal:9020/ws")

            result = write_frontend_deployment_evidence(
                env_path=env_path,
                expected_live_url="ws://gateway.internal:9020/ws",
                output_path=output_path,
                verified_at="2026-05-22T13:30:02+08:00",
            )
            loaded_env = load_env_file(env_path)
            persisted = json.loads(output_path.read_text(encoding="utf-8"))

        self.assertTrue(result["passed"])
        self.assertTrue(persisted["frontend_default_v2_deployed"])
        self.assertEqual(persisted["verified_at"], "2026-05-22T13:30:02+08:00")
        self.assertEqual(loaded_env["VITE_MARKET_DATA_MODE"], "auto")
        self.assertEqual(loaded_env["VITE_MARKET_PROTOCOL"], "terminal-message-v1")

    def test_legacy_decommission_evidence_requires_no_websocket_old_consumers_or_lag(self) -> None:
        passing = evaluate_legacy_decommission(
            LegacyDecommissionObservation(
                legacy_websocket_enabled=False,
                old_topic_consumers={"legacy_ticks": 0, "legacy_broker_queue": 0},
                old_topic_lag={"legacy_ticks": 0, "legacy_broker_queue": 0},
                observed_at="2026-05-22T16:15:00+08:00",
            )
        )
        blocked = evaluate_legacy_decommission(
            LegacyDecommissionObservation(
                legacy_websocket_enabled=True,
                old_topic_consumers={"legacy_ticks": 2},
                old_topic_lag={"legacy_ticks": 7},
                observed_at="2026-05-22T16:15:00+08:00",
            )
        )

        self.assertTrue(passing["passed"])
        self.assertTrue(passing["legacy_websocket_disabled"])
        self.assertFalse(blocked["passed"])
        self.assertIn("legacy_websocket_still_enabled", blocked["blockers"])
        self.assertIn("old_topic_consumers_still_enabled", blocked["blockers"])
        self.assertIn("old_topic_lag_still_present", blocked["blockers"])

        incomplete = evaluate_legacy_decommission(
            LegacyDecommissionObservation(
                legacy_websocket_enabled=False,
                old_topic_consumers={},
                old_topic_lag={},
                observed_at="",
            )
        )
        self.assertFalse(incomplete["passed"])
        self.assertIn("legacy_decommission_observed_at_missing", incomplete["blockers"])
        self.assertIn("old_topic_consumer_observation_incomplete", incomplete["blockers"])
        self.assertIn("old_topic_lag_observation_incomplete", incomplete["blockers"])

        malformed_timestamp = evaluate_legacy_decommission(
            LegacyDecommissionObservation(
                legacy_websocket_enabled=False,
                old_topic_consumers={"legacy_ticks": 0, "legacy_broker_queue": 0},
                old_topic_lag={"legacy_ticks": 0, "legacy_broker_queue": 0},
                observed_at="20260522 161500",
            )
        )
        self.assertFalse(malformed_timestamp["passed"])
        self.assertIn("legacy_decommission_observed_at_invalid", malformed_timestamp["blockers"])

        invalid_counts = evaluate_legacy_decommission(
            LegacyDecommissionObservation(
                legacy_websocket_enabled=False,
                old_topic_consumers={"legacy_ticks": "0", "legacy_broker_queue": -1},
                old_topic_lag={"legacy_ticks": "0", "legacy_broker_queue": -1},
                observed_at="2026-05-22T16:15:00+08:00",
            )
        )
        self.assertFalse(invalid_counts["passed"])
        self.assertIn("old_topic_consumer_observation_invalid", invalid_counts["blockers"])
        self.assertIn("old_topic_lag_observation_invalid", invalid_counts["blockers"])

    def test_writes_legacy_decommission_evidence_from_observation_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            observation_path = root / "legacy-observation.json"
            output_path = root / "legacy-decommission.json"
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

            result = write_legacy_decommission_evidence(
                observation_path=observation_path,
                output_path=output_path,
            )
            loaded = load_legacy_decommission_observation(observation_path)
            persisted = json.loads(output_path.read_text(encoding="utf-8"))

        self.assertTrue(result["passed"])
        self.assertFalse(loaded.legacy_websocket_enabled)
        self.assertTrue(persisted["no_legacy_consumers_observed"])

    def test_multi_trader_smoke_evidence_requires_phase_6_lan_workflows(self) -> None:
        passing = evaluate_multi_trader_smoke(multi_trader_smoke_payload())
        derived_payload = multi_trader_smoke_payload()
        del derived_payload["metrics"]
        derived_payload["performance_samples"] = {"subscribe_snapshot_ms": [50.0, 80.0, 100.0]}
        derived_payload["runtime_health"] = {
            "passed": True,
            "symbol_runtime": {
                "00700.HK": {"hydrate_count": 1},
                "00939.HK": {"hydrate_count": 1},
                "00005.HK": {"hydrate_count": 1},
            },
            "gateway_websocket": gateway_smoke_evidence(),
        }
        derived = evaluate_multi_trader_smoke(derived_payload)
        runtime_health_performance_payload = multi_trader_smoke_payload()
        del runtime_health_performance_payload["metrics"]
        runtime_health_performance_payload.pop("performance_samples", None)
        runtime_health_performance_payload["runtime_health"] = {
            "passed": True,
            "symbol_runtime": {
                "00700.HK": {"hydrate_count": 1},
                "00939.HK": {"hydrate_count": 1},
                "00005.HK": {"hydrate_count": 1},
            },
            "performance_samples": {"subscribe_snapshot_ms": [60.0, 80.0, 100.0]},
            "gateway_websocket": gateway_smoke_evidence(),
        }
        runtime_health_performance = evaluate_multi_trader_smoke(runtime_health_performance_payload)
        runtime_health_metric_payload = multi_trader_smoke_payload()
        del runtime_health_metric_payload["metrics"]
        runtime_health_metric_payload.pop("performance_samples", None)
        runtime_health_metric_payload["runtime_health"] = {
            "passed": True,
            "symbol_runtime": {
                "00700.HK": {"hydrate_count": 1},
                "00939.HK": {"hydrate_count": 1},
                "00005.HK": {"hydrate_count": 1},
            },
            "performance_metrics": {"warm_snapshot_p95_ms": 96.0},
            "gateway_websocket": gateway_smoke_evidence(),
        }
        runtime_health_metric = evaluate_multi_trader_smoke(runtime_health_metric_payload)
        performance_artifacts_payload = multi_trader_smoke_payload()
        performance_artifacts_payload["performance_artifacts"] = [
            {
                "path": "performance/desk-a.json",
                "machine_id": "desk-a",
                "exported_at": "2026-05-25T10:59:00+08:00",
                "subscribe_snapshot_count": 2,
            },
            {
                "path": "performance/desk-b.json",
                "machine_id": "desk-b",
                "exported_at": "2026-05-25T10:59:00+08:00",
                "subscribe_snapshot_count": 2,
            },
        ]
        performance_artifacts = evaluate_multi_trader_smoke(performance_artifacts_payload)
        bad_performance_artifacts_payload = multi_trader_smoke_payload()
        bad_performance_artifacts_payload["performance_artifacts"] = [
            {
                "path": "performance/desk-a.json",
                "machine_id": "desk-a",
                "exported_at": "2026-05-25T10:59:00+08:00",
                "subscribe_snapshot_count": 2,
            },
            {
                "path": "performance/other.json",
                "machine_id": "other-desk",
                "exported_at": "2026-05-25T10:59:00+08:00",
                "subscribe_snapshot_count": 1,
            },
            {
                "path": "performance/missing-machine.json",
                "machine_id": "",
                "exported_at": "2026-05-25T10:59:00+08:00",
                "subscribe_snapshot_count": 1,
            },
        ]
        bad_performance_artifacts = evaluate_multi_trader_smoke(bad_performance_artifacts_payload)
        stale_performance_artifacts_payload = multi_trader_smoke_payload()
        stale_performance_artifacts_payload["performance_artifacts"] = [
            {
                "path": "performance/desk-a.json",
                "machine_id": "desk-a",
                "exported_at": "2026-05-25T10:00:00+08:00",
                "subscribe_snapshot_count": 2,
            },
            {
                "path": "performance/desk-b.json",
                "machine_id": "desk-b",
                "exported_at": "2026-05-25T11:05:00+08:00",
                "subscribe_snapshot_count": 2,
            },
        ]
        stale_performance_artifacts = evaluate_multi_trader_smoke(stale_performance_artifacts_payload)
        missing_runtime_reference_payload = multi_trader_smoke_payload()
        missing_runtime_reference_payload["runtime_health"] = {"passed": True}
        missing_runtime_reference = evaluate_multi_trader_smoke(missing_runtime_reference_payload)
        loopback_gateway_payload = multi_trader_smoke_payload()
        loopback_gateway_payload["runtime_health"]["gateway_websocket"]["host"] = "127.0.0.1"
        loopback_gateway = evaluate_multi_trader_smoke(loopback_gateway_payload)
        missing_gateway_payload = multi_trader_smoke_payload()
        missing_gateway_payload["runtime_health"].pop("gateway_websocket")
        missing_gateway = evaluate_multi_trader_smoke(missing_gateway_payload)
        insufficient_gateway_clients_payload = multi_trader_smoke_payload()
        insufficient_gateway_clients_payload["runtime_health"]["gateway_activity"]["client_queue"] = {
            **gateway_client_queue_payload(),
            "observed_client_count": 1,
            "observed_client_ids": ["desk-a"],
            "max_connected_clients": 1,
        }
        insufficient_gateway_clients = evaluate_multi_trader_smoke(insufficient_gateway_clients_payload)
        mismatched_gateway_observed_clients_payload = multi_trader_smoke_payload()
        mismatched_gateway_observed_clients_payload["runtime_health"]["gateway_activity"]["client_queue"] = {
            **gateway_client_queue_payload(),
            "observed_client_count": 2,
            "observed_client_ids": ["desk-a"],
        }
        mismatched_gateway_observed_clients = evaluate_multi_trader_smoke(mismatched_gateway_observed_clients_payload)
        duplicate_gateway_observed_clients_payload = multi_trader_smoke_payload()
        duplicate_gateway_observed_clients_payload["runtime_health"]["gateway_activity"]["client_queue"] = {
            **gateway_client_queue_payload(),
            "observed_client_count": 1,
            "observed_client_ids": ["desk-a", "desk-a"],
            "max_connected_clients": 2,
        }
        duplicate_gateway_observed_clients = evaluate_multi_trader_smoke(duplicate_gateway_observed_clients_payload)
        missing_declared_gateway_clients_payload = multi_trader_smoke_payload()
        missing_declared_gateway_clients_payload["runtime_health"]["gateway_activity"]["client_queue"] = {
            **gateway_client_queue_payload(),
            "observed_declared_client_count": 1,
            "observed_declared_client_ids": ["desk-a"],
        }
        missing_declared_gateway_clients = evaluate_multi_trader_smoke(missing_declared_gateway_clients_payload)
        whitespace_declared_gateway_clients_payload = multi_trader_smoke_payload()
        whitespace_declared_gateway_clients_payload["runtime_health"]["gateway_activity"]["client_queue"] = {
            **gateway_client_queue_payload(),
            "observed_declared_client_ids": ["desk-a", " desk-b "],
        }
        whitespace_declared_gateway_clients = evaluate_multi_trader_smoke(whitespace_declared_gateway_clients_payload)
        duplicate_declared_gateway_clients_payload = multi_trader_smoke_payload()
        duplicate_declared_gateway_clients_payload["runtime_health"]["gateway_activity"]["client_queue"] = {
            **gateway_client_queue_payload(),
            "observed_declared_client_count": 2,
            "observed_declared_client_ids": ["desk-a", "desk-a"],
        }
        duplicate_declared_gateway_clients = evaluate_multi_trader_smoke(duplicate_declared_gateway_clients_payload)
        missing_declared_gateway_fields_payload = multi_trader_smoke_payload()
        missing_declared_gateway_fields_payload["runtime_health"]["gateway_activity"]["client_queue"].pop(
            "observed_declared_client_ids"
        )
        missing_declared_gateway_fields_payload["runtime_health"]["gateway_activity"]["client_queue"].pop(
            "observed_declared_client_count"
        )
        missing_declared_gateway_fields = evaluate_multi_trader_smoke(missing_declared_gateway_fields_payload)
        preflight_payload = multi_trader_smoke_payload()
        preflight_payload["preflight"] = multi_trader_smoke_preflight_payload()
        preflight = evaluate_multi_trader_smoke(preflight_payload)
        missing_preflight_payload = multi_trader_smoke_payload()
        missing_preflight_payload.pop("preflight")
        missing_preflight = evaluate_multi_trader_smoke(missing_preflight_payload)
        missing_prepared_at_payload = multi_trader_smoke_payload()
        missing_prepared_at_payload["preflight"].pop("prepared_at")
        missing_prepared_at = evaluate_multi_trader_smoke(missing_prepared_at_payload)
        invalid_prepared_at_payload = multi_trader_smoke_payload()
        invalid_prepared_at_payload["preflight"]["prepared_at"] = "20260525 105500"
        invalid_prepared_at = evaluate_multi_trader_smoke(invalid_prepared_at_payload)
        missing_service_preflight_payload = multi_trader_smoke_payload()
        missing_service_preflight_payload["preflight"].pop("service_checks")
        missing_service_preflight = evaluate_multi_trader_smoke(missing_service_preflight_payload)
        stale_service_preflight_payload = multi_trader_smoke_payload()
        stale_service_preflight_payload["preflight"]["service_checks"]["checked_at"] = "2026-05-25T10:00:00+08:00"
        stale_service_preflight = evaluate_multi_trader_smoke(stale_service_preflight_payload)
        future_service_preflight_payload = multi_trader_smoke_payload()
        future_service_preflight_payload["preflight"]["service_checks"]["checked_at"] = "2026-05-25T11:05:00+08:00"
        future_service_preflight = evaluate_multi_trader_smoke(future_service_preflight_payload)
        mismatched_service_url_payload = multi_trader_smoke_payload()
        mismatched_service_url_payload["preflight"]["service_checks"]["checks"]["frontend"] = {
            **mismatched_service_url_payload["preflight"]["service_checks"]["checks"]["frontend"],
            "url": "http://other-gateway.internal:5173/",
        }
        mismatched_service_url_payload["preflight"]["service_checks"]["checks"]["gateway"] = {
            **mismatched_service_url_payload["preflight"]["service_checks"]["checks"]["gateway"],
            "url": "ws://other-gateway.internal:9020/ws",
        }
        mismatched_service_url = evaluate_multi_trader_smoke(mismatched_service_url_payload)
        failing_preflight_payload = multi_trader_smoke_payload()
        failing_preflight_payload["preflight"] = {
            **multi_trader_smoke_preflight_payload(),
            "passed": False,
            "blockers": ["multi_trader_smoke_lan_host_not_client_routable"],
            "page_url": "http://127.0.0.1:5173/",
        }
        failing_preflight = evaluate_multi_trader_smoke(failing_preflight_payload)
        mock_client_payload = multi_trader_smoke_payload()
        mock_client_payload["clients"][0]["data_source_mode"] = "mock"
        mock_client = evaluate_multi_trader_smoke(mock_client_payload)
        loopback_client_payload = multi_trader_smoke_payload()
        loopback_client_payload["clients"][0]["page_url"] = "http://127.0.0.1:5173/"
        loopback_client_payload["clients"][0]["gateway_url"] = "ws://127.0.0.1:9020/ws"
        loopback_client = evaluate_multi_trader_smoke(loopback_client_payload)
        mismatched_client_network_payload = multi_trader_smoke_payload()
        mismatched_client_network_payload["clients"][0]["page_url"] = "http://other-gateway.internal:5173/#/"
        mismatched_client_network_payload["clients"][1]["gateway_url"] = "ws://other-gateway.internal:9020/ws"
        mismatched_client_network = evaluate_multi_trader_smoke(mismatched_client_network_payload)
        stale_client_artifact_payload = multi_trader_smoke_payload()
        stale_client_artifact_payload["client_artifacts"] = [
            {"path": "clients/desk-a.json", "exported_at": "2026-05-25T10:00:00+08:00", "machine_ids": ["desk-a"]},
            {"path": "clients/desk-b.json", "exported_at": "2026-05-25T11:05:00+08:00", "machine_ids": ["desk-b"]},
            {"path": "clients/bad.json", "exported_at": "20260525 110000", "machine_ids": ["desk-c"]},
            {"path": "clients/missing.json", "machine_ids": ["desk-d"]},
            {"path": "clients/dirty-machine.json", "exported_at": "2026-05-25T10:59:00+08:00", "machine_ids": [" desk-a "]},
        ]
        stale_client_artifact = evaluate_multi_trader_smoke(stale_client_artifact_payload)
        stale_runtime_health_payload = multi_trader_smoke_payload()
        stale_runtime_health_payload["runtime_health"]["generated_at"] = "2026-05-25T10:00:00+08:00"
        stale_runtime_health = evaluate_multi_trader_smoke(stale_runtime_health_payload)
        future_runtime_health_payload = multi_trader_smoke_payload()
        future_runtime_health_payload["runtime_health"]["generated_at"] = "2026-05-25T11:05:00+08:00"
        future_runtime_health = evaluate_multi_trader_smoke(future_runtime_health_payload)
        invalid_runtime_health_payload = multi_trader_smoke_payload()
        invalid_runtime_health_payload["runtime_health"]["generated_at"] = "20260525 105900"
        invalid_runtime_health = evaluate_multi_trader_smoke(invalid_runtime_health_payload)
        missing_runtime_health_generated_at_payload = multi_trader_smoke_payload()
        missing_runtime_health_generated_at_payload["runtime_health"].pop("generated_at")
        missing_runtime_health_generated_at = evaluate_multi_trader_smoke(missing_runtime_health_generated_at_payload)
        stale_workflow_payload = multi_trader_smoke_payload()
        stale_workflow_payload["workflows"]["cold_query"]["observed_at"] = "2026-05-25T10:00:00+08:00"
        stale_workflow = evaluate_multi_trader_smoke(stale_workflow_payload)
        future_workflow_payload = multi_trader_smoke_payload()
        future_workflow_payload["workflows"]["add_to_watchlist"]["observed_at"] = "2026-05-25T11:05:00+08:00"
        future_workflow = evaluate_multi_trader_smoke(future_workflow_payload)
        invalid_workflow_payload = multi_trader_smoke_payload()
        invalid_workflow_payload["workflows"]["refresh_recovery"]["observed_at"] = "20260525 105820"
        invalid_workflow = evaluate_multi_trader_smoke(invalid_workflow_payload)
        missing_workflow_observed_payload = multi_trader_smoke_payload()
        missing_workflow_observed_payload["workflows"]["redis_clear_recovery"].pop("observed_at")
        missing_workflow_observed = evaluate_multi_trader_smoke(missing_workflow_observed_payload)
        duplicate_machine_payload = multi_trader_smoke_payload()
        duplicate_machine_payload["clients"][1]["machine_id"] = "desk-a"
        duplicate_machine = evaluate_multi_trader_smoke(duplicate_machine_payload)
        whitespace_machine_payload = multi_trader_smoke_payload()
        whitespace_machine_payload["clients"][0]["machine_id"] = " desk-a "
        whitespace_machine = evaluate_multi_trader_smoke(whitespace_machine_payload)
        whitespace_performance_artifact_payload = multi_trader_smoke_payload()
        whitespace_performance_artifact_payload["performance_artifacts"] = [
            {
                "path": "performance/desk-a.json",
                "machine_id": " desk-a ",
                "exported_at": "2026-05-25T10:59:00+08:00",
                "subscribe_snapshot_count": 2,
            },
            {
                "path": "performance/desk-b.json",
                "machine_id": "desk-b",
                "exported_at": "2026-05-25T10:59:00+08:00",
                "subscribe_snapshot_count": 2,
            },
        ]
        whitespace_performance_artifact = evaluate_multi_trader_smoke(whitespace_performance_artifact_payload)
        active_hydration_payload = multi_trader_smoke_payload()
        active_hydration_payload["runtime_health"] = {
            "passed": True,
            "symbol_runtime": {
                "00700.HK": {"hydrate_count": 1},
                "00939.HK": {"hydrate_count": 1},
                "00005.HK": {"hydrate_count": 1},
            },
            "symbol_runtime_manager": {
                "active_hydrations": 1,
                "max_concurrent_hydrations": 8,
                "capacity_rejections": 1,
                "hydrating_symbols": ["00005.HK"],
            },
            "gateway_websocket": gateway_smoke_evidence(),
        }
        active_hydration = evaluate_multi_trader_smoke(active_hydration_payload)
        invalid_symbol_status_payload = multi_trader_smoke_payload()
        invalid_symbol_status_payload["clients"][0]["symbol_statuses"]["00939.HK"] = {
            **invalid_symbol_status_payload["clients"][0]["symbol_statuses"]["00939.HK"],
            "effective_trade_date": "20260525",
            "source_dates": {},
        }
        invalid_symbol_status = evaluate_multi_trader_smoke(invalid_symbol_status_payload)
        missing_closed_market_dates_payload = multi_trader_smoke_payload()
        missing_closed_market_dates_payload["workflows"]["closed_market_effective_date"] = {
            "passed": True,
            "expected_closed_market": True,
            "source_dates_visible": True,
        }
        missing_closed_market_dates = evaluate_multi_trader_smoke(missing_closed_market_dates_payload)
        failing_payload = multi_trader_smoke_payload()
        failing_payload["clients"] = [
            {
                "machine_id": "desk-a",
                "data_source_mode": "live",
                "page_url": "http://gateway.internal:5173/",
                "gateway_url": "ws://gateway.internal:9020/ws",
                "connected": True,
                "watchlist": ["00700.HK"],
                "refresh_recovered": False,
            }
        ]
        failing_payload["workflows"]["cold_query"] = {"passed": False}
        failing_payload["workflows"]["redis_clear_recovery"] = {"passed": False}
        failing_payload["workflows"]["add_to_watchlist"] = {"passed": True, "symbol": "00005.HK", "persisted": False}
        failing_payload["workflows"]["refresh_recovery"] = {"passed": True, "watchlist_restored": False, "snapshots_visible": False}
        failing_payload["workflows"]["process_restart_recovery"] = {"passed": True, "first_screen_restored": False}
        failing_payload["workflows"]["closed_market_effective_date"] = {
            "passed": True,
            "expected_closed_market": True,
            "requested_trade_date": "20260525",
            "effective_trade_date": "20260525",
            "source_dates_visible": False,
        }
        failing_payload["metrics"]["warm_snapshot_p95_ms"] = 250
        failing_payload["metrics"]["duplicate_hydrations"] = 1
        failing_payload["runtime_health"] = {"passed": False}
        failing = evaluate_multi_trader_smoke(failing_payload)

        self.assertTrue(passing["passed"])
        self.assertEqual(passing["watchlist_overlap"], ["00700.HK"])
        self.assertEqual(passing["gateway_client_activity"]["observed_client_count"], 2)
        self.assertEqual(passing["gateway_client_activity"]["max_connected_clients"], 2)
        self.assertTrue(derived["passed"])
        self.assertEqual(derived["metrics"]["warm_snapshot_p95_ms"], 98.0)
        self.assertEqual(derived["metrics"]["duplicate_hydrations"], 0)
        self.assertTrue(runtime_health_performance["passed"])
        self.assertEqual(runtime_health_performance["metrics"]["warm_snapshot_p95_ms"], 98.0)
        self.assertTrue(runtime_health_metric["passed"])
        self.assertEqual(runtime_health_metric["metrics"]["warm_snapshot_p95_ms"], 96.0)
        self.assertTrue(performance_artifacts["passed"])
        self.assertEqual(performance_artifacts["performance_artifacts"]["machine_ids"], ["desk-a", "desk-b"])
        self.assertFalse(bad_performance_artifacts["passed"])
        self.assertIn(
            "multi_trader_smoke_performance_artifact_machine_missing",
            bad_performance_artifacts["blockers"],
        )
        self.assertIn(
            "multi_trader_smoke_performance_artifact_machine_unknown",
            bad_performance_artifacts["blockers"],
        )
        self.assertIn(
            "multi_trader_smoke_performance_artifact_machine_coverage_missing",
            bad_performance_artifacts["blockers"],
        )
        self.assertEqual(bad_performance_artifacts["performance_artifacts"]["unknown_machine_ids"], ["other-desk"])
        self.assertEqual(bad_performance_artifacts["performance_artifacts"]["missing_client_machine_ids"], ["desk-b"])
        self.assertFalse(stale_performance_artifacts["passed"])
        self.assertIn(
            "multi_trader_smoke_performance_artifact_before_preflight",
            stale_performance_artifacts["blockers"],
        )
        self.assertIn(
            "multi_trader_smoke_performance_artifact_after_observed",
            stale_performance_artifacts["blockers"],
        )
        self.assertFalse(missing_runtime_reference["passed"])
        self.assertIn("multi_trader_smoke_runtime_health_reference_missing", missing_runtime_reference["blockers"])
        self.assertFalse(loopback_gateway["passed"])
        self.assertIn("multi_trader_smoke_gateway_host_loopback", loopback_gateway["blockers"])
        self.assertFalse(missing_gateway["passed"])
        self.assertIn("multi_trader_smoke_gateway_websocket_missing", missing_gateway["blockers"])
        self.assertFalse(insufficient_gateway_clients["passed"])
        self.assertIn(
            "multi_trader_smoke_gateway_observed_clients_insufficient",
            insufficient_gateway_clients["blockers"],
        )
        self.assertIn(
            "multi_trader_smoke_gateway_max_connected_clients_insufficient",
            insufficient_gateway_clients["blockers"],
        )
        self.assertFalse(mismatched_gateway_observed_clients["passed"])
        self.assertIn(
            "multi_trader_smoke_gateway_observed_client_count_mismatch",
            mismatched_gateway_observed_clients["blockers"],
        )
        self.assertFalse(duplicate_gateway_observed_clients["passed"])
        self.assertIn(
            "multi_trader_smoke_gateway_client_ids_duplicate",
            duplicate_gateway_observed_clients["blockers"],
        )
        self.assertIn(
            "multi_trader_smoke_gateway_max_connected_clients_exceeds_observed",
            duplicate_gateway_observed_clients["blockers"],
        )
        self.assertFalse(missing_declared_gateway_clients["passed"])
        self.assertIn(
            "multi_trader_smoke_gateway_declared_client_coverage_missing",
            missing_declared_gateway_clients["blockers"],
        )
        self.assertEqual(
            missing_declared_gateway_clients["gateway_client_activity"]["missing_declared_client_machines"],
            ["desk-b"],
        )
        self.assertFalse(whitespace_declared_gateway_clients["passed"])
        self.assertIn(
            "multi_trader_smoke_gateway_client_activity_invalid",
            whitespace_declared_gateway_clients["blockers"],
        )
        self.assertIn(
            "multi_trader_smoke_gateway_declared_client_coverage_missing",
            whitespace_declared_gateway_clients["blockers"],
        )
        self.assertFalse(duplicate_declared_gateway_clients["passed"])
        self.assertIn(
            "multi_trader_smoke_gateway_declared_client_ids_duplicate",
            duplicate_declared_gateway_clients["blockers"],
        )
        self.assertIn(
            "multi_trader_smoke_gateway_declared_client_count_mismatch",
            duplicate_declared_gateway_clients["blockers"],
        )
        self.assertFalse(missing_declared_gateway_fields["passed"])
        self.assertIn(
            "multi_trader_smoke_gateway_declared_client_activity_missing",
            missing_declared_gateway_fields["blockers"],
        )
        self.assertTrue(preflight["passed"])
        self.assertTrue(preflight["preflight"]["present"])
        self.assertTrue(preflight["preflight"]["service_checks_present"])
        self.assertTrue(preflight["preflight"]["service_checks_passed"])
        self.assertFalse(missing_preflight["passed"])
        self.assertIn("multi_trader_smoke_preflight_missing", missing_preflight["blockers"])
        self.assertFalse(missing_prepared_at["passed"])
        self.assertIn("multi_trader_smoke_preflight_prepared_at_missing", missing_prepared_at["blockers"])
        self.assertFalse(invalid_prepared_at["passed"])
        self.assertIn("multi_trader_smoke_preflight_prepared_at_invalid", invalid_prepared_at["blockers"])
        self.assertFalse(missing_service_preflight["passed"])
        self.assertIn("multi_trader_smoke_service_preflight_missing", missing_service_preflight["blockers"])
        self.assertFalse(stale_service_preflight["passed"])
        self.assertIn("multi_trader_smoke_service_preflight_before_preflight", stale_service_preflight["blockers"])
        self.assertFalse(future_service_preflight["passed"])
        self.assertIn("multi_trader_smoke_service_preflight_after_observed", future_service_preflight["blockers"])
        self.assertFalse(mismatched_service_url["passed"])
        self.assertIn(
            "multi_trader_smoke_frontend_service_check_url_mismatch",
            mismatched_service_url["blockers"],
        )
        self.assertIn(
            "multi_trader_smoke_gateway_service_check_url_mismatch",
            mismatched_service_url["blockers"],
        )
        self.assertFalse(failing_preflight["passed"])
        self.assertIn("multi_trader_smoke_preflight_blocked", failing_preflight["blockers"])
        self.assertIn("multi_trader_smoke_preflight_blockers_present", failing_preflight["blockers"])
        self.assertIn("multi_trader_smoke_preflight_page_url_not_client_routable", failing_preflight["blockers"])
        self.assertFalse(mock_client["passed"])
        self.assertIn("multi_trader_smoke_client_not_live", mock_client["blockers"])
        self.assertFalse(loopback_client["passed"])
        self.assertIn("multi_trader_smoke_client_page_url_loopback", loopback_client["blockers"])
        self.assertIn("multi_trader_smoke_client_gateway_url_loopback", loopback_client["blockers"])
        self.assertFalse(mismatched_client_network["passed"])
        self.assertIn(
            "multi_trader_smoke_client_page_url_preflight_mismatch",
            mismatched_client_network["blockers"],
        )
        self.assertIn(
            "multi_trader_smoke_client_gateway_url_preflight_mismatch",
            mismatched_client_network["blockers"],
        )
        self.assertEqual(
            mismatched_client_network["client_preflight_network"]["page_url_mismatched_machines"],
            ["desk-a"],
        )
        self.assertEqual(
            mismatched_client_network["client_preflight_network"]["gateway_url_mismatched_machines"],
            ["desk-b"],
        )
        self.assertFalse(stale_client_artifact["passed"])
        self.assertIn("multi_trader_smoke_client_artifact_before_preflight", stale_client_artifact["blockers"])
        self.assertIn("multi_trader_smoke_client_artifact_after_observed", stale_client_artifact["blockers"])
        self.assertIn("multi_trader_smoke_client_artifact_exported_at_invalid", stale_client_artifact["blockers"])
        self.assertIn("multi_trader_smoke_client_artifact_exported_at_missing", stale_client_artifact["blockers"])
        self.assertIn("multi_trader_smoke_client_artifact_machine_ids_invalid", stale_client_artifact["blockers"])
        self.assertFalse(stale_runtime_health["passed"])
        self.assertIn("multi_trader_smoke_runtime_health_before_preflight", stale_runtime_health["blockers"])
        self.assertFalse(future_runtime_health["passed"])
        self.assertIn("multi_trader_smoke_runtime_health_after_observed", future_runtime_health["blockers"])
        self.assertFalse(invalid_runtime_health["passed"])
        self.assertIn("multi_trader_smoke_runtime_health_generated_at_invalid", invalid_runtime_health["blockers"])
        self.assertFalse(missing_runtime_health_generated_at["passed"])
        self.assertIn(
            "multi_trader_smoke_runtime_health_generated_at_missing",
            missing_runtime_health_generated_at["blockers"],
        )
        self.assertFalse(stale_workflow["passed"])
        self.assertIn("multi_trader_smoke_workflow_before_preflight", stale_workflow["blockers"])
        self.assertEqual(stale_workflow["workflow_timing"]["before_preflight_workflows"], ["cold_query"])
        self.assertFalse(future_workflow["passed"])
        self.assertIn("multi_trader_smoke_workflow_after_observed", future_workflow["blockers"])
        self.assertFalse(invalid_workflow["passed"])
        self.assertIn("multi_trader_smoke_workflow_observed_at_invalid", invalid_workflow["blockers"])
        self.assertFalse(missing_workflow_observed["passed"])
        self.assertIn("multi_trader_smoke_workflow_observed_at_missing", missing_workflow_observed["blockers"])
        self.assertFalse(duplicate_machine["passed"])
        self.assertIn("multi_trader_smoke_client_machine_duplicate", duplicate_machine["blockers"])
        self.assertFalse(whitespace_machine["passed"])
        self.assertIn("multi_trader_smoke_client_machine_invalid", whitespace_machine["blockers"])
        self.assertFalse(whitespace_performance_artifact["passed"])
        self.assertIn(
            "multi_trader_smoke_performance_artifact_machine_invalid",
            whitespace_performance_artifact["blockers"],
        )
        self.assertFalse(active_hydration["passed"])
        self.assertIn("multi_trader_smoke_runtime_hydration_still_active", active_hydration["blockers"])
        self.assertIn("multi_trader_smoke_runtime_capacity_rejections_present", active_hydration["blockers"])
        self.assertFalse(invalid_symbol_status["passed"])
        self.assertIn("multi_trader_smoke_symbol_status_closed_date_evidence_missing", invalid_symbol_status["blockers"])
        self.assertIn("multi_trader_smoke_symbol_status_closed_source_dates_missing", invalid_symbol_status["blockers"])
        self.assertFalse(missing_closed_market_dates["passed"])
        self.assertIn("multi_trader_smoke_requested_date_missing", missing_closed_market_dates["blockers"])
        self.assertIn("multi_trader_smoke_effective_date_missing", missing_closed_market_dates["blockers"])
        self.assertFalse(failing["passed"])
        self.assertIn("multi_trader_smoke_insufficient_client_machines", failing["blockers"])
        self.assertIn("multi_trader_smoke_client_refresh_not_recovered", failing["blockers"])
        self.assertIn("multi_trader_smoke_insufficient_watchlists", failing["blockers"])
        self.assertIn("multi_trader_smoke_cold_query_missing", failing["blockers"])
        self.assertIn("multi_trader_smoke_redis_clear_recovery_missing", failing["blockers"])
        self.assertIn("multi_trader_smoke_closed_market_dates_not_distinct", failing["blockers"])
        self.assertIn("multi_trader_smoke_add_watchlist_persistence_missing", failing["blockers"])
        self.assertIn("multi_trader_smoke_refresh_watchlist_not_restored", failing["blockers"])
        self.assertIn("multi_trader_smoke_refresh_snapshots_not_visible", failing["blockers"])
        self.assertIn("multi_trader_smoke_process_restart_first_screen_missing", failing["blockers"])
        self.assertIn("multi_trader_smoke_closed_market_source_dates_missing", failing["blockers"])
        self.assertIn("multi_trader_smoke_warm_snapshot_p95_exceeded", failing["blockers"])
        self.assertIn("multi_trader_smoke_duplicate_hydrations_present", failing["blockers"])
        self.assertIn("multi_trader_smoke_runtime_health_evidence_missing", failing["blockers"])

    def test_writes_multi_trader_smoke_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            observation_path = root / "multi-trader-smoke-observation.json"
            output_path = root / "multi-trader-smoke-evidence.json"
            observation_path.write_text(json.dumps(multi_trader_smoke_payload()), encoding="utf-8")

            result = write_multi_trader_smoke_evidence(
                observation_path=observation_path,
                output_path=output_path,
            )
            persisted = json.loads(output_path.read_text(encoding="utf-8"))

        self.assertTrue(result["passed"])
        self.assertEqual(persisted["client_count"], 2)

    def test_legacy_retirement_requires_cutover_gate_and_execution_evidence(self) -> None:
        readiness = evaluate_cutover_readiness(
            [passing_report()],
            policy=CutoverPolicy(min_session_duration_seconds=60),
        )
        result = evaluate_legacy_retirement(
            readiness,
            LegacyRetirementEvidence(
                frontend_default_v2_deployed=True,
                legacy_websocket_disabled=True,
                old_topic_consumers_disabled=True,
                no_legacy_consumers_observed=True,
                rollback_window_completed=True,
                rollback_window_started_at="2026-05-22T14:00:00+08:00",
                rollback_window_completed_at="2026-05-22T16:00:00+08:00",
                operator_approved=True,
                operator_approved_at="2026-05-22T16:05:00+08:00",
                notes="legacy /ws and old topic consumers removed from process config",
            ),
        )

        self.assertTrue(result["legacy_retired"])
        self.assertEqual(result["blockers"], [])
        self.assertEqual(result["cutover_readiness"]["accepted_report_ids"], ["session-1"])

        early_approval = evaluate_legacy_retirement(
            readiness,
            LegacyRetirementEvidence(
                frontend_default_v2_deployed=True,
                legacy_websocket_disabled=True,
                old_topic_consumers_disabled=True,
                no_legacy_consumers_observed=True,
                rollback_window_completed=True,
                rollback_window_started_at="2026-05-22T14:00:00+08:00",
                rollback_window_completed_at="2026-05-22T16:00:00+08:00",
                operator_approved=True,
                operator_approved_at="2026-05-22T15:59:00+08:00",
            ),
        )
        self.assertFalse(early_approval["legacy_retired"])
        self.assertIn("operator_approved_before_rollback_window_completed", early_approval["blockers"])

    def test_legacy_retirement_evidence_can_be_composed_from_frontend_and_decommission_artifacts(self) -> None:
        evidence = legacy_retirement_evidence_from_artifacts(
            frontend_deployment={"frontend_default_v2_deployed": True},
            legacy_decommission={
                "legacy_websocket_disabled": True,
                "old_topic_consumers_disabled": True,
                "no_legacy_consumers_observed": True,
            },
            rollback_window_completed=True,
            rollback_window_started_at="2026-05-22T14:00:00+08:00",
            rollback_window_completed_at="2026-05-22T16:00:00+08:00",
            operator_approved=True,
            operator_approved_at="2026-05-22T16:05:00+08:00",
            notes="verified from evidence artifacts",
        )

        self.assertTrue(evidence.frontend_default_v2_deployed)
        self.assertTrue(evidence.legacy_websocket_disabled)
        self.assertEqual(evidence.rollback_window_completed_at, "2026-05-22T16:00:00+08:00")
        self.assertEqual(evidence.operator_approved_at, "2026-05-22T16:05:00+08:00")
        self.assertEqual(evidence.notes, "verified from evidence artifacts")

    def test_legacy_retirement_blocks_without_execution_evidence(self) -> None:
        readiness = evaluate_cutover_readiness(
            [passing_report()],
            policy=CutoverPolicy(min_session_duration_seconds=60),
        )

        result = evaluate_legacy_retirement(readiness, LegacyRetirementEvidence())

        self.assertFalse(result["legacy_retired"])
        self.assertIn("frontend_default_v2_not_deployed", result["blockers"])
        self.assertIn("legacy_websocket_still_enabled", result["blockers"])
        self.assertIn("old_topic_consumers_still_enabled", result["blockers"])
        self.assertIn("rollback_window_started_at_missing", result["blockers"])
        self.assertIn("rollback_window_completed_at_missing", result["blockers"])
        self.assertIn("operator_approval_missing", result["blockers"])
        self.assertIn("operator_approved_at_missing", result["blockers"])

    def test_writes_legacy_retirement_evidence_artifact(self) -> None:
        readiness = evaluate_cutover_readiness(
            [passing_report()],
            policy=CutoverPolicy(min_session_duration_seconds=60),
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "legacy-retirement.json"

            result = write_legacy_retirement_evidence(
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
                    operator_approved_at="2026-05-22T16:05:00+08:00",
                ),
                output_path=output_path,
            )

            self.assertTrue(result["passed"])
            self.assertTrue(json.loads(output_path.read_text(encoding="utf-8"))["legacy_retired"])

    def test_writes_legacy_retirement_from_artifacts(self) -> None:
        readiness = evaluate_cutover_readiness(
            [passing_report()],
            policy=CutoverPolicy(min_session_duration_seconds=60),
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            readiness_path = root / "cutover-readiness.json"
            frontend_path = root / "frontend-deployment.json"
            decommission_path = root / "legacy-decommission.json"
            output_path = root / "legacy-retirement.json"
            readiness_path.write_text(json.dumps(readiness), encoding="utf-8")
            frontend_path.write_text(
                json.dumps({"frontend_default_v2_deployed": True}),
                encoding="utf-8",
            )
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

            result = write_legacy_retirement_from_artifacts(
                readiness_path=readiness_path,
                frontend_deployment_path=frontend_path,
                legacy_decommission_path=decommission_path,
                rollback_window_completed=True,
                rollback_window_started_at="2026-05-22T14:00:00+08:00",
                rollback_window_completed_at="2026-05-22T16:00:00+08:00",
                operator_approved=True,
                operator_approved_at="2026-05-22T16:05:00+08:00",
                output_path=output_path,
                notes="artifact-composed",
            )
            blocked = write_legacy_retirement_from_artifacts(
                readiness_path=readiness_path,
                frontend_deployment_path=frontend_path,
                legacy_decommission_path=decommission_path,
                rollback_window_completed=False,
                rollback_window_started_at="2026-05-22T16:00:00+08:00",
                rollback_window_completed_at="2026-05-22T14:00:00+08:00",
                operator_approved=False,
                operator_approved_at="bad timestamp",
                output_path=root / "legacy-retirement-blocked.json",
            )

        self.assertTrue(result["legacy_retired"])
        self.assertEqual(result["evidence"]["notes"], "artifact-composed")
        self.assertFalse(blocked["legacy_retired"])
        self.assertIn("rollback_window_not_completed", blocked["blockers"])
        self.assertIn("rollback_window_completed_before_start", blocked["blockers"])
        self.assertIn("operator_approval_missing", blocked["blockers"])
        self.assertIn("operator_approved_at_invalid", blocked["blockers"])

    def test_evidence_bundle_requires_shadow_manifest_health_cutover_and_retirement_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            paths = write_complete_evidence_bundle(root)
            result = evaluate_evidence_bundle(paths)
            persisted = write_evidence_bundle_verification(
                paths=paths,
                output_path=root / "evidence-bundle.json",
            )
            smoke_path = root / "multi-trader-smoke-evidence.json"
            smoke_path.write_text(
                json.dumps(evaluate_multi_trader_smoke(multi_trader_smoke_payload())),
                encoding="utf-8",
            )
            observation_path = root / "multi-trader-smoke-observation.json"
            observation_path.write_text(json.dumps(multi_trader_smoke_payload()), encoding="utf-8")
            (root / "clients").mkdir()
            (root / "performance").mkdir()
            (root / "clients" / "client-a.json").write_text(json.dumps({"clients": []}), encoding="utf-8")
            (root / "performance" / "perf-a.json").write_text(
                json.dumps({"performance_samples": {"subscribe_snapshot_ms": [80]}}),
                encoding="utf-8",
            )
            preflight_path = root / "lan-preflight.json"
            preflight_path.write_text(json.dumps(multi_trader_smoke_preflight_payload()), encoding="utf-8")
            service_preflight_path = root / "service-preflight.json"
            service_preflight_path.write_text(json.dumps(multi_trader_smoke_service_checks_payload()), encoding="utf-8")
            (root / "smoke-import-manifest.json").write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "source_path": "/downloads",
                        "imported_count": 2,
                        "skipped_count": 0,
                        "imported": [
                            {"kind": "client", "input_path": "/downloads/client-a.json", "output_path": "clients/client-a.json"},
                            {"kind": "performance", "input_path": "/downloads/perf-a.json", "output_path": "performance/perf-a.json"},
                        ],
                        "skipped": [],
                        "runs": [
                            {
                                "source_path": "/downloads",
                                "imported_count": 2,
                                "skipped_count": 0,
                                "imported": [
                                    {"kind": "client", "input_path": "/downloads/client-a.json", "output_path": "clients/client-a.json"},
                                    {"kind": "performance", "input_path": "/downloads/perf-a.json", "output_path": "performance/perf-a.json"},
                                ],
                                "skipped": [],
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            manifest_path = write_smoke_run_manifest(
                root,
                smoke_path,
                preflight_path,
                service_preflight_path,
                observation_path,
                root / "smoke-import-manifest.json",
                root / "clients" / "client-a.json",
                root / "performance" / "perf-a.json",
            )
            package_path = root / "multi-trader-smoke-evidence.zip"
            package_files = [
                "multi-trader-smoke-evidence.json",
                "multi-trader-smoke-observation.json",
                "lan-preflight.json",
                "service-preflight.json",
                "smoke-run-manifest.json",
                "smoke-import-manifest.json",
                "clients/client-a.json",
                "performance/perf-a.json",
            ]
            with zipfile.ZipFile(package_path, mode="w") as archive:
                for name in package_files:
                    archive.write(root / name, name)
            package_bytes = package_path.read_bytes()
            package_metadata_path = root / "smoke-run-package.json"
            package_metadata_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "package_path": str(package_path),
                        "bytes": len(package_bytes),
                        "sha256": hashlib.sha256(package_bytes).hexdigest(),
                        "file_count": len(package_files),
                        "files": package_files,
                    }
                ),
                encoding="utf-8",
            )
            smoke_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                    runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                    multi_trader_smoke_path=smoke_path,
                    multi_trader_smoke_preflight_path=preflight_path,
                    multi_trader_smoke_manifest_path=manifest_path,
                )
            )
            package_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                    runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                    multi_trader_smoke_path=smoke_path,
                    multi_trader_smoke_preflight_path=preflight_path,
                    multi_trader_smoke_manifest_path=manifest_path,
                    multi_trader_smoke_package_path=package_path,
                    multi_trader_smoke_package_metadata_path=package_metadata_path,
                )
            )
            missing_service_package_path = root / "missing-service-preflight-smoke.zip"
            missing_service_package_files = [name for name in package_files if name != "service-preflight.json"]
            with zipfile.ZipFile(missing_service_package_path, mode="w") as archive:
                for name in missing_service_package_files:
                    archive.write(root / name, name)
            missing_service_package_bytes = missing_service_package_path.read_bytes()
            missing_service_package_metadata_path = root / "missing-service-smoke-run-package.json"
            missing_service_package_metadata_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "package_path": str(missing_service_package_path),
                        "bytes": len(missing_service_package_bytes),
                        "sha256": hashlib.sha256(missing_service_package_bytes).hexdigest(),
                        "file_count": len(missing_service_package_files),
                        "files": missing_service_package_files,
                    }
                ),
                encoding="utf-8",
            )
            missing_service_package_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                    runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                    multi_trader_smoke_path=smoke_path,
                    multi_trader_smoke_preflight_path=preflight_path,
                    multi_trader_smoke_manifest_path=manifest_path,
                    multi_trader_smoke_package_path=missing_service_package_path,
                    multi_trader_smoke_package_metadata_path=missing_service_package_metadata_path,
                )
            )
            mismatched_service_preflight_path = root / "mismatched-service-preflight.json"
            mismatched_service_preflight = multi_trader_smoke_service_checks_payload()
            mismatched_service_preflight["checked_at"] = "2026-05-25T11:05:00+08:00"
            mismatched_service_preflight_path.write_text(json.dumps(mismatched_service_preflight), encoding="utf-8")
            mismatched_service_package_path = root / "mismatched-service-preflight-smoke.zip"
            with zipfile.ZipFile(mismatched_service_package_path, mode="w") as archive:
                for name in package_files:
                    if name == "service-preflight.json":
                        archive.write(mismatched_service_preflight_path, name)
                    else:
                        archive.write(root / name, name)
            mismatched_service_package_bytes = mismatched_service_package_path.read_bytes()
            mismatched_service_package_metadata_path = root / "mismatched-service-smoke-run-package.json"
            mismatched_service_package_metadata_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "package_path": str(mismatched_service_package_path),
                        "bytes": len(mismatched_service_package_bytes),
                        "sha256": hashlib.sha256(mismatched_service_package_bytes).hexdigest(),
                        "file_count": len(package_files),
                        "files": package_files,
                    }
                ),
                encoding="utf-8",
            )
            mismatched_service_package_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                    runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                    multi_trader_smoke_path=smoke_path,
                    multi_trader_smoke_preflight_path=preflight_path,
                    multi_trader_smoke_manifest_path=manifest_path,
                    multi_trader_smoke_package_path=mismatched_service_package_path,
                    multi_trader_smoke_package_metadata_path=mismatched_service_package_metadata_path,
                )
            )
            stale_service_preflight = multi_trader_smoke_service_checks_payload()
            stale_service_preflight["checked_at"] = "2026-05-25T10:00:00+08:00"
            stale_preflight = multi_trader_smoke_preflight_payload()
            stale_preflight["service_checks"] = stale_service_preflight
            stale_preflight_path = root / "stale-lan-preflight.json"
            stale_service_preflight_path = root / "stale-service-preflight.json"
            stale_preflight_path.write_text(json.dumps(stale_preflight), encoding="utf-8")
            stale_service_preflight_path.write_text(json.dumps(stale_service_preflight), encoding="utf-8")
            stale_service_package_path = root / "stale-service-preflight-smoke.zip"
            with zipfile.ZipFile(stale_service_package_path, mode="w") as archive:
                for name in package_files:
                    if name == "lan-preflight.json":
                        archive.write(stale_preflight_path, name)
                    elif name == "service-preflight.json":
                        archive.write(stale_service_preflight_path, name)
                    else:
                        archive.write(root / name, name)
            stale_service_package_bytes = stale_service_package_path.read_bytes()
            stale_service_package_metadata_path = root / "stale-service-smoke-run-package.json"
            stale_service_package_metadata_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "package_path": str(stale_service_package_path),
                        "bytes": len(stale_service_package_bytes),
                        "sha256": hashlib.sha256(stale_service_package_bytes).hexdigest(),
                        "file_count": len(package_files),
                        "files": package_files,
                    }
                ),
                encoding="utf-8",
            )
            stale_service_package_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                    runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                    multi_trader_smoke_path=smoke_path,
                    multi_trader_smoke_preflight_path=preflight_path,
                    multi_trader_smoke_manifest_path=manifest_path,
                    multi_trader_smoke_package_path=stale_service_package_path,
                    multi_trader_smoke_package_metadata_path=stale_service_package_metadata_path,
                )
            )
            extra_json_path = root / "unexpected-extra.json"
            extra_json_path.write_text(json.dumps({"unexpected": True}), encoding="utf-8")
            unmanifested_package_path = root / "unmanifested-smoke-evidence.zip"
            unmanifested_package_files = [*package_files, "unexpected-extra.json"]
            with zipfile.ZipFile(unmanifested_package_path, mode="w") as archive:
                for name in unmanifested_package_files:
                    archive.write(root / name, name)
            unmanifested_package_bytes = unmanifested_package_path.read_bytes()
            unmanifested_package_metadata_path = root / "unmanifested-smoke-run-package.json"
            unmanifested_package_metadata_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "package_path": str(unmanifested_package_path),
                        "bytes": len(unmanifested_package_bytes),
                        "sha256": hashlib.sha256(unmanifested_package_bytes).hexdigest(),
                        "file_count": len(unmanifested_package_files),
                        "files": unmanifested_package_files,
                    }
                ),
                encoding="utf-8",
            )
            unmanifested_package_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                    runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                    multi_trader_smoke_path=smoke_path,
                    multi_trader_smoke_preflight_path=preflight_path,
                    multi_trader_smoke_manifest_path=manifest_path,
                    multi_trader_smoke_package_path=unmanifested_package_path,
                    multi_trader_smoke_package_metadata_path=unmanifested_package_metadata_path,
                )
            )
            package_without_external_smoke_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                    runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                    multi_trader_smoke_package_path=package_path,
                    multi_trader_smoke_package_metadata_path=package_metadata_path,
                )
            )
            stale_package_metadata_path = root / "stale-smoke-run-package.json"
            stale_package = json.loads(package_metadata_path.read_text(encoding="utf-8"))
            stale_package["sha256"] = "0" * 64
            stale_package_metadata_path.write_text(json.dumps(stale_package), encoding="utf-8")
            stale_package_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                    runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                    multi_trader_smoke_path=smoke_path,
                    multi_trader_smoke_preflight_path=preflight_path,
                    multi_trader_smoke_manifest_path=manifest_path,
                    multi_trader_smoke_package_path=package_path,
                    multi_trader_smoke_package_metadata_path=stale_package_metadata_path,
                )
            )
            no_import_manifest_package_path = root / "no-import-manifest-smoke.zip"
            no_import_manifest_files = [
                "multi-trader-smoke-evidence.json",
                "lan-preflight.json",
                "service-preflight.json",
                "smoke-run-manifest.json",
            ]
            with zipfile.ZipFile(no_import_manifest_package_path, mode="w") as archive:
                for name in no_import_manifest_files:
                    archive.write(root / name, name)
            no_import_manifest_bytes = no_import_manifest_package_path.read_bytes()
            no_import_manifest_metadata_path = root / "no-import-smoke-run-package.json"
            no_import_manifest_metadata_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "package_path": str(no_import_manifest_package_path),
                        "bytes": len(no_import_manifest_bytes),
                        "sha256": hashlib.sha256(no_import_manifest_bytes).hexdigest(),
                        "file_count": len(no_import_manifest_files),
                        "files": no_import_manifest_files,
                    }
                ),
                encoding="utf-8",
            )
            no_import_manifest_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                    runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                    multi_trader_smoke_path=smoke_path,
                    multi_trader_smoke_preflight_path=preflight_path,
                    multi_trader_smoke_manifest_path=manifest_path,
                    multi_trader_smoke_package_path=no_import_manifest_package_path,
                    multi_trader_smoke_package_metadata_path=no_import_manifest_metadata_path,
                )
            )
            invalid_import_manifest_package_path = root / "invalid-import-manifest-smoke.zip"
            invalid_import_manifest_path = root / "invalid-smoke-import-manifest.json"
            invalid_import_manifest_path.write_text(json.dumps({"schema_version": 1, "runs": []}), encoding="utf-8")
            with zipfile.ZipFile(invalid_import_manifest_package_path, mode="w") as archive:
                for name in no_import_manifest_files:
                    archive.write(root / name, name)
                archive.write(invalid_import_manifest_path, "smoke-import-manifest.json")
            invalid_import_manifest_bytes = invalid_import_manifest_package_path.read_bytes()
            invalid_import_manifest_metadata_path = root / "invalid-import-smoke-run-package.json"
            invalid_import_manifest_files = [*no_import_manifest_files, "smoke-import-manifest.json"]
            invalid_import_manifest_metadata_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "package_path": str(invalid_import_manifest_package_path),
                        "bytes": len(invalid_import_manifest_bytes),
                        "sha256": hashlib.sha256(invalid_import_manifest_bytes).hexdigest(),
                        "file_count": len(invalid_import_manifest_files),
                        "files": invalid_import_manifest_files,
                    }
                ),
                encoding="utf-8",
            )
            invalid_import_manifest_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                    runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                    multi_trader_smoke_path=smoke_path,
                    multi_trader_smoke_preflight_path=preflight_path,
                    multi_trader_smoke_manifest_path=manifest_path,
                    multi_trader_smoke_package_path=invalid_import_manifest_package_path,
                    multi_trader_smoke_package_metadata_path=invalid_import_manifest_metadata_path,
                )
            )
            missing_import_output_package_path = root / "missing-import-output-smoke.zip"
            missing_import_output_files = [
                "multi-trader-smoke-evidence.json",
                "lan-preflight.json",
                "service-preflight.json",
                "smoke-run-manifest.json",
                "smoke-import-manifest.json",
            ]
            with zipfile.ZipFile(missing_import_output_package_path, mode="w") as archive:
                for name in missing_import_output_files:
                    archive.write(root / name, name)
            missing_import_output_bytes = missing_import_output_package_path.read_bytes()
            missing_import_output_metadata_path = root / "missing-import-output-smoke-run-package.json"
            missing_import_output_metadata_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "package_path": str(missing_import_output_package_path),
                        "bytes": len(missing_import_output_bytes),
                        "sha256": hashlib.sha256(missing_import_output_bytes).hexdigest(),
                        "file_count": len(missing_import_output_files),
                        "files": missing_import_output_files,
                    }
                ),
                encoding="utf-8",
            )
            missing_import_output_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                    runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                    multi_trader_smoke_path=smoke_path,
                    multi_trader_smoke_preflight_path=preflight_path,
                    multi_trader_smoke_manifest_path=manifest_path,
                    multi_trader_smoke_package_path=missing_import_output_package_path,
                    multi_trader_smoke_package_metadata_path=missing_import_output_metadata_path,
                )
            )
            unimported_client_path = root / "clients" / "client-b.json"
            unimported_client_path.write_text(json.dumps({"clients": []}), encoding="utf-8")
            unimported_client_package_path = root / "unimported-client-smoke.zip"
            unimported_client_package_files = [*package_files, "clients/client-b.json"]
            with zipfile.ZipFile(unimported_client_package_path, mode="w") as archive:
                for name in unimported_client_package_files:
                    archive.write(root / name, name)
            unimported_client_package_bytes = unimported_client_package_path.read_bytes()
            unimported_client_metadata_path = root / "unimported-client-smoke-run-package.json"
            unimported_client_metadata_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "package_path": str(unimported_client_package_path),
                        "bytes": len(unimported_client_package_bytes),
                        "sha256": hashlib.sha256(unimported_client_package_bytes).hexdigest(),
                        "file_count": len(unimported_client_package_files),
                        "files": unimported_client_package_files,
                    }
                ),
                encoding="utf-8",
            )
            unimported_client_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                    runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                    multi_trader_smoke_path=smoke_path,
                    multi_trader_smoke_preflight_path=preflight_path,
                    multi_trader_smoke_manifest_path=manifest_path,
                    multi_trader_smoke_package_path=unimported_client_package_path,
                    multi_trader_smoke_package_metadata_path=unimported_client_metadata_path,
                )
            )
            mismatched_package_evidence_path = root / "mismatched-package-smoke-evidence.json"
            mismatched_package_evidence = evaluate_multi_trader_smoke(multi_trader_smoke_payload())
            mismatched_package_evidence["client_count"] = 99
            mismatched_package_evidence_path.write_text(json.dumps(mismatched_package_evidence), encoding="utf-8")
            mismatched_package_path = root / "mismatched-smoke-evidence.zip"
            with zipfile.ZipFile(mismatched_package_path, mode="w") as archive:
                archive.write(mismatched_package_evidence_path, "multi-trader-smoke-evidence.json")
                for name in package_files:
                    if name != "multi-trader-smoke-evidence.json":
                        archive.write(root / name, name)
            mismatched_package_bytes = mismatched_package_path.read_bytes()
            mismatched_package_metadata_path = root / "mismatched-smoke-run-package.json"
            mismatched_package_metadata_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "package_path": str(mismatched_package_path),
                        "bytes": len(mismatched_package_bytes),
                        "sha256": hashlib.sha256(mismatched_package_bytes).hexdigest(),
                        "file_count": len(package_files),
                        "files": package_files,
                    }
                ),
                encoding="utf-8",
            )
            mismatched_package_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                    runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                    multi_trader_smoke_path=smoke_path,
                    multi_trader_smoke_preflight_path=preflight_path,
                    multi_trader_smoke_manifest_path=manifest_path,
                    multi_trader_smoke_package_path=mismatched_package_path,
                    multi_trader_smoke_package_metadata_path=mismatched_package_metadata_path,
                )
            )
            mismatched_observation_path = root / "mismatched-package-observation.json"
            mismatched_observation = multi_trader_smoke_payload()
            mismatched_observation["observed_at"] = "2026-05-25T12:00:00+08:00"
            mismatched_observation_path.write_text(json.dumps(mismatched_observation), encoding="utf-8")
            mismatched_observation_package_path = root / "mismatched-observation-smoke-evidence.zip"
            with zipfile.ZipFile(mismatched_observation_package_path, mode="w") as archive:
                for name in package_files:
                    if name == "multi-trader-smoke-observation.json":
                        archive.write(mismatched_observation_path, name)
                    else:
                        archive.write(root / name, name)
            mismatched_observation_package_bytes = mismatched_observation_package_path.read_bytes()
            mismatched_observation_metadata_path = root / "mismatched-observation-smoke-run-package.json"
            mismatched_observation_metadata_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "package_path": str(mismatched_observation_package_path),
                        "bytes": len(mismatched_observation_package_bytes),
                        "sha256": hashlib.sha256(mismatched_observation_package_bytes).hexdigest(),
                        "file_count": len(package_files),
                        "files": package_files,
                    }
                ),
                encoding="utf-8",
            )
            mismatched_observation_package_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                    runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                    multi_trader_smoke_path=smoke_path,
                    multi_trader_smoke_preflight_path=preflight_path,
                    multi_trader_smoke_manifest_path=manifest_path,
                    multi_trader_smoke_package_path=mismatched_observation_package_path,
                    multi_trader_smoke_package_metadata_path=mismatched_observation_metadata_path,
                )
            )
            missing_preflight_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                    runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                    multi_trader_smoke_path=smoke_path,
                    multi_trader_smoke_manifest_path=manifest_path,
                )
            )
            missing_manifest_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                    runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                    multi_trader_smoke_path=smoke_path,
                    multi_trader_smoke_preflight_path=preflight_path,
                )
            )
            stale_manifest_path = root / "stale-smoke-run-manifest.json"
            stale_manifest_path.write_text(manifest_path.read_text(encoding="utf-8"), encoding="utf-8")
            smoke_path.write_text(json.dumps({**json.loads(smoke_path.read_text(encoding="utf-8")), "client_count": 3}), encoding="utf-8")
            stale_manifest_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                    runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                    multi_trader_smoke_path=smoke_path,
                    multi_trader_smoke_preflight_path=preflight_path,
                    multi_trader_smoke_manifest_path=stale_manifest_path,
                )
            )
            smoke_path.write_text(
                json.dumps(evaluate_multi_trader_smoke(multi_trader_smoke_payload())),
                encoding="utf-8",
            )
            mismatched_preflight_path = root / "mismatched-lan-preflight.json"
            mismatched_preflight = multi_trader_smoke_preflight_payload()
            mismatched_preflight["gateway_url"] = "ws://other-gateway.internal:9020/ws"
            mismatched_preflight_path.write_text(json.dumps(mismatched_preflight), encoding="utf-8")
            mismatched_preflight_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                    runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                    multi_trader_smoke_path=smoke_path,
                    multi_trader_smoke_preflight_path=mismatched_preflight_path,
                    multi_trader_smoke_manifest_path=manifest_path,
                )
            )
            failing_smoke_path = root / "failing-multi-trader-smoke-evidence.json"
            failing_smoke = evaluate_multi_trader_smoke(multi_trader_smoke_payload())
            failing_smoke["passed"] = False
            failing_smoke["blockers"] = ["multi_trader_smoke_runtime_health_evidence_missing"]
            failing_smoke_path.write_text(json.dumps(failing_smoke), encoding="utf-8")
            failing_smoke_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                    runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                    multi_trader_smoke_path=failing_smoke_path,
                    multi_trader_smoke_preflight_path=preflight_path,
                    multi_trader_smoke_manifest_path=manifest_path,
                )
            )
            missing_runtime_config_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=root / "missing-runtime-config.json",
                    runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                )
            )
            mismatched_runtime_config_path = root / "mismatched-runtime-config.json"
            mismatched_runtime_config = runtime_config_payload(root)
            mismatched_runtime_config["trade_date"] = "20260523"
            mismatched_runtime_config_path.write_text(json.dumps(mismatched_runtime_config), encoding="utf-8")
            mismatched_runtime_config_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=mismatched_runtime_config_path,
                    runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                )
            )
            malformed_shadow_reports = root / "malformed-shadow-reports"
            malformed_shadow_reports.mkdir()
            malformed_report = passing_report()
            malformed_report["performance"] = {"passed": True}
            save_shadow_run_report(malformed_report, malformed_shadow_reports)
            malformed_shadow_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=malformed_shadow_reports,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                )
            )
            mismatched_source_reports = root / "mismatched-source-shadow-reports"
            mismatched_source_reports.mkdir()
            mismatched_source_report = passing_report()
            write_shadow_source_files(root / "mismatched-source-streams", mismatched_source_report)
            v2_source_path = Path(mismatched_source_report["evidence_source"]["files"]["v2_events"])
            v2_source_path.write_text(
                json.dumps(shadow_source_event("v2-1", seq=1)) + "\n",
                encoding="utf-8",
            )
            save_shadow_run_report(mismatched_source_report, mismatched_source_reports)
            mismatched_source_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=mismatched_source_reports,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                )
            )
            invalid_source_reports = root / "invalid-source-shadow-reports"
            invalid_source_reports.mkdir()
            invalid_source_report = passing_report()
            write_shadow_source_files(root / "invalid-source-streams", invalid_source_report)
            legacy_source_path = Path(invalid_source_report["evidence_source"]["files"]["legacy_events"])
            legacy_source_path.write_text(
                json.dumps({**shadow_source_event("legacy-1", seq=1), "symbol": "700"}) + "\n",
                encoding="utf-8",
            )
            save_shadow_run_report(invalid_source_report, invalid_source_reports)
            invalid_source_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=invalid_source_reports,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                )
            )

            empty_manifest_paths = EvidenceBundlePaths(
                shadow_reports_directory=paths.shadow_reports_directory,
                manifest_directory=root / "empty-manifests",
                runtime_config_path=paths.runtime_config_path,
                runtime_health_path=paths.runtime_health_path,
                readiness_path=paths.readiness_path,
                frontend_deployment_path=paths.frontend_deployment_path,
                legacy_decommission_path=paths.legacy_decommission_path,
                legacy_retirement_path=paths.legacy_retirement_path,
            )
            blocked = evaluate_evidence_bundle(empty_manifest_paths)
            incomplete_manifest_root = root / "incomplete-manifests"
            incomplete_manifest_root.mkdir()
            write_manifest(incomplete_manifest_root, "daily_bars")
            incomplete_manifest_paths = EvidenceBundlePaths(
                shadow_reports_directory=paths.shadow_reports_directory,
                manifest_directory=incomplete_manifest_root,
                runtime_config_path=paths.runtime_config_path,
                runtime_health_path=paths.runtime_health_path,
                readiness_path=paths.readiness_path,
                frontend_deployment_path=paths.frontend_deployment_path,
                legacy_decommission_path=paths.legacy_decommission_path,
                legacy_retirement_path=paths.legacy_retirement_path,
            )
            incomplete_manifest_result = evaluate_evidence_bundle(incomplete_manifest_paths)
            malformed_manifest_root = root / "malformed-manifests"
            malformed_manifest_root.mkdir()
            write_required_manifests(malformed_manifest_root)
            (malformed_manifest_root / "daily_bars.20260522-20260522.v2.manifest.json").write_text(
                json.dumps({"schema_version": 1, "data_type": "daily_bars", "quality_checks": {"passed": True}}),
                encoding="utf-8",
            )
            malformed_manifest_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=malformed_manifest_root,
                    runtime_config_path=paths.runtime_config_path,
                runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                )
            )
            bad_health = json.loads(paths.runtime_health_path.read_text(encoding="utf-8"))
            bad_health["topics"]["raw_market_events_v1"]["lag"] = 3
            bad_health["health"]["collector"]["symbol_freshness"]["00700.HK"]["degraded"] = True
            bad_health["redis_snapshot"]["missing_symbols"] = ["00700.HK"]
            (root / "bad-runtime-health.json").write_text(json.dumps(bad_health), encoding="utf-8")
            missing_processed_topic_health = json.loads(paths.runtime_health_path.read_text(encoding="utf-8"))
            del missing_processed_topic_health["topics"]["processed_market_events_v1"]
            (root / "missing-processed-topic-health.json").write_text(
                json.dumps(missing_processed_topic_health),
                encoding="utf-8",
            )
            bad_health_paths = EvidenceBundlePaths(
                shadow_reports_directory=paths.shadow_reports_directory,
                manifest_directory=paths.manifest_directory,
                runtime_config_path=paths.runtime_config_path,
                runtime_health_path=root / "bad-runtime-health.json",
                readiness_path=paths.readiness_path,
                frontend_deployment_path=paths.frontend_deployment_path,
                legacy_decommission_path=paths.legacy_decommission_path,
                legacy_retirement_path=paths.legacy_retirement_path,
            )
            bad_health_result = evaluate_evidence_bundle(bad_health_paths)
            missing_processed_topic_paths = EvidenceBundlePaths(
                shadow_reports_directory=paths.shadow_reports_directory,
                manifest_directory=paths.manifest_directory,
                runtime_config_path=paths.runtime_config_path,
                runtime_health_path=root / "missing-processed-topic-health.json",
                readiness_path=paths.readiness_path,
                frontend_deployment_path=paths.frontend_deployment_path,
                legacy_decommission_path=paths.legacy_decommission_path,
                legacy_retirement_path=paths.legacy_retirement_path,
            )
            missing_processed_topic_result = evaluate_evidence_bundle(missing_processed_topic_paths)
            mismatched_runtime_date = json.loads(paths.runtime_health_path.read_text(encoding="utf-8"))
            mismatched_runtime_date["trade_date"] = "20260523"
            mismatched_runtime_date["redis_snapshot"]["trade_date"] = "20260523"
            (root / "mismatched-runtime-date-health.json").write_text(
                json.dumps(mismatched_runtime_date),
                encoding="utf-8",
            )
            mismatched_runtime_date_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                runtime_health_path=root / "mismatched-runtime-date-health.json",
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                )
            )
            early_runtime_window_path = root / "early-runtime-window-health.json"
            early_runtime_window_path.write_text(json.dumps(runtime_health_payload()), encoding="utf-8")
            early_runtime_window_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                runtime_health_path=early_runtime_window_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                )
            )
            mismatched_manifest_root = root / "mismatched-date-manifests"
            mismatched_manifest_root.mkdir()
            write_required_manifests(mismatched_manifest_root, start="20260523", end="20260523")
            mismatched_manifest_date_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=mismatched_manifest_root,
                    runtime_config_path=paths.runtime_config_path,
                runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                )
            )
            mismatched_manifest_symbols_root = root / "mismatched-symbol-manifests"
            mismatched_manifest_symbols_root.mkdir()
            write_required_manifests(mismatched_manifest_symbols_root, symbols=["00939.HK"])
            mismatched_manifest_symbols_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=mismatched_manifest_symbols_root,
                    runtime_config_path=paths.runtime_config_path,
                runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                )
            )
            runtime_extra_symbol = json.loads(paths.runtime_health_path.read_text(encoding="utf-8"))
            runtime_extra_symbol["subscription"]["subscribed_symbols"] = ["00700.HK", "00939.HK"]
            runtime_extra_symbol["redis_snapshot"]["checked_symbols"] = ["00700.HK", "00939.HK"]
            runtime_extra_symbol["redis_snapshot"]["present_symbols"] = ["00700.HK", "00939.HK"]
            runtime_extra_symbol["gateway_activity"]["delivered_terminal_symbols"] = ["00700.HK", "00939.HK"]
            runtime_extra_symbol["health"]["collector"]["symbol_freshness"]["00939.HK"] = {
                "subscribed": True,
                "degraded": False,
                "latest_event_at": "2026-05-22T09:30:00+08:00",
            }
            (root / "runtime-extra-shadow-symbol-health.json").write_text(
                json.dumps(runtime_extra_symbol),
                encoding="utf-8",
            )
            shadow_missing_symbol_manifest_root = root / "shadow-missing-symbol-manifests"
            shadow_missing_symbol_manifest_root.mkdir()
            write_required_manifests(shadow_missing_symbol_manifest_root, symbols=["00700.HK", "00939.HK"])
            shadow_missing_runtime_symbol_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=shadow_missing_symbol_manifest_root,
                    runtime_config_path=paths.runtime_config_path,
                runtime_health_path=root / "runtime-extra-shadow-symbol-health.json",
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                )
            )
            unlinked_readiness_path = root / "unlinked-cutover-readiness.json"
            unlinked_readiness_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "passed": True,
                        "frontend_default_v2_allowed": True,
                        "legacy_retirement_allowed": True,
                        "blockers": [],
                        "legacy_retirement_blockers": [],
                        "report_count": 1,
                        "accepted_report_ids": ["missing-session"],
                        "rejected_reports": [],
                        "policy": default_cutover_policy_payload(),
                    }
                ),
                encoding="utf-8",
            )
            unlinked_readiness_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                runtime_health_path=paths.runtime_health_path,
                    readiness_path=unlinked_readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                )
            )
            weak_readiness_path = root / "weak-cutover-readiness.json"
            weak_readiness_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "passed": True,
                        "frontend_default_v2_allowed": True,
                        "legacy_retirement_allowed": True,
                        "accepted_report_ids": ["session-1"],
                    }
                ),
                encoding="utf-8",
            )
            weak_readiness_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                runtime_health_path=paths.runtime_health_path,
                    readiness_path=weak_readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                )
            )
            mismatched_frontend_path = root / "mismatched-frontend-deployment.json"
            mismatched_frontend_path.write_text(
                json.dumps(
                    {
                        "frontend_default_v2_deployed": True,
                        "deployed_env": {
                            "VITE_MARKET_PROTOCOL": "terminal-message-v1",
                            "VITE_MARKET_CUTOVER_READINESS": {
                                "schema_version": 1,
                                "passed": True,
                                "frontend_default_v2_allowed": True,
                                "accepted_report_ids": ["other-session"],
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            mismatched_frontend_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=mismatched_frontend_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                )
            )
            weak_frontend_artifact_path = root / "weak-frontend-deployment.json"
            weak_frontend_artifact = json.loads(paths.frontend_deployment_path.read_text(encoding="utf-8"))
            weak_frontend_artifact["schema_version"] = 2
            weak_frontend_artifact["passed"] = False
            weak_frontend_artifact_path.write_text(json.dumps(weak_frontend_artifact), encoding="utf-8")
            weak_frontend_artifact_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=weak_frontend_artifact_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                )
            )
            mismatched_protocol_frontend_path = root / "mismatched-protocol-frontend-deployment.json"
            mismatched_protocol_frontend = json.loads(paths.frontend_deployment_path.read_text(encoding="utf-8"))
            mismatched_protocol_frontend["deployed_env"]["VITE_MARKET_PROTOCOL"] = "legacy-message"
            mismatched_protocol_frontend_path.write_text(json.dumps(mismatched_protocol_frontend), encoding="utf-8")
            mismatched_protocol_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=mismatched_protocol_frontend_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                )
            )
            mock_mode_frontend_path = root / "mock-mode-frontend-deployment.json"
            mock_mode_frontend = json.loads(paths.frontend_deployment_path.read_text(encoding="utf-8"))
            mock_mode_frontend["deployed_env"]["VITE_MARKET_DATA_MODE"] = "mock"
            mock_mode_frontend_path.write_text(json.dumps(mock_mode_frontend), encoding="utf-8")
            mock_mode_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=mock_mode_frontend_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                )
            )
            early_frontend_verified_path = root / "early-frontend-verified-deployment.json"
            early_frontend_verified = json.loads(paths.frontend_deployment_path.read_text(encoding="utf-8"))
            early_frontend_verified["verified_at"] = "2026-05-22T09:30:02+08:00"
            early_frontend_verified_path.write_text(json.dumps(early_frontend_verified), encoding="utf-8")
            early_frontend_verified_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=early_frontend_verified_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                )
            )
            mismatched_live_url_path = root / "mismatched-live-url-frontend-deployment.json"
            mismatched_live_url = json.loads(paths.frontend_deployment_path.read_text(encoding="utf-8"))
            mismatched_live_url["expected_live_url"] = "ws://gateway.internal:9020/legacy"
            mismatched_live_url["deployed_env"]["VITE_MARKET_WS_URL"] = "ws://gateway.internal:9020/legacy"
            mismatched_live_url_path.write_text(json.dumps(mismatched_live_url), encoding="utf-8")
            mismatched_live_url_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=mismatched_live_url_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                )
            )
            mismatched_live_port_path = root / "mismatched-live-port-frontend-deployment.json"
            mismatched_live_port = json.loads(paths.frontend_deployment_path.read_text(encoding="utf-8"))
            mismatched_live_port["expected_live_url"] = "ws://gateway.internal:9999/ws"
            mismatched_live_port["deployed_env"]["VITE_MARKET_WS_URL"] = "ws://gateway.internal:9999/ws"
            mismatched_live_port_path.write_text(json.dumps(mismatched_live_port), encoding="utf-8")
            mismatched_live_port_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=mismatched_live_port_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                )
            )
            missing_live_port_path = root / "missing-live-port-frontend-deployment.json"
            missing_live_port = json.loads(paths.frontend_deployment_path.read_text(encoding="utf-8"))
            missing_live_port["expected_live_url"] = "ws://gateway.internal/ws"
            missing_live_port["deployed_env"]["VITE_MARKET_WS_URL"] = "ws://gateway.internal/ws"
            missing_live_port_path.write_text(json.dumps(missing_live_port), encoding="utf-8")
            missing_live_port_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=missing_live_port_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                )
            )
            invalid_live_scheme_path = root / "invalid-live-scheme-frontend-deployment.json"
            invalid_live_scheme = json.loads(paths.frontend_deployment_path.read_text(encoding="utf-8"))
            invalid_live_scheme["expected_live_url"] = "http://gateway.internal:9020/ws"
            invalid_live_scheme["deployed_env"]["VITE_MARKET_WS_URL"] = "http://gateway.internal:9020/ws"
            invalid_live_scheme_path.write_text(json.dumps(invalid_live_scheme), encoding="utf-8")
            invalid_live_scheme_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=invalid_live_scheme_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                )
            )
            narrow_decommission_path = root / "narrow-legacy-decommission.json"
            narrow_decommission_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "passed": True,
                        "legacy_websocket_disabled": True,
                        "old_topic_consumers_disabled": True,
                        "no_legacy_consumers_observed": True,
                        "observation": {
                            "observed_at": "2026-05-22T16:15:00+08:00",
                            "expected_old_topics": ["legacy_ticks"],
                            "legacy_websocket_enabled": False,
                            "old_topic_consumers": {"legacy_ticks": 0},
                            "old_topic_lag": {"legacy_ticks": 0},
                        },
                    }
                ),
                encoding="utf-8",
            )
            narrow_decommission_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=narrow_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                )
            )
            malformed_observed_at_decommission_path = root / "malformed-observed-at-legacy-decommission.json"
            malformed_observed_at_decommission = json.loads(paths.legacy_decommission_path.read_text(encoding="utf-8"))
            malformed_observed_at_decommission["observation"]["observed_at"] = "20260522 161500"
            malformed_observed_at_decommission_path.write_text(
                json.dumps(malformed_observed_at_decommission),
                encoding="utf-8",
            )
            malformed_observed_at_decommission_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=malformed_observed_at_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                )
            )
            early_observed_decommission_path = root / "early-observed-legacy-decommission.json"
            early_observed_decommission = json.loads(paths.legacy_decommission_path.read_text(encoding="utf-8"))
            early_observed_decommission["observation"]["observed_at"] = "2026-05-22T13:00:00+08:00"
            early_observed_decommission_path.write_text(
                json.dumps(early_observed_decommission),
                encoding="utf-8",
            )
            early_observed_decommission_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=early_observed_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                )
            )
            malformed_count_decommission_path = root / "malformed-count-legacy-decommission.json"
            malformed_count_decommission = json.loads(paths.legacy_decommission_path.read_text(encoding="utf-8"))
            malformed_count_decommission["observation"]["old_topic_consumers"]["legacy_ticks"] = "0"
            malformed_count_decommission["observation"]["old_topic_lag"]["legacy_broker_queue"] = "0"
            malformed_count_decommission_path.write_text(
                json.dumps(malformed_count_decommission),
                encoding="utf-8",
            )
            malformed_count_decommission_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=malformed_count_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                )
            )
            spoofed_decommission_path = root / "spoofed-legacy-decommission.json"
            spoofed_decommission = json.loads(paths.legacy_decommission_path.read_text(encoding="utf-8"))
            spoofed_decommission["observation"]["legacy_websocket_enabled"] = True
            spoofed_decommission["old_topic_consumers_disabled"] = False
            spoofed_decommission_path.write_text(json.dumps(spoofed_decommission), encoding="utf-8")
            spoofed_decommission_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=spoofed_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                )
            )
            weak_retirement_path = root / "weak-legacy-retirement.json"
            weak_retirement_path.write_text(json.dumps({"legacy_retired": True}), encoding="utf-8")
            weak_retirement_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=weak_retirement_path,
                )
            )
            mismatched_retirement_path = root / "mismatched-legacy-retirement.json"
            mismatched_retirement = json.loads(paths.legacy_retirement_path.read_text(encoding="utf-8"))
            mismatched_retirement["cutover_readiness"]["accepted_report_ids"] = ["other-session"]
            mismatched_retirement["evidence"]["legacy_websocket_disabled"] = False
            mismatched_retirement_path.write_text(json.dumps(mismatched_retirement), encoding="utf-8")
            mismatched_retirement_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=mismatched_retirement_path,
                )
            )
            early_approval_retirement_path = root / "early-approval-legacy-retirement.json"
            early_approval_retirement = json.loads(paths.legacy_retirement_path.read_text(encoding="utf-8"))
            early_approval_retirement["evidence"]["operator_approved_at"] = "2026-05-22T15:59:00+08:00"
            early_approval_retirement_path.write_text(json.dumps(early_approval_retirement), encoding="utf-8")
            early_approval_retirement_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=early_approval_retirement_path,
                )
            )
            pre_decommission_approval_retirement_path = root / "pre-decommission-approval-legacy-retirement.json"
            pre_decommission_approval_retirement = json.loads(paths.legacy_retirement_path.read_text(encoding="utf-8"))
            pre_decommission_approval_retirement["evidence"]["operator_approved_at"] = "2026-05-22T16:05:00+08:00"
            pre_decommission_approval_retirement_path.write_text(
                json.dumps(pre_decommission_approval_retirement),
                encoding="utf-8",
            )
            pre_decommission_approval_retirement_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=paths.shadow_reports_directory,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=pre_decommission_approval_retirement_path,
                )
            )
            short_shadow_reports = root / "short-shadow-reports"
            short_shadow_reports.mkdir()
            short_report = passing_report()
            short_report["duration_seconds"] = 60
            short_report["legacy_source_coverage_seconds"] = 60
            short_report["v2_source_coverage_seconds"] = 60
            short_report["comparison"]["symbols"]["00700.HK"]["legacy_source_coverage_seconds"] = 60
            short_report["comparison"]["symbols"]["00700.HK"]["v2_source_coverage_seconds"] = 60
            save_shadow_run_report(short_report, short_shadow_reports)
            short_shadow_result = evaluate_evidence_bundle(
                EvidenceBundlePaths(
                    shadow_reports_directory=short_shadow_reports,
                    manifest_directory=paths.manifest_directory,
                    runtime_config_path=paths.runtime_config_path,
                runtime_health_path=paths.runtime_health_path,
                    readiness_path=paths.readiness_path,
                    frontend_deployment_path=paths.frontend_deployment_path,
                    legacy_decommission_path=paths.legacy_decommission_path,
                    legacy_retirement_path=paths.legacy_retirement_path,
                )
            )

        self.assertTrue(result["passed"])
        self.assertTrue(persisted["passed"])
        self.assertTrue(smoke_result["passed"])
        self.assertTrue(smoke_result["evidence"]["multi_trader_smoke_present"])
        self.assertTrue(smoke_result["evidence"]["multi_trader_smoke_preflight_passed"])
        self.assertEqual(smoke_result["evidence"]["multi_trader_smoke_manifest_file_count"], 7)
        self.assertEqual(smoke_result["evidence"]["multi_trader_smoke_client_count"], 2)
        self.assertTrue(package_result["passed"])
        self.assertEqual(package_result["evidence"]["multi_trader_smoke_package_file_count"], 8)
        self.assertEqual(package_result["evidence"]["multi_trader_smoke_package_zip_files"], sorted(package_files))
        self.assertFalse(missing_service_package_result["passed"])
        self.assertIn(
            "multi_trader_smoke_package_service_preflight_missing",
            missing_service_package_result["blockers"],
        )
        self.assertFalse(mismatched_service_package_result["passed"])
        self.assertIn(
            "multi_trader_smoke_package_service_preflight_mismatch",
            mismatched_service_package_result["blockers"],
        )
        self.assertFalse(stale_service_package_result["passed"])
        self.assertIn(
            "multi_trader_smoke_package_service_preflight_before_preflight",
            stale_service_package_result["blockers"],
        )
        self.assertFalse(unmanifested_package_result["passed"])
        self.assertIn(
            "multi_trader_smoke_package_manifest_unmanifested_files",
            unmanifested_package_result["blockers"],
        )
        self.assertEqual(
            unmanifested_package_result["evidence"]["multi_trader_smoke_package_manifest_unmanifested_paths"],
            ["unexpected-extra.json"],
        )
        self.assertFalse(package_without_external_smoke_result["passed"])
        self.assertIn("missing_multi_trader_smoke", package_without_external_smoke_result["blockers"])
        self.assertIn("missing_multi_trader_smoke_preflight", package_without_external_smoke_result["blockers"])
        self.assertIn("missing_multi_trader_smoke_manifest", package_without_external_smoke_result["blockers"])
        self.assertFalse(stale_package_result["passed"])
        self.assertIn("multi_trader_smoke_package_hash_mismatch", stale_package_result["blockers"])
        self.assertFalse(no_import_manifest_result["passed"])
        self.assertIn("multi_trader_smoke_package_import_manifest_missing", no_import_manifest_result["blockers"])
        self.assertFalse(invalid_import_manifest_result["passed"])
        self.assertIn(
            "multi_trader_smoke_package_import_manifest_imported_invalid",
            invalid_import_manifest_result["blockers"],
        )
        self.assertFalse(missing_import_output_result["passed"])
        self.assertIn(
            "multi_trader_smoke_package_import_manifest_output_missing",
            missing_import_output_result["blockers"],
        )
        self.assertFalse(unimported_client_result["passed"])
        self.assertIn(
            "multi_trader_smoke_package_import_manifest_coverage_missing",
            unimported_client_result["blockers"],
        )
        self.assertEqual(
            unimported_client_result["evidence"]["multi_trader_smoke_import_manifest_missing_frontend_artifact_paths"],
            ["clients/client-b.json"],
        )
        self.assertFalse(mismatched_package_result["passed"])
        self.assertIn(
            "multi_trader_smoke_package_multi_trader_smoke_evidence_json_hash_mismatch",
            mismatched_package_result["blockers"],
        )
        self.assertFalse(mismatched_observation_package_result["passed"])
        self.assertIn(
            "multi_trader_smoke_package_manifest_file_hash_mismatch",
            mismatched_observation_package_result["blockers"],
        )
        self.assertEqual(
            mismatched_observation_package_result["evidence"]["multi_trader_smoke_package_manifest_hash_mismatch_paths"],
            ["multi-trader-smoke-observation.json"],
        )
        self.assertFalse(missing_preflight_result["passed"])
        self.assertIn("missing_multi_trader_smoke_preflight", missing_preflight_result["blockers"])
        self.assertFalse(missing_manifest_result["passed"])
        self.assertIn("missing_multi_trader_smoke_manifest", missing_manifest_result["blockers"])
        self.assertFalse(stale_manifest_result["passed"])
        self.assertIn("multi_trader_smoke_manifest_smoke_hash_mismatch", stale_manifest_result["blockers"])
        self.assertFalse(mismatched_preflight_result["passed"])
        self.assertIn("multi_trader_smoke_preflight_gateway_url_mismatch", mismatched_preflight_result["blockers"])
        self.assertFalse(failing_smoke_result["passed"])
        self.assertIn("multi_trader_smoke_blocked", failing_smoke_result["blockers"])
        self.assertEqual(result["evidence"]["shadow_report_count"], 1)
        self.assertTrue(result["evidence"]["runtime_config_present"])
        self.assertIn("missing_runtime_config", missing_runtime_config_result["blockers"])
        self.assertIn("runtime_config_trade_date_mismatch", mismatched_runtime_config_result["blockers"])
        self.assertEqual(
            result["evidence"]["manifest_data_types"],
            ["broker_mapping", "broker_queue", "ccass_holdings", "daily_bars", "minute_bars", "participant_history", "trade_ticks"],
        )
        self.assertIn("shadow_run_report_schema_invalid", malformed_shadow_result["blockers"])
        self.assertIn(
            "performance_missing_sample_keys_invalid",
            malformed_shadow_result["evidence"]["invalid_shadow_reports"][0]["errors"],
        )
        self.assertIn("shadow_run_evidence_source_files_invalid", mismatched_source_result["blockers"])
        self.assertIn(
            "evidence_source_v2_event_file_count_mismatch",
            mismatched_source_result["evidence"]["shadow_source_file_audits"][0]["errors"],
        )
        self.assertIn("shadow_run_evidence_source_files_invalid", invalid_source_result["blockers"])
        self.assertIn(
            "evidence_source_files_parse_invalid",
            invalid_source_result["evidence"]["shadow_source_file_audits"][0]["errors"],
        )
        self.assertIn("missing_historical_manifests", blocked["blockers"])
        self.assertIn("historical_manifest_coverage_incomplete", incomplete_manifest_result["blockers"])
        self.assertIn("trade_ticks", incomplete_manifest_result["evidence"]["missing_manifest_data_types"])
        self.assertIn("historical_manifest_schema_invalid", malformed_manifest_result["blockers"])
        self.assertIn(
            "source_data_type_mismatch",
            malformed_manifest_result["evidence"]["invalid_historical_manifests"][0]["errors"],
        )
        self.assertIn("runtime_health_kafka_lag_present", bad_health_result["blockers"])
        self.assertIn("runtime_health_symbol_freshness_degraded", bad_health_result["blockers"])
        self.assertIn("runtime_health_redis_snapshot_missing_symbols", bad_health_result["blockers"])
        self.assertNotIn("runtime_health_missing_required_topics", missing_processed_topic_result["blockers"])
        self.assertEqual(missing_processed_topic_result["evidence"]["runtime_health"]["missing_runtime_topics"], [])
        self.assertIn("runtime_trade_date_mismatch", mismatched_runtime_date_result["blockers"])
        self.assertIn(
            "runtime_health_last_tick_before_shadow_report_finish",
            early_runtime_window_result["blockers"],
        )
        self.assertIn(
            "runtime_health_generated_before_shadow_report_finish",
            early_runtime_window_result["blockers"],
        )
        self.assertEqual(
            early_runtime_window_result["evidence"]["runtime_shadow_window"]["last_tick_before_report_finish"],
            ["session-1"],
        )
        self.assertIn("historical_manifest_date_range_mismatch", mismatched_manifest_date_result["blockers"])
        self.assertIn("daily_bars", mismatched_manifest_date_result["evidence"]["manifest_date_range_mismatches"])
        self.assertIn("historical_manifest_symbol_coverage_mismatch", mismatched_manifest_symbols_result["blockers"])
        self.assertEqual(
            mismatched_manifest_symbols_result["evidence"]["manifest_symbol_coverage_mismatches"]["daily_bars"],
            ["00700.HK"],
        )
        self.assertIn(
            "shadow_run_comparison_missing_subscribed_symbols",
            shadow_missing_runtime_symbol_result["blockers"],
        )
        self.assertIn(
            "runtime_gateway_delivery_without_shadow_comparison",
            shadow_missing_runtime_symbol_result["blockers"],
        )
        self.assertEqual(
            shadow_missing_runtime_symbol_result["evidence"]["shadow_comparison_missing_subscribed_symbols"],
            ["00939.HK"],
        )
        self.assertEqual(
            shadow_missing_runtime_symbol_result["evidence"]["delivered_symbols_without_shadow_comparison"],
            ["00939.HK"],
        )
        self.assertEqual(result["evidence"]["readiness_accepted_report_ids"], ["session-1"])
        self.assertIn("cutover_readiness_accepted_reports_not_in_bundle", unlinked_readiness_result["blockers"])
        self.assertIn("cutover_readiness_schema_invalid", weak_readiness_result["blockers"])
        self.assertIn("policy_missing", weak_readiness_result["evidence"]["invalid_cutover_readiness_errors"])
        self.assertIn("frontend_deployment_readiness_mismatch", mismatched_frontend_result["blockers"])
        self.assertIn("frontend_deployment_schema_invalid", weak_frontend_artifact_result["blockers"])
        self.assertIn("frontend_deployment_artifact_not_passed", weak_frontend_artifact_result["blockers"])
        self.assertIn("frontend_deployment_protocol_mismatch", mismatched_protocol_result["blockers"])
        self.assertIn("frontend_deployment_data_mode_not_live_or_auto", mock_mode_result["blockers"])
        self.assertIn(
            "frontend_deployment_verified_before_shadow_report_finish",
            early_frontend_verified_result["blockers"],
        )
        self.assertIn("frontend_live_url_gateway_path_mismatch", mismatched_live_url_result["blockers"])
        self.assertIn("frontend_live_url_gateway_port_mismatch", mismatched_live_port_result["blockers"])
        self.assertIn("frontend_live_url_gateway_port_missing", missing_live_port_result["blockers"])
        self.assertIn("frontend_live_url_invalid", invalid_live_scheme_result["blockers"])
        self.assertIn("legacy_decommission_default_topic_coverage_incomplete", narrow_decommission_result["blockers"])
        self.assertIn("legacy_decommission_default_topic_consumers_not_zero", narrow_decommission_result["blockers"])
        self.assertIn("legacy_decommission_default_topic_lag_not_zero", narrow_decommission_result["blockers"])
        self.assertIn(
            "legacy_decommission_observed_at_invalid",
            malformed_observed_at_decommission_result["blockers"],
        )
        self.assertIn(
            "legacy_decommission_observed_before_frontend_verified",
            early_observed_decommission_result["blockers"],
        )
        self.assertIn(
            "legacy_decommission_default_topic_consumers_invalid",
            malformed_count_decommission_result["blockers"],
        )
        self.assertIn(
            "legacy_decommission_default_topic_lag_invalid",
            malformed_count_decommission_result["blockers"],
        )
        self.assertIn("legacy_decommission_websocket_observed_enabled", spoofed_decommission_result["blockers"])
        self.assertIn("legacy_decommission_consumers_flag_not_disabled", spoofed_decommission_result["blockers"])
        self.assertIn("legacy_retirement_artifact_not_passed", weak_retirement_result["blockers"])
        self.assertIn("legacy_retirement_readiness_missing", weak_retirement_result["blockers"])
        self.assertIn("legacy_retirement_readiness_mismatch", mismatched_retirement_result["blockers"])
        self.assertIn("legacy_retirement_decommission_mismatch", mismatched_retirement_result["blockers"])
        self.assertIn(
            "legacy_retirement_operator_approved_before_rollback_window_completed",
            early_approval_retirement_result["blockers"],
        )
        self.assertIn(
            "legacy_retirement_operator_approved_before_decommission_observed",
            pre_decommission_approval_retirement_result["blockers"],
        )
        self.assertIn("shadow_run_reports_fail_default_cutover_policy", short_shadow_result["blockers"])
        self.assertIn("cutover_readiness_accepted_reports_fail_default_policy", short_shadow_result["blockers"])

    def test_runtime_health_verification_checks_lag_redis_freshness_and_dead_letters(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            passing = runtime_health_payload()
            passed = evaluate_runtime_health(passing)
            path = root / "runtime-health.json"
            output_path = root / "runtime-health-verification.json"
            path.write_text(json.dumps(passing), encoding="utf-8")
            persisted = write_runtime_health_verification(
                runtime_health_path=path,
                output_path=output_path,
            )
            persisted_file = json.loads(output_path.read_text(encoding="utf-8"))
            direct_gateway = runtime_health_payload()
            direct_gateway["gateway_activity"]["processed_records_consumed"] = 0
            direct_gateway["gateway_activity"]["shadow_processed_records_drained"] = 0
            direct_gateway["gateway_activity"]["direct_runtime_messages_emitted"] = 1
            direct_gateway_result = evaluate_runtime_health(direct_gateway)
            failing = runtime_health_payload()
            failing["generated_at"] = ""
            failing["topics"]["raw_market_events_v1"]["lag"] = 1
            failing["supervisor"]["ticks"] = 0
            failing["supervisor"]["ingested_events"] = 0
            failing["supervisor"]["processed_events"] = 0
            failing["supervisor"]["started_at"] = ""
            failing["supervisor"]["last_tick_at"] = ""
            failing["queues"]["raw_callback_backlog"] = 3
            failing["queues"]["raw_callback_rejected"] = 2
            failing["queues"]["raw_callback_rejection_path"] = ""
            failing["queues"]["raw_consumer_dead_letter_path"] = ""
            failing["workers"]["ingest"]["dead_letters"] = [{"reason": "bad_callback"}]
            failing["workers"]["ingest"]["processed"] = 0
            failing["workers"]["raw_consumer"]["processed"] = 0
            failing["producer"]["dead_letters"] = 1
            failing["producer"]["spooled_records"] = 1
            failing["producer"]["quarantined_spool_records"] = 1
            failing["producer"]["publish_attempts"] = 0
            failing["producer"]["spool_path"] = ""
            failing["producer"]["spool_quarantine_path"] = "artifacts/runtime-state/kafka-spool/not-a-quarantine.jsonl"
            failing["redis"]["write_stats"]["failures"] = 1
            failing["subscription"]["running"] = False
            failing["subscription"]["subscribed_symbols"] = []
            failing["symbol_runtime"]["00700.HK"]["state"] = "DEGRADED"
            failing["symbol_runtime"]["00700.HK"]["hydration_failures"] = 1
            failing["symbol_runtime"]["00700.HK"]["capacity_rejections"] = 1
            failing["symbol_runtime_manager"]["capacity_rejections"] = 1
            failing["symbol_runtime_manager"]["state_sink_failures"] = 1
            failing["symbol_runtime_manager"]["last_state_sink_error"] = "runtime_state_sink_write_failed: redis down"
            failing["symbol_runtime_manager"]["state_sink_failure_symbols"] = ["00700.HK"]
            failing["symbol_runtime_manager"]["snapshot_sink_failures"] = 1
            failing["symbol_runtime_manager"]["last_snapshot_sink_error"] = "runtime_snapshot_sink_write_failed: redis down"
            failing["symbol_runtime_manager"]["snapshot_sink_failure_symbols"] = ["00700.HK"]
            failing["redis_snapshot"]["trade_date"] = "20260523"
            failing["redis_snapshot"]["missing_symbols"] = ["00700.HK"]
            failing["gateway_websocket"]["running"] = False
            failing["gateway_websocket"]["host"] = ""
            failing["gateway_websocket"]["port"] = 0
            failing["gateway_websocket"]["path"] = "/legacy"
            failing["gateway_websocket"]["request_schema_version"] = 0
            failing["gateway_websocket"]["accepted_protocol"] = "legacy-message"
            failing["gateway_activity"]["processed_records_consumed"] = 0
            failing["gateway_activity"]["shadow_processed_records_drained"] = 0
            failing["gateway_activity"]["direct_runtime_messages_emitted"] = 0
            failing["gateway_activity"]["terminal_messages_emitted"] = 0
            failing["gateway_activity"]["terminal_messages_delivered"] = 0
            failing["gateway_activity"]["delivered_terminal_symbols"] = []
            failing["gateway_activity"]["last_terminal_message_delivered_at"] = ""
            loopback_gateway = runtime_health_payload()
            loopback_gateway["gateway_websocket"]["host"] = "127.0.0.1"
            loopback_gateway_result = evaluate_runtime_health(loopback_gateway)
            failing["health"]["octopus"]["redis"] = "degraded"
            failing["health"]["collector"]["symbol_freshness"]["00700.HK"]["degraded"] = True
            failed = evaluate_runtime_health(failing)
            invalid_counters = runtime_health_payload()
            invalid_counters["supervisor"]["ticks"] = "1"
            invalid_counters["queues"]["raw_callback_backlog"] = "0"
            invalid_counters["queues"]["raw_callback_rejected"] = "0"
            invalid_counters["workers"]["ingest"]["processed"] = "1"
            invalid_counters["producer"]["dead_letters"] = "0"
            invalid_counters["producer"]["spooled_records"] = "0"
            invalid_counters["producer"]["quarantined_spool_records"] = "0"
            invalid_counters["producer"]["publish_attempts"] = "2"
            invalid_counters["redis"]["write_stats"]["writes"] = "1"
            invalid_counters["redis"]["write_stats"]["failures"] = "0"
            invalid_counters["redis"]["write_stats"]["last_latency_ms"] = "0"
            invalid_counters["redis"]["write_stats"]["max_latency_ms"] = "0"
            invalid_counters["symbol_runtime"]["00700.HK"]["hydrate_count"] = "1"
            invalid_counters["symbol_runtime"]["00700.HK"]["hydration_failures"] = "0"
            invalid_counters["symbol_runtime"]["00700.HK"]["last_hydration_latency_ms"] = "0"
            invalid_counters["symbol_runtime"]["00700.HK"]["max_hydration_latency_ms"] = "0"
            invalid_counters["symbol_runtime"]["00700.HK"]["max_concurrent_hydrations"] = "8"
            invalid_counters["symbol_runtime"]["00700.HK"]["capacity_rejections"] = "0"
            invalid_counters["symbol_runtime_manager"]["runtime_count"] = "1"
            invalid_counters["symbol_runtime_manager"]["total_ref_count"] = "1"
            invalid_counters["symbol_runtime_manager"]["active_hydrations"] = "0"
            invalid_counters["symbol_runtime_manager"]["max_concurrent_hydrations"] = "8"
            invalid_counters["symbol_runtime_manager"]["capacity_rejections"] = "0"
            invalid_counters["symbol_runtime_manager"]["state_sink_failures"] = "0"
            invalid_counters["symbol_runtime_manager"]["snapshot_sink_failures"] = "0"
            invalid_counters["gateway_activity"]["processed_records_consumed"] = "1"
            invalid_counters_result = evaluate_runtime_health(invalid_counters)
            low_publish_attempts = runtime_health_payload()
            low_publish_attempts["producer"]["publish_attempts"] = 1
            low_publish_attempts_result = evaluate_runtime_health(low_publish_attempts)
            invalid_redis_latency_order = runtime_health_payload()
            invalid_redis_latency_order["redis"]["write_stats"]["last_latency_ms"] = 5.0
            invalid_redis_latency_order["redis"]["write_stats"]["max_latency_ms"] = 4.0
            invalid_redis_latency_order_result = evaluate_runtime_health(invalid_redis_latency_order)
            invalid_performance_samples = runtime_health_payload()
            invalid_performance_samples["performance_samples"]["subscribe_snapshot_ms"] = [10.0, -1.0]
            invalid_performance_samples_result = evaluate_runtime_health(invalid_performance_samples)
            missing_performance_samples = runtime_health_payload()
            del missing_performance_samples["performance_samples"]
            missing_performance_samples_result = evaluate_runtime_health(missing_performance_samples)
            empty_performance_samples = runtime_health_payload()
            empty_performance_samples["performance_samples"]["subscribe_snapshot_ms"] = []
            empty_performance_samples_result = evaluate_runtime_health(empty_performance_samples)
            slow_subscribe_snapshot = runtime_health_payload()
            slow_subscribe_snapshot["performance_samples"]["subscribe_snapshot_ms"] = [180.0, 220.0, 260.0]
            slow_subscribe_snapshot_result = evaluate_runtime_health(slow_subscribe_snapshot)
            stale_freshness = runtime_health_payload()
            stale_freshness["health"]["collector"]["symbol_freshness"]["00700.HK"]["subscribed"] = False
            stale_freshness["health"]["collector"]["symbol_freshness"]["00700.HK"]["latest_event_at"] = ""
            stale_freshness_result = evaluate_runtime_health(stale_freshness)
            missing_topic = runtime_health_payload()
            del missing_topic["topics"]["raw_market_events_v1"]
            missing_topic_result = evaluate_runtime_health(missing_topic)
            missing_committed_offset = runtime_health_payload()
            del missing_committed_offset["topics"]["raw_market_events_v1"]["committed_offset"]
            missing_committed_offset_result = evaluate_runtime_health(missing_committed_offset)
            invalid_committed_offset = runtime_health_payload()
            invalid_committed_offset["topics"]["raw_market_events_v1"]["committed_offset"] = -1
            invalid_committed_offset_result = evaluate_runtime_health(invalid_committed_offset)
            missing_lag = runtime_health_payload()
            del missing_lag["topics"]["raw_market_events_v1"]["lag"]
            missing_lag_result = evaluate_runtime_health(missing_lag)
            invalid_lag = runtime_health_payload()
            invalid_lag["topics"]["raw_market_events_v1"]["lag"] = "0"
            invalid_lag_result = evaluate_runtime_health(invalid_lag)
            low_committed_offsets = runtime_health_payload()
            low_committed_offsets["topics"]["raw_market_events_v1"]["committed_offset"] = 0
            low_committed_offsets["topics"]["processed_market_events_v1"]["committed_offset"] = 0
            low_committed_offsets_result = evaluate_runtime_health(low_committed_offsets)
            empty_snapshot_probe = runtime_health_payload()
            empty_snapshot_probe["redis_snapshot"]["checked_symbols"] = []
            empty_snapshot_probe_result = evaluate_runtime_health(empty_snapshot_probe)
            invalid_subscription_symbols = runtime_health_payload()
            invalid_subscription_symbols["subscription"]["subscribed_symbols"] = ["700"]
            invalid_subscription_symbols_result = evaluate_runtime_health(invalid_subscription_symbols)
            duplicate_subscription_symbols = runtime_health_payload()
            duplicate_subscription_symbols["subscription"]["subscribed_symbols"] = ["00700.HK", "00700.HK"]
            duplicate_subscription_symbols_result = evaluate_runtime_health(duplicate_subscription_symbols)
            invalid_snapshot_symbols = runtime_health_payload()
            invalid_snapshot_symbols["redis_snapshot"]["checked_symbols"] = "00700.HK"
            invalid_snapshot_symbols_result = evaluate_runtime_health(invalid_snapshot_symbols)
            duplicate_snapshot_symbols = runtime_health_payload()
            duplicate_snapshot_symbols["redis_snapshot"]["checked_symbols"] = ["00700.HK", "00700.HK"]
            duplicate_snapshot_symbols_result = evaluate_runtime_health(duplicate_snapshot_symbols)
            conflicting_snapshot_symbols = runtime_health_payload()
            conflicting_snapshot_symbols["redis_snapshot"]["missing_symbols"] = ["00700.HK"]
            conflicting_snapshot_symbols_result = evaluate_runtime_health(conflicting_snapshot_symbols)
            unresolved_snapshot_symbols = runtime_health_payload()
            unresolved_snapshot_symbols["redis_snapshot"]["present_symbols"] = []
            unresolved_snapshot_symbols["redis_snapshot"]["missing_symbols"] = []
            unresolved_snapshot_symbols_result = evaluate_runtime_health(unresolved_snapshot_symbols)
            unchecked_snapshot_symbols = runtime_health_payload()
            unchecked_snapshot_symbols["redis_snapshot"]["present_symbols"] = ["00700.HK", "00939.HK"]
            unchecked_snapshot_symbols_result = evaluate_runtime_health(unchecked_snapshot_symbols)
            missing_checked_subscription = runtime_health_payload()
            missing_checked_subscription["redis_snapshot"]["checked_symbols"] = ["00939.HK"]
            missing_checked_subscription["redis_snapshot"]["present_symbols"] = ["00939.HK"]
            missing_checked_subscription_result = evaluate_runtime_health(missing_checked_subscription)
            missing_present_subscription = runtime_health_payload()
            missing_present_subscription["redis_snapshot"]["present_symbols"] = []
            missing_present_subscription_result = evaluate_runtime_health(missing_present_subscription)
            missing_key_family = runtime_health_payload()
            missing_key_family["redis_snapshot"]["key_family_coverage"]["terminal_queue"]["present_symbols"] = []
            missing_key_family["redis_snapshot"]["key_family_coverage"]["terminal_queue"]["missing_symbols"] = ["00700.HK"]
            missing_key_family_result = evaluate_runtime_health(missing_key_family)
            missing_key_family_coverage = runtime_health_payload()
            del missing_key_family_coverage["redis_snapshot"]["key_family_coverage"]["terminal_queue"]
            missing_key_family_coverage_result = evaluate_runtime_health(missing_key_family_coverage)
            missing_required_key_family = runtime_health_payload()
            missing_required_key_family["redis_snapshot"]["required_key_families"] = ["terminal_snapshot"]
            missing_required_key_family_result = evaluate_runtime_health(missing_required_key_family)
            missing_history_key = runtime_health_payload()
            missing_history_key["redis_snapshot"]["key_family_coverage"]["ccass_history"]["missing_keys"] = {
                "00700.HK": ["ccass:history:00700.HK:C00010"]
            }
            missing_history_key_result = evaluate_runtime_health(missing_history_key)
            missing_history_participants = runtime_health_payload()
            missing_history_participants["redis_snapshot"]["key_family_coverage"]["ccass_history"][
                "participants_by_symbol"
            ] = {"00700.HK": []}
            missing_history_participants_result = evaluate_runtime_health(missing_history_participants)
            missing_key_family_updated_at = runtime_health_payload()
            missing_key_family_updated_at["redis_snapshot"]["key_family_coverage"]["terminal_queue"][
                "updated_at_by_symbol"
            ] = {}
            missing_key_family_updated_at_result = evaluate_runtime_health(missing_key_family_updated_at)
            invalid_key_family_updated_at = runtime_health_payload()
            invalid_key_family_updated_at["redis_snapshot"]["key_family_coverage"]["terminal_queue"][
                "updated_at_by_symbol"
            ] = {"00700.HK": "20260522 093000"}
            invalid_key_family_updated_at_result = evaluate_runtime_health(invalid_key_family_updated_at)
            future_key_family_updated_at = runtime_health_payload()
            future_key_family_updated_at["redis_snapshot"]["key_family_coverage"]["terminal_queue"][
                "updated_at_by_symbol"
            ] = {"00700.HK": "2026-05-22T09:30:03+08:00"}
            future_key_family_updated_at_result = evaluate_runtime_health(future_key_family_updated_at)
            missing_key_family_ttl = runtime_health_payload()
            missing_key_family_ttl["redis_snapshot"]["key_family_coverage"]["terminal_queue"][
                "ttl_seconds_by_symbol"
            ] = {}
            missing_key_family_ttl_result = evaluate_runtime_health(missing_key_family_ttl)
            invalid_key_family_ttl = runtime_health_payload()
            invalid_key_family_ttl["redis_snapshot"]["key_family_coverage"]["terminal_queue"][
                "ttl_seconds_by_symbol"
            ] = {"00700.HK": 0}
            invalid_key_family_ttl_result = evaluate_runtime_health(invalid_key_family_ttl)
            missing_key_family_contract = runtime_health_payload()
            del missing_key_family_contract["redis_snapshot"]["key_family_coverage"]["terminal_queue"][
                "contract_missing_by_symbol"
            ]
            missing_key_family_contract_result = evaluate_runtime_health(missing_key_family_contract)
            invalid_key_family_contract = runtime_health_payload()
            invalid_key_family_contract["redis_snapshot"]["key_family_coverage"]["terminal_queue"][
                "contract_missing_by_symbol"
            ] = {"00700.HK": ["schema_version", "freshness"]}
            invalid_key_family_contract_result = evaluate_runtime_health(invalid_key_family_contract)
            missing_client_queue = runtime_health_payload()
            del missing_client_queue["gateway_activity"]["client_queue"]
            missing_client_queue_result = evaluate_runtime_health(missing_client_queue)
            missing_declared_client_ids = runtime_health_payload()
            del missing_declared_client_ids["gateway_activity"]["client_queue"]["observed_declared_client_ids"]
            missing_declared_client_ids_result = evaluate_runtime_health(missing_declared_client_ids)
            whitespace_declared_client_ids = runtime_health_payload()
            whitespace_declared_client_ids["gateway_activity"]["client_queue"]["observed_declared_client_ids"] = [
                "desk-a",
                " desk-b ",
            ]
            whitespace_declared_client_ids_result = evaluate_runtime_health(whitespace_declared_client_ids)
            mismatched_declared_client_count = runtime_health_payload()
            mismatched_declared_client_count["gateway_activity"]["client_queue"]["observed_declared_client_count"] = 1
            mismatched_declared_client_count_result = evaluate_runtime_health(mismatched_declared_client_count)
            alert_dropped = runtime_health_payload()
            alert_dropped["gateway_activity"]["client_queue"]["alert_dropped"] = 1
            alert_dropped_result = evaluate_runtime_health(alert_dropped)
            noncritical_dropped = runtime_health_payload()
            noncritical_dropped["gateway_activity"]["client_queue"]["dropped"] = 2
            noncritical_dropped["gateway_activity"]["client_queue"]["alert_overflow"] = 1
            noncritical_dropped_result = evaluate_runtime_health(noncritical_dropped)
            critical_overflow = runtime_health_payload()
            critical_overflow["gateway_activity"]["client_queue"]["critical_overflow"] = 1
            critical_overflow_result = evaluate_runtime_health(critical_overflow)
            backlog_mismatch = runtime_health_payload()
            backlog_mismatch["gateway_activity"]["client_queue"]["connected_clients"] = 1
            backlog_mismatch["gateway_activity"]["client_queue"]["current_backlog_by_client"] = {"client-1": 3}
            backlog_mismatch["gateway_activity"]["client_queue"]["total_current_backlog"] = 2
            backlog_mismatch["gateway_activity"]["client_queue"]["max_current_backlog"] = 3
            backlog_mismatch_result = evaluate_runtime_health(backlog_mismatch)
            invalid_stop_reason = runtime_health_payload()
            invalid_stop_reason["supervisor"]["stop_reason"] = "killed"
            invalid_stop_reason_result = evaluate_runtime_health(invalid_stop_reason)
            missing_subscription = runtime_health_payload()
            del missing_subscription["subscription"]
            missing_subscription_result = evaluate_runtime_health(missing_subscription)
            missing_symbol_runtime = runtime_health_payload()
            del missing_symbol_runtime["symbol_runtime"]
            missing_symbol_runtime_result = evaluate_runtime_health(missing_symbol_runtime)
            missing_symbol_runtime_manager = runtime_health_payload()
            del missing_symbol_runtime_manager["symbol_runtime_manager"]
            missing_symbol_runtime_manager_result = evaluate_runtime_health(missing_symbol_runtime_manager)
            mismatched_symbol_runtime_summary = runtime_health_payload()
            mismatched_symbol_runtime_summary["symbol_runtime_manager"]["runtime_count"] = 2
            mismatched_symbol_runtime_summary["symbol_runtime_manager"]["total_ref_count"] = 2
            mismatched_symbol_runtime_summary["symbol_runtime_manager"]["active_hydrations"] = 1
            mismatched_symbol_runtime_summary["symbol_runtime_manager"]["state_counts"]["LIVE"] = 0
            mismatched_symbol_runtime_summary["symbol_runtime_manager"]["state_counts"]["HYDRATING"] = 1
            mismatched_symbol_runtime_summary["symbol_runtime_manager"]["hydrating_symbols"] = ["00700.HK"]
            mismatched_symbol_runtime_summary["symbol_runtime_manager"]["realtime_attached_symbols"] = []
            mismatched_symbol_runtime_summary_result = evaluate_runtime_health(mismatched_symbol_runtime_summary)
            mismatched_symbol_runtime_symbol = runtime_health_payload()
            mismatched_symbol_runtime_symbol["symbol_runtime"]["00700.HK"]["symbol"] = "00939.HK"
            mismatched_symbol_runtime_symbol_result = evaluate_runtime_health(mismatched_symbol_runtime_symbol)
            invalid_symbol_runtime_symbol = runtime_health_payload()
            invalid_symbol_runtime_symbol["symbol_runtime"]["700"] = invalid_symbol_runtime_symbol[
                "symbol_runtime"
            ].pop("00700.HK")
            invalid_symbol_runtime_symbol["symbol_runtime"]["700"]["symbol"] = "700"
            invalid_symbol_runtime_symbol_result = evaluate_runtime_health(invalid_symbol_runtime_symbol)
            missing_queues = runtime_health_payload()
            del missing_queues["queues"]
            missing_queues_result = evaluate_runtime_health(missing_queues)
            missing_freshness = runtime_health_payload()
            del missing_freshness["health"]["collector"]["symbol_freshness"]["00700.HK"]
            missing_freshness_result = evaluate_runtime_health(missing_freshness)
            missing_delivery_timestamp = runtime_health_payload()
            missing_delivery_timestamp["gateway_activity"]["last_terminal_message_delivered_at"] = ""
            missing_delivery_timestamp_result = evaluate_runtime_health(missing_delivery_timestamp)
            invalid_delivery_timestamp = runtime_health_payload()
            invalid_delivery_timestamp["gateway_activity"]["last_terminal_message_delivered_at"] = "20260522 093001"
            invalid_delivery_timestamp_result = evaluate_runtime_health(invalid_delivery_timestamp)
            future_delivery_timestamp = runtime_health_payload()
            future_delivery_timestamp["gateway_activity"][
                "last_terminal_message_delivered_at"
            ] = "2026-05-22T09:30:03+08:00"
            future_delivery_timestamp_result = evaluate_runtime_health(future_delivery_timestamp)
            delivered_count_exceeds_emitted = runtime_health_payload()
            delivered_count_exceeds_emitted["gateway_activity"]["terminal_messages_emitted"] = 1
            delivered_count_exceeds_emitted["gateway_activity"]["terminal_messages_delivered"] = 2
            delivered_count_exceeds_emitted_result = evaluate_runtime_health(delivered_count_exceeds_emitted)
            emitted_count_exceeds_sources = runtime_health_payload()
            emitted_count_exceeds_sources["gateway_activity"]["terminal_messages_emitted"] = 2
            emitted_count_exceeds_sources["gateway_activity"]["terminal_messages_delivered"] = 1
            emitted_count_exceeds_sources_result = evaluate_runtime_health(emitted_count_exceeds_sources)
            gateway_activity_exceeds_raw_consumer = runtime_health_payload()
            gateway_activity_exceeds_raw_consumer["gateway_activity"]["processed_records_consumed"] = 2
            gateway_activity_exceeds_raw_consumer["gateway_activity"]["shadow_processed_records_drained"] = 2
            gateway_activity_exceeds_raw_consumer["gateway_activity"]["direct_runtime_messages_emitted"] = 2
            gateway_activity_exceeds_raw_consumer["gateway_activity"]["terminal_messages_emitted"] = 2
            gateway_activity_exceeds_raw_consumer_result = evaluate_runtime_health(gateway_activity_exceeds_raw_consumer)
            delivered_symbol_count_exceeds_deliveries = runtime_health_payload()
            delivered_symbol_count_exceeds_deliveries["subscription"]["subscribed_symbols"] = ["00700.HK", "00939.HK"]
            delivered_symbol_count_exceeds_deliveries["redis_snapshot"]["checked_symbols"] = ["00700.HK", "00939.HK"]
            delivered_symbol_count_exceeds_deliveries["redis_snapshot"]["present_symbols"] = ["00700.HK", "00939.HK"]
            delivered_symbol_count_exceeds_deliveries["health"]["collector"]["symbol_freshness"]["00939.HK"] = {
                "subscribed": True,
                "degraded": False,
                "latest_event_at": "2026-05-22T09:30:00+08:00",
            }
            delivered_symbol_count_exceeds_deliveries["gateway_activity"]["terminal_messages_delivered"] = 1
            delivered_symbol_count_exceeds_deliveries["gateway_activity"]["delivered_terminal_symbols"] = [
                "00700.HK",
                "00939.HK",
            ]
            delivered_symbol_count_exceeds_deliveries_result = evaluate_runtime_health(
                delivered_symbol_count_exceeds_deliveries
            )
            redis_trade_date_mismatch = runtime_health_payload()
            redis_trade_date_mismatch["redis_snapshot"]["trade_date"] = "20260523"
            redis_trade_date_mismatch_result = evaluate_runtime_health(redis_trade_date_mismatch)
            invalid_generated_at = runtime_health_payload()
            invalid_generated_at["generated_at"] = "20260522 093002"
            invalid_generated_at_result = evaluate_runtime_health(invalid_generated_at)
            invalid_latest_event = runtime_health_payload()
            invalid_latest_event["health"]["collector"]["symbol_freshness"]["00700.HK"][
                "latest_event_at"
            ] = "20260522 093000"
            invalid_latest_event_result = evaluate_runtime_health(invalid_latest_event)
            future_latest_event = runtime_health_payload()
            future_latest_event["health"]["collector"]["symbol_freshness"]["00700.HK"][
                "latest_event_at"
            ] = "2026-05-22T09:30:03+08:00"
            future_latest_event_result = evaluate_runtime_health(future_latest_event)
            missing_delivered_subscription = runtime_health_payload()
            missing_delivered_subscription["subscription"]["subscribed_symbols"] = ["00700.HK", "00939.HK"]
            missing_delivered_subscription["redis_snapshot"]["checked_symbols"] = ["00700.HK", "00939.HK"]
            missing_delivered_subscription["redis_snapshot"]["present_symbols"] = ["00700.HK", "00939.HK"]
            missing_delivered_subscription["health"]["collector"]["symbol_freshness"]["00939.HK"] = {
                "subscribed": True,
                "degraded": False,
                "latest_event_at": "2026-05-22T09:30:00+08:00",
            }
            missing_delivered_subscription_result = evaluate_runtime_health(missing_delivered_subscription)
            invalid_delivered_symbols = runtime_health_payload()
            invalid_delivered_symbols["gateway_activity"]["delivered_terminal_symbols"] = "00700.HK"
            invalid_delivered_symbols_result = evaluate_runtime_health(invalid_delivered_symbols)
            bad_format_delivered_symbols = runtime_health_payload()
            bad_format_delivered_symbols["gateway_activity"]["delivered_terminal_symbols"] = ["700"]
            bad_format_delivered_symbols_result = evaluate_runtime_health(bad_format_delivered_symbols)
            duplicate_delivered_symbols = runtime_health_payload()
            duplicate_delivered_symbols["gateway_activity"]["delivered_terminal_symbols"] = ["00700.HK", "00700.HK"]
            duplicate_delivered_symbols_result = evaluate_runtime_health(duplicate_delivered_symbols)
            extra_delivered_symbols = runtime_health_payload()
            extra_delivered_symbols["gateway_activity"]["delivered_terminal_symbols"] = ["00700.HK", "00939.HK"]
            extra_delivered_symbols_result = evaluate_runtime_health(extra_delivered_symbols)
            invalid_supervisor_started_at = runtime_health_payload()
            invalid_supervisor_started_at["supervisor"]["started_at"] = "20260522 093000"
            invalid_supervisor_started_at_result = evaluate_runtime_health(invalid_supervisor_started_at)
            future_supervisor_started_at = runtime_health_payload()
            future_supervisor_started_at["supervisor"]["started_at"] = "2026-05-22T09:30:03+08:00"
            future_supervisor_started_at_result = evaluate_runtime_health(future_supervisor_started_at)
            invalid_supervisor_last_tick_at = runtime_health_payload()
            invalid_supervisor_last_tick_at["supervisor"]["last_tick_at"] = "20260522 093001"
            invalid_supervisor_last_tick_at_result = evaluate_runtime_health(invalid_supervisor_last_tick_at)
            future_supervisor_last_tick_at = runtime_health_payload()
            future_supervisor_last_tick_at["supervisor"]["last_tick_at"] = "2026-05-22T09:30:03+08:00"
            future_supervisor_last_tick_at_result = evaluate_runtime_health(future_supervisor_last_tick_at)
            last_tick_before_start = runtime_health_payload()
            last_tick_before_start["supervisor"]["last_tick_at"] = "2026-05-22T09:29:59+08:00"
            last_tick_before_start_result = evaluate_runtime_health(last_tick_before_start)
            stopped_while_running = runtime_health_payload()
            stopped_while_running["running"] = True
            stopped_while_running["supervisor"]["stopped_at"] = "2026-05-22T09:30:01+08:00"
            stopped_while_running_result = evaluate_runtime_health(stopped_while_running)
            invalid_supervisor_stopped_at = runtime_health_payload()
            invalid_supervisor_stopped_at["supervisor"]["stopped_at"] = "20260522 093001"
            invalid_supervisor_stopped_at_result = evaluate_runtime_health(invalid_supervisor_stopped_at)

        self.assertTrue(passed["passed"])
        self.assertTrue(persisted["passed"])
        self.assertTrue(persisted_file["passed"])
        self.assertNotIn("runtime_health_gateway_no_terminal_message_source", direct_gateway_result["blockers"])
        self.assertNotIn("runtime_health_processed_topic_committed_offset_below_gateway_consumed", direct_gateway_result["blockers"])
        self.assertFalse(failed["passed"])
        self.assertIn("runtime_health_generated_at_missing", failed["blockers"])
        self.assertIn("runtime_health_generated_at_invalid", invalid_generated_at_result["blockers"])
        self.assertIn("runtime_health_kafka_lag_present", failed["blockers"])
        self.assertIn("runtime_health_supervisor_no_ticks", failed["blockers"])
        self.assertIn("runtime_health_supervisor_no_ingested_events", failed["blockers"])
        self.assertIn("runtime_health_supervisor_no_processed_events", failed["blockers"])
        self.assertIn("runtime_health_supervisor_counter_invalid", invalid_counters_result["blockers"])
        self.assertIn("runtime_health_supervisor_started_at_missing", failed["blockers"])
        self.assertIn("runtime_health_supervisor_last_tick_at_missing", failed["blockers"])
        self.assertIn("runtime_health_supervisor_started_at_invalid", invalid_supervisor_started_at_result["blockers"])
        self.assertIn(
            "runtime_health_supervisor_started_after_generated_at",
            future_supervisor_started_at_result["blockers"],
        )
        self.assertIn("runtime_health_supervisor_last_tick_at_invalid", invalid_supervisor_last_tick_at_result["blockers"])
        self.assertIn(
            "runtime_health_supervisor_last_tick_after_generated_at",
            future_supervisor_last_tick_at_result["blockers"],
        )
        self.assertIn("runtime_health_supervisor_last_tick_before_start", last_tick_before_start_result["blockers"])
        self.assertIn("runtime_health_supervisor_stopped_while_running", stopped_while_running_result["blockers"])
        self.assertIn("runtime_health_supervisor_stopped_at_invalid", invalid_supervisor_stopped_at_result["blockers"])
        self.assertIn("runtime_health_callback_backlog_present", failed["blockers"])
        self.assertIn("runtime_health_callback_backlog_invalid", invalid_counters_result["blockers"])
        self.assertIn("runtime_health_callback_rejections_present", failed["blockers"])
        self.assertIn("runtime_health_callback_rejections_invalid", invalid_counters_result["blockers"])
        self.assertIn("runtime_health_callback_rejection_path_invalid", failed["blockers"])
        self.assertIn("runtime_health_raw_consumer_dead_letter_path_invalid", failed["blockers"])
        self.assertIn("runtime_health_worker_dead_letters_present", failed["blockers"])
        self.assertIn("runtime_health_worker_processed_invalid", invalid_counters_result["blockers"])
        self.assertIn("runtime_health_ingest_worker_no_processed_events", failed["blockers"])
        self.assertIn("runtime_health_raw_consumer_no_processed_events", failed["blockers"])
        self.assertIn("runtime_health_producer_dead_letters_present", failed["blockers"])
        self.assertIn("runtime_health_producer_dead_letters_invalid", invalid_counters_result["blockers"])
        self.assertIn("runtime_health_producer_spooled_records_present", failed["blockers"])
        self.assertIn("runtime_health_producer_spooled_records_invalid", invalid_counters_result["blockers"])
        self.assertIn("runtime_health_producer_quarantined_spool_records_present", failed["blockers"])
        self.assertIn("runtime_health_producer_quarantined_spool_records_invalid", invalid_counters_result["blockers"])
        self.assertIn("runtime_health_producer_publish_attempts_invalid", invalid_counters_result["blockers"])
        self.assertIn(
            "runtime_health_producer_publish_attempts_below_worker_activity",
            low_publish_attempts_result["blockers"],
        )
        self.assertEqual(low_publish_attempts_result["evidence"]["producer_expected_min_publish_attempts"], 2)
        self.assertIn("runtime_health_producer_spool_path_invalid", failed["blockers"])
        self.assertIn("runtime_health_producer_spool_quarantine_path_invalid", failed["blockers"])
        self.assertIn("runtime_health_producer_no_publish_attempts", failed["blockers"])
        self.assertIn("runtime_health_redis_write_failures_present", failed["blockers"])
        self.assertIn("runtime_health_redis_write_count_invalid", invalid_counters_result["blockers"])
        self.assertIn("runtime_health_redis_write_failures_invalid", invalid_counters_result["blockers"])
        self.assertIn("runtime_health_redis_last_latency_invalid", invalid_counters_result["blockers"])
        self.assertIn("runtime_health_redis_max_latency_invalid", invalid_counters_result["blockers"])
        self.assertIn("runtime_health_redis_latency_order_invalid", invalid_redis_latency_order_result["blockers"])
        self.assertIn("runtime_health_symbol_runtime_degraded", failed["blockers"])
        self.assertIn("runtime_health_symbol_runtime_hydration_failures_present", failed["blockers"])
        self.assertIn("runtime_health_symbol_runtime_capacity_rejections_present", failed["blockers"])
        self.assertIn("runtime_health_symbol_runtime_manager_capacity_rejections_present", failed["blockers"])
        self.assertIn("runtime_health_symbol_runtime_manager_state_sink_failures_present", failed["blockers"])
        self.assertIn("runtime_health_symbol_runtime_manager_snapshot_sink_failures_present", failed["blockers"])
        self.assertIn("runtime_health_symbol_runtime_hydration_metric_invalid", invalid_counters_result["blockers"])
        self.assertIn("runtime_health_symbol_runtime_manager_metric_invalid", invalid_counters_result["blockers"])
        self.assertIn("runtime_health_symbol_runtime_missing", missing_symbol_runtime_result["blockers"])
        self.assertIn("runtime_health_symbol_runtime_manager_missing", missing_symbol_runtime_manager_result["blockers"])
        self.assertIn(
            "runtime_health_symbol_runtime_manager_runtime_count_mismatch",
            mismatched_symbol_runtime_summary_result["blockers"],
        )
        self.assertIn(
            "runtime_health_symbol_runtime_manager_ref_count_mismatch",
            mismatched_symbol_runtime_summary_result["blockers"],
        )
        self.assertIn(
            "runtime_health_symbol_runtime_manager_active_hydrations_mismatch",
            mismatched_symbol_runtime_summary_result["blockers"],
        )
        self.assertIn(
            "runtime_health_symbol_runtime_manager_state_counts_mismatch",
            mismatched_symbol_runtime_summary_result["blockers"],
        )
        self.assertIn(
            "runtime_health_symbol_runtime_manager_hydrating_symbols_mismatch",
            mismatched_symbol_runtime_summary_result["blockers"],
        )
        self.assertIn(
            "runtime_health_symbol_runtime_manager_realtime_attached_symbols_mismatch",
            mismatched_symbol_runtime_summary_result["blockers"],
        )
        self.assertIn(
            "runtime_health_symbol_runtime_symbol_mismatch",
            mismatched_symbol_runtime_symbol_result["blockers"],
        )
        self.assertIn(
            "runtime_health_symbol_runtime_symbol_format_invalid",
            invalid_symbol_runtime_symbol_result["blockers"],
        )
        self.assertIn("runtime_health_subscription_not_running", failed["blockers"])
        self.assertIn("runtime_health_subscription_symbols_empty", failed["blockers"])
        self.assertIn("runtime_health_redis_snapshot_missing_symbols", failed["blockers"])
        self.assertIn("runtime_health_redis_snapshot_trade_date_mismatch", failed["blockers"])
        self.assertIn("runtime_health_redis_snapshot_trade_date_mismatch", redis_trade_date_mismatch_result["blockers"])
        self.assertIn("runtime_health_gateway_websocket_not_running", failed["blockers"])
        self.assertIn("runtime_health_gateway_host_invalid", failed["blockers"])
        self.assertIn("runtime_health_gateway_host_loopback", loopback_gateway_result["blockers"])
        self.assertIn("runtime_health_gateway_port_invalid", failed["blockers"])
        self.assertIn("runtime_health_gateway_websocket_path_mismatch", failed["blockers"])
        self.assertIn("runtime_health_gateway_request_schema_version_mismatch", failed["blockers"])
        self.assertIn("runtime_health_gateway_protocol_mismatch", failed["blockers"])
        self.assertIn("runtime_health_gateway_no_terminal_message_source", failed["blockers"])
        self.assertIn("runtime_health_gateway_activity_counter_invalid", invalid_counters_result["blockers"])
        self.assertIn("runtime_health_gateway_no_terminal_messages_emitted", failed["blockers"])
        self.assertIn("runtime_health_gateway_no_terminal_messages_delivered", failed["blockers"])
        self.assertIn(
            "runtime_health_gateway_delivery_missing_subscribed_symbols",
            missing_delivered_subscription_result["blockers"],
        )
        self.assertEqual(
            missing_delivered_subscription_result["evidence"]["gateway_activity"]["missing_delivered_subscribed_symbols"],
            ["00939.HK"],
        )
        self.assertIn(
            "runtime_health_gateway_delivered_symbols_invalid",
            invalid_delivered_symbols_result["blockers"],
        )
        self.assertIn(
            "runtime_health_gateway_delivered_symbol_format_invalid",
            bad_format_delivered_symbols_result["blockers"],
        )
        self.assertIn(
            "runtime_health_gateway_delivered_symbols_duplicate",
            duplicate_delivered_symbols_result["blockers"],
        )
        self.assertIn(
            "runtime_health_gateway_delivered_unsubscribed_symbols",
            extra_delivered_symbols_result["blockers"],
        )
        self.assertIn(
            "runtime_health_gateway_terminal_delivery_timestamp_missing",
            missing_delivery_timestamp_result["blockers"],
        )
        self.assertIn(
            "runtime_health_gateway_terminal_delivery_timestamp_invalid",
            invalid_delivery_timestamp_result["blockers"],
        )
        self.assertIn(
            "runtime_health_gateway_terminal_delivery_after_generated_at",
            future_delivery_timestamp_result["blockers"],
        )
        self.assertIn(
            "runtime_health_gateway_delivery_count_exceeds_emitted",
            delivered_count_exceeds_emitted_result["blockers"],
        )
        self.assertIn(
            "runtime_health_gateway_emitted_count_exceeds_sources",
            emitted_count_exceeds_sources_result["blockers"],
        )
        self.assertEqual(
            emitted_count_exceeds_sources_result["evidence"]["gateway_activity"][
                "expected_max_terminal_messages_emitted"
            ],
            1,
        )
        self.assertIn(
            "runtime_health_gateway_processed_consumed_exceeds_raw_consumer_processed",
            gateway_activity_exceeds_raw_consumer_result["blockers"],
        )
        self.assertIn(
            "runtime_health_gateway_shadow_drained_exceeds_raw_consumer_processed",
            gateway_activity_exceeds_raw_consumer_result["blockers"],
        )
        self.assertIn(
            "runtime_health_gateway_direct_emitted_exceeds_raw_consumer_processed",
            gateway_activity_exceeds_raw_consumer_result["blockers"],
        )
        self.assertIn(
            "runtime_health_gateway_delivered_symbol_count_exceeds_deliveries",
            delivered_symbol_count_exceeds_deliveries_result["blockers"],
        )
        self.assertIn("runtime_health_octopus_redis_not_connected", failed["blockers"])
        self.assertIn("runtime_health_symbol_freshness_degraded", failed["blockers"])
        self.assertIn("runtime_health_symbol_freshness_not_subscribed", stale_freshness_result["blockers"])
        self.assertIn("runtime_health_symbol_freshness_latest_event_missing", stale_freshness_result["blockers"])
        self.assertIn(
            "runtime_health_symbol_freshness_latest_event_invalid",
            invalid_latest_event_result["blockers"],
        )
        self.assertEqual(
            invalid_latest_event_result["evidence"]["freshness_invalid_latest_event_symbols"],
            ["00700.HK"],
        )
        self.assertIn(
            "runtime_health_symbol_freshness_latest_event_after_generated_at",
            future_latest_event_result["blockers"],
        )
        self.assertEqual(
            future_latest_event_result["evidence"]["freshness_future_latest_event_symbols"],
            ["00700.HK"],
        )
        self.assertIn("runtime_health_missing_required_topics", missing_topic_result["blockers"])
        self.assertIn("runtime_health_topic_committed_offset_missing", missing_committed_offset_result["blockers"])
        self.assertIn("runtime_health_topic_committed_offset_invalid", invalid_committed_offset_result["blockers"])
        self.assertIn("runtime_health_topic_lag_missing", missing_lag_result["blockers"])
        self.assertIn("runtime_health_topic_lag_invalid", invalid_lag_result["blockers"])
        self.assertIn(
            "runtime_health_raw_topic_committed_offset_below_ingest_processed",
            low_committed_offsets_result["blockers"],
        )
        self.assertIn(
            "runtime_health_processed_topic_committed_offset_below_raw_consumer_processed",
            low_committed_offsets_result["blockers"],
        )
        self.assertIn(
            "runtime_health_processed_topic_committed_offset_below_gateway_consumed",
            low_committed_offsets_result["blockers"],
        )
        self.assertIn("runtime_health_redis_snapshot_probe_empty", empty_snapshot_probe_result["blockers"])
        self.assertIn(
            "runtime_health_subscription_symbol_format_invalid",
            invalid_subscription_symbols_result["blockers"],
        )
        self.assertIn(
            "runtime_health_subscription_symbols_duplicate",
            duplicate_subscription_symbols_result["blockers"],
        )
        self.assertIn(
            "runtime_health_redis_snapshot_symbol_list_invalid",
            invalid_snapshot_symbols_result["blockers"],
        )
        self.assertIn(
            "runtime_health_redis_snapshot_symbol_list_duplicate",
            duplicate_snapshot_symbols_result["blockers"],
        )
        self.assertIn(
            "runtime_health_redis_snapshot_symbol_status_conflict",
            conflicting_snapshot_symbols_result["blockers"],
        )
        self.assertIn(
            "runtime_health_redis_snapshot_checked_symbols_unresolved",
            unresolved_snapshot_symbols_result["blockers"],
        )
        self.assertIn(
            "runtime_health_redis_snapshot_result_symbols_unchecked",
            unchecked_snapshot_symbols_result["blockers"],
        )
        self.assertIn(
            "runtime_health_redis_snapshot_probe_missing_subscribed_symbols",
            missing_checked_subscription_result["blockers"],
        )
        self.assertIn(
            "runtime_health_redis_snapshot_subscribed_symbols_not_present",
            missing_checked_subscription_result["blockers"],
        )
        self.assertIn(
            "runtime_health_redis_snapshot_subscribed_symbols_not_present",
            missing_present_subscription_result["blockers"],
        )
        self.assertIn(
            "runtime_health_redis_snapshot_key_family_missing_symbols",
            missing_key_family_result["blockers"],
        )
        self.assertIn(
            "runtime_health_redis_snapshot_key_family_subscribed_symbols_not_present",
            missing_key_family_result["blockers"],
        )
        self.assertIn(
            "runtime_health_redis_snapshot_key_family_coverage_missing",
            missing_key_family_coverage_result["blockers"],
        )
        self.assertIn(
            "runtime_health_redis_snapshot_required_key_families_invalid",
            missing_required_key_family_result["blockers"],
        )
        self.assertIn(
            "runtime_health_redis_snapshot_history_keys_missing",
            missing_history_key_result["blockers"],
        )
        self.assertIn(
            "runtime_health_redis_snapshot_history_participants_missing",
            missing_history_participants_result["blockers"],
        )
        self.assertIn(
            "runtime_health_redis_snapshot_key_family_updated_at_missing",
            missing_key_family_updated_at_result["blockers"],
        )
        self.assertIn(
            "runtime_health_redis_snapshot_key_family_updated_at_invalid",
            invalid_key_family_updated_at_result["blockers"],
        )
        self.assertIn(
            "runtime_health_redis_snapshot_key_family_updated_at_after_generated_at",
            future_key_family_updated_at_result["blockers"],
        )
        self.assertIn(
            "runtime_health_redis_snapshot_key_family_ttl_missing",
            missing_key_family_ttl_result["blockers"],
        )
        self.assertIn(
            "runtime_health_redis_snapshot_key_family_ttl_invalid",
            invalid_key_family_ttl_result["blockers"],
        )
        self.assertIn(
            "runtime_health_redis_snapshot_key_family_contract_evidence_missing",
            missing_key_family_contract_result["blockers"],
        )
        self.assertIn(
            "runtime_health_redis_snapshot_key_family_contract_invalid",
            invalid_key_family_contract_result["blockers"],
        )
        self.assertIn("runtime_health_gateway_client_queue_missing", missing_client_queue_result["blockers"])
        self.assertIn(
            "runtime_health_gateway_client_queue_client_ids_invalid",
            missing_declared_client_ids_result["blockers"],
        )
        self.assertIn(
            "runtime_health_gateway_client_queue_client_ids_invalid",
            whitespace_declared_client_ids_result["blockers"],
        )
        self.assertIn(
            "runtime_health_gateway_client_queue_client_ids_invalid",
            mismatched_declared_client_count_result["blockers"],
        )
        self.assertIn("runtime_health_performance_samples_invalid", invalid_performance_samples_result["blockers"])
        self.assertIn("runtime_health_performance_samples_missing", missing_performance_samples_result["blockers"])
        self.assertIn("runtime_health_performance_samples_empty", empty_performance_samples_result["blockers"])
        self.assertIn("runtime_health_subscribe_snapshot_p95_exceeded", slow_subscribe_snapshot_result["blockers"])
        self.assertEqual(passed["evidence"]["performance_samples"]["sample_counts"]["subscribe_snapshot_ms"], 3)
        self.assertIn("runtime_health_gateway_alert_drops_present", alert_dropped_result["blockers"])
        self.assertTrue(noncritical_dropped_result["evidence"]["gateway_activity"]["client_queue"]["noncritical_drops_present"])
        self.assertTrue(noncritical_dropped_result["evidence"]["gateway_activity"]["client_queue"]["alert_overflow_present"])
        self.assertNotIn("runtime_health_gateway_alert_drops_present", noncritical_dropped_result["blockers"])
        self.assertIn("runtime_health_gateway_critical_overflow_present", critical_overflow_result["blockers"])
        self.assertTrue(critical_overflow_result["evidence"]["gateway_activity"]["client_queue"]["critical_overflow_present"])
        self.assertIn(
            "runtime_health_gateway_client_queue_total_backlog_mismatch",
            backlog_mismatch_result["blockers"],
        )
        self.assertIn("runtime_health_supervisor_stop_reason_invalid", invalid_stop_reason_result["blockers"])
        self.assertIn("runtime_health_missing_subscription", missing_subscription_result["blockers"])
        self.assertIn("runtime_health_queues_missing", missing_queues_result["blockers"])
        self.assertIn("runtime_health_symbol_freshness_missing", missing_freshness_result["blockers"])


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
                    "legacy_count": 2,
                    "v2_count": 2,
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
        "legacy_event_count": 2,
        "v2_event_count": 2,
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
            "legacy_event_count": 2,
            "v2_event_count": 2,
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


def write_complete_evidence_bundle(root: Path) -> EvidenceBundlePaths:
    shadow_reports = root / "shadow-reports"
    manifests = root / "manifests"
    shadow_reports.mkdir()
    manifests.mkdir()
    report = passing_report()
    write_shadow_source_files(root / "shadow-sources", report)
    save_shadow_run_report(report, shadow_reports)
    write_required_manifests(manifests)
    runtime_health_path = root / "runtime-health.json"
    runtime_config_path = root / "runtime-config.json"
    readiness_path = root / "cutover-readiness.json"
    frontend_path = root / "frontend-deployment.json"
    decommission_path = root / "legacy-decommission.json"
    retirement_path = root / "legacy-retirement.json"
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
        "policy": default_cutover_policy_payload(),
    }
    runtime_config_path.write_text(json.dumps(runtime_config_payload(root)), encoding="utf-8")
    runtime_health_path.write_text(json.dumps(runtime_health_payload(for_completed_shadow_session=True)), encoding="utf-8")
    readiness_path.write_text(json.dumps(readiness), encoding="utf-8")
    frontend_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
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
            }
        ),
        encoding="utf-8",
    )
    decommission_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
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
            }
        ),
        encoding="utf-8",
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
        output_path=retirement_path,
    )
    return EvidenceBundlePaths(
        shadow_reports_directory=shadow_reports,
        manifest_directory=manifests,
        runtime_config_path=runtime_config_path,
        runtime_health_path=runtime_health_path,
        readiness_path=readiness_path,
        frontend_deployment_path=frontend_path,
        legacy_decommission_path=decommission_path,
        legacy_retirement_path=retirement_path,
    )


def default_cutover_policy_payload() -> dict:
    return {
        "min_parallel_session_count": 1,
        "min_session_duration_seconds": 14400,
        "min_stream_coverage_ratio": 0.9,
        "require_non_empty_streams": True,
        "require_no_failed_symbols": True,
        "allow_legacy_retirement": True,
    }


def runtime_config_payload(root: Path) -> dict:
    silver_root = root / "silver"
    silver_root.mkdir(exist_ok=True)
    return {
        "schema_version": 1,
        "trade_date": "20260522",
        "silver_root": str(silver_root),
        "gateway": {"host": "0.0.0.0", "port": 9020, "path": "/ws"},
        "kafka": {
            "raw_topic": "raw_market_events_v1",
            "processed_topic": "processed_market_events_v1",
            "consumer_group": "beast-terminal-v2",
            "poll_timeout_ms": 1000,
            "auto_offset_reset": "latest",
        },
        "redis": {"terminal_ttl_seconds": 28800, "history_ttl_seconds": 2592000},
        "runtime": {
            "raw_queue_max_size": 10000,
            "client_queue_size": 100,
            "kafka_retries": 3,
            "symbol_eviction_grace_seconds": 300,
            "max_concurrent_hydrations": 8,
            "big_trade_volume_baseline_ratio": 0.0005,
            "install_signal_handlers": True,
        },
        "freshness": {"max_event_age_seconds": 60, "max_queue_backlog": 1000},
        "production_clients": {
            "duckdb_connection": True,
            "kafka_producer": True,
            "kafka_consumer": True,
            "redis_client": True,
            "market_data_client": True,
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
                "process": "running",
                "symbol_freshness": {
                    "00700.HK": {
                        "subscribed": True,
                        "degraded": False,
                        "latest_event_at": latest_event_at,
                    }
                },
            },
            "octopus": {"redis": "connected"},
            "gateway": {"redis": "connected"},
        },
    }


def multi_trader_smoke_payload() -> dict:
    return {
        "schema_version": 1,
        "observed_at": "2026-05-25T11:00:00+08:00",
        "clients": [
            {
                "machine_id": "desk-a",
                "data_source_mode": "live",
                "page_url": "http://gateway.internal:5173/",
                "gateway_url": "ws://gateway.internal:9020/ws",
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
                "page_url": "http://gateway.internal:5173/",
                "gateway_url": "ws://gateway.internal:9020/ws",
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
        "metrics": {
            "warm_snapshot_p95_ms": 150.0,
            "duplicate_hydrations": 0,
        },
        "runtime_health": {
            "passed": True,
            "path": "artifacts/runtime-health.json",
            "generated_at": "2026-05-25T10:59:00+08:00",
            "gateway_websocket": gateway_smoke_evidence(),
            "gateway_activity": {"client_queue": gateway_client_queue_payload()},
        },
        "preflight": multi_trader_smoke_preflight_payload(),
    }


def multi_trader_smoke_preflight_payload() -> dict:
    return {
        "schema_version": 1,
        "prepared_at": "2026-05-25T10:55:00+08:00",
        "passed": True,
        "blockers": [],
        "lan_host": "gateway.internal",
        "frontend_port": 5173,
        "gateway_port": 9020,
        "page_url": "http://gateway.internal:5173/",
        "gateway_url": "ws://gateway.internal:9020/ws",
        "service_checks": multi_trader_smoke_service_checks_payload(),
    }


def multi_trader_smoke_service_checks_payload() -> dict:
    return {
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
    }


def write_smoke_run_manifest(root: Path, *paths: Path) -> Path:
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
    manifest_path = root / "smoke-run-manifest.json"
    manifest_path.write_text(
        json.dumps({"schema_version": 1, "file_count": len(files), "files": files}),
        encoding="utf-8",
    )
    return manifest_path


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


def write_required_manifests(
    directory: Path,
    *,
    start: str = "20260522",
    end: str = "20260522",
    symbols: list[str] | None = None,
) -> None:
    for data_type in ["daily_bars", "minute_bars", "trade_ticks", "ccass_holdings", "participant_history", "broker_queue", "broker_mapping"]:
        write_manifest(directory, data_type, start=start, end=end, symbols=symbols)


def write_manifest(
    directory: Path,
    data_type: str,
    *,
    start: str = "20260522",
    end: str = "20260522",
    symbols: list[str] | None = None,
) -> None:
    (directory / f"{data_type}.{start}-{end}.v2.manifest.json").write_text(
        json.dumps(manifest_payload(data_type, start=start, end=end, symbols=symbols)),
        encoding="utf-8",
    )


def write_shadow_source_files(directory: Path, report: dict) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    source_files = {
        "metadata": directory / "20260522.session-1.metadata.json",
        "legacy_events": directory / "20260522.session-1.legacy.ndjson",
        "v2_events": directory / "20260522.session-1.v2.ndjson",
        "performance_samples": directory / "20260522.session-1.performance.ndjson",
    }
    source_files["metadata"].write_text(
        json.dumps(
            {
                "schema_version": 1,
                "session_id": report["session_id"],
                "trading_date": report["trading_date"],
                "started_at": report["started_at"],
            }
        ),
        encoding="utf-8",
    )
    source_files["legacy_events"].write_text(
        "".join(
            json.dumps(shadow_source_event(f"legacy-{index + 1}", seq=index + 1)) + "\n"
            for index in range(report["legacy_event_count"])
        ),
        encoding="utf-8",
    )
    source_files["v2_events"].write_text(
        "".join(
            json.dumps(shadow_source_event(f"v2-{index + 1}", seq=index + 1)) + "\n"
            for index in range(report["v2_event_count"])
        ),
        encoding="utf-8",
    )
    performance_lines = []
    for key, count in report["performance"]["sample_counts"].items():
        performance_lines.extend(json.dumps({"key": key, "value_ms": 1.0}) + "\n" for _ in range(count))
    source_files["performance_samples"].write_text("".join(performance_lines), encoding="utf-8")
    report["evidence_source"]["files"] = {key: str(path) for key, path in source_files.items()}


def shadow_source_event(event_id: str, *, seq: int) -> dict:
    return {
        "event_id": event_id,
        "symbol": "00700.HK",
        "seq": seq,
        "source_ts": "2026-05-22T09:30:00+08:00",
        "ingest_ts": "2026-05-22T09:30:00.010+08:00",
    }


def manifest_payload(
    data_type: str,
    *,
    start: str = "20260522",
    end: str = "20260522",
    symbols: list[str] | None = None,
) -> dict:
    source_data_type = "ccass_holdings" if data_type == "participant_history" else data_type
    scoped_symbols = [] if data_type == "broker_mapping" else list(symbols or ["00700.HK"])
    return {
        "schema_version": 1,
        "data_type": data_type,
        "source_data_type": source_data_type,
        "table": f"silver_{source_data_type}_v1",
        "date_range": {"start": start, "end": end},
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


if __name__ == "__main__":
    unittest.main()
