from __future__ import annotations

import json
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

from beast_market.healthcheck import evaluate_runtime_health_file, evaluate_runtime_health_snapshot


class RuntimeHealthcheckTest(unittest.TestCase):
    def test_accepts_current_live_runtime_snapshot(self) -> None:
        now = datetime(2026, 5, 22, 1, 30, tzinfo=UTC)

        result = evaluate_runtime_health_snapshot(runtime_health_payload(now), now=now)

        self.assertTrue(result.healthy)
        self.assertEqual(result.blockers, [])

    def test_rejects_stale_stopped_and_degraded_runtime_snapshot(self) -> None:
        now = datetime(2026, 5, 22, 1, 31, tzinfo=UTC)
        payload = runtime_health_payload(now - timedelta(seconds=120))
        payload["running"] = False
        payload["runtime_state"] = "DEGRADED"
        payload["supervisor"]["stop_reason"] = "finished"
        payload["topics"]["raw_market_events_v1"]["lag"] = 3
        payload["producer"]["spooled_records"] = 1
        payload["redis"]["write_stats"]["failures"] = 1
        payload["gateway_websocket"]["running"] = False
        payload["health"]["gateway"]["process"] = "degraded"

        result = evaluate_runtime_health_snapshot(payload, now=now, max_age_seconds=60, max_topic_lag=0)

        self.assertFalse(result.healthy)
        self.assertIn("runtime_health_stale", result.blockers)
        self.assertIn("runtime_not_running", result.blockers)
        self.assertIn("runtime_state_not_live", result.blockers)
        self.assertIn("runtime_stop_reason_present", result.blockers)
        self.assertIn("topic_raw_market_events_v1_lag_exceeded", result.blockers)
        self.assertIn("producer_spooled_records_present", result.blockers)
        self.assertIn("redis_write_failures_present", result.blockers)
        self.assertIn("gateway_websocket_not_running", result.blockers)
        self.assertIn("gateway_degraded", result.blockers)

    def test_file_probe_reports_missing_and_unreadable_health_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            missing = evaluate_runtime_health_file(root / "missing.json")
            bad_path = root / "runtime-health.json"
            bad_path.write_text("{", encoding="utf-8")
            unreadable = evaluate_runtime_health_file(bad_path)

        self.assertFalse(missing.healthy)
        self.assertEqual(missing.blockers, ["runtime_health_missing"])
        self.assertFalse(unreadable.healthy)
        self.assertEqual(unreadable.blockers, ["runtime_health_unreadable"])

    def test_file_probe_reads_valid_health_artifact(self) -> None:
        now = datetime(2026, 5, 22, 1, 30, tzinfo=UTC)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "runtime-health.json"
            path.write_text(json.dumps(runtime_health_payload(now)), encoding="utf-8")
            result = evaluate_runtime_health_file(path, now=now)

        self.assertTrue(result.healthy)


def runtime_health_payload(generated_at: datetime) -> dict:
    return {
        "schema_version": 1,
        "generated_at": generated_at.isoformat(),
        "running": True,
        "runtime_state": "LIVE",
        "supervisor": {
            "last_tick_at": "2026-05-22T09:30:00+08:00",
            "stop_reason": None,
        },
        "topics": {
            "raw_market_events_v1": {"lag": 0},
            "processed_market_events_v1": {"lag": 0},
        },
        "producer": {
            "dead_letters": 0,
            "spooled_records": 0,
        },
        "redis": {
            "write_stats": {"failures": 0},
        },
        "gateway_websocket": {
            "running": True,
        },
        "health": {
            "collector": {"process": "running"},
            "octopus": {"process": "running"},
            "gateway": {"process": "running"},
        },
    }


if __name__ == "__main__":
    unittest.main()
