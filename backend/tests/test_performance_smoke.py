import tempfile
import unittest
from pathlib import Path

from beast_market.performance_smoke import (
    BackendPerformanceSmokeConfig,
    main as performance_smoke_main,
    run_backend_performance_smoke,
)


class BackendPerformanceSmokeTest(unittest.TestCase):
    def test_backend_performance_smoke_exercises_ten_clients_and_two_hundred_symbols(self) -> None:
        result = run_backend_performance_smoke(
            BackendPerformanceSmokeConfig(
                client_count=10,
                symbol_count=200,
                overlap_symbol_count=20,
                trade_date="20260526",
            )
        )

        self.assertTrue(result.passed)
        self.assertEqual(result.blockers, [])
        self.assertEqual(result.metrics["hydrate_symbol_count"], 200)
        self.assertEqual(result.metrics["duplicate_hydrations"], 0)
        self.assertEqual(result.metrics["cold_subscribe_sample_count"], 200)
        self.assertEqual(result.metrics["hot_subscribe_sample_count"], 180)
        self.assertEqual(result.metrics["max_overlap_ref_count"], 10)
        self.assertLessEqual(result.metrics["warm_subscribe_p95_ms"], 200)
        self.assertLessEqual(result.metrics["hot_subscribe_p95_ms"], 100)
        self.assertEqual(result.symbol_runtime_manager["runtime_count"], 200)
        self.assertEqual(result.client_queue["connected_clients"], 10)
        self.assertEqual(result.client_queue["critical_overflow"], 0)

    def test_backend_performance_smoke_cli_writes_output(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output_path = Path(directory) / "performance-smoke.json"

            exit_code = performance_smoke_main(
                [
                    "--client-count",
                    "2",
                    "--symbol-count",
                    "5",
                    "--overlap-symbol-count",
                    "2",
                    "--output-path",
                    str(output_path),
                ]
            )

            self.assertEqual(exit_code, 0)
            self.assertTrue(output_path.exists())
            self.assertIn('"passed": true', output_path.read_text(encoding="utf-8"))

    def test_backend_performance_smoke_rejects_invalid_overlap(self) -> None:
        with self.assertRaisesRegex(ValueError, "overlap_symbol_count"):
            run_backend_performance_smoke(
                BackendPerformanceSmokeConfig(client_count=2, symbol_count=1, overlap_symbol_count=2)
            )


if __name__ == "__main__":
    unittest.main()
