import ast
import importlib.util
import json
import sys
import unittest
from io import StringIO
from pathlib import Path
from contextlib import redirect_stdout
from tempfile import TemporaryDirectory

import pandas as pd

from beast_market import InMemoryEventBus, InMemoryRedisSnapshotCache, MammothAPI, OctopusComputeV2


REAL_DATA_RUNNER = Path(__file__).resolve().parents[1] / "tools" / "real_data_runner.py"
XTQUANT_EXPORT = Path(__file__).resolve().parents[1] / "tools" / "xtquant_silver_export.py"
PIPELINE = Path(__file__).resolve().parents[1] / "beast_market" / "pipeline.py"
GATEWAY_TRANSPORT = Path(__file__).resolve().parents[1] / "beast_market" / "gateway_transport.py"
BEAST_MARKET_PACKAGE = Path(__file__).resolve().parents[1] / "beast_market"


def load_tool_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class ToolDataPathTest(unittest.TestCase):
    def test_guardrail_gateway_does_not_hydrate_or_compute_business_state(self) -> None:
        pipeline_tree = ast.parse(PIPELINE.read_text(encoding="utf-8"))
        gateway_class = next(
            node
            for node in ast.walk(pipeline_tree)
            if isinstance(node, ast.ClassDef) and node.name == "GatewayV2"
        )
        forbidden_gateway_calls = sorted(
            {
                node.func.attr
                for node in ast.walk(gateway_class)
                if isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and (
                    node.func.attr.startswith("get_")
                    or node.func.attr in {"process_raw_event", "process_raw_event_with_state"}
                )
                and node.func.attr not in {"get", "get_terminal_snapshot"}
            }
        )
        forbidden_gateway_names = sorted(
            {
                node.id
                for node in ast.walk(gateway_class)
                if isinstance(node, ast.Name) and node.id in {"MammothAPI", "OctopusComputeV2", "XtQuant"}
            }
        )
        transport_tree = ast.parse(GATEWAY_TRANSPORT.read_text(encoding="utf-8"))
        forbidden_transport_names = sorted(
            {
                node.name
                for node in ast.walk(transport_tree)
                if isinstance(node, ast.alias) and node.name in {"MammothAPI", "OctopusComputeV2", "XtQuant"}
            }
        )

        self.assertEqual(forbidden_gateway_calls, [])
        self.assertEqual(forbidden_gateway_names, [])
        self.assertEqual(forbidden_transport_names, [])

    def test_guardrail_runtime_modules_do_not_bypass_mammoth_api_for_silver_reads(self) -> None:
        allowed_files = {BEAST_MARKET_PACKAGE / "mammoth_api.py"}
        violations: list[str] = []
        for path in sorted(BEAST_MARKET_PACKAGE.glob("*.py")):
            if path in allowed_files:
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr in {"read_table", "_read_table"}
                ):
                    violations.append(f"{path.name}:{node.lineno}:{node.func.attr}")

        self.assertEqual(violations, [])

    def test_guardrail_real_data_bootstrap_does_not_replay_ticks_through_chart_path(self) -> None:
        tree = ast.parse(REAL_DATA_RUNNER.read_text(encoding="utf-8"))
        bootstrap = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef) and node.name == "bootstrap_snapshots"
        )
        forbidden_calls = [
            node
            for node in ast.walk(bootstrap)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "process_raw_event"
        ]

        self.assertEqual(forbidden_calls, [])

    def test_guardrail_historical_tick_replay_does_not_mutate_chart_bars(self) -> None:
        tree = ast.parse(PIPELINE.read_text(encoding="utf-8"))
        replay = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef) and node.name == "process_historical_alert_event"
        )
        minute_bar_assignments = [
            node.lineno
            for node in ast.walk(replay)
            if isinstance(node, ast.Assign)
            for target in node.targets
            if (
                isinstance(target, ast.Subscript)
                and isinstance(target.slice, ast.Constant)
                and target.slice.value == "minute_bars"
            )
        ]
        minute_bar_updates = [
            node.lineno
            for node in ast.walk(replay)
            if isinstance(node, ast.Call)
            and (
                (isinstance(node.func, ast.Name) and node.func.id == "upsert_minute_bar")
                or (isinstance(node.func, ast.Attribute) and node.func.attr == "upsert_minute_bar")
            )
        ]

        self.assertEqual(minute_bar_assignments, [])
        self.assertEqual(minute_bar_updates, [])

    def test_xtquant_exporter_writes_native_minute_bar_silver_rows(self) -> None:
        exporter = load_tool_module(XTQUANT_EXPORT, "xtquant_silver_export_test")
        rows = exporter.minute_rows(
            {
                "00700.HK": pd.DataFrame(
                    [
                        {
                            "time": 1779413400000,
                            "open": 388.0,
                            "high": 389.0,
                            "low": 387.5,
                            "close": 388.4,
                            "volume": 1000,
                            "amount": 388400.0,
                        }
                    ]
                )
            }
        )

        self.assertEqual(exporter.TABLE_FILES["minute_bars"], "silver_minute_bars_v1.csv")
        self.assertEqual(rows[0]["symbol"], "00700.HK")
        self.assertEqual(rows[0]["trade_date"], "20260522")
        self.assertEqual(rows[0]["bar_ts"], "2026-05-22T09:30:00.000+08:00")
        self.assertEqual(rows[0]["source"], "xtquant.1m")

    def test_real_data_bootstrap_uses_minute_bars_for_chart_and_ticks_only_for_alerts(self) -> None:
        runner = load_tool_module(REAL_DATA_RUNNER, "real_data_runner_test")
        reader = StaticSilverReader()
        mammoth = MammothAPI(reader=reader)
        bus = InMemoryEventBus()
        cache = InMemoryRedisSnapshotCache()
        octopus = OctopusComputeV2(
            mammoth,
            bus,
            cache,
            big_trade_volume_baseline_ratio=0.0005,
            big_trade_min_volume_threshold=1,
            big_trade_turnover_threshold=100_000_000,
        )

        results = runner.bootstrap_snapshots(
            mammoth=mammoth,
            reader=reader,
            bus=bus,
            cache=cache,
            octopus=octopus,
            requested_trade_date="20260525",
        )
        snapshot = cache.get_terminal_snapshot("20260525", "00700.HK")

        self.assertEqual(results[0].effective_trade_date, "20260522")
        self.assertEqual(results[0].minute_count, 1)
        self.assertEqual(results[0].alert_count, 1)
        self.assertEqual(snapshot["minute_bars"][0]["timestamp"], "2026-05-22T09:30:00+08:00")
        self.assertEqual(snapshot["minute_bars"][0]["volume"], 1000)
        self.assertEqual(snapshot["snapshot"]["volume"], 1000)
        self.assertEqual(snapshot["alerts"][0]["participantName"], "Test Participant")

    def test_csv_silver_reader_adapter_caches_raw_tables_before_symbol_filtering(self) -> None:
        runner = load_tool_module(REAL_DATA_RUNNER, "real_data_runner_csv_cache_test")

        class CountingReader:
            def __init__(self) -> None:
                self.calls: list[str] = []

            def read_table(self, data_type: str) -> list[dict]:
                self.calls.append(data_type)
                if data_type == "broker_mapping":
                    return [{"broker_code": "001", "participant_name": "Broker"}]
                return [
                    {"symbol": "00700.HK", "value": "initial"},
                    {"symbol": "00005.HK", "value": "dynamic"},
                ]

        adapter = runner.CsvSilverReaderAdapter(Path("/tmp/unused"), symbols=["00700.HK"])
        counting_reader = CountingReader()
        adapter.reader = counting_reader

        initial_rows = adapter.read_table("daily_bars")
        repeated_rows = adapter.read_table("daily_bars")
        adapter.ensure_symbol("00005.HK")
        dynamic_rows = adapter.read_table("daily_bars")
        broker_rows = adapter.read_table("broker_mapping")
        repeated_broker_rows = adapter.read_table("broker_mapping")

        self.assertEqual(counting_reader.calls, ["daily_bars", "broker_mapping"])
        self.assertEqual([row["symbol"] for row in initial_rows], ["00700.HK"])
        self.assertEqual([row["symbol"] for row in repeated_rows], ["00700.HK"])
        self.assertEqual([row["symbol"] for row in dynamic_rows], ["00700.HK", "00005.HK"])
        self.assertEqual(broker_rows, repeated_broker_rows)

    def test_real_data_service_subscribe_uses_symbol_runtime_manager(self) -> None:
        runner = load_tool_module(REAL_DATA_RUNNER, "real_data_runner_service_test")
        reader = StaticSilverReader()

        _mammoth, _bus, _cache, service = runner.build_real_data_runtime(
            reader=reader,
            requested_trade_date="20260525",
            host="127.0.0.1",
            port=9020,
            path="/ws",
            big_trade_volume_baseline_ratio=0.0005,
            big_trade_min_volume_threshold=1,
            big_trade_turnover_threshold=100_000_000,
        )
        manager = service.manager
        runtime_manager = manager.symbol_runtime_manager

        self.assertIsNotNone(runtime_manager)
        self.assertIn("00700.HK", runtime_manager.runtimes)
        self.assertEqual(runtime_manager.runtimes["00700.HK"].hydrate_count, 0)

        manager.connect("client")
        manager.flush("client")
        manager.handle_message("client", {"schema_version": 1, "protocol": "terminal-message-v1", "action": "subscribe", "symbol": "00700.HK"})
        snapshot = json.loads(manager.flush("client")[0])

        self.assertEqual(snapshot["type"], "snapshot")
        self.assertEqual(snapshot["symbol"], "00700.HK")
        self.assertEqual(snapshot["payload"]["freshness"]["effective_trade_date"], "20260522")
        self.assertEqual(runtime_manager.runtimes["00700.HK"].hydrate_count, 0)
        self.assertEqual(runtime_manager.runtimes["00700.HK"].ref_count, 1)

    def test_real_data_service_can_hydrate_symbol_not_preloaded_at_startup(self) -> None:
        runner = load_tool_module(REAL_DATA_RUNNER, "real_data_runner_dynamic_symbol_test")
        reader = StaticSilverReader()

        _mammoth, _bus, _cache, service = runner.build_real_data_runtime(
            reader=reader,
            requested_trade_date="20260525",
            host="127.0.0.1",
            port=9020,
            path="/ws",
            big_trade_volume_baseline_ratio=0.0005,
            big_trade_min_volume_threshold=1,
            big_trade_turnover_threshold=100_000_000,
        )
        manager = service.manager
        runtime_manager = manager.symbol_runtime_manager

        self.assertEqual(reader.symbols, ["00700.HK"])
        self.assertNotIn("00005.HK", runtime_manager.runtimes)

        manager.connect("client")
        manager.flush("client")
        manager.handle_message("client", {"schema_version": 1, "protocol": "terminal-message-v1", "action": "subscribe", "symbol": "00005.HK"})
        snapshot = json.loads(manager.flush("client")[0])

        self.assertEqual(reader.symbols, ["00700.HK", "00005.HK"])
        self.assertEqual(snapshot["type"], "snapshot")
        self.assertEqual(snapshot["symbol"], "00005.HK")
        self.assertEqual(snapshot["payload"]["snapshot"]["name"], "00005.HK")
        self.assertEqual(snapshot["payload"]["freshness"]["effective_trade_date"], "20260522")
        self.assertEqual(snapshot["payload"]["alerts"][0]["participantName"], "Test Participant")
        self.assertEqual(snapshot["payload"]["ccass_holdings"][0]["participantName"], "Large Holder")
        self.assertEqual(runtime_manager.runtimes["00005.HK"].hydrate_count, 1)
        self.assertEqual(runtime_manager.runtimes["00005.HK"].ref_count, 1)

    def test_real_data_service_hot_subscribe_restores_cleared_runtime_cache(self) -> None:
        runner = load_tool_module(REAL_DATA_RUNNER, "real_data_runner_cache_restore_test")
        reader = StaticSilverReader()

        _mammoth, _bus, cache, service = runner.build_real_data_runtime(
            reader=reader,
            requested_trade_date="20260525",
            host="127.0.0.1",
            port=9020,
            path="/ws",
            big_trade_volume_baseline_ratio=0.0005,
        )
        manager = service.manager
        snapshot_key = "terminal:20260525:snapshot:00700.HK"
        minute_key = "terminal:20260525:minute:00700.HK"

        manager.connect("first")
        manager.flush("first")
        manager.handle_message("first", {"schema_version": 1, "protocol": "terminal-message-v1", "action": "subscribe", "symbol": "00700.HK"})
        manager.flush("first")
        del cache.values[snapshot_key]
        del cache.values[minute_key]

        manager.connect("second")
        manager.flush("second")
        manager.handle_message("second", {"schema_version": 1, "protocol": "terminal-message-v1", "action": "subscribe", "symbol": "00700.HK"})
        snapshot = json.loads(manager.flush("second")[0])

        self.assertEqual(snapshot["type"], "snapshot")
        self.assertEqual(snapshot["payload"]["snapshot"]["price"], 388.4)
        self.assertIn(snapshot_key, cache.values)
        self.assertIn(minute_key, cache.values)
        self.assertEqual(cache.values[minute_key]["data"][0]["volume"], 1000)

    def test_real_data_runner_defaults_to_lan_bind_and_prints_client_url_hint(self) -> None:
        runner = load_tool_module(REAL_DATA_RUNNER, "real_data_runner_lan_test")

        args = runner.parse_args([])
        output = StringIO()
        with redirect_stdout(output):
            runner.print_bootstrap_summary(
                [
                    runner.RealDataBootstrapResult(
                        symbol="00700.HK",
                        requested_trade_date="20260525",
                        effective_trade_date="20260522",
                        tick_count=1,
                        minute_count=1,
                        alert_count=0,
                        chart_source="20260522",
                        ccass_current_date="20260522",
                        ccass_previous_date="20260521",
                    )
                ],
                host=args.host,
                port=args.port,
                path=args.path,
            )
        payload = json.loads(output.getvalue())

        self.assertEqual(args.host, "0.0.0.0")
        self.assertEqual(args.min_chart_minute_bars, 1)
        self.assertFalse(args.skip_xtquant_refresh_on_start)
        self.assertEqual(payload["bind_host"], "0.0.0.0")
        self.assertEqual(payload["websocket_url"], "ws://0.0.0.0:9020/ws")
        self.assertEqual(payload["client_websocket_url"], "ws://<this-machine-lan-ip>:9020/ws")

    def test_real_data_runner_validates_trade_date_and_symbol_args(self) -> None:
        runner = load_tool_module(REAL_DATA_RUNNER, "real_data_runner_args_test")

        with self.assertRaises(SystemExit):
            runner.parse_args(["--trade-date", "2026-05-22"])
        with self.assertRaises(SystemExit):
            runner.parse_args(["--trade-date", "20260230"])
        with self.assertRaises(SystemExit):
            runner.parse_args(["--symbols", ""])
        with self.assertRaises(SystemExit):
            runner.parse_args(["--symbols", "ABC"])

        args = runner.parse_args(["--trade-date", "20260522", "--symbols", "700,00939.HK,700"])

        self.assertEqual(args.trade_date, "20260522")
        self.assertEqual(args.symbols, ["00700.HK", "00939.HK"])

    def test_real_data_runner_validates_gateway_and_health_interval_args(self) -> None:
        runner = load_tool_module(REAL_DATA_RUNNER, "real_data_runner_gateway_args_test")

        for argv in (
            ["--port", "0"],
            ["--port", "65536"],
            ["--port", "bad"],
            ["--path", "/socket"],
            ["--runtime-health-interval-seconds", "0"],
            ["--runtime-health-interval-seconds", "-1"],
            ["--xtquant-port", "0"],
        ):
            with self.subTest(argv=argv):
                with self.assertRaises(SystemExit):
                    runner.parse_args(argv)

        args = runner.parse_args([
            "--port",
            "9021",
            "--path",
            "/ws",
            "--runtime-health-interval-seconds",
            "0.5",
            "--skip-xtquant-refresh-on-start",
        ])

        self.assertEqual(args.port, 9021)
        self.assertEqual(args.path, "/ws")
        self.assertEqual(args.runtime_health_interval_seconds, 0.5)
        self.assertTrue(args.skip_xtquant_refresh_on_start)

    def test_real_data_runner_rejects_invalid_big_trade_cli_knobs(self) -> None:
        runner = load_tool_module(REAL_DATA_RUNNER, "real_data_runner_big_trade_cli_test")

        with self.assertRaises(SystemExit):
            runner.parse_args(["--big-trade-volume-baseline-ratio", "0"])
        with self.assertRaises(SystemExit):
            runner.parse_args(["--big-trade-volume-baseline-ratio", "-0.01"])
        with self.assertRaises(SystemExit):
            runner.parse_args(["--big-trade-min-volume-threshold", "0"])
        with self.assertRaises(SystemExit):
            runner.parse_args(["--big-trade-turnover-threshold", "0"])

        args = runner.parse_args([
            "--big-trade-volume-baseline-ratio",
            "0.001",
            "--big-trade-min-volume-threshold",
            "3000",
            "--big-trade-turnover-threshold",
            "2000000",
        ])

        self.assertEqual(args.big_trade_volume_baseline_ratio, 0.001)
        self.assertEqual(args.big_trade_min_volume_threshold, 3000)
        self.assertEqual(args.big_trade_turnover_threshold, 2_000_000)

    def test_real_data_runner_merges_on_demand_xtquant_refresh_for_cold_symbol(self) -> None:
        runner = load_tool_module(REAL_DATA_RUNNER, "real_data_runner_on_demand_xtquant_test")

        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            pd.DataFrame(
                [
                    {
                        "schema_version": 1,
                        "symbol": "00700.HK",
                        "trade_date": "20260526",
                        "bar_ts": "2026-05-26T09:30:00.000+08:00",
                        "open": 390,
                        "high": 391,
                        "low": 389,
                        "close": 390,
                        "volume": 1000,
                        "turnover": 390000,
                        "source": "fixture",
                        "ingest_ts": "2026-05-26T09:31:00Z",
                        "row_hash": "existing",
                    }
                ]
            ).to_csv(root / "silver_minute_bars_v1.csv", index=False)

            original_refresh = runner.refresh_xtquant_silver_on_start

            def fake_refresh(**kwargs):
                output_root = Path(kwargs["output_root"])
                self.assertEqual(kwargs["symbols"], ["00005.HK"])
                self.assertEqual(kwargs["start_date"], "")
                pd.DataFrame(
                    [
                        {
                            "schema_version": 1,
                            "symbol": "00005.HK",
                            "trade_date": "20260526",
                            "bar_ts": "2026-05-26T09:30:00.000+08:00",
                            "open": 80,
                            "high": 81,
                            "low": 79,
                            "close": 80,
                            "volume": 2000,
                            "turnover": 160000,
                            "source": "xtquant.1m",
                            "ingest_ts": "2026-05-26T09:31:00Z",
                            "row_hash": "new",
                        }
                    ]
                ).to_csv(output_root / "silver_minute_bars_v1.csv", index=False)

            runner.refresh_xtquant_silver_on_start = fake_refresh
            try:
                reader = runner.CsvSilverReaderAdapter(
                    root,
                    symbols=["00700.HK"],
                    refresh_config=runner.XtQuantRefreshConfig(
                        exporter=Path("/tmp/exporter.py"),
                        python=Path("/tmp/python"),
                        output_root=root,
                        trade_date="20260526",
                        port=58628,
                    ),
                )
                reader.ensure_symbol("5")
                rows = reader.read_table("minute_bars")
            finally:
                runner.refresh_xtquant_silver_on_start = original_refresh

        self.assertEqual(reader.symbols, ["00700.HK", "00005.HK"])
        self.assertEqual(sorted({row["symbol"] for row in rows}), ["00005.HK", "00700.HK"])

    def test_real_data_runner_writes_phase_6_smoke_health_artifact(self) -> None:
        runner = load_tool_module(REAL_DATA_RUNNER, "real_data_runner_smoke_health_test")
        reader = StaticSilverReader()

        _mammoth, _bus, _cache, service = runner.build_real_data_runtime(
            reader=reader,
            requested_trade_date="20260525",
            host="0.0.0.0",
            port=9020,
            path="/ws",
            big_trade_volume_baseline_ratio=0.0005,
        )
        manager = service.manager
        manager.connect("desk-a")
        manager.connect("desk-b")
        manager.flush("desk-a")
        manager.flush("desk-b")
        manager.handle_message("desk-a", {"schema_version": 1, "protocol": "terminal-message-v1", "client_id": "desk-a", "action": "subscribe", "symbol": "00700.HK"})
        manager.handle_message("desk-b", {"schema_version": 1, "protocol": "terminal-message-v1", "client_id": "desk-b", "action": "subscribe", "symbol": "00700.HK"})
        output_path = Path("/tmp/real-data-runner-smoke-health-test.json")

        try:
            payload = runner.write_real_data_runner_smoke_health(service, output_path)
            persisted = json.loads(output_path.read_text(encoding="utf-8"))
        finally:
            output_path.unlink(missing_ok=True)

        self.assertTrue(payload["passed"])
        self.assertEqual(persisted["schema_version"], 1)
        self.assertEqual(persisted["evidence"]["gateway_websocket"]["host"], "0.0.0.0")
        self.assertEqual(persisted["evidence"]["gateway_activity"]["client_queue"]["observed_client_count"], 2)
        self.assertEqual(persisted["evidence"]["gateway_activity"]["client_queue"]["observed_declared_client_count"], 2)
        self.assertEqual(persisted["evidence"]["gateway_activity"]["client_queue"]["max_connected_clients"], 2)
        self.assertIn("00700.HK", persisted["evidence"]["symbol_runtime"])
        self.assertEqual(persisted["evidence"]["symbol_runtime"]["00700.HK"]["hydrate_count"], 0)
        self.assertEqual(persisted["evidence"]["symbol_runtime_manager"]["active_hydrations"], 0)
        self.assertEqual(len(persisted["evidence"]["performance_samples"]["subscribe_snapshot_ms"]), 2)

    def test_real_data_runner_smoke_health_requires_two_observed_clients(self) -> None:
        runner = load_tool_module(REAL_DATA_RUNNER, "real_data_runner_smoke_health_client_gate_test")
        reader = StaticSilverReader()

        _mammoth, _bus, _cache, service = runner.build_real_data_runtime(
            reader=reader,
            requested_trade_date="20260525",
            host="0.0.0.0",
            port=9020,
            path="/ws",
            big_trade_volume_baseline_ratio=0.0005,
        )
        manager = service.manager
        manager.connect("desk-a")
        manager.flush("desk-a")
        manager.handle_message("desk-a", {"schema_version": 1, "protocol": "terminal-message-v1", "client_id": "desk-a", "action": "subscribe", "symbol": "00700.HK"})
        output_path = Path("/tmp/real-data-runner-smoke-health-client-gate-test.json")

        try:
            payload = runner.write_real_data_runner_smoke_health(service, output_path)
            persisted = json.loads(output_path.read_text(encoding="utf-8"))
        finally:
            output_path.unlink(missing_ok=True)

        self.assertFalse(payload["passed"])
        self.assertIn("real_data_runner_insufficient_observed_clients", payload["blockers"])
        self.assertIn("real_data_runner_insufficient_declared_clients", payload["blockers"])
        self.assertIn("real_data_runner_max_connected_clients_insufficient", payload["blockers"])
        self.assertEqual(persisted["evidence"]["gateway_activity"]["client_queue"]["observed_client_count"], 1)


class StaticSilverReader:
    def __init__(self) -> None:
        self.symbols = ["00700.HK"]
        self.instrument_names = {"00700.HK": "Tencent"}

    def ensure_symbol(self, symbol: str) -> None:
        if symbol not in self.symbols:
            self.symbols.append(symbol)

    def read_table(self, data_type: str) -> list[dict]:
        rows = {
            "daily_bars": self.daily_bars(),
            "minute_bars": self.minute_bars(),
            "trade_ticks": self.trade_ticks(),
            "ccass_holdings": self.ccass_holdings(),
            "broker_queue": [],
            "broker_mapping": self.broker_mapping(),
        }[data_type]
        if data_type == "broker_mapping":
            return rows
        return [row for row in rows if row.get("symbol") in self.symbols]

    def daily_bars(self) -> list[dict]:
        return [
            self.row(
                symbol="00700.HK",
                trade_date="20260521",
                open=380,
                high=386,
                low=379,
                close=384,
                volume=1_000_000,
                turnover=384_000_000,
                source="fixture.daily",
            ),
            self.row(
                symbol="00700.HK",
                trade_date="20260522",
                open=386,
                high=390,
                low=385,
                close=388,
                volume=1_000_000,
                turnover=388_000_000,
                source="fixture.daily",
            ),
            self.row(
                symbol="00005.HK",
                trade_date="20260521",
                open=80,
                high=82,
                low=79,
                close=81,
                volume=2_000_000,
                turnover=162_000_000,
                source="fixture.daily",
            ),
            self.row(
                symbol="00005.HK",
                trade_date="20260522",
                open=81,
                high=83,
                low=80,
                close=82,
                volume=2_000_000,
                turnover=164_000_000,
                source="fixture.daily",
            ),
        ]

    def minute_bars(self) -> list[dict]:
        return [
            self.row(
                symbol="00700.HK",
                trade_date="20260522",
                bar_ts="2026-05-22T09:30:00+08:00",
                open=388.0,
                high=388.5,
                low=387.8,
                close=388.4,
                volume=1000,
                turnover=388400.0,
                source="fixture.minute",
            ),
            self.row(
                symbol="00005.HK",
                trade_date="20260522",
                bar_ts="2026-05-22T09:30:00+08:00",
                open=82.0,
                high=82.5,
                low=81.8,
                close=82.4,
                volume=2000,
                turnover=164800.0,
                source="fixture.minute",
            )
        ]

    def trade_ticks(self) -> list[dict]:
        return [
            self.row(
                symbol="00700.HK",
                trade_date="20260522",
                tick_ts="2026-05-22T10:00:00+08:00",
                price=389.0,
                volume=1000,
                turnover=389000.0,
                side="buy",
                broker_code="1234",
                participant_name="Test Participant",
                source="fixture.tick",
            ),
            self.row(
                symbol="00005.HK",
                trade_date="20260522",
                tick_ts="2026-05-22T10:00:00+08:00",
                price=82.5,
                volume=2000,
                turnover=165000.0,
                side="buy",
                broker_code="1234",
                participant_name="Test Participant",
                source="fixture.tick",
            )
        ]

    def ccass_holdings(self) -> list[dict]:
        return [
            self.row(
                symbol="00700.HK",
                trade_date="20260521",
                participant_id="P1",
                participant_name="Test Participant",
                shares=100,
                percent=1.0,
                source="fixture.ccass",
            ),
            self.row(
                symbol="00700.HK",
                trade_date="20260522",
                participant_id="P1",
                participant_name="Test Participant",
                shares=110,
                percent=1.1,
                source="fixture.ccass",
            ),
            self.row(
                symbol="00005.HK",
                trade_date="20260521",
                participant_id="P1",
                participant_name="Test Participant",
                shares=200,
                percent=2.0,
                source="fixture.ccass",
            ),
            self.row(
                symbol="00005.HK",
                trade_date="20260522",
                participant_id="P1",
                participant_name="Test Participant",
                shares=210,
                percent=2.1,
                source="fixture.ccass",
            ),
            self.row(
                symbol="00005.HK",
                trade_date="20260522",
                participant_id="P2",
                participant_name="Large Holder",
                shares=900,
                percent=9.0,
                source="fixture.ccass",
            ),
        ]

    def broker_mapping(self) -> list[dict]:
        return [
            self.row(
                broker_code="1234",
                broker_name="Test Participant",
                participant_id="P1",
                participant_name="Test Participant",
                effective_from="20200101",
                source="fixture.mapping",
            )
        ]

    @staticmethod
    def row(**values):
        values.setdefault("schema_version", 1)
        values.setdefault("ingest_ts", "2026-05-22T00:00:00+08:00")
        values.setdefault("row_hash", "fixture")
        return values
