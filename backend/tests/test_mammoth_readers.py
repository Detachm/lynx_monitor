import tempfile
import unittest
import ast
from pathlib import Path

from beast_market import DuckDBParquetSilverTableReader, MammothAPI, load_manifest_directory
from beast_market.mammoth_api import MammothAPIError


class MammothReaderTest(unittest.TestCase):
    def test_business_modules_do_not_bypass_mammoth_api_for_silver_reads(self) -> None:
        package_root = Path(__file__).resolve().parents[1] / "beast_market"
        allowed_files = {package_root / "mammoth_api.py"}
        forbidden_tokens = (
            "csv.DictReader",
            "read_parquet(",
            "read_csv(",
            "import duckdb",
            "import pandas",
            ".read_table(",
        )
        violations: list[str] = []

        for path in sorted(package_root.glob("*.py")):
            if path in allowed_files:
                continue
            text = path.read_text(encoding="utf-8")
            for token in forbidden_tokens:
                if token in text:
                    violations.append(f"{path.name}:{token}")

        self.assertEqual(violations, [])

    def test_gateway_layer_does_not_hydrate_or_read_historical_storage(self) -> None:
        package_root = Path(__file__).resolve().parents[1] / "beast_market"
        gateway_files = [
            package_root / "gateway.py",
            package_root / "gateway_transport.py",
            package_root / "websocket_server.py",
        ]
        forbidden_imports = {"beast_market.mammoth_api", ".mammoth_api"}
        forbidden_call_names = {
            "get_daily_bars",
            "get_recent_daily_bars",
            "get_previous_daily_bar",
            "get_trade_ticks",
            "get_minute_bars",
            "get_latest_available_trade_date",
            "get_latest_ccass_holdings",
            "get_ccass_holding_pair",
            "get_participant_history",
            "get_broker_queue",
            "get_broker_mapping",
            "read_table",
        }
        violations: list[str] = []

        for path in gateway_files:
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom):
                    module = node.module or ""
                    if module in forbidden_imports or module.endswith("mammoth_api"):
                        violations.append(f"{path.name}:import:{module}")
                elif isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name in forbidden_imports or alias.name.endswith("mammoth_api"):
                            violations.append(f"{path.name}:import:{alias.name}")
                elif (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr in forbidden_call_names
                ):
                    violations.append(f"{path.name}:call:{node.func.attr}")

        self.assertEqual(violations, [])

    def test_mammoth_api_uses_injected_reader_without_business_callers_touching_storage(self) -> None:
        api = MammothAPI(
            reader=StaticReader(
                {
                    "daily_bars": [
                        {
                            "schema_version": 1,
                            "symbol": "00700.HK",
                            "trade_date": "20260522",
                            "open": 1,
                            "high": 2,
                            "low": 1,
                            "close": 1.5,
                            "volume": 100,
                            "turnover": 150,
                            "source": "fixture",
                            "ingest_ts": "2026-05-22T00:00:00Z",
                            "row_hash": "row-1",
                        }
                    ]
                }
            )
        )

        rows = api.get_daily_bars("00700.HK", "20260522", "20260522")

        self.assertEqual(rows[0]["close"], 1.5)

    def test_latest_available_trade_date_ignores_tick_only_dates_for_chart_hydration(self) -> None:
        api = MammothAPI(
            reader=StaticReader(
                {
                    "minute_bars": [
                        {
                            "schema_version": 1,
                            "symbol": "00700.HK",
                            "trade_date": "20260522",
                            "bar_ts": "2026-05-22T09:30:00+08:00",
                            "open": 388,
                            "high": 389,
                            "low": 387,
                            "close": 388.4,
                            "volume": 1000,
                            "turnover": 388400,
                            "source": "fixture",
                            "ingest_ts": "2026-05-22T09:31:00+08:00",
                            "row_hash": "minute-1",
                        }
                    ],
                    "trade_ticks": [
                        {
                            "schema_version": 1,
                            "symbol": "00700.HK",
                            "trade_date": "20260525",
                            "tick_ts": "2026-05-25T09:30:00+08:00",
                            "price": 390,
                            "volume": 1000,
                            "turnover": 390000,
                            "side": "buy",
                            "source": "fixture",
                            "ingest_ts": "2026-05-25T09:30:01+08:00",
                            "row_hash": "tick-only",
                        }
                    ],
                    "daily_bars": [
                        {
                            "schema_version": 1,
                            "symbol": "00700.HK",
                            "trade_date": "20260525",
                            "open": 390,
                            "high": 391,
                            "low": 389,
                            "close": 390,
                            "volume": 1000,
                            "turnover": 390000,
                            "source": "fixture",
                            "ingest_ts": "2026-05-25T00:00:00Z",
                            "row_hash": "daily-tick-only",
                        }
                    ],
                }
            )
        )

        self.assertEqual(api.get_latest_available_trade_date("00700.HK", "20260525"), "20260522")

    def test_latest_available_trade_date_prefers_same_day_as_soon_as_minute_exists(self) -> None:
        minute_rows = []
        for index in range(30):
            minute_rows.append(
                {
                    "schema_version": 1,
                    "symbol": "00700.HK",
                    "trade_date": "20260522",
                    "bar_ts": f"2026-05-22T09:{30 + index:02d}:00+08:00",
                    "open": 388,
                    "high": 389,
                    "low": 387,
                    "close": 388.4,
                    "volume": 1000,
                    "turnover": 388400,
                    "source": "fixture",
                    "ingest_ts": "2026-05-22T09:31:00+08:00",
                    "row_hash": f"minute-complete-{index}",
                }
            )
        minute_rows.append(
            {
                "schema_version": 1,
                "symbol": "00700.HK",
                "trade_date": "20260526",
                "bar_ts": "2026-05-26T09:30:00+08:00",
                "open": 390,
                "high": 390,
                "low": 390,
                "close": 390,
                "volume": 1000,
                "turnover": 390000,
                "source": "fixture",
                "ingest_ts": "2026-05-26T09:31:00+08:00",
                "row_hash": "minute-incomplete",
            }
        )
        api = MammothAPI(reader=StaticReader({"minute_bars": minute_rows}))

        self.assertEqual(api.get_latest_available_trade_date("00700.HK", "20260526"), "20260526")

    def test_latest_available_trade_date_can_skip_incomplete_minute_date_when_threshold_is_set(self) -> None:
        minute_rows = []
        for index in range(30):
            minute_rows.append(
                {
                    "schema_version": 1,
                    "symbol": "00700.HK",
                    "trade_date": "20260522",
                    "bar_ts": f"2026-05-22T09:{30 + index:02d}:00+08:00",
                    "open": 388,
                    "high": 389,
                    "low": 387,
                    "close": 388.4,
                    "volume": 1000,
                    "turnover": 388400,
                    "source": "fixture",
                    "ingest_ts": "2026-05-22T09:31:00+08:00",
                    "row_hash": f"minute-complete-{index}",
                }
            )
        minute_rows.append(
            {
                "schema_version": 1,
                "symbol": "00700.HK",
                "trade_date": "20260526",
                "bar_ts": "2026-05-26T09:30:00+08:00",
                "open": 390,
                "high": 390,
                "low": 390,
                "close": 390,
                "volume": 1000,
                "turnover": 390000,
                "source": "fixture",
                "ingest_ts": "2026-05-26T09:31:00+08:00",
                "row_hash": "minute-incomplete",
            }
        )
        api = MammothAPI(reader=StaticReader({"minute_bars": minute_rows}))

        self.assertEqual(
            api.get_latest_available_trade_date("00700.HK", "20260526", min_minute_bars=30),
            "20260522",
        )

    def test_ccass_holding_pair_uses_previous_distinct_effective_snapshot(self) -> None:
        rows = [
            {
                "schema_version": 1,
                "symbol": "00700.HK",
                "trade_date": "20260521",
                "participant_id": "B00001",
                "participant_name": "Broker A",
                "shares": 100,
                "percent": 1.0,
                "source": "fixture",
                "ingest_ts": "2026-05-21T00:00:00Z",
                "row_hash": "ccass-21",
            },
            {
                "schema_version": 1,
                "symbol": "00700.HK",
                "trade_date": "20260522",
                "participant_id": "B00001",
                "participant_name": "Broker A",
                "shares": 120,
                "percent": 1.2,
                "source": "fixture",
                "ingest_ts": "2026-05-22T00:00:00Z",
                "row_hash": "ccass-22",
            },
            {
                "schema_version": 1,
                "symbol": "00700.HK",
                "trade_date": "20260525",
                "participant_id": "B00001",
                "participant_name": "Broker A",
                "shares": 120,
                "percent": 1.2,
                "source": "fixture",
                "ingest_ts": "2026-05-25T00:00:00Z",
                "row_hash": "ccass-25-repeated",
            },
        ]
        api = MammothAPI(reader=StaticReader({"ccass_holdings": rows}))

        pair = api.get_ccass_holding_pair("00700.HK", "20260526")

        self.assertEqual(pair["current_date"], "20260525")
        self.assertEqual(pair["previous_date"], "20260521")

    def test_duckdb_parquet_reader_queries_silver_table_path_and_normalizes_rows(self) -> None:
        connection = FakeDuckDBConnection(
            columns=[
                "schema_version",
                "symbol",
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "turnover",
                "source",
                "ingest_ts",
                "row_hash",
            ],
            rows=[
                (
                    1,
                    "00700.HK",
                    20260522,
                    386.8,
                    389.0,
                    385.4,
                    386.2,
                    1000,
                    386200,
                    "mammoth",
                    "2026-05-22T00:00:00Z",
                    "daily-1",
                )
            ],
        )
        api = MammothAPI(reader=DuckDBParquetSilverTableReader("/silver", connection))

        rows = api.get_daily_bars("00700.HK", "20260522", "20260522")

        self.assertEqual(connection.executed_sql, "select * from read_parquet(?)")
        self.assertEqual(connection.executed_params, ["/silver/silver_daily_bars_v1/*.parquet"])
        self.assertEqual(rows[0]["trade_date"], "20260522")
        self.assertEqual(rows[0]["close"], 386.2)

    def test_duckdb_parquet_reader_caches_loaded_tables(self) -> None:
        connection = FakeDuckDBConnection(
            columns=[
                "schema_version",
                "symbol",
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "turnover",
                "source",
                "ingest_ts",
                "row_hash",
            ],
            rows=[
                (
                    1,
                    "00700.HK",
                    20260522,
                    386.8,
                    389.0,
                    385.4,
                    386.2,
                    1000,
                    386200,
                    "mammoth",
                    "2026-05-22T00:00:00Z",
                    "daily-1",
                )
            ],
        )
        reader = DuckDBParquetSilverTableReader("/silver", connection)

        initial_rows = reader.read_table("daily_bars")
        repeated_rows = reader.read_table("daily_bars")

        self.assertIs(initial_rows, repeated_rows)
        self.assertEqual(connection.execute_calls, 1)

    def test_duckdb_parquet_reader_rejects_missing_required_columns(self) -> None:
        api = MammothAPI(reader=DuckDBParquetSilverTableReader("/silver", FakeDuckDBConnection(columns=["symbol"], rows=[])))

        with self.assertRaises(MammothAPIError):
            api.get_daily_bars("00700.HK", "20260522", "20260522")

    def test_mammoth_manifest_can_be_persisted_and_loaded_for_a_historical_job(self) -> None:
        api = MammothAPI(
            reader=StaticReader(
                {
                    "daily_bars": [
                        {
                            "schema_version": 1,
                            "symbol": "00700.HK",
                            "trade_date": "20260522",
                            "open": 1,
                            "high": 2,
                            "low": 1,
                            "close": 1.5,
                            "volume": 100,
                            "turnover": 150,
                            "source": "fixture",
                            "ingest_ts": "2026-05-22T00:00:00Z",
                            "row_hash": "row-1",
                        }
                    ]
                }
            )
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            path = api.build_and_save_manifest(
                data_type="daily_bars",
                start_date="20260522",
                end_date="20260522",
                symbols=["00700.HK"],
                code_version="v2/test",
                manifest_root=temp_dir,
            )
            manifests = load_manifest_directory(temp_dir)

        self.assertEqual(path.name, "daily_bars.20260522-20260522.v2-test.manifest.json")
        self.assertEqual(len(manifests), 1)
        self.assertTrue(manifests[0]["quality_checks"]["passed"])
        self.assertEqual(manifests[0]["row_count"], 1)

    def test_participant_history_manifest_uses_ccass_holdings_source_table(self) -> None:
        api = MammothAPI(
            reader=StaticReader(
                {
                    "ccass_holdings": [
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
                            "row_hash": "holding-1",
                        }
                    ]
                }
            )
        )

        manifest = api.build_manifest(
            data_type="participant_history",
            start_date="20260522",
            end_date="20260522",
            symbols=["00700.HK"],
            code_version="v2-test",
        )

        self.assertEqual(manifest["data_type"], "participant_history")
        self.assertEqual(manifest["source_data_type"], "ccass_holdings")
        self.assertEqual(manifest["table"], "silver_ccass_holdings_v1")
        self.assertEqual(manifest["symbols"], ["00700.HK"])
        self.assertTrue(manifest["quality_checks"]["passed"])

    def test_broker_mapping_manifest_is_not_filtered_out_by_symbol_scope(self) -> None:
        api = MammothAPI(
            reader=StaticReader(
                {
                    "broker_mapping": [
                        {
                            "schema_version": 1,
                            "broker_code": "JPM",
                            "broker_name": "JPMorgan",
                            "effective_from": "20260101",
                            "source": "fixture",
                            "ingest_ts": "2026-05-22T00:00:00Z",
                            "row_hash": "mapping-1",
                        }
                    ]
                }
            )
        )

        manifest = api.build_manifest(
            data_type="broker_mapping",
            start_date="20260522",
            end_date="20260522",
            symbols=["00700.HK"],
            code_version="v2-test",
        )

        self.assertEqual(manifest["row_count"], 1)
        self.assertTrue(manifest["quality_checks"]["passed"])


class StaticReader:
    def __init__(self, rows_by_type: dict):
        self.rows_by_type = rows_by_type

    def read_table(self, data_type: str) -> list[dict]:
        return self.rows_by_type.get(data_type, [])


class FakeDuckDBConnection:
    def __init__(self, columns: list[str], rows: list[tuple]) -> None:
        self.description = [(column,) for column in columns]
        self.rows = rows
        self.executed_sql = ""
        self.executed_params = []
        self.execute_calls = 0

    def execute(self, sql: str, params: list[str]):
        self.execute_calls += 1
        self.executed_sql = sql
        self.executed_params = params
        return self

    def fetchall(self):
        return self.rows


if __name__ == "__main__":
    unittest.main()
