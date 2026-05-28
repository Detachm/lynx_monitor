import csv
import tempfile
import unittest
from pathlib import Path

from beast_market import (
    ActivePoolConfig,
    ActiveSymbolPoolManager,
    BeastMarketRuntimeClients,
    BeastMarketRuntimeConfig,
    BeastMarketRuntimeSupervisor,
    build_beast_market_runtime,
)
from beast_market.ops import explain_active_pool_symbol, generate_active_pool, pin_active_pool_symbol, unpin_active_pool_symbol


class ActivePoolTest(unittest.TestCase):
    def test_top_symbols_filter_etf_derivative_and_rank_by_average_turnover(self) -> None:
        manager = ActiveSymbolPoolManager(
            FakeMammoth(
                daily_rows=[
                    daily("00001.HK", "20260520", turnover=1000, volume=10),
                    daily("00001.HK", "20260521", turnover=3000, volume=10),
                    daily("00002.HK", "20260521", turnover=1500, volume=1000),
                    daily("02800.HK", "20260521", turnover=100000, volume=1000),
                    daily("12000.HK", "20260521", turnover=200000, volume=1000),
                ],
                instruments=[
                    instrument("00001.HK", "EQUITY", "Alpha Ltd"),
                    instrument("00002.HK", "EQUITY", "Beta Ltd"),
                    instrument("02800.HK", "ETF", "Tracker ETF"),
                    instrument("12000.HK", "WARRANT", "Alpha Call Warrant"),
                ],
            ),
            trade_date="20260522",
            config=ActivePoolConfig(target_size=2, rank_window_days=5),
        )

        active = manager.rebuild_base_active()
        snapshot = manager.snapshot()

        self.assertEqual(active, ["00001.HK", "00002.HK"])
        self.assertEqual(manager.ranks_by_symbol["00001.HK"].avg_turnover, 2000)
        self.assertEqual(manager.ranks_by_symbol["00002.HK"].avg_volume, 1000)
        self.assertIn("02800.HK", manager.excluded_symbols)
        self.assertIn("12000.HK", manager.excluded_symbols)
        self.assertEqual(snapshot["instrument_classification_source"], "instrument_table")

    def test_query_outside_pool_pins_symbol_and_replaces_lowest_base_symbol(self) -> None:
        manager = ActiveSymbolPoolManager(
            FakeMammoth(
                daily_rows=[
                    daily("00001.HK", "20260521", turnover=500),
                    daily("00002.HK", "20260521", turnover=400),
                    daily("00003.HK", "20260521", turnover=300),
                    daily("00004.HK", "20260521", turnover=10),
                ],
            ),
            trade_date="20260522",
            config=ActivePoolConfig(target_size=2, pinned_max_size=2),
        )
        manager.rebuild_base_active()

        change = manager.note_query("00004.HK")

        self.assertTrue(change.promoted)
        self.assertEqual(change.added_symbols, ["00004.HK"])
        self.assertEqual(change.evicted_symbols, ["00002.HK"])
        self.assertEqual(manager.active_symbols(), ["00004.HK", "00001.HK"])
        self.assertEqual(manager.query_pinned, ["00004.HK"])
        self.assertEqual(manager.explain("00004.HK")["reason"], "query_pinned")

    def test_pinned_full_keeps_new_query_temporary_without_changing_active_pool(self) -> None:
        manager = ActiveSymbolPoolManager(
            FakeMammoth(
                daily_rows=[
                    daily("00001.HK", "20260521", turnover=500),
                    daily("00002.HK", "20260521", turnover=400),
                    daily("00003.HK", "20260521", turnover=300),
                    daily("00004.HK", "20260521", turnover=200),
                ],
            ),
            trade_date="20260522",
            config=ActivePoolConfig(target_size=2, pinned_max_size=1),
        )
        manager.rebuild_base_active()
        manager.note_query("00003.HK")
        before = manager.active_symbols()

        change = manager.note_query("00004.HK")

        self.assertEqual(change.disposition, "temporary")
        self.assertFalse(change.promoted)
        self.assertEqual(change.reason, "pinned_pool_full")
        self.assertEqual(manager.active_symbols(), before)
        self.assertEqual(manager.query_pinned, ["00003.HK"])
        self.assertTrue(manager.is_temporary("00004.HK"))
        self.assertEqual(manager.snapshot()["pinned_pool_full_rejections"], 1)

    def test_supervisor_without_explicit_symbols_bootstraps_generated_active_pool(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_runtime_silver(root)
            market_data_client = FakeMarketDataClient()
            runtime = build_beast_market_runtime(
                BeastMarketRuntimeConfig(
                    trade_date="20260522",
                    silver_root=root,
                    runtime_state_root=root / "artifacts" / "runtime-state",
                    active_pool=ActivePoolConfig(target_size=1),
                ),
                BeastMarketRuntimeClients(
                    kafka_producer=FakeKafkaProducer(),
                    kafka_consumer=FakeKafkaConsumer(),
                    redis_client=RecordingRedis(),
                    market_data_client=market_data_client,
                ),
            )
            supervisor = BeastMarketRuntimeSupervisor(runtime)

            supervisor.start([])
            supervisor.stop()

        self.assertEqual(runtime.active_pool_manager.active_symbols(), ["00001.HK"])
        self.assertEqual(market_data_client.subscribed_symbols, ["00001.HK"])
        self.assertIn("00001.HK", runtime.symbol_runtime_manager.runtimes)

    def test_ops_can_generate_explain_pin_and_unpin_active_pool(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_active_pool_silver(root)
            pinned_path = root / "pinned.json"

            generated = generate_active_pool(silver_root=root, trade_date="20260522", target_size=1)
            pinned = pin_active_pool_symbol(
                silver_root=root,
                trade_date="20260522",
                symbol="00002.HK",
                pinned_path=pinned_path,
                target_size=1,
            )
            explanation = explain_active_pool_symbol(
                silver_root=root,
                trade_date="20260522",
                symbol="00002.HK",
                pinned_path=pinned_path,
                target_size=1,
            )
            unpinned = unpin_active_pool_symbol(
                silver_root=root,
                trade_date="20260522",
                symbol="00002.HK",
                pinned_path=pinned_path,
                target_size=1,
            )

        self.assertEqual(generated.snapshot["active_symbols"], ["00001.HK"])
        self.assertEqual(pinned.snapshot["active_symbols"], ["00002.HK"])
        self.assertEqual(explanation.snapshot["explanation"]["reason"], "query_pinned")
        self.assertEqual(unpinned.snapshot["query_pinned"], [])
        self.assertEqual(unpinned.snapshot["active_symbols"], ["00001.HK"])


class FakeMammoth:
    def __init__(self, *, daily_rows: list[dict], instruments: list[dict] | None = None) -> None:
        self.daily_rows = daily_rows
        self.instruments = instruments or []

    def get_all_daily_bars(self) -> list[dict]:
        return list(self.daily_rows)

    def get_instruments(self) -> list[dict]:
        return list(self.instruments)


class FakeKafkaProducer:
    def produce(self, topic: str, key: bytes, value: bytes) -> None:
        return None

    def flush(self) -> None:
        return None


class FakeKafkaConsumer:
    def poll(self, topic: str, offset: int, timeout_ms: int) -> list[dict]:
        return []

    def committed(self, topic: str) -> int:
        return 0

    def high_watermark(self, topic: str) -> int:
        return 0


class RecordingRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.ttls: dict[str, int] = {}

    def set(self, key: str, value: str, ex: int) -> None:
        self.values[key] = value
        self.ttls[key] = ex

    def get(self, key: str):
        return self.values.get(key)


class FakeMarketDataClient:
    def __init__(self) -> None:
        self.started = False
        self.stopped = False
        self.subscribed_symbols: list[str] = []
        self.unsubscribed_symbols: list[str] = []

    def start(self) -> None:
        self.started = True

    def stop(self) -> None:
        self.stopped = True

    def subscribe(self, symbol: str) -> None:
        if symbol not in self.subscribed_symbols:
            self.subscribed_symbols.append(symbol)

    def unsubscribe(self, symbol: str) -> None:
        self.unsubscribed_symbols.append(symbol)


def daily(symbol: str, trade_date: str, *, turnover: float, volume: float = 1) -> dict:
    return {
        "symbol": symbol,
        "trade_date": trade_date,
        "turnover": turnover,
        "volume": volume,
    }


def instrument(symbol: str, instrument_type: str, name: str) -> dict:
    return {
        "schema_version": 1,
        "symbol": symbol,
        "instrument_type": instrument_type,
        "name": name,
    }


def write_active_pool_silver(root: Path) -> None:
    write_table(
        root / "silver_daily_bars_v1.csv",
        [
            silver_daily("00001.HK", "20260521", 1000),
            silver_daily("00001.HK", "20260522", 1100),
            silver_daily("00002.HK", "20260521", 500),
            silver_daily("00002.HK", "20260522", 600),
        ],
    )


def write_runtime_silver(root: Path) -> None:
    write_active_pool_silver(root)
    write_table(
        root / "silver_minute_bars_v1.csv",
        [
            {
                "schema_version": 1,
                "symbol": "00001.HK",
                "trade_date": "20260522",
                "bar_ts": "2026-05-22T09:30:00+08:00",
                "open": 10,
                "high": 11,
                "low": 10,
                "close": 10.5,
                "volume": 1000,
                "turnover": 10500,
                "source": "fixture",
                "ingest_ts": "2026-05-22T09:31:00+08:00",
                "row_hash": "minute-00001",
            }
        ],
    )
    write_table(
        root / "silver_ccass_holdings_v1.csv",
        [
            ccass("00001.HK", "20260521", 900, "holding-prev"),
            ccass("00001.HK", "20260522", 1000, "holding-current"),
        ],
    )
    write_table(
        root / "silver_broker_queue_v1.csv",
        [
            {
                "schema_version": 1,
                "symbol": "00001.HK",
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


def silver_daily(symbol: str, trade_date: str, turnover: float) -> dict:
    return {
        "schema_version": 1,
        "symbol": symbol,
        "trade_date": trade_date,
        "open": 10,
        "high": 11,
        "low": 9,
        "close": 10,
        "volume": 1000,
        "turnover": turnover,
        "source": "fixture",
        "ingest_ts": "2026-05-22T00:00:00Z",
        "row_hash": f"daily-{symbol}-{trade_date}",
    }


def ccass(symbol: str, trade_date: str, shares: int, row_hash: str) -> dict:
    return {
        "schema_version": 1,
        "symbol": symbol,
        "trade_date": trade_date,
        "participant_id": "C00010",
        "participant_name": "JPMorgan",
        "shares": shares,
        "percent": 1.0,
        "source": "fixture",
        "ingest_ts": "2026-05-22T00:00:00Z",
        "row_hash": row_hash,
    }


def write_table(path: Path, rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    unittest.main()
