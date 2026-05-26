from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import signal
import subprocess
import tempfile
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from beast_market.adapters import InMemoryEventBus, InMemoryRedisSnapshotCache
from beast_market.contracts import PROCESSED_TOPIC, RAW_TOPIC, SCHEMA_VERSION, make_raw_market_event, now_iso
from beast_market.gateway_transport import GatewayV2SessionManager
from beast_market.mammoth_api import CsvSilverTableReader, MammothAPI, SILVER_TABLES
from beast_market.pipeline import GatewayV2, OctopusComputeV2
from beast_market.symbol_runtime import SymbolRuntimeManager
from beast_market.websocket_server import GatewayV2WebSocketService


DEFAULT_MAMMOTH_REFRESH_ROOT = Path("/home/hliu/beast/data/mammoth_refresh/silver/delta/patch")
DEFAULT_L2_ROOTS = (
    Path("/home/hliu/beast/data/ipo_l2_retry/silver/delta/patch"),
    Path("/home/hliu/beast/data/ipo_l2_first30d/silver/delta/patch"),
)
DEFAULT_XTQUANT_PYTHON = Path("/home/hliu/miniconda3/envs/mammoth/bin/python")
DEFAULT_XTQUANT_EXPORTER = Path(__file__).with_name("xtquant_silver_export.py")
DEFAULT_SYMBOLS = ("00068.HK", "02476.HK", "01879.HK", "03296.HK")
HK_TZ = timezone(timedelta(hours=8))
DEFAULT_MIN_CHART_MINUTE_BARS = 1
SILVER_CSV_FILES = {
    "instruments": "silver_instruments_v1.csv",
    "daily_bars": "silver_daily_bars_v1.csv",
    "minute_bars": "silver_minute_bars_v1.csv",
    "trade_ticks": "silver_trade_ticks_v1.csv",
    "ccass_holdings": "silver_ccass_holdings_v1.csv",
    "broker_queue": "silver_broker_queue_v1.csv",
    "broker_mapping": "silver_broker_mapping_v1.csv",
}
SILVER_CSV_DEDUPE_KEYS = {
    "instruments": ("symbol",),
    "daily_bars": ("symbol", "trade_date"),
    "minute_bars": ("symbol", "bar_ts"),
    "trade_ticks": ("symbol", "tick_ts", "trade_id", "price", "volume"),
    "ccass_holdings": ("symbol", "trade_date", "participant_id"),
    "broker_queue": ("symbol", "queue_ts", "side", "position"),
    "broker_mapping": ("broker_code",),
}


@dataclass
class RealDataBootstrapResult:
    symbol: str
    requested_trade_date: str
    effective_trade_date: str
    tick_count: int
    minute_count: int
    alert_count: int
    chart_source: str
    ccass_current_date: str
    ccass_previous_date: str


@dataclass(frozen=True)
class XtQuantRefreshConfig:
    exporter: Path
    python: Path
    output_root: Path
    trade_date: str
    port: int
    start_date: str = ""


class MammothPatchSilverReader:
    """Mammoth reader backed by local real parquet patch outputs.

    This runner is intentionally local-dev shaped: it exposes the same MammothAPI
    contract used by the runtime while reading only the selected symbols from the
    available parquet patch directories.
    """

    def __init__(
        self,
        *,
        symbols: list[str],
        requested_trade_date: str,
        mammoth_refresh_root: Path = DEFAULT_MAMMOTH_REFRESH_ROOT,
        l2_roots: tuple[Path, ...] = DEFAULT_L2_ROOTS,
    ) -> None:
        self.symbols = [canonical_symbol(symbol) for symbol in symbols]
        self.requested_trade_date = requested_trade_date
        self.mammoth_refresh_root = mammoth_refresh_root
        self.l2_roots = l2_roots
        self._tables: dict[str, list[dict[str, Any]]] = {}
        self._frames: dict[str, pd.DataFrame] = {}
        self.instrument_names = self._load_instrument_names()

    def ensure_symbol(self, symbol: str) -> None:
        canonical = canonical_symbol(symbol)
        if canonical in self.symbols:
            return
        self.symbols.append(canonical)
        self._tables.clear()
        self._frames.clear()
        self.instrument_names = self._load_instrument_names()

    def read_table(self, data_type: str) -> list[dict[str, Any]]:
        if data_type not in SILVER_TABLES:
            raise ValueError(f"unknown silver data_type: {data_type}")
        if data_type not in self._tables:
            loaders = {
                "daily_bars": self._load_daily_bars,
                "minute_bars": self._load_minute_bars,
                "trade_ticks": self._load_trade_ticks,
                "ccass_holdings": self._load_ccass_holdings,
                "broker_queue": self._load_broker_queue,
                "broker_mapping": self._load_broker_mapping,
            }
            self._tables[data_type] = loaders[data_type]()
        return list(self._tables[data_type])

    def latest_tick_date(self, symbol: str) -> str:
        frame = self._trade_tick_frame()
        subset = frame[frame["instrument_id"] == symbol]
        return str(subset["date"].max()) if not subset.empty else ""

    def _load_daily_bars(self) -> list[dict[str, Any]]:
        files = sorted(self.mammoth_refresh_root.glob("*/daily_bar.parquet"))
        frame = read_selected_parquet(
            files,
            columns=("instrument_id", "date", "open", "high", "low", "close", "volume", "turnover"),
            symbols=self.symbols,
            end_date=self.requested_trade_date,
        )
        if frame.empty:
            return []
        frame = frame.sort_values(["instrument_id", "date", "__source_order"])
        frame = frame.drop_duplicates(["instrument_id", "date"], keep="last")
        rows = []
        for record in frame.to_dict("records"):
            rows.append(
                silver_row(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "symbol": clean_str(record["instrument_id"]),
                        "trade_date": clean_str(record["date"]),
                        "open": clean_float(record["open"]),
                        "high": clean_float(record["high"]),
                        "low": clean_float(record["low"]),
                        "close": clean_float(record["close"]),
                        "volume": clean_int(record["volume"]),
                        "turnover": clean_float(record["turnover"]),
                        "source": "mammoth_refresh.parquet.daily_bar",
                        "ingest_ts": now_iso(),
                    }
                )
            )
        return sorted(rows, key=lambda row: (row["symbol"], row["trade_date"]))

    def _load_minute_bars(self) -> list[dict[str, Any]]:
        files = sorted(self.mammoth_refresh_root.glob("*/minute_bar.parquet"))
        frame = read_selected_parquet(
            files,
            columns=("instrument_id", "date", "time", "open", "high", "low", "close", "volume", "turnover"),
            symbols=self.symbols,
            end_date=self.requested_trade_date,
            allow_missing=True,
        )
        if frame.empty:
            files = sorted(self.mammoth_refresh_root.glob("*/kline_1m.parquet"))
            frame = read_selected_parquet(
                files,
                columns=("instrument_id", "date", "time", "open", "high", "low", "close", "volume", "turnover"),
                symbols=self.symbols,
                end_date=self.requested_trade_date,
                allow_missing=True,
            )
        if frame.empty:
            return []
        frame = frame.sort_values(["instrument_id", "date", "time", "__source_order"])
        frame = frame.drop_duplicates(["instrument_id", "date", "time"], keep="last")
        rows = []
        for record in frame.to_dict("records"):
            trade_date = clean_str(record.get("date")) or compact_date(record)
            if not trade_date:
                continue
            rows.append(
                silver_row(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "symbol": clean_str(record["instrument_id"]),
                        "trade_date": trade_date,
                        "bar_ts": hk_iso_or_timestamp(trade_date, record.get("time")),
                        "open": clean_float(record["open"]),
                        "high": clean_float(record["high"]),
                        "low": clean_float(record["low"]),
                        "close": clean_float(record["close"]),
                        "volume": clean_int(record["volume"]),
                        "turnover": clean_float(record["turnover"]),
                        "source": "mammoth_refresh.parquet.minute_bar",
                        "ingest_ts": now_iso(),
                    }
                )
            )
        return sorted(rows, key=lambda row: (row["symbol"], row["trade_date"], row["bar_ts"]))

    def _load_ccass_holdings(self) -> list[dict[str, Any]]:
        files = sorted(self.mammoth_refresh_root.glob("*/ccass_shareholding.parquet"))
        frame = read_selected_parquet(
            files,
            columns=(
                "instrument_id",
                "date",
                "participant_id",
                "participant_name",
                "shareholding",
                "shareholding_pct",
            ),
            symbols=self.symbols,
            end_date=self.requested_trade_date,
        )
        if frame.empty:
            return []
        frame = frame.sort_values(["instrument_id", "date", "participant_id", "__source_order"])
        frame = frame.drop_duplicates(["instrument_id", "date", "participant_id"], keep="last")
        rows = []
        for record in frame.to_dict("records"):
            rows.append(
                silver_row(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "symbol": clean_str(record["instrument_id"]),
                        "trade_date": clean_str(record["date"]),
                        "participant_id": clean_str(record["participant_id"]),
                        "participant_name": clean_str(record["participant_name"]),
                        "shares": clean_int(record["shareholding"]),
                        "percent": clean_float(record["shareholding_pct"]),
                        "source": "mammoth_refresh.parquet.ccass_shareholding",
                        "ingest_ts": now_iso(),
                    }
                )
            )
        return sorted(rows, key=lambda row: (row["symbol"], row["trade_date"], row["participant_id"]))

    def _load_trade_ticks(self) -> list[dict[str, Any]]:
        frame = self._trade_tick_frame()
        if frame.empty:
            return []
        frame = frame.sort_values(["instrument_id", "date", "time", "__source_order"])
        frame = frame.drop_duplicates(
            ["instrument_id", "date", "time", "price", "volume", "broker_id", "participant_id"],
            keep="last",
        )
        rows = []
        sequence_by_key: dict[tuple[str, str], int] = {}
        for record in frame.to_dict("records"):
            symbol = clean_str(record["instrument_id"])
            trade_date = clean_str(record["date"])
            key = (symbol, trade_date)
            sequence_by_key[key] = sequence_by_key.get(key, 0) + 1
            price = clean_float(record["price"])
            volume = clean_int(record["volume"])
            rows.append(
                silver_row(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "symbol": symbol,
                        "trade_date": trade_date,
                        "tick_ts": hk_iso(trade_date, clean_str(record["time"])),
                        "price": price,
                        "volume": volume,
                        "turnover": price * volume,
                        "side": normalize_side(record.get("passive_dir")),
                        "source": "ipo_l2.parquet.trade_tick",
                        "ingest_ts": now_iso(),
                        "broker_code": clean_str(record.get("broker_id")),
                        "participant_id": clean_str(record.get("participant_id")),
                        "participant_name": clean_str(record.get("participant_name")),
                        "trade_id": f"{symbol}-{trade_date}-{sequence_by_key[key]}",
                    }
                )
            )
        return sorted(rows, key=lambda row: (row["symbol"], row["trade_date"], row["tick_ts"], row["trade_id"]))

    def _load_broker_queue(self) -> list[dict[str, Any]]:
        latest_dates = {symbol: self.latest_tick_date(symbol) for symbol in self.symbols}
        files = [path for root in self.l2_roots for path in sorted(root.glob("**/hkorder.parquet"))]
        frame = read_selected_parquet(
            files,
            columns=(
                "instrument_id",
                "date",
                "time",
                "price",
                "volume",
                "level",
                "broker_id",
                "participant_id",
                "participant_name",
            ),
            symbols=self.symbols,
            end_date=self.requested_trade_date,
        )
        if frame.empty:
            return []
        wanted_dates = pd.Series(frame["instrument_id"]).map(latest_dates).to_numpy()
        frame = frame[frame["date"].astype(str).to_numpy() == wanted_dates].copy()
        if frame.empty:
            return []

        rows = []
        trade_ticks = self._trade_tick_frame()
        for symbol in self.symbols:
            trade_date = latest_dates.get(symbol, "")
            symbol_orders = frame[(frame["instrument_id"] == symbol) & (frame["date"].astype(str) == trade_date)].copy()
            if symbol_orders.empty:
                continue
            symbol_ticks = trade_ticks[(trade_ticks["instrument_id"] == symbol) & (trade_ticks["date"].astype(str) == trade_date)]
            last_price = clean_float(symbol_ticks.sort_values("time").iloc[-1]["price"]) if not symbol_ticks.empty else clean_float(symbol_orders["price"].median())
            recent_orders = symbol_orders.sort_values("time").tail(20000).copy()
            recent_orders["side"] = recent_orders["price"].apply(lambda price: "ask" if clean_float(price) >= last_price else "bid")
            for side in ("ask", "bid"):
                side_orders = recent_orders[recent_orders["side"] == side].copy()
                if side_orders.empty:
                    continue
                ascending = side == "ask"
                side_orders = side_orders.sort_values(["price", "time"], ascending=[ascending, False]).head(1000)
                for position, record in enumerate(side_orders.to_dict("records"), start=1):
                    rows.append(
                        silver_row(
                            {
                                "schema_version": SCHEMA_VERSION,
                                "symbol": symbol,
                                "trade_date": trade_date,
                                "queue_ts": hk_iso(trade_date, clean_str(record["time"])),
                                "side": side,
                                "position": position,
                                "broker_code": clean_str(record.get("broker_id")),
                                "broker_name": clean_str(record.get("participant_name")),
                                "participant_id": clean_str(record.get("participant_id")),
                                "participant_name": clean_str(record.get("participant_name")),
                                "price": clean_float(record.get("price")),
                                "volume": clean_int(record.get("volume")),
                                "source": "ipo_l2.parquet.hkorder",
                                "ingest_ts": now_iso(),
                            }
                        )
                    )
        return sorted(rows, key=lambda row: (row["symbol"], row["side"], row["position"]))

    def _load_broker_mapping(self) -> list[dict[str, Any]]:
        frame = self._trade_tick_frame()
        if frame.empty:
            return []
        frame = frame.sort_values(["broker_id", "date", "time"])
        frame = frame.drop_duplicates(["broker_id"], keep="last")
        rows = []
        for record in frame.to_dict("records"):
            broker_code = clean_str(record.get("broker_id"))
            participant_name = clean_str(record.get("participant_name")) or "Unknown Participant"
            rows.append(
                silver_row(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "broker_code": broker_code,
                        "broker_name": participant_name,
                        "participant_id": clean_str(record.get("participant_id")),
                        "participant_name": participant_name,
                        "effective_from": clean_str(record.get("date")) or "19000101",
                        "source": "ipo_l2.parquet.trade_tick",
                        "ingest_ts": now_iso(),
                    }
                )
            )
        return rows

    def _load_instrument_names(self) -> dict[str, str]:
        files = sorted(self.mammoth_refresh_root.glob("*/reference_data.parquet"))
        frame = read_selected_parquet(
            files,
            columns=("instrument_id", "date", "instrument_name"),
            symbols=self.symbols,
            end_date=self.requested_trade_date,
        )
        if frame.empty:
            return {}
        frame = frame.sort_values(["instrument_id", "date", "__source_order"])
        frame = frame.drop_duplicates(["instrument_id"], keep="last")
        return {
            clean_str(record["instrument_id"]): clean_str(record["instrument_name"])
            for record in frame.to_dict("records")
        }

    def _trade_tick_frame(self) -> pd.DataFrame:
        if "trade_tick_frame" not in self._frames:
            files = [path for root in self.l2_roots for path in sorted(root.glob("**/trade_tick.parquet"))]
            self._frames["trade_tick_frame"] = read_selected_parquet(
                files,
                columns=(
                    "instrument_id",
                    "date",
                    "time",
                    "passive_dir",
                    "price",
                    "volume",
                    "broker_id",
                    "participant_id",
                    "participant_name",
                ),
                symbols=self.symbols,
                end_date=self.requested_trade_date,
            )
        return self._frames["trade_tick_frame"]


class CsvSilverReaderAdapter:
    def __init__(
        self,
        silver_root: Path,
        *,
        symbols: list[str],
        refresh_config: XtQuantRefreshConfig | None = None,
    ) -> None:
        self.silver_root = silver_root
        self.symbols = [canonical_symbol(symbol) for symbol in symbols]
        self.instrument_names: dict[str, str] = {}
        self.reader = CsvSilverTableReader(silver_root)
        self._tables: dict[str, list[dict[str, Any]]] = {}
        self.refresh_config = refresh_config
        self._refresh_lock = threading.Lock()
        self.instrument_names = self._load_instrument_names()

    def ensure_symbol(self, symbol: str) -> None:
        canonical = canonical_symbol(symbol)
        if canonical not in self.symbols:
            with self._refresh_lock:
                if canonical not in self.symbols:
                    refreshed = self._refresh_symbol(canonical)
                    self.symbols.append(canonical)
                    if refreshed:
                        self._tables.clear()
                        self.reader = CsvSilverTableReader(self.silver_root)
                    self.instrument_names = self._load_instrument_names()

    def _load_instrument_names(self) -> dict[str, str]:
        path = self.silver_root / SILVER_CSV_FILES["instruments"]
        if not path.exists():
            return {}
        try:
            frame = pd.read_csv(path, dtype=str).fillna("")
        except Exception:
            return {}
        if not {"symbol", "name"}.issubset(frame.columns):
            return {}
        frame = frame[frame["symbol"].astype(str).isin(self.symbols)]
        frame = frame.drop_duplicates(["symbol"], keep="last")
        return {
            clean_str(record["symbol"]): clean_str(record["name"])
            for record in frame.to_dict("records")
            if clean_str(record.get("symbol")) and clean_str(record.get("name"))
        }

    def _refresh_symbol(self, symbol: str) -> bool:
        if self.refresh_config is None:
            return False
        with tempfile.TemporaryDirectory(prefix=f"xtquant-{symbol}-") as temp_dir:
            temp_root = Path(temp_dir)
            refresh_xtquant_silver_on_start(
                exporter=self.refresh_config.exporter,
                python=self.refresh_config.python,
                output_root=temp_root,
                trade_date=self.refresh_config.trade_date,
                symbols=[symbol],
                port=self.refresh_config.port,
                start_date=self.refresh_config.start_date,
            )
            merge_silver_csv_roots(self.silver_root, temp_root)
        return True

    def read_table(self, data_type: str) -> list[dict[str, Any]]:
        if data_type not in self._tables:
            self._tables[data_type] = self.reader.read_table(data_type)
        rows = self._tables[data_type]
        if data_type == "broker_mapping":
            return list(rows)
        return [
            row
            for row in rows
            if "symbol" not in row or row.get("symbol") in self.symbols
        ]


def build_real_data_runtime(
    *,
    reader: MammothPatchSilverReader,
    requested_trade_date: str,
    host: str,
    port: int,
    path: str,
    big_trade_volume_baseline_ratio: float,
    min_chart_minute_bars: int = DEFAULT_MIN_CHART_MINUTE_BARS,
) -> tuple[MammothAPI, InMemoryEventBus, InMemoryRedisSnapshotCache, GatewayV2WebSocketService]:
    mammoth = MammothAPI(reader=reader)
    bus = InMemoryEventBus()
    cache = InMemoryRedisSnapshotCache()
    octopus = OctopusComputeV2(
        mammoth,
        bus,
        cache,
        big_trade_volume_baseline_ratio=big_trade_volume_baseline_ratio,
    )
    results = bootstrap_snapshots(
        mammoth=mammoth,
        reader=reader,
        bus=bus,
        cache=cache,
        octopus=octopus,
        requested_trade_date=requested_trade_date,
        min_chart_minute_bars=min_chart_minute_bars,
    )
    gateway = GatewayV2(bus, cache)
    gateway.health.latest_event_at_by_symbol = {
        result.symbol: now_iso()
        for result in results
    }
    runtime_manager = SymbolRuntimeManager(
        gateway,
        trade_date=requested_trade_date,
        hydrate_symbol=lambda symbol: hydrate_real_data_symbol(
            reader=reader,
            mammoth=mammoth,
            cache=cache,
            octopus=octopus,
            requested_trade_date=requested_trade_date,
            symbol=symbol,
            min_chart_minute_bars=min_chart_minute_bars,
        ),
        state_sink=lambda symbol, state: cache.set_terminal_state(requested_trade_date, symbol, state),
        snapshot_sink=lambda symbol, snapshot: restore_terminal_snapshot_if_missing(
            cache,
            requested_trade_date,
            symbol,
            snapshot,
        ),
    )
    for result in results:
        snapshot = cache.get_terminal_snapshot(requested_trade_date, result.symbol)
        if snapshot is not None:
            runtime_manager.seed_snapshot(result.symbol, snapshot)
    manager = GatewayV2SessionManager(
        gateway,
        trade_date=requested_trade_date,
        history_provider=history_provider(mammoth, requested_trade_date),
        symbol_runtime_manager=runtime_manager,
    )
    service = GatewayV2WebSocketService(manager, host=host, port=port, path=path)
    bus.commit(RAW_TOPIC, len(bus.records[RAW_TOPIC]))
    bus.commit(PROCESSED_TOPIC, len(bus.records[PROCESSED_TOPIC]))
    print_bootstrap_summary(results, host=host, port=port, path=path)
    return mammoth, bus, cache, service


def hydrate_real_data_symbol(
    *,
    reader: Any,
    mammoth: MammothAPI,
    cache: InMemoryRedisSnapshotCache,
    octopus: OctopusComputeV2,
    requested_trade_date: str,
    symbol: str,
    min_chart_minute_bars: int = DEFAULT_MIN_CHART_MINUTE_BARS,
) -> dict[str, Any]:
    ensure_symbol = getattr(reader, "ensure_symbol", None)
    if callable(ensure_symbol):
        ensure_symbol(symbol)
    effective_trade_date = mammoth.get_latest_available_trade_date(
        symbol,
        requested_trade_date,
        min_minute_bars=min_chart_minute_bars,
    )
    if not effective_trade_date:
        raise RuntimeError(f"missing minute bars and daily bars for {symbol} <= {requested_trade_date}")
    cached = cache.get_terminal_snapshot(requested_trade_date, symbol)
    if cached is not None and is_real_data_snapshot_fresh(cached, effective_trade_date):
        apply_instrument_name(reader, symbol, cached, cache, requested_trade_date)
        octopus.set_state(symbol, cached)
        octopus.ensure_bod_context(symbol, effective_trade_date, hydrate_participant_history=True)
        return cached
    snapshot = octopus.preload_bod(
        symbol,
        effective_trade_date,
        cache_trade_date=requested_trade_date,
        requested_trade_date=requested_trade_date,
    )
    apply_instrument_name(reader, symbol, snapshot, cache, requested_trade_date)
    replay_historical_alert_ticks(
        mammoth=mammoth,
        bus=None,
        octopus=octopus,
        requested_trade_date=requested_trade_date,
        effective_trade_date=effective_trade_date,
        symbol=symbol,
    )
    return cache.get_terminal_snapshot(requested_trade_date, symbol) or snapshot


def apply_instrument_name(
    reader: Any,
    symbol: str,
    snapshot: dict[str, Any],
    cache: InMemoryRedisSnapshotCache,
    requested_trade_date: str,
) -> None:
    name = clean_str(getattr(reader, "instrument_names", {}).get(symbol))
    if not name:
        return
    payload = snapshot.get("snapshot")
    if not isinstance(payload, dict):
        return
    if payload.get("name") == name:
        return
    payload["name"] = name
    cache.set_terminal_snapshot(requested_trade_date, symbol, snapshot)


def restore_terminal_snapshot_if_missing(
    cache: InMemoryRedisSnapshotCache,
    trade_date: str,
    symbol: str,
    snapshot: dict[str, Any],
) -> None:
    if cache.get_terminal_snapshot(trade_date, symbol) is None:
        cache.set_terminal_snapshot(trade_date, symbol, snapshot)


def replay_historical_alert_ticks(
    *,
    mammoth: MammothAPI,
    bus: InMemoryEventBus | None,
    octopus: OctopusComputeV2,
    requested_trade_date: str,
    effective_trade_date: str,
    symbol: str,
) -> int:
    tick_rows = mammoth.get_trade_ticks(symbol, effective_trade_date)
    tick_rows.sort(key=lambda row: str(row["tick_ts"]))
    for seq, row in enumerate(tick_rows, start=1):
        raw_event = make_raw_market_event(
            kind="tick",
            symbol=symbol,
            source="mammoth-real-data-runner",
            period="hktransaction",
            seq=seq,
            source_ts=str(row["tick_ts"]),
            payload={
                "price": clean_float(row["price"]),
                "volume": clean_int(row["volume"]),
                "turnover": clean_float(row["turnover"]),
                "side": clean_str(row.get("side")) or "neutral",
                "trade_type": clean_str(row.get("trade_type")),
                "bid_order_id": clean_str(row.get("bid_order_id")),
                "ask_order_id": clean_str(row.get("ask_order_id")),
                "broker_code": clean_str(row.get("broker_code")),
                "broker_name": clean_str(row.get("broker_name")),
                "participant_id": clean_str(row.get("participant_id")),
                "participant_name": clean_str(row.get("participant_name")),
                "active_broker_code": clean_str(row.get("active_broker_code")),
                "active_broker_name": clean_str(row.get("active_broker_name")),
                "active_participant_id": clean_str(row.get("active_participant_id")),
                "active_participant_name": clean_str(row.get("active_participant_name")),
                "supplemental_broker_code": clean_str(row.get("supplemental_broker_code")),
                "supplemental_broker_name": clean_str(row.get("supplemental_broker_name")),
                "supplemental_order_id": clean_str(row.get("supplemental_order_id")),
                "broker_code_source": clean_str(row.get("broker_code_source")),
                "trade_id": clean_str(row.get("trade_id")),
            },
            event_id=f"raw-mammoth-real-{symbol}-{effective_trade_date}-{seq}",
        )
        if bus is not None:
            bus.publish(RAW_TOPIC, symbol, raw_event)
        octopus.process_historical_alert_event(raw_event, requested_trade_date)
    return len(tick_rows)


def is_real_data_snapshot_fresh(snapshot: dict[str, Any], effective_trade_date: str) -> bool:
    freshness = snapshot.get("freshness")
    if not isinstance(freshness, dict):
        return False
    source_dates = freshness.get("source_dates")
    if not isinstance(source_dates, dict):
        return False
    if freshness.get("effective_trade_date") != effective_trade_date:
        return False
    if source_dates.get("minute_bars") != effective_trade_date:
        return False
    return isinstance(snapshot.get("minute_bars"), list) and bool(snapshot["minute_bars"])


def bootstrap_snapshots(
    *,
    mammoth: MammothAPI,
    reader: MammothPatchSilverReader,
    bus: InMemoryEventBus,
    cache: InMemoryRedisSnapshotCache,
    octopus: OctopusComputeV2,
    requested_trade_date: str,
    min_chart_minute_bars: int = DEFAULT_MIN_CHART_MINUTE_BARS,
) -> list[RealDataBootstrapResult]:
    results = []
    for symbol in reader.symbols:
        effective_trade_date = mammoth.get_latest_available_trade_date(
            symbol,
            requested_trade_date,
            min_minute_bars=min_chart_minute_bars,
        )
        if not effective_trade_date:
            raise RuntimeError(f"missing trade ticks and daily bars for {symbol} <= {requested_trade_date}")
        snapshot = octopus.preload_bod(
            symbol,
            effective_trade_date,
            cache_trade_date=requested_trade_date,
            requested_trade_date=requested_trade_date,
        )
        apply_instrument_name(reader, symbol, snapshot, cache, requested_trade_date)
        tick_count = replay_historical_alert_ticks(
            mammoth=mammoth,
            bus=bus,
            octopus=octopus,
            requested_trade_date=requested_trade_date,
            effective_trade_date=effective_trade_date,
            symbol=symbol,
        )
        final_snapshot = cache.get_terminal_snapshot(requested_trade_date, symbol) or snapshot
        evidence = final_snapshot.get("ccass_evidence", {})
        results.append(
            RealDataBootstrapResult(
                symbol=symbol,
                requested_trade_date=requested_trade_date,
                effective_trade_date=effective_trade_date,
                tick_count=tick_count,
                minute_count=len(final_snapshot.get("minute_bars", [])),
                alert_count=len(final_snapshot.get("alerts", [])),
                chart_source=clean_str(
                    ((final_snapshot.get("freshness") or {}).get("source_dates") or {}).get("minute_bars")
                ),
                ccass_current_date=clean_str(evidence.get("current_date")),
                ccass_previous_date=clean_str(evidence.get("previous_date")),
            )
        )
    return results


def history_provider(mammoth: MammothAPI, requested_trade_date: str):
    def load_history(symbol: str, participant_name: str, days: int) -> list[dict[str, Any]]:
        pair = mammoth.get_ccass_holding_pair(symbol, requested_trade_date)
        participant_id = ""
        for row in pair["current_rows"]:
            if clean_str(row.get("participant_name")).casefold() == participant_name.casefold():
                participant_id = clean_str(row.get("participant_id"))
                break
        if not participant_id:
            return []
        rows = mammoth.get_participant_history(symbol, participant_id, 2, trade_date=requested_trade_date)
        previous_shares: int | None = None
        points = []
        for row in rows:
            shares = clean_int(row["shares"])
            points.append(
                {
                    "date": clean_str(row["trade_date"]),
                    "shares": shares,
                    "percent": clean_float(row["percent"]),
                    "change": 0 if previous_shares is None else shares - previous_shares,
                }
            )
            previous_shares = shares
        return points[-2:]

    return load_history


async def run_service(
    service: GatewayV2WebSocketService,
    *,
    runtime_health_path: Path | None = None,
    runtime_health_interval_seconds: float = 2.0,
) -> None:
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    last_health_write = 0.0
    for signal_value in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signal_value, stop_event.set)
        except (NotImplementedError, RuntimeError):
            pass
    async with service.serve():
        if runtime_health_path is not None:
            write_real_data_runner_smoke_health(service, runtime_health_path)
            last_health_write = loop.time()
        while not stop_event.is_set():
            await service.broadcast_once()
            if runtime_health_path is not None and loop.time() - last_health_write >= runtime_health_interval_seconds:
                write_real_data_runner_smoke_health(service, runtime_health_path)
                last_health_write = loop.time()
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=0.25)
            except TimeoutError:
                continue
    if runtime_health_path is not None:
        write_real_data_runner_smoke_health(service, runtime_health_path)


def write_real_data_runner_smoke_health(service: GatewayV2WebSocketService, path: str | Path) -> dict[str, Any]:
    """Write Phase 6 smoke health evidence for the lightweight real-data runner.

    The production runtime health verifier targets the full supervised app
    runtime. This runner is a local real-data smoke harness, so it emits the
    verified shape needed by the multi-trader smoke verifier: symbol runtime,
    manager counters, websocket bind evidence, and subscribe latency samples.
    """

    manager = service.manager.symbol_runtime_manager
    blockers: list[str] = []
    if manager is None:
        blockers.append("real_data_runner_symbol_runtime_manager_missing")
        symbol_runtime_manager: dict[str, Any] = {}
        symbol_runtime: dict[str, Any] = {}
    else:
        symbol_runtime_manager = manager.manager_snapshot()
        symbol_runtime = manager.snapshot()
        if int(symbol_runtime_manager.get("active_hydrations") or 0) > 0:
            blockers.append("real_data_runner_active_hydrations_present")
        if int(symbol_runtime_manager.get("capacity_rejections") or 0) > 0:
            blockers.append("real_data_runner_capacity_rejections_present")
        if int(symbol_runtime_manager.get("state_sink_failures") or 0) > 0:
            blockers.append("real_data_runner_state_sink_failures_present")
        if int(symbol_runtime_manager.get("snapshot_sink_failures") or 0) > 0:
            blockers.append("real_data_runner_snapshot_sink_failures_present")
    client_queue = service.manager.client_queue_snapshot()
    performance_samples = service.manager.performance_snapshot()
    if int(client_queue.get("observed_client_count") or 0) < 2:
        blockers.append("real_data_runner_insufficient_observed_clients")
    if int(client_queue.get("observed_declared_client_count") or 0) < 2:
        blockers.append("real_data_runner_insufficient_declared_clients")
    if int(client_queue.get("max_connected_clients") or 0) < 2:
        blockers.append("real_data_runner_max_connected_clients_insufficient")
    if not performance_samples.get("subscribe_snapshot_ms"):
        blockers.append("real_data_runner_subscribe_snapshot_samples_missing")
    payload = {
        "schema_version": 1,
        "passed": not blockers,
        "blockers": blockers,
        "generated_at": now_iso(),
        "evidence": {
            "symbol_runtime": symbol_runtime,
            "symbol_runtime_manager": symbol_runtime_manager,
            "gateway_activity": {"client_queue": client_queue},
            "performance_samples": performance_samples,
            "gateway_websocket": {
                "host": service.host,
                "port": service.port,
                "path": service.path,
            },
        },
    }
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
    return payload


def read_selected_parquet(
    files: list[Path],
    *,
    columns: tuple[str, ...],
    symbols: list[str],
    end_date: str,
    allow_missing: bool = False,
) -> pd.DataFrame:
    frames = []
    for source_order, path in enumerate(files):
        if allow_missing:
            frame = pd.read_parquet(path)
            missing = [column for column in ("instrument_id", "date", "time") if column not in frame.columns]
            if missing:
                continue
            for column in columns:
                if column not in frame.columns:
                    frame[column] = 0
            frame = frame[list(columns)]
        else:
            frame = pd.read_parquet(path, columns=list(columns))
        if "instrument_id" in frame.columns:
            frame = frame[frame["instrument_id"].astype(str).isin(symbols)]
        if "date" in frame.columns:
            frame = frame[frame["date"].astype(str) <= end_date]
        if frame.empty:
            continue
        frame = frame.copy()
        frame["__source_order"] = source_order
        frames.append(frame)
    if not frames:
        return pd.DataFrame(columns=[*columns, "__source_order"])
    return pd.concat(frames, ignore_index=True)


def print_bootstrap_summary(
    results: list[RealDataBootstrapResult],
    *,
    host: str,
    port: int,
    path: str,
) -> None:
    websocket_url = f"ws://{host}:{port}{path}"
    payload = {
        "websocket_url": websocket_url,
        "client_websocket_url": "ws://<this-machine-lan-ip>:{port}{path}".format(port=port, path=path)
        if host == "0.0.0.0"
        else websocket_url,
        "bind_host": host,
        "symbols": [result.symbol for result in results],
        "results": [result.__dict__ for result in results],
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), flush=True)


def refresh_xtquant_silver_on_start(
    *,
    exporter: Path,
    python: Path,
    output_root: Path,
    trade_date: str,
    symbols: list[str],
    port: int,
    start_date: str = "",
) -> None:
    effective_start_date = start_date or previous_calendar_date(trade_date, 10)
    last_return_code = 0
    for offset in range(5):
        candidate_port = port + offset
        command = [
            str(python),
            str(exporter),
            "--trade-date",
            trade_date,
            "--start-date",
            effective_start_date,
            "--symbols",
            ",".join(symbols),
            "--output-root",
            str(output_root),
            "--port",
            str(candidate_port),
        ]
        print(
            json.dumps(
                {
                    "event": "xtquant_refresh_start",
                    "trade_date": trade_date,
                    "start_date": effective_start_date,
                    "symbols": symbols,
                    "output_root": str(output_root),
                    "port": candidate_port,
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            flush=True,
        )
        completed = subprocess.run(command, text=True)
        if completed.returncode == 0:
            return
        last_return_code = completed.returncode
        print(
            json.dumps(
                {
                    "event": "xtquant_refresh_retry",
                    "trade_date": trade_date,
                    "symbols": symbols,
                    "failed_port": candidate_port,
                    "exit_code": completed.returncode,
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            flush=True,
        )
    raise RuntimeError(f"xtquant refresh failed after port retries; last exit code {last_return_code}")


def merge_silver_csv_roots(target_root: Path, patch_root: Path) -> None:
    target_root.mkdir(parents=True, exist_ok=True)
    for data_type, file_name in SILVER_CSV_FILES.items():
        patch_path = patch_root / file_name
        if not patch_path.exists():
            continue
        target_path = target_root / file_name
        patch_frame = pd.read_csv(patch_path)
        if patch_frame.empty:
            if not target_path.exists():
                patch_frame.to_csv(target_path, index=False)
            continue
        if target_path.exists():
            target_frame = pd.read_csv(target_path)
            merged = pd.concat([target_frame, patch_frame], ignore_index=True)
        else:
            merged = patch_frame
        keys = [key for key in SILVER_CSV_DEDUPE_KEYS[data_type] if key in merged.columns]
        if keys:
            merged = merged.drop_duplicates(keys, keep="last")
        else:
            merged = merged.drop_duplicates(keep="last")
        merged.to_csv(target_path, index=False)


def silver_row(row: dict[str, Any]) -> dict[str, Any]:
    enriched = dict(row)
    enriched["row_hash"] = stable_hash(enriched)
    return enriched


def stable_hash(row: dict[str, Any]) -> str:
    payload = "|".join(f"{key}={row[key]}" for key in sorted(row) if key != "row_hash")
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def hk_iso(trade_date: str, raw_time: str) -> str:
    time_text = raw_time.strip()
    if "." in time_text:
        main, fraction = time_text.split(".", 1)
        fraction = (fraction + "000")[:3]
    else:
        main = time_text
        fraction = "000"
    return f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}T{main}.{fraction}+08:00"


def compact_date(record: dict[str, Any]) -> str:
    date_text = clean_str(record.get("date"))
    if len(date_text) >= 8 and date_text[:8].isdigit():
        return date_text[:8]
    time_value = record.get("time")
    if isinstance(time_value, (int, float)) and time_value > 10_000_000_000:
        return datetime.fromtimestamp(float(time_value) / 1000, tz=timezone.utc).astimezone(HK_TZ).strftime("%Y%m%d")
    time_text = clean_str(time_value)
    if time_text.isdigit() and len(time_text) >= 13:
        return datetime.fromtimestamp(float(time_text) / 1000, tz=timezone.utc).astimezone(HK_TZ).strftime("%Y%m%d")
    return ""


def hk_iso_or_timestamp(trade_date: str, raw_time: Any) -> str:
    if isinstance(raw_time, (int, float)) and raw_time > 10_000_000_000:
        return datetime.fromtimestamp(float(raw_time) / 1000, tz=timezone.utc).astimezone(HK_TZ).isoformat(timespec="milliseconds")
    time_text = clean_str(raw_time)
    if time_text.isdigit() and len(time_text) >= 13:
        return datetime.fromtimestamp(float(time_text) / 1000, tz=timezone.utc).astimezone(HK_TZ).isoformat(timespec="milliseconds")
    return hk_iso(trade_date, time_text)


def normalize_side(value: Any) -> str:
    normalized = clean_str(value).lower()
    if normalized in {"buy", "b"}:
        return "buy"
    if normalized in {"sell", "s"}:
        return "sell"
    return "neutral"


def canonical_symbol(value: str) -> str:
    text = value.strip().upper()
    if "." not in text and text.isdigit():
        text = f"{text.zfill(5)}.HK"
    if not text[:5].isdigit() or text[5:] != ".HK":
        raise ValueError(f"symbol must use 00068.HK format: {value}")
    return text


def parse_symbols(raw_symbols: str) -> list[str]:
    symbols = [canonical_symbol(symbol) for symbol in raw_symbols.split(",") if symbol.strip()]
    if not symbols:
        raise ValueError("at least one symbol is required")
    return list(dict.fromkeys(symbols))


def clean_str(value: Any) -> str:
    if value is None:
        return ""
    if pd.isna(value):
        return ""
    return str(value).strip()


def clean_int(value: Any) -> int:
    if value is None or pd.isna(value):
        return 0
    return int(value)


def clean_float(value: Any) -> float:
    if value is None or pd.isna(value):
        return 0.0
    return float(value)


def today_yyyymmdd() -> str:
    return datetime.now().strftime("%Y%m%d")


def previous_calendar_date(trade_date: str, days: int) -> str:
    parsed = datetime.strptime(trade_date, "%Y%m%d")
    return (parsed - timedelta(days=days)).strftime("%Y%m%d")


def positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be greater than 0")
    return parsed


def positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("value must be an integer") from error
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be greater than 0")
    return parsed


def port_arg(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("port must be an integer") from error
    if parsed <= 0 or parsed > 65535:
        raise argparse.ArgumentTypeError("port must be between 1 and 65535")
    return parsed


def websocket_path_arg(value: str) -> str:
    if value != "/ws":
        raise argparse.ArgumentTypeError("gateway path must be /ws")
    return value


def trade_date_arg(value: str) -> str:
    if len(value) != 8 or not value.isdigit():
        raise argparse.ArgumentTypeError("trade date must use YYYYMMDD")
    try:
        datetime.strptime(value, "%Y%m%d")
    except ValueError as error:
        raise argparse.ArgumentTypeError("trade date must be a valid calendar date") from error
    return value


def optional_trade_date_arg(value: str) -> str:
    if value == "":
        return ""
    return trade_date_arg(value)


def symbol_list_arg(value: str) -> list[str]:
    try:
        return parse_symbols(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from error


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Market Terminal against local real Mammoth parquet data.")
    parser.add_argument("--trade-date", type=trade_date_arg, default=today_yyyymmdd(), help="Requested dashboard trade date in YYYYMMDD.")
    parser.add_argument("--symbols", type=symbol_list_arg, default=list(DEFAULT_SYMBOLS), help="Comma-separated symbols to preload.")
    parser.add_argument("--silver-root", type=Path, default=None, help="CSV silver root to serve instead of Mammoth parquet patches.")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=port_arg, default=9020)
    parser.add_argument("--path", type=websocket_path_arg, default="/ws")
    parser.add_argument(
        "--runtime-health-path",
        type=Path,
        default=None,
        help="Optional Phase 6 smoke health artifact path written while the service runs.",
    )
    parser.add_argument(
        "--runtime-health-interval-seconds",
        type=positive_float,
        default=2.0,
        help="Interval for refreshing --runtime-health-path.",
    )
    parser.add_argument("--mammoth-refresh-root", type=Path, default=DEFAULT_MAMMOTH_REFRESH_ROOT)
    parser.add_argument(
        "--l2-root",
        action="append",
        type=Path,
        default=None,
        help="L2 silver patch root. Can be passed multiple times.",
    )
    parser.add_argument("--big-trade-volume-baseline-ratio", type=positive_float, default=0.0005)
    parser.add_argument(
        "--min-chart-minute-bars",
        type=positive_int,
        default=DEFAULT_MIN_CHART_MINUTE_BARS,
        help="Minimum native 1m bars required before a date is used as the chart effective date. Defaults to 1 so live sessions use same-day data as soon as it exists.",
    )
    parser.add_argument(
        "--skip-xtquant-refresh-on-start",
        action="store_true",
        help="When --silver-root is set, skip the default startup xtquant export refresh.",
    )
    parser.add_argument("--xtquant-python", type=Path, default=DEFAULT_XTQUANT_PYTHON)
    parser.add_argument("--xtquant-exporter", type=Path, default=DEFAULT_XTQUANT_EXPORTER)
    parser.add_argument("--xtquant-port", type=port_arg, default=58628)
    parser.add_argument(
        "--xtquant-start-date",
        type=optional_trade_date_arg,
        default="",
        help="Start date passed to xtquant export. Defaults to --trade-date.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    symbols = args.symbols
    l2_roots = tuple(args.l2_root) if args.l2_root else DEFAULT_L2_ROOTS
    if args.silver_root is not None:
        refresh_config = XtQuantRefreshConfig(
            exporter=args.xtquant_exporter,
            python=args.xtquant_python,
            output_root=args.silver_root,
            trade_date=args.trade_date,
            port=args.xtquant_port,
            start_date=args.xtquant_start_date,
        )
        if not args.skip_xtquant_refresh_on_start:
            refresh_xtquant_silver_on_start(
                exporter=refresh_config.exporter,
                python=refresh_config.python,
                output_root=refresh_config.output_root,
                trade_date=refresh_config.trade_date,
                symbols=symbols,
                port=refresh_config.port,
                start_date=refresh_config.start_date,
            )
        reader = CsvSilverReaderAdapter(
            args.silver_root,
            symbols=symbols,
            refresh_config=refresh_config,
        )
    else:
        reader = MammothPatchSilverReader(
            symbols=symbols,
            requested_trade_date=args.trade_date,
            mammoth_refresh_root=args.mammoth_refresh_root,
            l2_roots=l2_roots,
        )
    _, _, _, service = build_real_data_runtime(
        reader=reader,
        requested_trade_date=args.trade_date,
        host=args.host,
        port=args.port,
        path=args.path,
        big_trade_volume_baseline_ratio=args.big_trade_volume_baseline_ratio,
        min_chart_minute_bars=args.min_chart_minute_bars,
    )
    asyncio.run(
        run_service(
            service,
            runtime_health_path=args.runtime_health_path,
            runtime_health_interval_seconds=args.runtime_health_interval_seconds,
        )
    )


if __name__ == "__main__":
    main()
