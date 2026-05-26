from __future__ import annotations

import argparse
import csv
import hashlib
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd


SCHEMA_VERSION = 1
HK_TZ = timezone(timedelta(hours=8))
DEFAULT_DATA_HOME = Path("/home/hliu/xtbackend/.runtime/xtquant")
DEFAULT_CONFIG = Path("/home/hliu/beast/services/mammoth/historical-ingestion-service/config/bronze_ingest_routine.yaml")
DEFAULT_BROKER_MAPPING_FILE = Path(
    "/vault/core/data/Mammoth-v1/silver/base/broker_info/broker_id_to_participant_id.csv"
)
DEFAULT_SDK_PATH = Path(
    "/home/hliu/xtbackend/vendor/"
    "xtquant_251211_interim-release_cp36m-37m-38-39-310-311-312_linux-gnu_x86_64"
)
DEFAULT_ALLOW_OPTIMIZE_ADDRESSES = (
    "42.228.16.210:55300",
    "42.228.16.211:55300",
    "115.231.218.12:55300",
    "115.231.218.13:55300",
)
TABLE_FILES = {
    "instruments": "silver_instruments_v1.csv",
    "daily_bars": "silver_daily_bars_v1.csv",
    "minute_bars": "silver_minute_bars_v1.csv",
    "trade_ticks": "silver_trade_ticks_v1.csv",
    "ccass_holdings": "silver_ccass_holdings_v1.csv",
    "broker_queue": "silver_broker_queue_v1.csv",
    "broker_mapping": "silver_broker_mapping_v1.csv",
}
TABLE_COLUMNS = {
    "instruments": (
        "schema_version",
        "symbol",
        "name",
        "source",
        "ingest_ts",
        "row_hash",
    ),
    "daily_bars": (
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
    ),
    "trade_ticks": (
        "schema_version",
        "symbol",
        "trade_date",
        "tick_ts",
        "price",
        "volume",
        "turnover",
        "side",
        "source",
        "ingest_ts",
        "row_hash",
        "trade_type",
        "bid_order_id",
        "ask_order_id",
        "broker_code",
        "broker_name",
        "participant_id",
        "participant_name",
        "active_broker_code",
        "active_broker_name",
        "active_participant_id",
        "active_participant_name",
        "supplemental_broker_code",
        "supplemental_broker_name",
        "supplemental_order_id",
        "broker_code_source",
        "trade_id",
    ),
    "minute_bars": (
        "schema_version",
        "symbol",
        "trade_date",
        "bar_ts",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "turnover",
        "source",
        "ingest_ts",
        "row_hash",
    ),
    "ccass_holdings": (
        "schema_version",
        "symbol",
        "trade_date",
        "participant_id",
        "participant_name",
        "shares",
        "percent",
        "source",
        "ingest_ts",
        "row_hash",
    ),
    "broker_queue": (
        "schema_version",
        "symbol",
        "trade_date",
        "queue_ts",
        "side",
        "position",
        "broker_code",
        "broker_name",
        "participant_id",
        "participant_name",
        "order_id",
        "broker_code_source",
        "price",
        "volume",
        "source",
        "ingest_ts",
        "row_hash",
    ),
    "broker_mapping": (
        "schema_version",
        "broker_code",
        "broker_name",
        "participant_id",
        "participant_name",
        "effective_from",
        "source",
        "ingest_ts",
        "row_hash",
    ),
}


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    sys.path.insert(0, str(args.sdk_path))
    from xtquant import xtdatacenter as xtdc
    from xtquant import xtdata

    token = os.environ.get(args.token_env) or read_token_from_config(args.config)
    if token:
        xtdc.set_token(token)
    xtdc.set_allow_optmize_address(list(DEFAULT_ALLOW_OPTIMIZE_ADDRESSES))
    xtdc.set_data_home_dir(str(args.data_home))
    xtdc.init(False)
    xtdc.listen(port=args.port)
    connect_xtdata(xtdata, args.port)
    xtdata.enable_hello = False

    symbols = parse_symbols(args.symbols)
    start_date = args.start_date or previous_calendar_date(args.trade_date, 10)
    output_root = args.output_root
    output_root.mkdir(parents=True, exist_ok=True)

    daily_frames = fetch_period(xtdata, symbols, "1d", start_date, args.trade_date, download=True)
    minute_frames = fetch_period(xtdata, symbols, "1m", start_date, args.trade_date, download=True)
    tick_frames = fetch_period(xtdata, symbols, "hktransaction", start_date, args.trade_date, download=True)
    order_frames = fetch_period(xtdata, symbols, "hkorder", start_date, args.trade_date, download=True)
    order_aux_frames = fetch_period(xtdata, symbols, "hkorderaux", start_date, args.trade_date, download=True)
    ccass_frames = fetch_period(xtdata, symbols, "hktdetails", start_date, args.trade_date, download=True)
    broker_identities = fetch_broker_identities(xtdata, args.broker_mapping_file)
    order_aux_brokers = order_aux_brokers_by_symbol(order_aux_frames)

    rows = {
        "instruments": instrument_rows(xtdata, symbols),
        "daily_bars": daily_rows(daily_frames),
        "minute_bars": minute_rows(minute_frames),
        "trade_ticks": trade_tick_rows(tick_frames, broker_identities, order_aux_brokers),
        "ccass_holdings": ccass_rows(ccass_frames),
        "broker_queue": broker_queue_rows(order_frames, tick_frames, broker_identities, order_aux_brokers),
        "broker_mapping": broker_mapping_rows(tick_frames, order_frames, order_aux_frames, broker_identities),
    }
    for table, table_rows in rows.items():
        write_table(output_root / TABLE_FILES[table], TABLE_COLUMNS[table], table_rows)

    summary = {
        "output_root": str(output_root),
        "symbols": symbols,
        "requested_trade_date": args.trade_date,
        "row_counts": {table: len(table_rows) for table, table_rows in rows.items()},
        "latest_trade_tick_date_by_symbol": latest_date_by_symbol(rows["trade_ticks"]),
        "latest_minute_bar_date_by_symbol": latest_date_by_symbol(rows["minute_bars"]),
        "latest_daily_bar_date_by_symbol": latest_date_by_symbol(rows["daily_bars"]),
        "latest_ccass_date_by_symbol": latest_date_by_symbol(rows["ccass_holdings"]),
    }
    print(summary, flush=True)
    return 0


def instrument_rows(xtdata: Any, symbols: list[str]) -> list[dict[str, Any]]:
    rows = []
    for symbol in symbols:
        detail = {}
        try:
            detail = xtdata.get_instrument_detail(symbol) or {}
        except Exception as error:
            print(f"instrument detail fetch failed: {symbol}: {type(error).__name__}: {error}", flush=True)
        if not isinstance(detail, dict):
            detail = {}
        name = first_non_empty(
            detail.get("InstrumentName"),
            detail.get("instrument_name"),
            detail.get("Name"),
            detail.get("name"),
            detail.get("StockName"),
            detail.get("stock_name"),
        )
        if not name:
            continue
        rows.append(
            silver_row(
                {
                    "schema_version": SCHEMA_VERSION,
                    "symbol": symbol,
                    "name": name,
                    "source": "xtquant.get_instrument_detail",
                    "ingest_ts": now_iso(),
                }
            )
        )
    return sorted(deduplicate(rows, ("symbol",)), key=lambda row: row["symbol"])


def fetch_period(
    xtdata: Any,
    symbols: list[str],
    period: str,
    start_date: str,
    end_date: str,
    *,
    download: bool,
) -> dict[str, pd.DataFrame]:
    result: dict[str, pd.DataFrame] = {}
    for symbol in symbols:
        if download:
            try:
                xtdata.download_history_data(symbol, period, start_time=start_date, end_time=end_date)
            except Exception as error:
                print(f"download failed: {symbol} {period}: {type(error).__name__}: {error}", flush=True)
        try:
            data = xtdata.get_market_data_ex(
                [],
                [symbol],
                period,
                start_time=start_date,
                end_time=end_date,
                count=-1,
                fill_data=False,
            )
        except Exception as error:
            print(f"fetch failed: {symbol} {period}: {type(error).__name__}: {error}", flush=True)
            result[symbol] = pd.DataFrame()
            continue
        frame = data.get(symbol) if isinstance(data, dict) else None
        result[symbol] = normalize_frame(frame)
    return result


def daily_rows(frames: dict[str, pd.DataFrame]) -> list[dict[str, Any]]:
    rows = []
    for symbol, frame in frames.items():
        if frame.empty:
            continue
        for record in frame.to_dict("records"):
            trade_date = compact_date(record)
            if not trade_date:
                continue
            rows.append(
                silver_row(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "symbol": symbol,
                        "trade_date": trade_date,
                        "open": clean_float(record.get("open")),
                        "high": clean_float(record.get("high")),
                        "low": clean_float(record.get("low")),
                        "close": clean_float(record.get("close")),
                        "volume": clean_int(record.get("volume")),
                        "turnover": clean_float(record.get("amount")),
                        "source": "xtquant.1d",
                        "ingest_ts": now_iso(),
                    }
                )
            )
    return sorted(deduplicate(rows, ("symbol", "trade_date")), key=lambda row: (row["symbol"], row["trade_date"]))


def minute_rows(frames: dict[str, pd.DataFrame]) -> list[dict[str, Any]]:
    rows = []
    for symbol, frame in frames.items():
        if frame.empty:
            continue
        for record in frame.to_dict("records"):
            trade_date = compact_date(record)
            if not trade_date:
                continue
            rows.append(
                silver_row(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "symbol": symbol,
                        "trade_date": trade_date,
                        "bar_ts": iso_from_bar_record(record),
                        "open": clean_float(record.get("open")),
                        "high": clean_float(record.get("high")),
                        "low": clean_float(record.get("low")),
                        "close": clean_float(record.get("close")),
                        "volume": clean_int(record.get("volume")),
                        "turnover": clean_float(record.get("amount")),
                        "source": "xtquant.1m",
                        "ingest_ts": now_iso(),
                    }
                )
            )
    return sorted(
        deduplicate(rows, ("symbol", "bar_ts")),
        key=lambda row: (row["symbol"], row["trade_date"], row["bar_ts"]),
    )


def trade_tick_rows(
    frames: dict[str, pd.DataFrame],
    broker_identities: dict[str, dict[str, str]],
    order_aux_brokers: dict[str, dict[str, str]],
) -> list[dict[str, Any]]:
    rows = []
    for symbol, frame in frames.items():
        if frame.empty:
            continue
        for record in frame.to_dict("records"):
            trade_date = compact_date(record)
            price = clean_float(record.get("price"))
            volume = clean_int(record.get("volume"))
            if not trade_date or price <= 0:
                continue
            broker_code = broker_code_from_value(record.get("brokerNo"))
            broker_identity = broker_identity_for_code(broker_identities, broker_code)
            active_broker_code = broker_code_from_value(record.get("activeBrokerNo"))
            active_broker_identity = broker_identity_for_code(broker_identities, active_broker_code)
            participant_identity = broker_identity
            broker_code_source = "brokerNo"
            supplemental_broker_code = ""
            supplemental_broker_identity = empty_broker_identity()
            supplemental_order_id = ""
            if not participant_identity["participant_name"] and active_broker_identity["participant_name"]:
                participant_identity = active_broker_identity
                broker_code_source = "activeBrokerNo"
            if not participant_identity["participant_name"]:
                supplemental_order_id, supplemental_broker_code = supplemental_broker_for_tick(
                    record,
                    order_aux_brokers.get(symbol, {}),
                )
                supplemental_broker_identity = broker_identity_for_code(broker_identities, supplemental_broker_code)
                if supplemental_broker_identity["participant_name"]:
                    participant_identity = supplemental_broker_identity
                    broker_code_source = "hkorderaux"
            rows.append(
                silver_row(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "symbol": symbol,
                        "trade_date": trade_date,
                        "tick_ts": iso_from_timestamp_ms(record.get("time")),
                        "price": price,
                        "volume": volume,
                        "turnover": price * volume,
                        "side": side_from_dir(record.get("dir")),
                        "source": "xtquant.hktransaction",
                        "ingest_ts": now_iso(),
                        "trade_type": clean_str(record.get("tradeType")),
                        "bid_order_id": clean_str(record.get("bidOrderID")),
                        "ask_order_id": clean_str(record.get("askOrderID")),
                        "broker_code": broker_code,
                        "broker_name": participant_identity["broker_name"],
                        "participant_id": participant_identity["participant_id"],
                        "participant_name": participant_identity["participant_name"],
                        "active_broker_code": active_broker_code,
                        "active_broker_name": active_broker_identity["broker_name"],
                        "active_participant_id": active_broker_identity["participant_id"],
                        "active_participant_name": active_broker_identity["participant_name"],
                        "supplemental_broker_code": supplemental_broker_code,
                        "supplemental_broker_name": supplemental_broker_identity["broker_name"],
                        "supplemental_order_id": supplemental_order_id,
                        "broker_code_source": broker_code_source,
                        "trade_id": clean_str(record.get("tradeID")) or clean_str(record.get("seq")),
                    }
                )
            )
    return sorted(
        deduplicate(rows, ("symbol", "tick_ts", "trade_id", "price", "volume")),
        key=lambda row: (row["symbol"], row["trade_date"], row["tick_ts"], row["trade_id"]),
    )


def ccass_rows(frames: dict[str, pd.DataFrame]) -> list[dict[str, Any]]:
    rows = []
    for symbol, frame in frames.items():
        if frame.empty:
            continue
        for record in frame.to_dict("records"):
            trade_date = compact_date(record)
            details = record.get("details")
            if not trade_date or not isinstance(details, list):
                continue
            for detail in details:
                if not isinstance(detail, dict):
                    continue
                participant_id = clean_str(detail.get("ownSharesCode"))
                participant_name = clean_str(detail.get("ownSharesCompany"))
                if not participant_id or not participant_name:
                    continue
                rows.append(
                    silver_row(
                        {
                            "schema_version": SCHEMA_VERSION,
                            "symbol": symbol,
                            "trade_date": trade_date,
                            "participant_id": participant_id,
                            "participant_name": participant_name,
                            "shares": clean_int(detail.get("ownSharesAmount")),
                            "percent": clean_float(detail.get("ownSharesRatio")),
                            "source": "xtquant.hktdetails",
                            "ingest_ts": now_iso(),
                        }
                    )
                )
    return sorted(
        deduplicate(rows, ("symbol", "trade_date", "participant_id")),
        key=lambda row: (row["symbol"], row["trade_date"], row["participant_id"]),
    )


def broker_queue_rows(
    order_frames: dict[str, pd.DataFrame],
    tick_frames: dict[str, pd.DataFrame],
    broker_identities: dict[str, dict[str, str]],
    order_aux_brokers: dict[str, dict[str, str]],
) -> list[dict[str, Any]]:
    rows = []
    for symbol, frame in order_frames.items():
        if frame.empty:
            continue
        effective_date = latest_frame_date(tick_frames.get(symbol, pd.DataFrame())) or latest_frame_date(frame)
        if not effective_date:
            continue
        orders = frame[frame.apply(compact_date, axis=1) == effective_date].copy()
        if orders.empty:
            continue
        ticks = tick_frames.get(symbol, pd.DataFrame())
        tick_subset = ticks[ticks.apply(compact_date, axis=1) == effective_date] if not ticks.empty else ticks
        if tick_subset.empty:
            last_price = clean_float(orders["price"].median())
        else:
            last_price = clean_float(tick_subset.sort_values("time").iloc[-1].get("price"))
        recent = orders.sort_values("time").tail(20000).copy()
        recent["side"] = recent["price"].apply(lambda value: "ask" if clean_float(value) >= last_price else "bid")
        for side in ("ask", "bid"):
            side_orders = recent[recent["side"] == side].copy()
            if side_orders.empty:
                continue
            ascending = side == "ask"
            side_orders = side_orders.sort_values(["price", "time"], ascending=[ascending, False]).head(1000)
            for position, record in enumerate(side_orders.to_dict("records"), start=1):
                broker_code = broker_code_from_value(record.get("brokerNo"))
                broker_code_source = "brokerNo"
                if broker_code in {"", "0", "-1"}:
                    supplemental_broker_code = order_aux_brokers.get(symbol, {}).get(clean_str(record.get("orderId")), "")
                    if supplemental_broker_code:
                        broker_code = supplemental_broker_code
                        broker_code_source = "hkorderaux"
                broker_identity = broker_identity_for_code(broker_identities, broker_code)
                participant_name = broker_identity["participant_name"] or "未披露"
                rows.append(
                    silver_row(
                        {
                            "schema_version": SCHEMA_VERSION,
                            "symbol": symbol,
                            "trade_date": effective_date,
                            "queue_ts": iso_from_timestamp_ms(record.get("time")),
                            "side": side,
                            "position": position,
                            "broker_code": broker_code,
                            "broker_name": participant_name,
                            "participant_id": broker_identity["participant_id"],
                            "participant_name": participant_name,
                            "order_id": clean_str(record.get("orderId")),
                            "broker_code_source": broker_code_source,
                            "price": clean_float(record.get("price")),
                            "volume": clean_int(record.get("volume")),
                            "source": "xtquant.hkorder",
                            "ingest_ts": now_iso(),
                        }
                    )
                )
    return sorted(rows, key=lambda row: (row["symbol"], row["side"], row["position"]))


def broker_mapping_rows(
    tick_frames: dict[str, pd.DataFrame],
    order_frames: dict[str, pd.DataFrame],
    order_aux_frames: dict[str, pd.DataFrame],
    broker_identities: dict[str, dict[str, str]],
) -> list[dict[str, Any]]:
    broker_codes = set()
    first_dates: dict[str, str] = {}
    for frames in (tick_frames, order_frames):
        for frame in frames.values():
            if frame.empty:
                continue
            code_columns = [column for column in ("brokerNo", "activeBrokerNo") if column in frame.columns]
            if not code_columns:
                continue
            for record in frame.to_dict("records"):
                for code_column in code_columns:
                    broker_code = broker_code_from_value(record.get(code_column))
                    if not broker_code:
                        continue
                    broker_codes.add(broker_code)
                    trade_date = compact_date(record)
                    if trade_date and (broker_code not in first_dates or trade_date < first_dates[broker_code]):
                        first_dates[broker_code] = trade_date
    for frame in order_aux_frames.values():
        if frame.empty or "brokerNo" not in frame.columns:
            continue
        for record in frame.to_dict("records"):
            broker_code = broker_code_from_value(record.get("brokerNo"))
            if not broker_code:
                continue
            broker_codes.add(broker_code)
            trade_date = compact_date(record)
            if trade_date and (broker_code not in first_dates or trade_date < first_dates[broker_code]):
                first_dates[broker_code] = trade_date
    rows = []
    for broker_code in sorted(broker_codes):
        broker_identity = broker_identity_for_code(broker_identities, broker_code)
        rows.append(
            silver_row(
                {
                    "schema_version": SCHEMA_VERSION,
                    "broker_code": broker_code,
                    "broker_name": broker_identity["broker_name"],
                    "participant_id": broker_identity["participant_id"],
                    "participant_name": broker_identity["participant_name"],
                    "effective_from": first_dates.get(broker_code) or "19000101",
                    "source": "mammoth.broker_mapping+xtquant.get_hk_broker_dict",
                    "ingest_ts": now_iso(),
                }
            )
        )
    return rows


def order_aux_brokers_by_symbol(frames: dict[str, pd.DataFrame]) -> dict[str, dict[str, str]]:
    mappings: dict[str, dict[str, str]] = {}
    for symbol, frame in frames.items():
        if frame.empty or "orderId" not in frame.columns or "brokerNo" not in frame.columns:
            mappings[symbol] = {}
            continue
        symbol_mapping: dict[str, str] = {}
        for record in frame.to_dict("records"):
            order_id = clean_str(record.get("orderId"))
            broker_code = broker_code_from_value(record.get("brokerNo"))
            if order_id and broker_code and broker_code not in {"0", "-1"}:
                symbol_mapping[order_id] = broker_code
        mappings[symbol] = symbol_mapping
    return mappings


def supplemental_broker_for_tick(record: dict[str, Any], order_aux_brokers: dict[str, str]) -> tuple[str, str]:
    for order_id in passive_order_ids(record):
        broker_code = order_aux_brokers.get(order_id, "")
        if broker_code:
            return order_id, broker_code
    return "", ""


def passive_order_ids(record: dict[str, Any]) -> list[str]:
    direction = clean_int(record.get("dir"))
    bid_order_id = clean_str(record.get("bidOrderID"))
    ask_order_id = clean_str(record.get("askOrderID"))
    if direction == 1:
        candidates = [ask_order_id, bid_order_id]
    elif direction == 2:
        candidates = [bid_order_id, ask_order_id]
    else:
        candidates = [bid_order_id, ask_order_id]
    return [order_id for order_id in candidates if order_id and order_id != "0"]


def fetch_broker_identities(xtdata: Any, mapping_file: Path) -> dict[str, dict[str, str]]:
    identities: dict[str, dict[str, str]] = {}
    try:
        raw_mapping = xtdata.get_hk_broker_dict()
    except Exception as error:
        print(f"broker mapping fetch failed: {type(error).__name__}: {error}", flush=True)
        raw_mapping = {}
    if isinstance(raw_mapping, dict):
        for raw_code, raw_name in raw_mapping.items():
            broker_code = broker_code_from_value(raw_code)
            broker_name = clean_str(raw_name)
            if broker_code and broker_name:
                identities[broker_code] = {
                    "broker_name": broker_name,
                    "participant_id": "",
                    "participant_name": broker_name,
                }
    for broker_code, participant_id, participant_name in read_broker_mapping_file(mapping_file):
        if not broker_code:
            continue
        existing = identities.get(broker_code, {})
        fallback_name = clean_str(existing.get("broker_name"))
        name = participant_name or fallback_name
        identities[broker_code] = {
            "broker_name": name,
            "participant_id": participant_id,
            "participant_name": name,
        }
    return identities


def read_broker_mapping_file(path: Path) -> list[tuple[str, str, str]]:
    if not path.exists():
        print(f"broker mapping file not found: {path}", flush=True)
        return []
    try:
        frame = pd.read_csv(path)
    except Exception as error:
        print(f"broker mapping file read failed: {type(error).__name__}: {error}", flush=True)
        return []
    code_column = first_existing_column(frame, ("broker_id", "BrokerID", "broker_code", "BrokerCode"))
    participant_id_column = first_existing_column(frame, ("participant_id", "ParticipantID"))
    participant_name_column = first_existing_column(frame, ("participant_name", "ParticipantName"))
    if not code_column:
        print(f"broker mapping file has no broker code column: {path}", flush=True)
        return []
    rows = []
    for record in frame.to_dict("records"):
        rows.append(
            (
                broker_code_from_value(record.get(code_column)),
                clean_str(record.get(participant_id_column)) if participant_id_column else "",
                clean_str(record.get(participant_name_column)) if participant_name_column else "",
            )
        )
    return rows


def first_existing_column(frame: pd.DataFrame, candidates: tuple[str, ...]) -> str:
    for candidate in candidates:
        if candidate in frame.columns:
            return candidate
    return ""


def broker_identity_for_code(broker_identities: dict[str, dict[str, str]], broker_code: str) -> dict[str, str]:
    if not broker_code or broker_code in {"0", "-1"}:
        return empty_broker_identity()
    identity = broker_identities.get(broker_code, {})
    name = clean_str(identity.get("participant_name")) or clean_str(identity.get("broker_name"))
    if not name:
        name = f"Broker {broker_code}"
    return {
        "broker_name": name,
        "participant_id": clean_str(identity.get("participant_id")),
        "participant_name": name,
    }


def empty_broker_identity() -> dict[str, str]:
    return {"broker_name": "", "participant_id": "", "participant_name": ""}


def normalize_frame(frame: Any) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return pd.DataFrame()
    result = frame.reset_index()
    if "index" not in result.columns and result.columns.size:
        result = result.rename(columns={result.columns[0]: "index"})
    return result


def compact_date(record: Any) -> str:
    if isinstance(record, pd.Series):
        index_value = record.get("index")
        time_value = record.get("time")
    elif isinstance(record, dict):
        index_value = record.get("index")
        time_value = record.get("time")
    else:
        return ""
    index_text = clean_str(index_value)
    if len(index_text) >= 8 and index_text[:8].isdigit():
        return index_text[:8]
    if time_value not in (None, ""):
        return datetime.fromtimestamp(clean_float(time_value) / 1000, tz=timezone.utc).astimezone(HK_TZ).strftime("%Y%m%d")
    return ""


def latest_frame_date(frame: pd.DataFrame) -> str:
    if frame.empty:
        return ""
    dates = [compact_date(record) for record in frame.to_dict("records")]
    return max([date for date in dates if date], default="")


def latest_date_by_symbol(rows: list[dict[str, Any]]) -> dict[str, str]:
    latest: dict[str, str] = {}
    for row in rows:
        symbol = clean_str(row.get("symbol"))
        trade_date = clean_str(row.get("trade_date"))
        if symbol and trade_date and trade_date > latest.get(symbol, ""):
            latest[symbol] = trade_date
    return latest


def side_from_dir(value: Any) -> str:
    direction = clean_int(value)
    if direction == 1:
        return "buy"
    if direction == 2:
        return "sell"
    return "neutral"


def iso_from_timestamp_ms(value: Any) -> str:
    timestamp_ms = clean_float(value)
    if timestamp_ms <= 0:
        return now_iso()
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).astimezone(HK_TZ).isoformat(timespec="milliseconds")


def iso_from_bar_record(record: dict[str, Any]) -> str:
    time_value = record.get("time")
    if clean_float(time_value) > 10_000_000_000:
        return iso_from_timestamp_ms(time_value)
    index_text = clean_str(record.get("index"))
    if len(index_text) >= 12 and index_text[:12].isdigit():
        return datetime.strptime(index_text[:12], "%Y%m%d%H%M").replace(tzinfo=HK_TZ).isoformat(timespec="milliseconds")
    trade_date = compact_date(record)
    time_text = clean_str(time_value).replace(":", "")
    if len(time_text) >= 4 and time_text[:4].isdigit():
        return (
            datetime.strptime(f"{trade_date}{time_text[:4]}", "%Y%m%d%H%M")
            .replace(tzinfo=HK_TZ)
            .isoformat(timespec="milliseconds")
        )
    return iso_from_timestamp_ms(time_value)


def write_table(path: Path, fieldnames: tuple[str, ...], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def silver_row(row: dict[str, Any]) -> dict[str, Any]:
    enriched = dict(row)
    enriched["row_hash"] = stable_hash(enriched)
    return enriched


def stable_hash(row: dict[str, Any]) -> str:
    payload = "|".join(f"{key}={row[key]}" for key in sorted(row) if key != "row_hash")
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def deduplicate(rows: list[dict[str, Any]], keys: tuple[str, ...]) -> list[dict[str, Any]]:
    seen = set()
    result = []
    for row in rows:
        key = tuple(clean_str(row.get(item)) for item in keys)
        if key in seen:
            continue
        seen.add(key)
        result.append(row)
    return result


def connect_xtdata(xtdata: Any, port: int) -> None:
    for _ in range(30):
        try:
            xtdata.connect("127.0.0.1", port)
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError(f"failed to connect xtquant datacenter on {port}")


def parse_symbols(value: str) -> list[str]:
    symbols = []
    for raw in value.split(","):
        symbol = raw.strip().upper()
        if not symbol:
            continue
        if "." not in symbol and symbol.isdigit():
            symbol = f"{symbol.zfill(5)}.HK"
        symbols.append(symbol)
    return list(dict.fromkeys(symbols))


def read_token_from_config(path: Path) -> str:
    if not path.exists():
        return ""
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("token:"):
            return stripped.split(":", 1)[1].strip().strip("'\"")
    return ""


def previous_calendar_date(trade_date: str, days: int) -> str:
    parsed = datetime.strptime(trade_date, "%Y%m%d")
    return (parsed - timedelta(days=days)).strftime("%Y%m%d")


def clean_str(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value).strip()


def first_non_empty(*values: Any) -> str:
    for value in values:
        text = clean_str(value)
        if text:
            return text
    return ""


def broker_code_from_value(value: Any) -> str:
    text = clean_str(value)
    if not text:
        return ""
    try:
        number = float(text)
    except ValueError:
        return text
    if number.is_integer():
        return str(int(number))
    return text


def clean_int(value: Any) -> int:
    try:
        if value is None or pd.isna(value):
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0


def clean_float(value: Any) -> float:
    try:
        if value is None or pd.isna(value):
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export xtquant data into Market Terminal silver CSV tables.")
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--start-date", default="")
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--port", type=int, default=58617)
    parser.add_argument("--data-home", type=Path, default=DEFAULT_DATA_HOME)
    parser.add_argument("--sdk-path", type=Path, default=DEFAULT_SDK_PATH)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--broker-mapping-file", type=Path, default=DEFAULT_BROKER_MAPPING_FILE)
    parser.add_argument("--token-env", default="XTQUANT_TOKEN")
    return parser.parse_args(argv)


if __name__ == "__main__":
    try:
        code = main()
    except Exception as error:
        print(f"xtquant export failed: {type(error).__name__}: {error}", file=sys.stderr, flush=True)
        code = 1
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(code)
