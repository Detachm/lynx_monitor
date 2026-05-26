from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from .contracts import SCHEMA_VERSION


class MammothAPIError(RuntimeError):
    pass


@dataclass(frozen=True)
class SilverTable:
    name: str
    required_columns: tuple[str, ...]
    primary_key: tuple[str, ...]


class SilverTableReader(Protocol):
    def read_table(self, data_type: str) -> list[dict[str, Any]]:
        ...


SILVER_TABLES = {
    "daily_bars": SilverTable(
        "silver_daily_bars_v1",
        (
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
        ("symbol", "trade_date"),
    ),
    "trade_ticks": SilverTable(
        "silver_trade_ticks_v1",
        (
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
        ),
        ("symbol", "tick_ts"),
    ),
    "minute_bars": SilverTable(
        "silver_minute_bars_v1",
        (
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
        ("symbol", "bar_ts"),
    ),
    "ccass_holdings": SilverTable(
        "silver_ccass_holdings_v1",
        (
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
        ("symbol", "trade_date", "participant_id"),
    ),
    "broker_queue": SilverTable(
        "silver_broker_queue_v1",
        (
            "schema_version",
            "symbol",
            "trade_date",
            "queue_ts",
            "side",
            "position",
            "broker_code",
            "source",
            "ingest_ts",
            "row_hash",
        ),
        ("symbol", "queue_ts", "side", "position"),
    ),
    "broker_mapping": SilverTable(
        "silver_broker_mapping_v1",
        (
            "schema_version",
            "broker_code",
            "broker_name",
            "effective_from",
            "source",
            "ingest_ts",
            "row_hash",
        ),
        ("broker_code", "effective_from"),
    ),
}

OPTIONAL_SILVER_TABLES = {
    "trading_calendar": SilverTable(
        "silver_trading_calendar_v1",
        (
            "schema_version",
            "market",
            "trade_date",
            "is_trading_day",
            "source",
            "ingest_ts",
            "row_hash",
        ),
        ("market", "trade_date"),
    ),
}

ALL_SILVER_TABLES = {**SILVER_TABLES, **OPTIONAL_SILVER_TABLES}

HISTORICAL_MANIFEST_SOURCE_TYPES = {
    "participant_history": "ccass_holdings",
}

REQUIRED_HISTORICAL_MANIFEST_TYPES = tuple(sorted(set(SILVER_TABLES) | set(HISTORICAL_MANIFEST_SOURCE_TYPES)))


class MammothAPI:
    """Unified business entrypoint for Mammoth silver data."""

    def __init__(self, silver_root: str | Path | None = None, *, reader: SilverTableReader | None = None):
        if reader is None and silver_root is None:
            raise MammothAPIError("MammothAPI requires silver_root or reader")
        self.reader = reader or CsvSilverTableReader(Path(silver_root or ""))

    def get_daily_bars(self, symbol: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
        rows = self._read_table("daily_bars")
        return [
            row
            for row in rows
            if row["symbol"] == symbol and start_date <= row["trade_date"] <= end_date
        ]

    def get_recent_daily_bars(self, symbol: str, trade_date: str, days: int = 2) -> list[dict[str, Any]]:
        if days < 1:
            raise MammothAPIError("days must be positive")
        rows = [
            row
            for row in self._read_table("daily_bars")
            if row["symbol"] == symbol and row["trade_date"] <= trade_date
        ]
        rows.sort(key=lambda row: row["trade_date"])
        return rows[-days:]

    def get_previous_daily_bar(self, symbol: str, trade_date: str) -> dict[str, Any] | None:
        rows = [
            row
            for row in self._read_table("daily_bars")
            if row["symbol"] == symbol and row["trade_date"] < trade_date
        ]
        if not rows:
            return None
        rows.sort(key=lambda row: row["trade_date"])
        return rows[-1]

    def get_trade_ticks(self, symbol: str, trade_date: str) -> list[dict[str, Any]]:
        return [
            row
            for row in self._read_table("trade_ticks")
            if row["symbol"] == symbol and row["trade_date"] == trade_date
        ]

    def get_minute_bars(self, symbol: str, trade_date: str) -> list[dict[str, Any]]:
        rows = [
            row
            for row in self._read_table("minute_bars")
            if row["symbol"] == symbol and row["trade_date"] == trade_date
        ]
        rows.sort(key=lambda row: str(row["bar_ts"]))
        return rows

    def get_latest_available_trade_date(self, symbol: str, trade_date: str, *, min_minute_bars: int = 1) -> str:
        if min_minute_bars < 1:
            raise MammothAPIError("min_minute_bars must be positive")
        try:
            minute_rows = self._read_table("minute_bars")
        except MammothAPIError:
            minute_rows = []
        minute_counts: dict[str, int] = {}
        for row in minute_rows:
            if row["symbol"] == symbol and row["trade_date"] <= trade_date:
                row_trade_date = str(row["trade_date"])
                minute_counts[row_trade_date] = minute_counts.get(row_trade_date, 0) + 1
        minute_dates = sorted(
            {
                row_trade_date
                for row_trade_date, count in minute_counts.items()
                if count >= min_minute_bars
            }
        )
        if minute_dates:
            return minute_dates[-1]
        daily_dates = sorted(
            {
                str(row["trade_date"])
                for row in self._read_table("daily_bars")
                if row["symbol"] == symbol and row["trade_date"] <= trade_date
            }
        )
        return daily_dates[-1] if daily_dates else ""

    def is_trading_day(self, trade_date: str, *, market: str = "HK") -> bool | None:
        try:
            rows = self._read_table("trading_calendar")
        except MammothAPIError:
            return None
        market_value = market.strip().upper()
        for row in rows:
            if str(row.get("market") or "").strip().upper() == market_value and str(row.get("trade_date") or "") == trade_date:
                value = row.get("is_trading_day")
                if isinstance(value, bool):
                    return value
                if isinstance(value, str):
                    return value.strip().lower() in {"1", "true", "yes", "y"}
                if isinstance(value, int):
                    return value == 1
        return None

    def get_latest_ccass_holdings(self, symbol: str, trade_date: str | None = None) -> list[dict[str, Any]]:
        rows = [
            row
            for row in self._read_table("ccass_holdings")
            if row["symbol"] == symbol and (trade_date is None or row["trade_date"] <= trade_date)
        ]
        if not rows:
            return []
        latest_date = max(row["trade_date"] for row in rows)
        return [row for row in rows if row["trade_date"] == latest_date]

    def get_ccass_holding_pair(self, symbol: str, trade_date: str) -> dict[str, Any]:
        rows = [
            row
            for row in self._read_table("ccass_holdings")
            if row["symbol"] == symbol and row["trade_date"] <= trade_date
        ]
        dates = sorted(set(str(row["trade_date"]) for row in rows))
        current_date = dates[-1] if dates else ""
        current_rows = [row for row in rows if row["trade_date"] == current_date]
        current_signature = ccass_rows_signature(current_rows)
        previous_date = ""
        for candidate in reversed(dates[:-1]):
            candidate_rows = [row for row in rows if row["trade_date"] == candidate]
            if ccass_rows_signature(candidate_rows) != current_signature:
                previous_date = candidate
                break
        if not previous_date and len(dates) >= 2:
            previous_date = dates[-2]
        return {
            "current_date": current_date,
            "previous_date": previous_date,
            "current_rows": current_rows,
            "previous_rows": [row for row in rows if row["trade_date"] == previous_date],
        }

    def get_participant_history(
        self,
        symbol: str,
        participant_id: str,
        days: int,
        trade_date: str | None = None,
    ) -> list[dict[str, Any]]:
        if days < 1:
            raise MammothAPIError("days must be positive")
        rows = [
            row
            for row in self._read_table("ccass_holdings")
            if row["symbol"] == symbol and row["participant_id"] == participant_id
            and (trade_date is None or row["trade_date"] <= trade_date)
        ]
        rows.sort(key=lambda row: row["trade_date"])
        return rows[-days:]

    def get_broker_queue(self, symbol: str, trade_date: str) -> list[dict[str, Any]]:
        return [
            row
            for row in self._read_table("broker_queue")
            if row["symbol"] == symbol and row["trade_date"] == trade_date
        ]

    def get_broker_mapping(self) -> list[dict[str, Any]]:
        return self._read_table("broker_mapping")

    def build_manifest(
        self,
        *,
        data_type: str,
        start_date: str,
        end_date: str,
        symbols: list[str],
        code_version: str,
    ) -> dict[str, Any]:
        started_at = now_iso()
        source_data_type = source_data_type_for_manifest(data_type)
        table = SILVER_TABLES[source_data_type]
        rows = self._read_table(source_data_type)
        scoped_rows = [
            row
            for row in rows
            if (not symbols or "symbol" not in row or row.get("symbol") in symbols)
            and start_date <= row.get("trade_date", start_date) <= end_date
        ]
        checks = self.run_quality_checks(data_type, scoped_rows)
        scoped_symbols = sorted(set(str(row.get("symbol")) for row in scoped_rows if row.get("symbol")))
        return {
            "schema_version": SCHEMA_VERSION,
            "data_type": data_type,
            "source_data_type": source_data_type,
            "table": table.name,
            "date_range": {"start": start_date, "end": end_date},
            "symbols": scoped_symbols,
            "symbol_count": len(scoped_symbols),
            "row_count": len(scoped_rows),
            "failed_items": checks["failed_items"],
            "code_version": code_version,
            "started_at": started_at,
            "finished_at": now_iso(),
            "quality_checks": checks,
        }

    def build_and_save_manifest(
        self,
        *,
        data_type: str,
        start_date: str,
        end_date: str,
        symbols: list[str],
        code_version: str,
        manifest_root: str | Path,
    ) -> Path:
        manifest = self.build_manifest(
            data_type=data_type,
            start_date=start_date,
            end_date=end_date,
            symbols=symbols,
            code_version=code_version,
        )
        return save_manifest(manifest, manifest_root)

    def run_quality_checks(self, data_type: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
        table = SILVER_TABLES[source_data_type_for_manifest(data_type)]
        missing_required_columns = [
            column for column in table.required_columns if any(column not in row for row in rows)
        ]
        duplicate_primary_keys: list[str] = []
        seen: set[tuple[str, ...]] = set()
        for row in rows:
            key = tuple(str(row.get(column, "")) for column in table.primary_key)
            if key in seen:
                duplicate_primary_keys.append("|".join(key))
            seen.add(key)

        invalid_symbol_rows = [
            str(row.get("symbol", ""))
            for row in rows
            if "symbol" in row and not valid_terminal_symbol(str(row.get("symbol", "")))
        ]
        invalid_date_rows = [
            row.get("trade_date", "")
            for row in rows
            if "trade_date" in row and not valid_yyyymmdd(str(row.get("trade_date", "")))
        ]
        negative_value_rows = [
            row
            for row in rows
            if any(to_float(row.get(column, 0)) < 0 for column in ("volume", "shares", "turnover"))
        ]

        failed_items = []
        if missing_required_columns:
            failed_items.append("missing_required_columns")
        if duplicate_primary_keys:
            failed_items.append("duplicate_primary_keys")
        if invalid_symbol_rows:
            failed_items.append("invalid_symbol_format")
        if invalid_date_rows:
            failed_items.append("invalid_date_format")
        if negative_value_rows:
            failed_items.append("negative_values")
        if not rows:
            failed_items.append("empty_output")

        return {
            "missing_required_columns": missing_required_columns,
            "duplicate_primary_keys": duplicate_primary_keys,
            "invalid_symbol_rows": invalid_symbol_rows,
            "invalid_date_rows": invalid_date_rows,
            "negative_value_count": len(negative_value_rows),
            "empty_output": not rows,
            "passed": not failed_items,
            "failed_items": failed_items,
        }

    def _read_table(self, data_type: str) -> list[dict[str, Any]]:
        return self.reader.read_table(data_type)


def source_data_type_for_manifest(data_type: str) -> str:
    if data_type in SILVER_TABLES:
        return data_type
    if data_type in HISTORICAL_MANIFEST_SOURCE_TYPES:
        return HISTORICAL_MANIFEST_SOURCE_TYPES[data_type]
    raise MammothAPIError(f"unknown historical manifest data type: {data_type}")


class CsvSilverTableReader:
    """CSV-backed silver reader for tests, local fixtures, and compatibility."""

    def __init__(self, silver_root: str | Path):
        self.silver_root = Path(silver_root)

    def read_table(self, data_type: str) -> list[dict[str, Any]]:
        if data_type not in ALL_SILVER_TABLES:
            raise MammothAPIError(f"unknown silver data type: {data_type}")
        table = ALL_SILVER_TABLES[data_type]
        path = self.silver_root / f"{table.name}.csv"
        if not path.exists():
            raise MammothAPIError(f"missing silver table: {path}")

        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            rows = [normalize_row(row) for row in reader]

        missing = [column for column in table.required_columns if column not in reader.fieldnames]
        if missing:
            raise MammothAPIError(f"{path.name} missing columns: {', '.join(missing)}")
        return rows


class DuckDBParquetSilverTableReader:
    """DuckDB/Parquet-shaped silver reader preserving the MammothAPI contract.

    The connection is injected so production can pass a real `duckdb.connect(...)`
    object, while tests can pass a fake connection. The connection must expose
    `execute(sql, params)` returning an object with `fetchall()` and a `description`
    sequence containing column names.
    """

    def __init__(self, silver_root: str | Path, connection: Any, *, file_glob: str = "*.parquet"):
        self.silver_root = Path(silver_root)
        self.connection = connection
        self.file_glob = file_glob

    def read_table(self, data_type: str) -> list[dict[str, Any]]:
        if data_type not in ALL_SILVER_TABLES:
            raise MammothAPIError(f"unknown silver data type: {data_type}")
        table = ALL_SILVER_TABLES[data_type]
        path = str(self.silver_root / table.name / self.file_glob)
        sql = f"select * from read_parquet(?)"
        cursor = self.connection.execute(sql, [path])
        columns = [column[0] for column in cursor.description]
        missing = [column for column in table.required_columns if column not in columns]
        if missing:
            raise MammothAPIError(f"{table.name} parquet missing columns: {', '.join(missing)}")
        return [normalize_duckdb_row(dict(zip(columns, row))) for row in cursor.fetchall()]


def normalize_row(row: dict[str, str]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in row.items():
        normalized[key] = parse_value(key, value)
    return normalized


def normalize_duckdb_row(row: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in row.items():
        if value is None:
            normalized[key] = ""
        elif isinstance(value, datetime):
            normalized[key] = value.isoformat()
        elif key.endswith("_date") or key in {"effective_from", "effective_to"}:
            normalized[key] = str(value)
        else:
            normalized[key] = value
    return normalized


def parse_value(key: str, value: str) -> Any:
    if value is None:
        return ""
    stripped = value.strip()
    if stripped == "":
        return ""
    if key.endswith("_date") or key.endswith("_ts") or key in {"effective_from", "effective_to"}:
        return stripped
    if stripped.lower() in {"true", "false"}:
        return stripped.lower() == "true"
    try:
        if "." in stripped:
            return float(stripped)
        return int(stripped)
    except ValueError:
        return stripped


def row_hash(row: dict[str, Any]) -> str:
    source = "|".join(f"{key}={row[key]}" for key in sorted(row))
    return hashlib.sha256(source.encode("utf-8")).hexdigest()


def ccass_rows_signature(rows: list[dict[str, Any]]) -> str:
    payload = [
        (
            str(row.get("participant_id") or ""),
            str(row.get("shares") or ""),
            str(row.get("percent") or ""),
        )
        for row in rows
    ]
    payload.sort()
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False).encode("utf-8")).hexdigest()


def save_manifest(manifest: dict[str, Any], manifest_root: str | Path) -> Path:
    root = Path(manifest_root)
    root.mkdir(parents=True, exist_ok=True)
    data_type = safe_path_part(str(manifest.get("data_type") or "unknown"))
    date_range = manifest.get("date_range") or {}
    start = safe_path_part(str(date_range.get("start") or "unknown-start"))
    end = safe_path_part(str(date_range.get("end") or "unknown-end"))
    code_version = safe_path_part(str(manifest.get("code_version") or "unknown-version"))
    path = root / f"{data_type}.{start}-{end}.{code_version}.manifest.json"
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
    return path


def load_manifest_directory(manifest_root: str | Path) -> list[dict[str, Any]]:
    manifests = []
    for path in sorted(Path(manifest_root).glob("*.manifest.json")):
        decoded = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(decoded, dict):
            manifests.append(decoded)
    return manifests


def safe_path_part(value: str) -> str:
    cleaned = "".join(character if character.isalnum() or character in {"-", "_"} else "-" for character in value)
    return cleaned.strip("-") or "unknown"


def valid_yyyymmdd(value: str) -> bool:
    if len(value) != 8 or not value.isdigit():
        return False
    try:
        datetime.strptime(value, "%Y%m%d")
        return True
    except ValueError:
        return False


def valid_terminal_symbol(value: str) -> bool:
    return len(value) == 8 and value[:5].isdigit() and value[5:] == ".HK"


def to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")
