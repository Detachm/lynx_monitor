from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any, Iterable

from .runtime import normalize_subscription_symbol


DEFAULT_EXCLUDE_INSTRUMENT_TYPES = ("ETF", "WARRANT", "CBBC", "FUND", "BOND", "DERIVATIVE")
EQUITY_INSTRUMENT_TYPES = {
    "EQUITY",
    "STOCK",
    "COMMON_STOCK",
    "ORDINARY",
    "ORDINARY_SHARE",
    "SHARE",
    "MAIN_BOARD",
    "GEM",
}
INSTRUMENT_TYPE_KEYS = (
    "instrument_type",
    "security_type",
    "product_type",
    "type",
    "category",
    "class",
)
INSTRUMENT_NAME_KEYS = ("name", "short_name", "display_name", "english_name", "chinese_name")
FALLBACK_EXCLUDE_NAME_PATTERN = re.compile(
    r"(ETF|WARRANT|CBBC|FUND|BOND|DERIVATIVE|TRUST|CALL|PUT|牛熊|牛证|熊证|窩輪|窝轮|認股|认股|認沽|认沽|基金|債|债)",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ActivePoolConfig:
    target_size: int = 200
    pinned_max_size: int = 100
    rank_window_days: int = 5
    rank_metric: str = "avg_turnover"
    exclude_instrument_types: tuple[str, ...] = DEFAULT_EXCLUDE_INSTRUMENT_TYPES
    eviction_grace_seconds: float = 300


@dataclass(frozen=True)
class InstrumentClassification:
    symbol: str
    eligible: bool
    source: str
    instrument_type: str = ""
    name: str = ""
    excluded_reason: str = ""


@dataclass(frozen=True)
class ActiveSymbolRank:
    symbol: str
    rank: int
    avg_turnover: float
    avg_volume: float
    observation_count: int
    latest_trade_date: str
    classification: InstrumentClassification


@dataclass(frozen=True)
class PoolChange:
    symbol: str
    disposition: str
    promoted: bool = False
    added_symbols: list[str] = field(default_factory=list)
    evicted_symbols: list[str] = field(default_factory=list)
    reason: str = ""


class ActiveSymbolPoolManager:
    """Maintains the active base, query-pinned, and temporary symbol pools."""

    def __init__(
        self,
        mammoth: Any,
        *,
        trade_date: str,
        config: ActivePoolConfig | None = None,
        query_pinned: Iterable[str] | None = None,
    ) -> None:
        self.mammoth = mammoth
        self.trade_date = trade_date
        self.config = config or ActivePoolConfig()
        self.query_pinned: list[str] = unique_normalized_symbols(query_pinned or [])
        self.temporary_symbols: set[str] = set()
        self.base_active: list[str] = []
        self.active_pool: list[str] = []
        self.ranks_by_symbol: dict[str, ActiveSymbolRank] = {}
        self.classification_by_symbol: dict[str, InstrumentClassification] = {}
        self.excluded_symbols: dict[str, InstrumentClassification] = {}
        self.pool_churn_count = 0
        self.last_evicted_symbols: list[str] = []
        self.evicted_symbols: list[str] = []
        self.pinned_pool_full_rejections = 0
        self._explicit_base_symbols: list[str] | None = None

    def rebuild_base_active(self) -> list[str]:
        ranks, excluded = rank_active_symbols(self.mammoth, self.trade_date, self.config)
        self.ranks_by_symbol = {rank.symbol: rank for rank in ranks}
        self.classification_by_symbol = {rank.symbol: rank.classification for rank in ranks}
        self.excluded_symbols = excluded
        self._explicit_base_symbols = None
        self._recompute_active_pool()
        return list(self.active_pool)

    def bootstrap_explicit_symbols(self, symbols: Iterable[str]) -> list[str]:
        self._explicit_base_symbols = unique_normalized_symbols(symbols)
        if not self.ranks_by_symbol:
            ranks, excluded = rank_active_symbols(self.mammoth, self.trade_date, self.config)
            self.ranks_by_symbol = {rank.symbol: rank for rank in ranks}
            self.classification_by_symbol = {rank.symbol: rank.classification for rank in ranks}
            self.excluded_symbols = excluded
        self._recompute_active_pool()
        return list(self.active_pool)

    def note_query(self, raw_symbol: str) -> PoolChange:
        symbol = normalize_subscription_symbol(raw_symbol)
        before = set(self.active_pool)
        if symbol in before:
            return PoolChange(symbol=symbol, disposition="active", reason="already_active")
        if symbol in self.query_pinned:
            self._recompute_active_pool()
            return PoolChange(symbol=symbol, disposition="pinned", promoted=symbol in self.active_pool)
        if len(self.query_pinned) >= self.config.pinned_max_size:
            self.temporary_symbols.add(symbol)
            self.pinned_pool_full_rejections += 1
            return PoolChange(
                symbol=symbol,
                disposition="temporary",
                reason="pinned_pool_full",
            )

        self.query_pinned.append(symbol)
        self._recompute_active_pool()
        after = set(self.active_pool)
        added = sorted(after - before)
        evicted = sorted(before - after)
        self._record_churn(evicted)
        return PoolChange(
            symbol=symbol,
            disposition="pinned",
            promoted=symbol in after,
            added_symbols=added,
            evicted_symbols=evicted,
            reason="query_pinned",
        )

    def manual_pin(self, raw_symbol: str) -> PoolChange:
        symbol = normalize_subscription_symbol(raw_symbol)
        if symbol in self.query_pinned:
            return PoolChange(symbol=symbol, disposition="pinned", promoted=symbol in self.active_pool, reason="already_pinned")
        return self.note_query(symbol)

    def manual_unpin(self, raw_symbol: str) -> PoolChange:
        symbol = normalize_subscription_symbol(raw_symbol)
        before = set(self.active_pool)
        if symbol not in self.query_pinned:
            self.temporary_symbols.discard(symbol)
            return PoolChange(symbol=symbol, disposition="not_pinned", reason="not_pinned")
        self.query_pinned = [pinned for pinned in self.query_pinned if pinned != symbol]
        self._recompute_active_pool()
        after = set(self.active_pool)
        added = sorted(after - before)
        evicted = sorted(before - after)
        self._record_churn(evicted)
        return PoolChange(
            symbol=symbol,
            disposition="unpinned",
            added_symbols=added,
            evicted_symbols=evicted,
            reason="manual_unpin",
        )

    def release_temporary(self, raw_symbol: str) -> None:
        self.temporary_symbols.discard(normalize_subscription_symbol(raw_symbol))

    def is_active(self, raw_symbol: str) -> bool:
        return normalize_subscription_symbol(raw_symbol) in self.active_pool

    def is_pinned(self, raw_symbol: str) -> bool:
        return normalize_subscription_symbol(raw_symbol) in self.query_pinned

    def is_temporary(self, raw_symbol: str) -> bool:
        return normalize_subscription_symbol(raw_symbol) in self.temporary_symbols

    def active_symbols(self) -> list[str]:
        return list(self.active_pool)

    def explain(self, raw_symbol: str) -> dict[str, Any]:
        symbol = normalize_subscription_symbol(raw_symbol)
        rank = self.ranks_by_symbol.get(symbol)
        classification = self.classification_by_symbol.get(symbol) or self.excluded_symbols.get(symbol)
        return {
            "symbol": symbol,
            "trade_date": self.trade_date,
            "active": symbol in self.active_pool,
            "base_active": symbol in self.base_active,
            "query_pinned": symbol in self.query_pinned,
            "temporary": symbol in self.temporary_symbols,
            "rank": asdict(rank) if rank is not None else None,
            "classification": asdict(classification) if classification is not None else None,
            "reason": self._explain_reason(symbol, rank, classification),
        }

    def snapshot(self) -> dict[str, Any]:
        classification_sources = {
            classification.source
            for classification in [*self.classification_by_symbol.values(), *self.excluded_symbols.values()]
            if classification.source
        }
        if not classification_sources:
            classification_source = "unknown"
        elif classification_sources == {"fallback"}:
            classification_source = "fallback"
        elif "fallback" in classification_sources:
            classification_source = "mixed"
        else:
            classification_source = "instrument_table"
        return {
            "trade_date": self.trade_date,
            "target_size": self.config.target_size,
            "pinned_max_size": self.config.pinned_max_size,
            "rank_window_days": self.config.rank_window_days,
            "rank_metric": self.config.rank_metric,
            "exclude_instrument_types": list(self.config.exclude_instrument_types),
            "active_size": len(self.active_pool),
            "base_size": len(self.base_active),
            "pinned_size": len(self.query_pinned),
            "temporary_size": len(self.temporary_symbols),
            "active_symbols": list(self.active_pool),
            "base_active": list(self.base_active),
            "query_pinned": list(self.query_pinned),
            "temporary_symbols": sorted(self.temporary_symbols),
            "pool_churn_count": self.pool_churn_count,
            "last_evicted_symbols": list(self.last_evicted_symbols),
            "evicted_symbols": list(self.evicted_symbols),
            "pinned_pool_full_rejections": self.pinned_pool_full_rejections,
            "instrument_classification_source": classification_source,
            "ranked_symbol_count": len(self.ranks_by_symbol),
            "excluded_symbol_count": len(self.excluded_symbols),
            "explicit_base_symbols": self._explicit_base_symbols is not None,
        }

    def _recompute_active_pool(self) -> None:
        pinned = list(self.query_pinned)
        target_size = max(0, int(self.config.target_size))
        if self._explicit_base_symbols is not None:
            base_candidates = [symbol for symbol in self._explicit_base_symbols if symbol not in pinned]
        else:
            base_candidates = [symbol for symbol in self.ranks_by_symbol if symbol not in pinned]
        remaining_slots = max(0, target_size - len(pinned))
        self.base_active = base_candidates[:remaining_slots]
        self.active_pool = [*pinned, *self.base_active]

    def _record_churn(self, evicted_symbols: list[str]) -> None:
        if not evicted_symbols:
            return
        self.pool_churn_count += 1
        self.last_evicted_symbols = list(evicted_symbols)
        self.evicted_symbols.extend(evicted_symbols)

    def _explain_reason(
        self,
        symbol: str,
        rank: ActiveSymbolRank | None,
        classification: InstrumentClassification | None,
    ) -> str:
        if symbol in self.query_pinned:
            return "query_pinned"
        if symbol in self.temporary_symbols:
            return "temporary_pinned_pool_full"
        if symbol in self.base_active:
            return f"ranked_by_{self.config.rank_metric}"
        if classification is not None and not classification.eligible:
            return classification.excluded_reason
        if rank is None:
            return "not_ranked"
        return "below_active_pool_cutoff"


def rank_active_symbols(
    mammoth: Any,
    trade_date: str,
    config: ActivePoolConfig,
) -> tuple[list[ActiveSymbolRank], dict[str, InstrumentClassification]]:
    daily_rows = read_all_daily_bars(mammoth)
    instrument_rows = read_instruments(mammoth)
    instrument_by_symbol = {
        normalize_subscription_symbol(str(row.get("symbol") or "")): row
        for row in instrument_rows
        if str(row.get("symbol") or "").strip()
    }
    rows_by_symbol: dict[str, list[dict[str, Any]]] = {}
    classification_by_symbol: dict[str, InstrumentClassification] = {}
    excluded: dict[str, InstrumentClassification] = {}
    for row in daily_rows:
        raw_symbol = str(row.get("symbol") or "")
        try:
            symbol = normalize_subscription_symbol(raw_symbol)
        except ValueError:
            continue
        if not symbol.endswith(".HK"):
            continue
        if str(row.get("trade_date") or "") > trade_date:
            continue
        classification = classification_by_symbol.get(symbol)
        if classification is None:
            classification = classify_instrument(
                symbol,
                instrument_by_symbol.get(symbol, {}),
                exclude_instrument_types=config.exclude_instrument_types,
            )
            classification_by_symbol[symbol] = classification
        if not classification.eligible:
            excluded[symbol] = classification
            continue
        rows_by_symbol.setdefault(symbol, []).append(row)

    ranks: list[ActiveSymbolRank] = []
    for symbol, rows in rows_by_symbol.items():
        rows.sort(key=lambda item: str(item.get("trade_date") or ""))
        window = rows[-max(1, config.rank_window_days):]
        avg_turnover = average(row.get("turnover") for row in window)
        avg_volume = average(row.get("volume") for row in window)
        ranks.append(
            ActiveSymbolRank(
                symbol=symbol,
                rank=0,
                avg_turnover=avg_turnover,
                avg_volume=avg_volume,
                observation_count=len(window),
                latest_trade_date=str(window[-1].get("trade_date") or "") if window else "",
                classification=classification_by_symbol[symbol],
            )
        )

    metric = "avg_volume" if config.rank_metric == "avg_volume" else "avg_turnover"
    ranks.sort(key=lambda item: (-float(getattr(item, metric)), item.symbol))
    ranked = [
        ActiveSymbolRank(
            symbol=item.symbol,
            rank=index,
            avg_turnover=item.avg_turnover,
            avg_volume=item.avg_volume,
            observation_count=item.observation_count,
            latest_trade_date=item.latest_trade_date,
            classification=item.classification,
        )
        for index, item in enumerate(ranks, start=1)
    ]
    return ranked, excluded


def classify_instrument(
    symbol: str,
    instrument: dict[str, Any],
    *,
    exclude_instrument_types: Iterable[str],
) -> InstrumentClassification:
    instrument_type = normalized_instrument_type(instrument)
    name = instrument_name(instrument)
    excluded_types = {normalize_type(value) for value in exclude_instrument_types}
    if instrument_type:
        if instrument_type in excluded_types:
            return InstrumentClassification(
                symbol=symbol,
                eligible=False,
                source="instrument_table",
                instrument_type=instrument_type,
                name=name,
                excluded_reason=f"excluded_instrument_type:{instrument_type}",
            )
        if FALLBACK_EXCLUDE_NAME_PATTERN.search(name):
            return InstrumentClassification(
                symbol=symbol,
                eligible=False,
                source="instrument_table",
                instrument_type=instrument_type,
                name=name,
                excluded_reason="excluded_name_pattern",
            )
        return InstrumentClassification(
            symbol=symbol,
            eligible=instrument_type in EQUITY_INSTRUMENT_TYPES or instrument_type not in excluded_types,
            source="instrument_table",
            instrument_type=instrument_type,
            name=name,
        )

    fallback_reason = fallback_excluded_reason(symbol, name)
    return InstrumentClassification(
        symbol=symbol,
        eligible=not fallback_reason,
        source="fallback",
        name=name,
        excluded_reason=fallback_reason,
    )


def normalized_instrument_type(row: dict[str, Any]) -> str:
    for key in INSTRUMENT_TYPE_KEYS:
        value = row.get(key)
        if value not in (None, ""):
            return normalize_type(str(value))
    return ""


def instrument_name(row: dict[str, Any]) -> str:
    for key in INSTRUMENT_NAME_KEYS:
        value = row.get(key)
        if value not in (None, ""):
            return str(value)
    return ""


def normalize_type(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", value.strip().upper()).strip("_")
    aliases = {
        "EXCHANGE_TRADED_FUND": "ETF",
        "WARRANTS": "WARRANT",
        "WARRANT": "WARRANT",
        "CALLABLE_BULL_BEAR_CONTRACT": "CBBC",
        "CALLABLE_BULL_BEAR_CONTRACTS": "CBBC",
        "BULL_BEAR": "CBBC",
        "COMMON": "COMMON_STOCK",
        "ORDINARY_STOCK": "ORDINARY_SHARE",
    }
    return aliases.get(normalized, normalized)


def fallback_excluded_reason(symbol: str, name: str) -> str:
    if FALLBACK_EXCLUDE_NAME_PATTERN.search(name):
        return "excluded_name_pattern"
    code = symbol.split(".", 1)[0]
    try:
        numeric_code = int(code)
    except ValueError:
        return "invalid_hk_symbol"
    if numeric_code >= 10000:
        return "excluded_derivative_code_range"
    return ""


def read_all_daily_bars(mammoth: Any) -> list[dict[str, Any]]:
    reader = getattr(mammoth, "get_all_daily_bars", None)
    if callable(reader):
        return list(reader())
    private_reader = getattr(mammoth, "_read_table", None)
    if callable(private_reader):
        return list(private_reader("daily_bars"))
    return []


def read_instruments(mammoth: Any) -> list[dict[str, Any]]:
    reader = getattr(mammoth, "get_instruments", None)
    if callable(reader):
        try:
            return list(reader())
        except Exception:
            return []
    return []


def average(values: Iterable[Any]) -> float:
    numeric_values = []
    for value in values:
        try:
            numeric_values.append(float(value))
        except (TypeError, ValueError):
            continue
    if not numeric_values:
        return 0.0
    return sum(numeric_values) / len(numeric_values)


def unique_normalized_symbols(symbols: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for raw_symbol in symbols:
        try:
            symbol = normalize_subscription_symbol(raw_symbol)
        except ValueError:
            continue
        if symbol not in seen:
            seen.add(symbol)
            result.append(symbol)
    return result
