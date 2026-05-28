from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from time import perf_counter
from typing import Any, Sequence

from .adapters import InMemoryEventBus, InMemoryRedisSnapshotCache
from .gateway_transport import GatewayV2SessionManager
from .performance import percentile
from .pipeline import GatewayV2
from .symbol_runtime import SymbolRuntimeManager


@dataclass(frozen=True)
class BackendPerformanceSmokeConfig:
    client_count: int = 10
    symbol_count: int = 200
    overlap_symbol_count: int = 20
    trade_date: str = "20260526"
    warm_subscribe_p95_target_ms: float = 200.0
    hot_subscribe_p95_target_ms: float = 100.0
    client_queue_size: int = 100


@dataclass(frozen=True)
class BackendPerformanceSmokeResult:
    passed: bool
    blockers: list[str]
    config: dict[str, Any]
    metrics: dict[str, Any]
    samples: dict[str, list[float]]
    symbol_runtime_manager: dict[str, Any]
    client_queue: dict[str, Any]


def run_backend_performance_smoke(config: BackendPerformanceSmokeConfig | None = None) -> BackendPerformanceSmokeResult:
    config = config or BackendPerformanceSmokeConfig()
    validate_backend_performance_smoke_config(config)
    symbols = generated_hk_symbols(config.symbol_count)
    clients = [f"client-{index + 1:02d}" for index in range(config.client_count)]
    hydrate_calls: dict[str, int] = {}
    bus = InMemoryEventBus()
    cache = InMemoryRedisSnapshotCache()
    gateway = GatewayV2(bus, cache)
    runtime_manager = SymbolRuntimeManager(
        gateway,
        trade_date=config.trade_date,
        hydrate_symbol=lambda symbol: hydrate_smoke_symbol(symbol, config.trade_date, hydrate_calls),
        max_concurrent_hydrations=max(1, min(32, config.symbol_count)),
    )
    session_manager = GatewayV2SessionManager(
        gateway,
        trade_date=config.trade_date,
        symbol_runtime_manager=runtime_manager,
        client_queue_size=config.client_queue_size,
        consume_processed_on_broadcast=False,
    )
    for client_id in clients:
        session_manager.connect(client_id)
        session_manager.flush(client_id)

    cold_samples: list[float] = []
    hot_samples: list[float] = []
    overlap_symbols = symbols[: config.overlap_symbol_count]
    remaining_symbols = symbols[config.overlap_symbol_count :]

    for index, symbol in enumerate(overlap_symbols):
        cold_samples.append(subscribe_once(session_manager, clients[index % len(clients)], symbol))

    for symbol in overlap_symbols:
        for client_id in clients[1:]:
            hot_samples.append(subscribe_once(session_manager, client_id, symbol))

    for index, symbol in enumerate(remaining_symbols):
        cold_samples.append(subscribe_once(session_manager, clients[index % len(clients)], symbol))

    all_subscribe_samples = session_manager.performance_snapshot().get("subscribe_snapshot_ms", [])
    duplicate_hydrations = sum(max(0, count - 1) for count in hydrate_calls.values())
    missing_hydrate_symbols = [symbol for symbol in symbols if hydrate_calls.get(symbol) != 1]
    warm_p95 = percentile(cold_samples, 0.95)
    hot_p95 = percentile(hot_samples, 0.95)
    manager_snapshot = runtime_manager.manager_snapshot()
    client_queue = session_manager.client_queue_snapshot()

    blockers: list[str] = []
    if len(hydrate_calls) != config.symbol_count:
        blockers.append("performance_smoke_hydrate_symbol_count_mismatch")
    if duplicate_hydrations:
        blockers.append("performance_smoke_duplicate_hydrations_present")
    if missing_hydrate_symbols:
        blockers.append("performance_smoke_missing_or_extra_hydrations")
    if warm_p95 > config.warm_subscribe_p95_target_ms:
        blockers.append("performance_smoke_warm_subscribe_p95_exceeded")
    if hot_p95 > config.hot_subscribe_p95_target_ms:
        blockers.append("performance_smoke_hot_subscribe_p95_exceeded")
    if client_queue["connected_clients"] != config.client_count:
        blockers.append("performance_smoke_connected_client_count_mismatch")
    if manager_snapshot["runtime_count"] != config.symbol_count:
        blockers.append("performance_smoke_runtime_count_mismatch")
    if client_queue["critical_overflow"] or client_queue["alert_dropped"]:
        blockers.append("performance_smoke_critical_message_drop_present")

    return BackendPerformanceSmokeResult(
        passed=not blockers,
        blockers=blockers,
        config=asdict(config),
        metrics={
            "warm_subscribe_p95_ms": warm_p95,
            "warm_subscribe_p95_target_ms": config.warm_subscribe_p95_target_ms,
            "hot_subscribe_p95_ms": hot_p95,
            "hot_subscribe_p95_target_ms": config.hot_subscribe_p95_target_ms,
            "subscribe_sample_count": len(all_subscribe_samples),
            "cold_subscribe_sample_count": len(cold_samples),
            "hot_subscribe_sample_count": len(hot_samples),
            "hydrate_symbol_count": len(hydrate_calls),
            "duplicate_hydrations": duplicate_hydrations,
            "missing_hydrate_symbols": missing_hydrate_symbols,
            "overlap_symbol_count": len(overlap_symbols),
            "max_overlap_ref_count": max(
                runtime_manager.runtimes[symbol].ref_count for symbol in overlap_symbols
            )
            if overlap_symbols
            else 0,
        },
        samples={
            "subscribe_snapshot_ms": list(all_subscribe_samples),
            "cold_subscribe_ms": cold_samples,
            "hot_subscribe_ms": hot_samples,
        },
        symbol_runtime_manager=manager_snapshot,
        client_queue=client_queue,
    )


def subscribe_once(session_manager: GatewayV2SessionManager, client_id: str, symbol: str) -> float:
    started = perf_counter()
    session_manager.handle_message(
        client_id,
        {
            "schema_version": 1,
            "protocol": "terminal-message-v1",
            "action": "subscribe",
            "symbol": symbol,
            "client_id": client_id,
        },
    )
    return max(0.0, (perf_counter() - started) * 1000)


def hydrate_smoke_symbol(symbol: str, trade_date: str, hydrate_calls: dict[str, int]) -> dict[str, Any]:
    hydrate_calls[symbol] = hydrate_calls.get(symbol, 0) + 1
    return smoke_snapshot_payload(symbol, trade_date)


def smoke_snapshot_payload(symbol: str, trade_date: str) -> dict[str, Any]:
    return {
        "snapshot": {
            "symbol": symbol,
            "name": symbol,
            "currency": "HKD",
            "tradeDate": trade_date,
            "requestedTradeDate": trade_date,
            "isHistoricalSession": False,
            "price": 10.0,
            "previousClose": 9.8,
            "open": 9.9,
            "high": 10.1,
            "low": 9.7,
            "volume": 1000,
            "turnover": 10000.0,
            "change": 0.2,
            "changePercent": 2.0408,
            "updatedAt": "2026-05-26T09:30:00+08:00",
        },
        "minute_bars": [
            {
                "timestamp": "2026-05-26T09:30:00+08:00",
                "price": 10.0,
                "open": 9.9,
                "high": 10.1,
                "low": 9.9,
                "close": 10.0,
                "volume": 1000,
                "turnover": 10000.0,
                "direction": "up",
            }
        ],
        "alerts": [],
        "broker_queue": {"ask": [], "bid": []},
        "ccass_holdings": [],
        "freshness": {
            "updated_at": "2026-05-26T09:30:00+08:00",
            "requested_trade_date": trade_date,
            "effective_trade_date": trade_date,
            "source_dates": {"minute_bars": trade_date, "daily_bars": trade_date},
            "runtime_state": "WARM",
            "degraded_reasons": [],
        },
    }


def generated_hk_symbols(count: int) -> list[str]:
    return [f"{index:05d}.HK" for index in range(1, count + 1)]


def validate_backend_performance_smoke_config(config: BackendPerformanceSmokeConfig) -> None:
    if config.client_count < 1:
        raise ValueError("client_count must be positive")
    if config.symbol_count < 1:
        raise ValueError("symbol_count must be positive")
    if config.overlap_symbol_count < 0 or config.overlap_symbol_count > config.symbol_count:
        raise ValueError("overlap_symbol_count must be between 0 and symbol_count")
    if config.client_queue_size < 1:
        raise ValueError("client_queue_size must be positive")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m beast_market.performance_smoke")
    parser.add_argument("--client-count", type=int, default=10)
    parser.add_argument("--symbol-count", type=int, default=200)
    parser.add_argument("--overlap-symbol-count", type=int, default=20)
    parser.add_argument("--trade-date", default="20260526")
    parser.add_argument("--output-path", default="")
    args = parser.parse_args(argv)
    result = run_backend_performance_smoke(
        BackendPerformanceSmokeConfig(
            client_count=args.client_count,
            symbol_count=args.symbol_count,
            overlap_symbol_count=args.overlap_symbol_count,
            trade_date=args.trade_date,
        )
    )
    payload = asdict(result)
    if args.output_path:
        path = Path(args.output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(payload, sort_keys=True))
    return 0 if result.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
