from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PerformanceSla:
    collector_to_kafka_p95_ms: float = 30
    collector_to_kafka_p99_ms: float = 100
    processed_to_gateway_p95_ms: float = 50
    gateway_to_frontend_p95_ms: float = 50
    subscribe_snapshot_p95_ms: float = 200
    frontend_store_update_p95_ms: float = 250
    min_samples_per_key: int = 3


def percentile(values: list[float], percentile_rank: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    index = (len(ordered) - 1) * percentile_rank
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = index - lower
    return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def evaluate_performance(samples: dict[str, list[float]], sla: PerformanceSla | None = None) -> dict[str, object]:
    sla = sla or PerformanceSla()
    required_sample_keys = (
        "collector_to_kafka_ms",
        "processed_to_gateway_ms",
        "gateway_to_frontend_ms",
        "subscribe_snapshot_ms",
        "frontend_store_update_ms",
    )
    missing_sample_keys = [key for key in required_sample_keys if not samples.get(key)]
    sample_counts = {key: len(samples.get(key, [])) for key in required_sample_keys}
    insufficient_sample_keys = [
        key
        for key in required_sample_keys
        if key not in missing_sample_keys and sample_counts[key] < sla.min_samples_per_key
    ]
    metrics = {
        "collector_to_kafka": {
            "p95_ms": percentile(samples.get("collector_to_kafka_ms", []), 0.95),
            "p99_ms": percentile(samples.get("collector_to_kafka_ms", []), 0.99),
            "p95_target_ms": sla.collector_to_kafka_p95_ms,
            "p99_target_ms": sla.collector_to_kafka_p99_ms,
        },
        "processed_to_gateway": {
            "p95_ms": percentile(samples.get("processed_to_gateway_ms", []), 0.95),
            "p95_target_ms": sla.processed_to_gateway_p95_ms,
        },
        "gateway_to_frontend": {
            "p95_ms": percentile(samples.get("gateway_to_frontend_ms", []), 0.95),
            "p95_target_ms": sla.gateway_to_frontend_p95_ms,
        },
        "subscribe_snapshot": {
            "p95_ms": percentile(samples.get("subscribe_snapshot_ms", []), 0.95),
            "p95_target_ms": sla.subscribe_snapshot_p95_ms,
        },
        "frontend_store_update": {
            "p95_ms": percentile(samples.get("frontend_store_update_ms", []), 0.95),
            "p95_target_ms": sla.frontend_store_update_p95_ms,
        },
    }
    passed = (
        not missing_sample_keys
        and not insufficient_sample_keys
        and metrics["collector_to_kafka"]["p95_ms"] <= sla.collector_to_kafka_p95_ms
        and metrics["collector_to_kafka"]["p99_ms"] <= sla.collector_to_kafka_p99_ms
        and metrics["processed_to_gateway"]["p95_ms"] <= sla.processed_to_gateway_p95_ms
        and metrics["gateway_to_frontend"]["p95_ms"] <= sla.gateway_to_frontend_p95_ms
        and metrics["subscribe_snapshot"]["p95_ms"] <= sla.subscribe_snapshot_p95_ms
        and metrics["frontend_store_update"]["p95_ms"] <= sla.frontend_store_update_p95_ms
    )
    return {
        "passed": passed,
        "missing_sample_keys": missing_sample_keys,
        "insufficient_sample_keys": insufficient_sample_keys,
        "min_samples_per_key": sla.min_samples_per_key,
        "sample_counts": sample_counts,
        "metrics": metrics,
    }
