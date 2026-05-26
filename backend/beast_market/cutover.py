from __future__ import annotations

import json
import hashlib
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .contracts import (
    PROCESSED_TOPIC,
    RAW_TOPIC,
    REDIS_RUNTIME_SNAPSHOT_KEY_FAMILIES,
    SCHEMA_VERSION,
    TERMINAL_MESSAGE_PROTOCOL,
    now_iso,
)
from .app_runtime import evaluate_runtime_config_artifact
from .mammoth_api import REQUIRED_HISTORICAL_MANIFEST_TYPES, source_data_type_for_manifest
from .performance import percentile
from .shadow_run import ShadowRunFiles, load_shadow_run_files

DEFAULT_LEGACY_TOPIC_NAMES = ("legacy_ticks", "legacy_broker_queue")
REQUIRED_RUNTIME_TOPICS = (RAW_TOPIC,)
OPTIONAL_RUNTIME_TOPICS = (PROCESSED_TOPIC,)
FRONTEND_PROTOCOL = TERMINAL_MESSAGE_PROTOCOL
GATEWAY_WEBSOCKET_PATH = "/ws"
SYMBOL_RUNTIME_STATES = ("COLD", "HYDRATING", "WARM", "LIVE", "DEGRADED", "EVICTING")
SYMBOL_SCOPED_HISTORICAL_MANIFEST_TYPES = (
    "daily_bars",
    "trade_ticks",
    "ccass_holdings",
    "participant_history",
    "broker_queue",
)


@dataclass(frozen=True)
class CutoverPolicy:
    min_parallel_session_count: int = 1
    min_session_duration_seconds: float = 4 * 60 * 60
    min_stream_coverage_ratio: float = 0.9
    require_non_empty_streams: bool = True
    require_no_failed_symbols: bool = True
    allow_legacy_retirement: bool = True


@dataclass(frozen=True)
class CutoverArtifactPaths:
    report_path: Path
    readiness_path: Path
    frontend_env_path: Path


@dataclass(frozen=True)
class EvidenceBundlePaths:
    shadow_reports_directory: Path
    manifest_directory: Path
    runtime_config_path: Path
    runtime_health_path: Path
    readiness_path: Path
    frontend_deployment_path: Path
    legacy_decommission_path: Path
    legacy_retirement_path: Path
    multi_trader_smoke_path: Path | None = None
    multi_trader_smoke_preflight_path: Path | None = None
    multi_trader_smoke_manifest_path: Path | None = None
    multi_trader_smoke_package_path: Path | None = None
    multi_trader_smoke_package_metadata_path: Path | None = None


@dataclass(frozen=True)
class LegacyRetirementEvidence:
    frontend_default_v2_deployed: bool = False
    legacy_websocket_disabled: bool = False
    old_topic_consumers_disabled: bool = False
    no_legacy_consumers_observed: bool = False
    rollback_window_completed: bool = False
    rollback_window_started_at: str = ""
    rollback_window_completed_at: str = ""
    operator_approved: bool = False
    operator_approved_at: str = ""
    notes: str = ""


@dataclass(frozen=True)
class FrontendDeploymentEvidence:
    expected_live_url: str
    deployed_env: dict[str, str]
    verified_at: str = ""


@dataclass(frozen=True)
class LegacyDecommissionObservation:
    legacy_websocket_enabled: bool
    old_topic_consumers: dict[str, int]
    old_topic_lag: dict[str, int]
    observed_at: str


def evaluate_cutover_readiness(
    reports: list[dict[str, Any]],
    policy: CutoverPolicy | None = None,
) -> dict[str, Any]:
    policy = policy or CutoverPolicy()
    blockers: list[str] = []
    accepted_report_ids: list[str] = []
    rejected_reports: list[dict[str, Any]] = []

    if not reports:
        blockers.append("no_shadow_run_reports")

    for index, report in enumerate(reports):
        report_blockers = blockers_for_report(report, policy)
        report_id = str(report.get("session_id") or f"report-{index + 1}")
        if report_blockers:
            rejected_reports.append({"session_id": report_id, "blockers": report_blockers})
        else:
            accepted_report_ids.append(report_id)

    if len(accepted_report_ids) < policy.min_parallel_session_count:
        blockers.append("insufficient_accepted_parallel_sessions")

    frontend_default_v2_allowed = not blockers
    legacy_retirement_blockers = []
    if frontend_default_v2_allowed and not policy.allow_legacy_retirement:
        legacy_retirement_blockers.append("legacy_retirement_requires_operator_approval")
    legacy_retirement_allowed = frontend_default_v2_allowed and not legacy_retirement_blockers

    return {
        "schema_version": 1,
        "passed": frontend_default_v2_allowed,
        "frontend_default_v2_allowed": frontend_default_v2_allowed,
        "legacy_retirement_allowed": legacy_retirement_allowed,
        "blockers": blockers,
        "legacy_retirement_blockers": legacy_retirement_blockers,
        "report_count": len(reports),
        "accepted_report_ids": accepted_report_ids,
        "rejected_reports": rejected_reports,
        "policy": {
            "min_parallel_session_count": policy.min_parallel_session_count,
            "min_session_duration_seconds": policy.min_session_duration_seconds,
            "min_stream_coverage_ratio": policy.min_stream_coverage_ratio,
            "require_non_empty_streams": policy.require_non_empty_streams,
            "require_no_failed_symbols": policy.require_no_failed_symbols,
            "allow_legacy_retirement": policy.allow_legacy_retirement,
        },
    }


def evaluate_legacy_retirement(
    readiness: dict[str, Any],
    evidence: LegacyRetirementEvidence,
) -> dict[str, Any]:
    blockers = []

    if readiness.get("legacy_retirement_allowed") is not True:
        blockers.append("cutover_gate_does_not_allow_legacy_retirement")
    if not evidence.frontend_default_v2_deployed:
        blockers.append("frontend_default_v2_not_deployed")
    if not evidence.legacy_websocket_disabled:
        blockers.append("legacy_websocket_still_enabled")
    if not evidence.old_topic_consumers_disabled:
        blockers.append("old_topic_consumers_still_enabled")
    if not evidence.no_legacy_consumers_observed:
        blockers.append("legacy_consumers_still_observed")
    if not evidence.rollback_window_completed:
        blockers.append("rollback_window_not_completed")
    if not evidence.rollback_window_started_at:
        blockers.append("rollback_window_started_at_missing")
    elif not is_iso_datetime(evidence.rollback_window_started_at):
        blockers.append("rollback_window_started_at_invalid")
    if not evidence.rollback_window_completed_at:
        blockers.append("rollback_window_completed_at_missing")
    elif not is_iso_datetime(evidence.rollback_window_completed_at):
        blockers.append("rollback_window_completed_at_invalid")
    if (
        is_iso_datetime(evidence.rollback_window_started_at)
        and is_iso_datetime(evidence.rollback_window_completed_at)
        and iso_datetime_is_before(evidence.rollback_window_completed_at, evidence.rollback_window_started_at)
    ):
        blockers.append("rollback_window_completed_before_start")
    if not evidence.operator_approved:
        blockers.append("operator_approval_missing")
    if not evidence.operator_approved_at:
        blockers.append("operator_approved_at_missing")
    elif not is_iso_datetime(evidence.operator_approved_at):
        blockers.append("operator_approved_at_invalid")
    if (
        is_iso_datetime(evidence.rollback_window_completed_at)
        and is_iso_datetime(evidence.operator_approved_at)
        and iso_datetime_is_before(evidence.operator_approved_at, evidence.rollback_window_completed_at)
    ):
        blockers.append("operator_approved_before_rollback_window_completed")

    return {
        "schema_version": 1,
        "passed": not blockers,
        "legacy_retired": not blockers,
        "blockers": blockers,
        "cutover_readiness": {
            "frontend_default_v2_allowed": readiness.get("frontend_default_v2_allowed") is True,
            "legacy_retirement_allowed": readiness.get("legacy_retirement_allowed") is True,
            "accepted_report_ids": list(readiness.get("accepted_report_ids") or []),
        },
        "evidence": {
            "frontend_default_v2_deployed": evidence.frontend_default_v2_deployed,
            "legacy_websocket_disabled": evidence.legacy_websocket_disabled,
            "old_topic_consumers_disabled": evidence.old_topic_consumers_disabled,
            "no_legacy_consumers_observed": evidence.no_legacy_consumers_observed,
            "rollback_window_completed": evidence.rollback_window_completed,
            "rollback_window_started_at": evidence.rollback_window_started_at,
            "rollback_window_completed_at": evidence.rollback_window_completed_at,
            "operator_approved": evidence.operator_approved,
            "operator_approved_at": evidence.operator_approved_at,
            "notes": evidence.notes,
        },
    }


def evaluate_frontend_deployment(evidence: FrontendDeploymentEvidence) -> dict[str, Any]:
    blockers = []
    mode = evidence.deployed_env.get("VITE_MARKET_DATA_MODE")
    live_url = evidence.deployed_env.get("VITE_MARKET_WS_URL")
    protocol = evidence.deployed_env.get("VITE_MARKET_PROTOCOL")
    readiness_raw = evidence.deployed_env.get("VITE_MARKET_CUTOVER_READINESS", "")
    readiness = parse_readiness(readiness_raw)

    if not evidence.verified_at:
        blockers.append("frontend_verified_at_missing")
    elif not is_iso_datetime(evidence.verified_at):
        blockers.append("frontend_verified_at_invalid")
    if mode not in {"auto", "live"}:
        blockers.append("frontend_data_mode_not_live_or_auto")
    if live_url != evidence.expected_live_url:
        blockers.append("frontend_live_url_mismatch")
    blockers.extend(validate_frontend_live_url(live_url))
    if protocol != FRONTEND_PROTOCOL:
        blockers.append("frontend_protocol_not_terminal_message_v1")
    if readiness is None:
        blockers.append("frontend_cutover_readiness_invalid")
    else:
        readiness_errors = validate_frontend_cutover_readiness_artifact(readiness)
        if readiness_errors:
            blockers.append("frontend_cutover_readiness_artifact_invalid")
            blockers.extend(f"frontend_cutover_readiness_{error}" for error in readiness_errors)
    if mode == "auto" and not frontend_readiness_allows_v2(readiness):
        blockers.append("frontend_auto_mode_would_select_mock")

    return {
        "schema_version": 1,
        "passed": not blockers,
        "frontend_default_v2_deployed": not blockers,
        "blockers": blockers,
        "verified_at": evidence.verified_at,
        "expected_live_url": evidence.expected_live_url,
        "deployed_env": {
            "VITE_MARKET_DATA_MODE": mode,
            "VITE_MARKET_WS_URL": live_url,
            "VITE_MARKET_PROTOCOL": protocol,
            "VITE_MARKET_CUTOVER_READINESS": readiness,
        },
    }


def frontend_readiness_allows_v2(readiness: dict[str, Any] | None) -> bool:
    if not isinstance(readiness, dict):
        return False
    return not validate_frontend_cutover_readiness_artifact(readiness)


def validate_frontend_cutover_readiness_artifact(readiness: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if readiness.get("schema_version") != 1:
        errors.append("schema_invalid")
    if readiness.get("passed") is not True:
        errors.append("not_passed")
    if readiness.get("frontend_default_v2_allowed") is not True:
        errors.append("does_not_allow_v2")
    if readiness.get("blockers") != []:
        errors.append("blockers_not_empty")
    if not non_negative_integer(readiness.get("report_count")) or int(readiness.get("report_count") or 0) <= 0:
        errors.append("report_count_invalid")

    accepted_report_ids = readiness.get("accepted_report_ids")
    if not isinstance(accepted_report_ids, list) or any(
        not isinstance(report_id, str) or not report_id.strip() for report_id in accepted_report_ids
    ):
        errors.append("accepted_report_ids_invalid")
    elif not accepted_report_ids:
        errors.append("accepted_report_ids_empty")
    elif len(accepted_report_ids) != len(set(accepted_report_ids)):
        errors.append("accepted_report_ids_duplicate")

    if not isinstance(readiness.get("rejected_reports"), list):
        errors.append("rejected_reports_invalid")

    policy = readiness.get("policy")
    if not isinstance(policy, dict):
        errors.append("policy_missing")
    else:
        default_policy = CutoverPolicy()
        expected_policy = {
            "min_parallel_session_count": default_policy.min_parallel_session_count,
            "min_session_duration_seconds": default_policy.min_session_duration_seconds,
            "min_stream_coverage_ratio": default_policy.min_stream_coverage_ratio,
            "require_non_empty_streams": default_policy.require_non_empty_streams,
            "require_no_failed_symbols": default_policy.require_no_failed_symbols,
        }
        for field, expected in expected_policy.items():
            if policy.get(field) != expected:
                errors.append(f"policy_{field}_mismatch")
    return errors


def validate_frontend_live_url(live_url: Any) -> list[str]:
    blockers: list[str] = []
    if not isinstance(live_url, str) or not live_url.strip():
        return ["frontend_live_url_missing"]
    parsed = urlparse(live_url)
    try:
        parsed_port = parsed.port
    except ValueError:
        parsed_port = None
        blockers.append("frontend_live_url_invalid")
    if parsed.scheme not in {"ws", "wss"} or not parsed.netloc:
        blockers.append("frontend_live_url_invalid")
    if is_loopback_host(parsed.hostname):
        blockers.append("frontend_live_url_loopback_host")
    if parsed.path != GATEWAY_WEBSOCKET_PATH:
        blockers.append("frontend_live_url_gateway_path_mismatch")
    if parsed_port is None:
        blockers.append("frontend_live_url_gateway_port_missing")
    return blockers


def evaluate_legacy_decommission(
    observation: LegacyDecommissionObservation,
    *,
    expected_old_topics: list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    blockers = []
    expected_topics = list(expected_old_topics or DEFAULT_LEGACY_TOPIC_NAMES)
    consumer_counts = topic_count_evidence(observation.old_topic_consumers)
    lag_counts = topic_count_evidence(observation.old_topic_lag)
    active_consumers = {
        topic: count
        for topic, count in consumer_counts["values"].items()
        if count > 0
    }
    lagging_topics = {
        topic: lag
        for topic, lag in lag_counts["values"].items()
        if lag > 0
    }
    missing_consumer_topics = [
        topic for topic in expected_topics if topic not in consumer_counts["keys"]
    ]
    missing_lag_topics = [
        topic for topic in expected_topics if topic not in lag_counts["keys"]
    ]

    if not observation.observed_at:
        blockers.append("legacy_decommission_observed_at_missing")
    elif not is_iso_datetime(observation.observed_at):
        blockers.append("legacy_decommission_observed_at_invalid")
    if observation.legacy_websocket_enabled:
        blockers.append("legacy_websocket_still_enabled")
    if missing_consumer_topics:
        blockers.append("old_topic_consumer_observation_incomplete")
    if missing_lag_topics:
        blockers.append("old_topic_lag_observation_incomplete")
    if consumer_counts["invalid_topics"]:
        blockers.append("old_topic_consumer_observation_invalid")
    if lag_counts["invalid_topics"]:
        blockers.append("old_topic_lag_observation_invalid")
    if active_consumers:
        blockers.append("old_topic_consumers_still_enabled")
    if lagging_topics:
        blockers.append("old_topic_lag_still_present")

    return {
        "schema_version": 1,
        "passed": not blockers,
        "legacy_websocket_disabled": not observation.legacy_websocket_enabled,
        "old_topic_consumers_disabled": not active_consumers,
        "no_legacy_consumers_observed": not active_consumers and not lagging_topics,
        "blockers": blockers,
        "observation": {
            "observed_at": observation.observed_at,
            "expected_old_topics": expected_topics,
            "legacy_websocket_enabled": observation.legacy_websocket_enabled,
            "old_topic_consumers": dict(observation.old_topic_consumers),
            "old_topic_lag": dict(observation.old_topic_lag),
            "missing_consumer_topics": missing_consumer_topics,
            "missing_lag_topics": missing_lag_topics,
            "invalid_consumer_topics": consumer_counts["invalid_topics"],
            "invalid_lag_topics": lag_counts["invalid_topics"],
            "active_consumers": active_consumers,
            "lagging_topics": lagging_topics,
        },
    }


def evaluate_multi_trader_smoke(evidence: dict[str, Any]) -> dict[str, Any]:
    blockers: list[str] = []
    if evidence.get("schema_version") != 1:
        blockers.append("multi_trader_smoke_schema_invalid")
    observed_at = str(evidence.get("observed_at") or "")
    if not observed_at:
        blockers.append("multi_trader_smoke_observed_at_missing")
    elif not is_iso_datetime(observed_at):
        blockers.append("multi_trader_smoke_observed_at_invalid")

    clients = evidence.get("clients")
    client_evidence = client_smoke_evidence(clients)
    blockers.extend(client_evidence["blockers"])
    performance_artifact_evidence = multi_trader_performance_artifact_evidence(
        evidence.get("performance_artifacts"),
        client_evidence["machines"],
    )
    blockers.extend(performance_artifact_evidence["blockers"])

    workflows = evidence.get("workflows") if isinstance(evidence.get("workflows"), dict) else {}
    required_workflows = {
        "cold_query": "multi_trader_smoke_cold_query_missing",
        "add_to_watchlist": "multi_trader_smoke_add_watchlist_missing",
        "refresh_recovery": "multi_trader_smoke_refresh_recovery_missing",
        "redis_clear_recovery": "multi_trader_smoke_redis_clear_recovery_missing",
        "process_restart_recovery": "multi_trader_smoke_process_restart_recovery_missing",
        "closed_market_effective_date": "multi_trader_smoke_closed_market_effective_date_missing",
    }
    workflow_results = {}
    for workflow, blocker in required_workflows.items():
        passed = workflow_passed(workflows.get(workflow))
        workflow_results[workflow] = passed
        if not passed:
            blockers.append(blocker)
    blockers.extend(workflow_evidence_blockers(workflows))

    closed_market = workflows.get("closed_market_effective_date") if isinstance(workflows, dict) else {}
    if isinstance(closed_market, dict):
        requested_date = str(closed_market.get("requested_trade_date") or "")
        effective_date = str(closed_market.get("effective_trade_date") or "")
        if not requested_date:
            blockers.append("multi_trader_smoke_requested_date_missing")
        elif not is_yyyymmdd(requested_date):
            blockers.append("multi_trader_smoke_requested_date_invalid")
        if not effective_date:
            blockers.append("multi_trader_smoke_effective_date_missing")
        elif not is_yyyymmdd(effective_date):
            blockers.append("multi_trader_smoke_effective_date_invalid")
        if requested_date and effective_date and requested_date == effective_date and closed_market.get("expected_closed_market") is True:
            blockers.append("multi_trader_smoke_closed_market_dates_not_distinct")

    metrics = evidence.get("metrics") if isinstance(evidence.get("metrics"), dict) else {}
    runtime_health = evidence.get("runtime_health")
    warm_snapshot_p95_ms = metrics.get("warm_snapshot_p95_ms")
    if warm_snapshot_p95_ms is None:
        warm_snapshot_p95_ms = derived_warm_snapshot_p95_ms(evidence.get("performance_samples"))
    if warm_snapshot_p95_ms is None and isinstance(runtime_health, dict):
        warm_snapshot_p95_ms = derived_warm_snapshot_p95_ms(runtime_health.get("performance_samples"))
    if warm_snapshot_p95_ms is None and isinstance(runtime_health, dict):
        runtime_performance_metrics = runtime_health.get("performance_metrics")
        if isinstance(runtime_performance_metrics, dict):
            warm_snapshot_p95_ms = runtime_performance_metrics.get("warm_snapshot_p95_ms")
    duplicate_hydrations = metrics.get("duplicate_hydrations")
    if duplicate_hydrations is None:
        duplicate_hydrations = derived_duplicate_hydrations(runtime_health, client_evidence["overlap"])
    if not non_negative_number(warm_snapshot_p95_ms):
        blockers.append("multi_trader_smoke_warm_snapshot_p95_invalid")
    elif float(warm_snapshot_p95_ms) > 200:
        blockers.append("multi_trader_smoke_warm_snapshot_p95_exceeded")
    if not non_negative_integer(duplicate_hydrations):
        blockers.append("multi_trader_smoke_duplicate_hydrations_invalid")
    elif int(duplicate_hydrations) > 0:
        blockers.append("multi_trader_smoke_duplicate_hydrations_present")

    runtime_health_evidence_present = isinstance(runtime_health, dict) and runtime_health.get("passed") is True
    if not runtime_health_evidence_present:
        blockers.append("multi_trader_smoke_runtime_health_evidence_missing")
    runtime_health_reference_present = runtime_health_reference_evidence_present(runtime_health)
    if runtime_health_evidence_present and not runtime_health_reference_present:
        blockers.append("multi_trader_smoke_runtime_health_reference_missing")
    runtime_manager_evidence = multi_trader_runtime_manager_evidence(runtime_health)
    blockers.extend(runtime_manager_evidence["blockers"])
    gateway_evidence = multi_trader_gateway_evidence(runtime_health)
    blockers.extend(gateway_evidence["blockers"])
    gateway_client_activity = multi_trader_gateway_client_activity_evidence(
        runtime_health,
        client_evidence["machines"],
    )
    blockers.extend(gateway_client_activity["blockers"])
    preflight_evidence = multi_trader_smoke_preflight_evidence(evidence.get("preflight"))
    blockers.extend(preflight_evidence["blockers"])
    client_preflight_network = multi_trader_client_preflight_network_evidence(
        client_evidence["network"],
        preflight_evidence["evidence"],
    )
    blockers.extend(client_preflight_network["blockers"])
    service_preflight_timing = multi_trader_service_preflight_timing_evidence(
        preflight_evidence["evidence"],
        observed_at,
    )
    blockers.extend(service_preflight_timing["blockers"])
    client_artifact_timing = multi_trader_client_artifact_timing_evidence(
        evidence.get("client_artifacts"),
        preflight_evidence["evidence"],
        observed_at,
    )
    blockers.extend(client_artifact_timing["blockers"])
    performance_artifact_timing = multi_trader_performance_artifact_timing_evidence(
        evidence.get("performance_artifacts"),
        preflight_evidence["evidence"],
        observed_at,
    )
    blockers.extend(performance_artifact_timing["blockers"])
    runtime_health_timing = multi_trader_runtime_health_timing_evidence(
        runtime_health,
        preflight_evidence["evidence"],
        observed_at,
    )
    blockers.extend(runtime_health_timing["blockers"])
    workflow_timing = multi_trader_workflow_timing_evidence(
        workflows,
        preflight_evidence["evidence"],
        observed_at,
    )
    blockers.extend(workflow_timing["blockers"])

    return {
        "schema_version": 1,
        "passed": not blockers,
        "blockers": blockers,
        "observed_at": observed_at,
        "client_count": client_evidence["client_count"],
        "client_machines": client_evidence["machines"],
        "client_data_source_modes": client_evidence["data_source_modes"],
        "client_network": client_evidence["network"],
        "watchlist_overlap": client_evidence["overlap"],
        "client_symbol_statuses": client_evidence["symbol_statuses"],
        "performance_artifacts": performance_artifact_evidence["evidence"],
        "workflows": workflow_results,
        "metrics": {
            "warm_snapshot_p95_ms": warm_snapshot_p95_ms,
            "duplicate_hydrations": duplicate_hydrations,
        },
        "runtime_health_evidence_present": runtime_health_evidence_present,
        "runtime_health_reference_present": runtime_health_reference_present,
        "runtime_manager": runtime_manager_evidence["evidence"],
        "gateway_websocket": gateway_evidence["evidence"],
        "gateway_client_activity": gateway_client_activity["evidence"],
        "preflight": preflight_evidence["evidence"],
        "client_preflight_network": client_preflight_network["evidence"],
        "service_preflight_timing": service_preflight_timing["evidence"],
        "client_artifacts": client_artifact_timing["evidence"],
        "performance_artifact_timing": performance_artifact_timing["evidence"],
        "runtime_health_timing": runtime_health_timing["evidence"],
        "workflow_timing": workflow_timing["evidence"],
    }


def multi_trader_performance_artifact_evidence(value: Any, client_machines: list[str]) -> dict[str, Any]:
    if value is None:
        return {
            "blockers": [],
            "evidence": {
                "present": False,
                "machine_ids": [],
                "unknown_machine_ids": [],
                "missing_client_machine_ids": [],
                "sample_counts_by_machine": {},
            },
        }
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        return {
            "blockers": ["multi_trader_smoke_performance_artifacts_invalid"],
            "evidence": {
                "present": True,
                "machine_ids": [],
                "unknown_machine_ids": [],
                "missing_client_machine_ids": client_machines,
                "sample_counts_by_machine": {},
            },
        }
    blockers: list[str] = []
    machine_ids: list[str] = []
    sample_counts_by_machine: dict[str, int] = {}
    invalid_paths = []
    invalid_counts = []
    missing_machine_id_count = 0
    invalid_machine_id_count = 0
    for index, artifact in enumerate(value):
        path = artifact.get("path")
        if not isinstance(path, str) or not path.strip():
            invalid_paths.append(index)
        raw_machine_id = artifact.get("machine_id")
        if raw_machine_id is None or raw_machine_id == "":
            missing_machine_id_count += 1
            machine_id = ""
        elif not normalized_client_id(raw_machine_id):
            invalid_machine_id_count += 1
            machine_id = ""
        else:
            machine_id = str(raw_machine_id)
            machine_ids.append(machine_id)
        sample_count = artifact.get("subscribe_snapshot_count")
        if not non_negative_integer(sample_count):
            invalid_counts.append(index)
        elif machine_id:
            sample_counts_by_machine[machine_id] = sample_counts_by_machine.get(machine_id, 0) + int(sample_count)
    client_machine_set = set(client_machines)
    unknown_machine_ids = sorted(set(machine_ids) - client_machine_set)
    missing_client_machine_ids = sorted(client_machine_set - set(machine_ids))
    if invalid_paths or invalid_counts:
        blockers.append("multi_trader_smoke_performance_artifacts_invalid")
    if missing_machine_id_count:
        blockers.append("multi_trader_smoke_performance_artifact_machine_missing")
    if invalid_machine_id_count:
        blockers.append("multi_trader_smoke_performance_artifact_machine_invalid")
    if unknown_machine_ids:
        blockers.append("multi_trader_smoke_performance_artifact_machine_unknown")
    if client_machines and missing_client_machine_ids:
        blockers.append("multi_trader_smoke_performance_artifact_machine_coverage_missing")
    return {
        "blockers": sorted(set(blockers)),
        "evidence": {
            "present": True,
            "machine_ids": sorted(set(machine_ids)),
            "unknown_machine_ids": unknown_machine_ids,
            "missing_client_machine_ids": missing_client_machine_ids,
            "sample_counts_by_machine": sample_counts_by_machine,
        },
    }


def derived_warm_snapshot_p95_ms(performance_samples: Any) -> float | None:
    if not isinstance(performance_samples, dict):
        return None
    values = performance_samples.get("subscribe_snapshot_ms")
    if not isinstance(values, list) or not values:
        return None
    numeric_values: list[float] = []
    for value in values:
        if not non_negative_number(value):
            return None
        numeric_values.append(float(value))
    return percentile(numeric_values, 0.95)


def derived_duplicate_hydrations(runtime_health: Any, overlap_symbols: list[str]) -> int | None:
    if not isinstance(runtime_health, dict):
        return None
    symbol_runtime = runtime_health.get("symbol_runtime")
    if not isinstance(symbol_runtime, dict):
        return None
    symbols = overlap_symbols or [str(symbol) for symbol in symbol_runtime]
    duplicate_count = 0
    for symbol in symbols:
        runtime = symbol_runtime.get(symbol)
        if not isinstance(runtime, dict):
            return None
        hydrate_count = runtime.get("hydrate_count")
        if not non_negative_integer(hydrate_count):
            return None
        duplicate_count += max(0, int(hydrate_count) - 1)
    return duplicate_count


def runtime_health_reference_evidence_present(runtime_health: Any) -> bool:
    if not isinstance(runtime_health, dict):
        return False
    path = runtime_health.get("path")
    if isinstance(path, str) and path.strip():
        return True
    return isinstance(runtime_health.get("symbol_runtime"), dict) or isinstance(
        runtime_health.get("symbol_runtime_manager"),
        dict,
    )


def multi_trader_runtime_manager_evidence(runtime_health: Any) -> dict[str, Any]:
    if not isinstance(runtime_health, dict):
        return {"blockers": [], "evidence": {}}
    manager = runtime_health.get("symbol_runtime_manager")
    if manager is None:
        return {"blockers": [], "evidence": {}}
    if not isinstance(manager, dict):
        return {
            "blockers": ["multi_trader_smoke_runtime_manager_invalid"],
            "evidence": {},
        }
    blockers: list[str] = []
    active_hydrations = manager.get("active_hydrations")
    max_concurrent_hydrations = manager.get("max_concurrent_hydrations")
    capacity_rejections = manager.get("capacity_rejections")
    hydrating_symbols = symbol_list_evidence(manager.get("hydrating_symbols"))
    evidence = {
        "active_hydrations": non_negative_int_value(active_hydrations),
        "max_concurrent_hydrations": non_negative_int_value(max_concurrent_hydrations),
        "capacity_rejections": non_negative_int_value(capacity_rejections),
        "hydrating_symbols": hydrating_symbols["symbols"],
    }
    if not non_negative_integer(active_hydrations):
        blockers.append("multi_trader_smoke_runtime_manager_metric_invalid")
    if not non_negative_integer(max_concurrent_hydrations) or int(max_concurrent_hydrations or 0) <= 0:
        blockers.append("multi_trader_smoke_runtime_manager_metric_invalid")
    if not non_negative_integer(capacity_rejections):
        blockers.append("multi_trader_smoke_runtime_manager_metric_invalid")
    if hydrating_symbols["shape_invalid"] or hydrating_symbols["invalid_symbols"]:
        blockers.append("multi_trader_smoke_runtime_manager_metric_invalid")
    if (
        non_negative_integer(active_hydrations)
        and non_negative_integer(max_concurrent_hydrations)
        and int(active_hydrations or 0) > int(max_concurrent_hydrations or 0)
    ):
        blockers.append("multi_trader_smoke_runtime_manager_metric_invalid")
    if non_negative_integer(active_hydrations) and int(active_hydrations or 0) > 0:
        blockers.append("multi_trader_smoke_runtime_hydration_still_active")
    if non_negative_integer(capacity_rejections) and int(capacity_rejections or 0) > 0:
        blockers.append("multi_trader_smoke_runtime_capacity_rejections_present")
    return {"blockers": sorted(set(blockers)), "evidence": evidence}


def multi_trader_gateway_evidence(runtime_health: Any) -> dict[str, Any]:
    if not isinstance(runtime_health, dict):
        return {"blockers": [], "evidence": {}}
    gateway = runtime_health.get("gateway_websocket")
    if not isinstance(gateway, dict):
        return {
            "blockers": ["multi_trader_smoke_gateway_websocket_missing"],
            "evidence": {},
        }
    blockers: list[str] = []
    host = gateway.get("host")
    port = gateway.get("port")
    path = gateway.get("path")
    evidence = {
        "host": host if isinstance(host, str) else "",
        "port": int(port) if non_negative_integer(port) else 0,
        "path": path if isinstance(path, str) else "",
    }
    if not isinstance(host, str) or not host.strip():
        blockers.append("multi_trader_smoke_gateway_host_invalid")
    elif is_loopback_host(host):
        blockers.append("multi_trader_smoke_gateway_host_loopback")
    if not non_negative_integer(port) or int(port or 0) <= 0:
        blockers.append("multi_trader_smoke_gateway_port_invalid")
    if path != GATEWAY_WEBSOCKET_PATH:
        blockers.append("multi_trader_smoke_gateway_path_invalid")
    return {"blockers": sorted(set(blockers)), "evidence": evidence}


def multi_trader_gateway_client_activity_evidence(runtime_health: Any, client_machines: list[str]) -> dict[str, Any]:
    if not isinstance(runtime_health, dict):
        return {"blockers": [], "evidence": {"present": False}}
    gateway_activity = runtime_health.get("gateway_activity")
    path = runtime_health.get("path")
    if gateway_activity is None and not (isinstance(path, str) and path.strip()):
        return {"blockers": [], "evidence": {"present": False}}
    if not isinstance(gateway_activity, dict):
        return {
            "blockers": ["multi_trader_smoke_gateway_client_activity_missing"],
            "evidence": {"present": False},
        }
    client_queue = gateway_activity.get("client_queue")
    if not isinstance(client_queue, dict):
        return {
            "blockers": ["multi_trader_smoke_gateway_client_activity_missing"],
            "evidence": {"present": False},
        }

    blockers: list[str] = []
    observed_client_count = client_queue.get("observed_client_count")
    max_connected_clients = client_queue.get("max_connected_clients")
    observed_client_ids = client_queue.get("observed_client_ids")
    observed_declared_client_count = client_queue.get("observed_declared_client_count")
    observed_declared_client_ids = client_queue.get("observed_declared_client_ids")
    observed_client_id_values = [str(client_id) for client_id in observed_client_ids] if isinstance(observed_client_ids, list) else []
    observed_declared_client_id_values = (
        [str(client_id) for client_id in observed_declared_client_ids]
        if isinstance(observed_declared_client_ids, list)
        else []
    )
    observed_duplicate_client_ids = sorted(
        {client_id for client_id in observed_client_id_values if observed_client_id_values.count(client_id) > 1}
    )
    observed_duplicate_declared_client_ids = sorted(
        {
            client_id
            for client_id in observed_declared_client_id_values
            if observed_declared_client_id_values.count(client_id) > 1
        }
    )
    evidence = {
        "present": True,
        "observed_client_count": non_negative_int_value(observed_client_count),
        "max_connected_clients": non_negative_int_value(max_connected_clients),
        "observed_client_ids": observed_client_id_values,
        "observed_declared_client_count": non_negative_int_value(observed_declared_client_count),
        "observed_declared_client_ids": observed_declared_client_id_values,
        "duplicate_observed_client_ids": observed_duplicate_client_ids,
        "duplicate_observed_declared_client_ids": observed_duplicate_declared_client_ids,
        "required_client_count": len(client_machines),
    }
    if not non_negative_integer(observed_client_count) or not non_negative_integer(max_connected_clients):
        blockers.append("multi_trader_smoke_gateway_client_activity_invalid")
    if not isinstance(observed_client_ids, list) or any(
        not normalized_client_id(client_id) for client_id in observed_client_ids
    ):
        blockers.append("multi_trader_smoke_gateway_client_activity_invalid")
    if observed_duplicate_client_ids:
        blockers.append("multi_trader_smoke_gateway_client_ids_duplicate")
    observed_set = {
        str(client_id)
        for client_id in observed_client_ids
        if normalized_client_id(client_id)
    } if isinstance(observed_client_ids, list) else set()
    if non_negative_integer(observed_client_count) and int(observed_client_count or 0) != len(observed_set):
        blockers.append("multi_trader_smoke_gateway_observed_client_count_mismatch")
    if (
        non_negative_integer(max_connected_clients)
        and non_negative_integer(observed_client_count)
        and int(max_connected_clients or 0) > int(observed_client_count or 0)
    ):
        blockers.append("multi_trader_smoke_gateway_max_connected_clients_exceeds_observed")
    if not isinstance(observed_declared_client_ids, list):
        blockers.append("multi_trader_smoke_gateway_declared_client_activity_missing")
        declared_set: set[str] = set()
    else:
        if any(not normalized_client_id(client_id) for client_id in observed_declared_client_ids):
            blockers.append("multi_trader_smoke_gateway_client_activity_invalid")
        if observed_duplicate_declared_client_ids:
            blockers.append("multi_trader_smoke_gateway_declared_client_ids_duplicate")
        declared_set = {str(client_id) for client_id in observed_declared_client_ids if normalized_client_id(client_id)}
        if not non_negative_integer(observed_declared_client_count) or int(observed_declared_client_count or 0) != len(declared_set):
            blockers.append("multi_trader_smoke_gateway_client_activity_invalid")
            blockers.append("multi_trader_smoke_gateway_declared_client_count_mismatch")
    missing_declared_machines = sorted(set(client_machines) - declared_set)
    evidence["missing_declared_client_machines"] = missing_declared_machines
    if missing_declared_machines:
        blockers.append("multi_trader_smoke_gateway_declared_client_coverage_missing")
    if (
        non_negative_integer(observed_client_count)
        and int(observed_client_count or 0) < max(2, len(client_machines))
    ):
        blockers.append("multi_trader_smoke_gateway_observed_clients_insufficient")
    if non_negative_integer(max_connected_clients) and int(max_connected_clients or 0) < 2:
        blockers.append("multi_trader_smoke_gateway_max_connected_clients_insufficient")
    return {"blockers": sorted(set(blockers)), "evidence": evidence}


def multi_trader_smoke_preflight_evidence(preflight: Any) -> dict[str, Any]:
    if preflight is None:
        return {
            "blockers": ["multi_trader_smoke_preflight_missing"],
            "evidence": {"present": False},
        }
    if not isinstance(preflight, dict):
        return {
            "blockers": ["multi_trader_smoke_preflight_invalid"],
            "evidence": {"present": False},
        }
    blockers = validate_multi_trader_smoke_preflight(preflight=preflight, frontend_live_url=None)
    return {
        "blockers": blockers,
        "evidence": {
            "present": True,
            "passed": preflight.get("passed") is True,
            "prepared_at": preflight.get("prepared_at") if isinstance(preflight.get("prepared_at"), str) else "",
            "page_url": preflight.get("page_url") if isinstance(preflight.get("page_url"), str) else "",
            "gateway_url": preflight.get("gateway_url") if isinstance(preflight.get("gateway_url"), str) else "",
            "blockers": list(preflight.get("blockers") or []) if isinstance(preflight.get("blockers"), list) else [],
            "service_checks_present": isinstance(preflight.get("service_checks"), dict),
            "service_checks_passed": (preflight.get("service_checks") or {}).get("passed") is True
            if isinstance(preflight.get("service_checks"), dict)
            else False,
            "service_checked_at": str((preflight.get("service_checks") or {}).get("checked_at") or "")
            if isinstance(preflight.get("service_checks"), dict)
            else "",
        },
    }


def client_smoke_evidence(clients: Any) -> dict[str, Any]:
    blockers: list[str] = []
    if not isinstance(clients, list):
        return {
            "blockers": ["multi_trader_smoke_clients_invalid"],
            "client_count": 0,
            "machines": [],
            "data_source_modes": {},
            "network": {},
            "overlap": [],
            "symbol_statuses": {},
        }
    machines = []
    watchlists: list[set[str]] = []
    symbol_statuses_by_machine: dict[str, dict[str, Any]] = {}
    data_source_modes_by_machine: dict[str, str] = {}
    network_by_machine: dict[str, dict[str, str]] = {}
    for client in clients:
        if not isinstance(client, dict):
            blockers.append("multi_trader_smoke_client_invalid")
            continue
        raw_machine = client.get("machine_id") or client.get("host")
        if raw_machine is None or raw_machine == "":
            blockers.append("multi_trader_smoke_client_machine_missing")
            machine = ""
        elif not normalized_client_id(raw_machine):
            blockers.append("multi_trader_smoke_client_machine_invalid")
            machine = ""
        else:
            machine = str(raw_machine)
            machines.append(machine)
        data_source_mode = str(client.get("data_source_mode") or "").strip()
        if data_source_mode != "live":
            blockers.append("multi_trader_smoke_client_not_live")
        if machine:
            data_source_modes_by_machine[machine] = data_source_mode
        page_url = str(client.get("page_url") or "").strip()
        gateway_url = str(client.get("gateway_url") or "").strip()
        blockers.extend(client_page_url_blockers(page_url))
        blockers.extend(client_gateway_url_blockers(gateway_url))
        if machine:
            network_by_machine[machine] = {"page_url": page_url, "gateway_url": gateway_url}
        watchlist_evidence = symbol_list_evidence(client.get("watchlist"))
        if watchlist_evidence["shape_invalid"] or watchlist_evidence["invalid_symbols"] or watchlist_evidence["duplicate_symbols"]:
            blockers.append("multi_trader_smoke_watchlist_invalid")
        watchlists.append(set(watchlist_evidence["symbols"]))
        status_evidence, status_blockers = client_symbol_status_evidence(
            client.get("symbol_statuses"),
            watchlist_evidence["symbols"],
        )
        blockers.extend(status_blockers)
        if machine:
            symbol_statuses_by_machine[machine] = status_evidence
        if client.get("connected") is not True:
            blockers.append("multi_trader_smoke_client_not_connected")
        if client.get("refresh_recovered") is not True:
            blockers.append("multi_trader_smoke_client_refresh_not_recovered")
    distinct_machines = sorted(set(machines))
    if len(machines) != len(distinct_machines):
        blockers.append("multi_trader_smoke_client_machine_duplicate")
    if len(distinct_machines) < 2:
        blockers.append("multi_trader_smoke_insufficient_client_machines")
    if len(watchlists) < 2:
        blockers.append("multi_trader_smoke_insufficient_watchlists")
        overlap: list[str] = []
    else:
        if len({tuple(sorted(watchlist)) for watchlist in watchlists}) < 2:
            blockers.append("multi_trader_smoke_watchlists_not_distinct")
        overlap = sorted(set.intersection(*watchlists)) if all(watchlists) else []
        if not overlap:
            blockers.append("multi_trader_smoke_watchlist_overlap_missing")
    return {
        "blockers": sorted(set(blockers)),
        "client_count": len(clients),
        "machines": distinct_machines,
        "data_source_modes": data_source_modes_by_machine,
        "network": network_by_machine,
        "overlap": overlap,
        "symbol_statuses": symbol_statuses_by_machine,
    }


def client_page_url_blockers(page_url: str) -> list[str]:
    if not page_url:
        return ["multi_trader_smoke_client_page_url_missing"]
    parsed = urlparse(page_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return ["multi_trader_smoke_client_page_url_invalid"]
    if is_loopback_host(parsed.hostname):
        return ["multi_trader_smoke_client_page_url_loopback"]
    return []


def client_gateway_url_blockers(gateway_url: str) -> list[str]:
    if not gateway_url:
        return ["multi_trader_smoke_client_gateway_url_missing"]
    blockers: list[str] = []
    parsed = urlparse(gateway_url)
    try:
        parsed_port = parsed.port
    except ValueError:
        parsed_port = None
        blockers.append("multi_trader_smoke_client_gateway_url_invalid")
    if parsed.scheme not in {"ws", "wss"} or not parsed.netloc:
        blockers.append("multi_trader_smoke_client_gateway_url_invalid")
    if is_loopback_host(parsed.hostname):
        blockers.append("multi_trader_smoke_client_gateway_url_loopback")
    if parsed.path != GATEWAY_WEBSOCKET_PATH:
        blockers.append("multi_trader_smoke_client_gateway_url_path_invalid")
    if parsed_port is None:
        blockers.append("multi_trader_smoke_client_gateway_url_port_missing")
    return blockers


def multi_trader_client_preflight_network_evidence(
    client_network: dict[str, dict[str, str]],
    preflight: dict[str, Any],
) -> dict[str, Any]:
    preflight_page_url = str(preflight.get("page_url") or "").strip()
    preflight_gateway_url = str(preflight.get("gateway_url") or "").strip()
    if not preflight.get("present") or not preflight_page_url or not preflight_gateway_url:
        return {
            "blockers": [],
            "evidence": {
                "checked": False,
                "preflight_page_origin": "",
                "preflight_gateway_url": preflight_gateway_url,
                "page_url_mismatched_machines": [],
                "gateway_url_mismatched_machines": [],
            },
        }

    preflight_page_origin = url_origin(preflight_page_url)
    page_url_mismatched_machines: list[str] = []
    gateway_url_mismatched_machines: list[str] = []
    for machine, network in sorted(client_network.items()):
        page_url = str(network.get("page_url") or "").strip()
        gateway_url = str(network.get("gateway_url") or "").strip()
        if not preflight_page_origin or url_origin(page_url) != preflight_page_origin:
            page_url_mismatched_machines.append(machine)
        if gateway_url != preflight_gateway_url:
            gateway_url_mismatched_machines.append(machine)

    blockers: list[str] = []
    if page_url_mismatched_machines:
        blockers.append("multi_trader_smoke_client_page_url_preflight_mismatch")
    if gateway_url_mismatched_machines:
        blockers.append("multi_trader_smoke_client_gateway_url_preflight_mismatch")
    return {
        "blockers": blockers,
        "evidence": {
            "checked": True,
            "preflight_page_origin": preflight_page_origin,
            "preflight_gateway_url": preflight_gateway_url,
            "page_url_mismatched_machines": page_url_mismatched_machines,
            "gateway_url_mismatched_machines": gateway_url_mismatched_machines,
        },
    }


def multi_trader_client_artifact_timing_evidence(
    value: Any,
    preflight: dict[str, Any],
    observed_at: str,
) -> dict[str, Any]:
    if value is None:
        return {"blockers": [], "evidence": {"present": False, "artifacts": []}}
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        return {
            "blockers": ["multi_trader_smoke_client_artifacts_invalid"],
            "evidence": {"present": True, "artifacts": []},
        }
    blockers: list[str] = []
    artifacts: list[dict[str, Any]] = []
    preflight_prepared_at = str(preflight.get("prepared_at") or "")
    prepared_dt = parse_iso_datetime(preflight_prepared_at) if preflight_prepared_at else None
    observed_dt = parse_iso_datetime(observed_at) if observed_at else None
    missing_exported_at_paths: list[str] = []
    invalid_exported_at_paths: list[str] = []
    invalid_machine_id_paths: list[str] = []
    before_preflight_paths: list[str] = []
    after_observed_paths: list[str] = []
    for artifact in value:
        path = str(artifact.get("path") or "")
        exported_at = str(artifact.get("exported_at") or "")
        machine_ids = artifact.get("machine_ids")
        if not isinstance(machine_ids, list) or any(not normalized_client_id(machine_id) for machine_id in machine_ids):
            invalid_machine_id_paths.append(path)
        artifacts.append(
            {
                "path": path,
                "exported_at": exported_at,
                "machine_ids": list(machine_ids) if isinstance(machine_ids, list) else [],
            }
        )
        if not exported_at:
            if preflight.get("present"):
                missing_exported_at_paths.append(path)
            continue
        exported_dt = parse_iso_datetime(exported_at)
        if exported_dt is None:
            invalid_exported_at_paths.append(path)
            continue
        if prepared_dt is not None:
            try:
                if exported_dt < prepared_dt:
                    before_preflight_paths.append(path)
            except TypeError:
                invalid_exported_at_paths.append(path)
        if observed_dt is not None:
            try:
                if observed_dt < exported_dt:
                    after_observed_paths.append(path)
            except TypeError:
                invalid_exported_at_paths.append(path)
    if missing_exported_at_paths:
        blockers.append("multi_trader_smoke_client_artifact_exported_at_missing")
    if invalid_exported_at_paths:
        blockers.append("multi_trader_smoke_client_artifact_exported_at_invalid")
    if invalid_machine_id_paths:
        blockers.append("multi_trader_smoke_client_artifact_machine_ids_invalid")
    if before_preflight_paths:
        blockers.append("multi_trader_smoke_client_artifact_before_preflight")
    if after_observed_paths:
        blockers.append("multi_trader_smoke_client_artifact_after_observed")
    return {
        "blockers": sorted(set(blockers)),
        "evidence": {
            "present": True,
            "prepared_at": preflight_prepared_at,
            "observed_at": observed_at,
            "artifacts": artifacts,
            "missing_exported_at_paths": sorted(set(missing_exported_at_paths)),
            "invalid_exported_at_paths": sorted(set(invalid_exported_at_paths)),
            "invalid_machine_id_paths": sorted(set(invalid_machine_id_paths)),
            "before_preflight_paths": sorted(set(before_preflight_paths)),
            "after_observed_paths": sorted(set(after_observed_paths)),
        },
    }


def multi_trader_service_preflight_timing_evidence(
    preflight: dict[str, Any],
    observed_at: str,
) -> dict[str, Any]:
    prepared_at = str(preflight.get("prepared_at") or "")
    checked_at = str(preflight.get("service_checked_at") or "")
    blockers: list[str] = []
    prepared_dt = parse_iso_datetime(prepared_at) if prepared_at else None
    checked_dt = parse_iso_datetime(checked_at) if checked_at else None
    observed_dt = parse_iso_datetime(observed_at) if observed_at else None

    if preflight.get("present") and preflight.get("service_checks_present"):
        if not checked_at:
            blockers.append("multi_trader_smoke_service_preflight_checked_at_missing")
        elif checked_dt is None:
            blockers.append("multi_trader_smoke_service_preflight_checked_at_invalid")
        else:
            if prepared_dt is not None:
                try:
                    if checked_dt < prepared_dt:
                        blockers.append("multi_trader_smoke_service_preflight_before_preflight")
                except TypeError:
                    blockers.append("multi_trader_smoke_service_preflight_checked_at_invalid")
            if observed_dt is not None:
                try:
                    if observed_dt < checked_dt:
                        blockers.append("multi_trader_smoke_service_preflight_after_observed")
                except TypeError:
                    blockers.append("multi_trader_smoke_service_preflight_checked_at_invalid")

    return {
        "blockers": sorted(set(blockers)),
        "evidence": {
            "present": preflight.get("present") is True and preflight.get("service_checks_present") is True,
            "prepared_at": prepared_at,
            "checked_at": checked_at,
            "observed_at": observed_at,
        },
    }


def multi_trader_performance_artifact_timing_evidence(
    value: Any,
    preflight: dict[str, Any],
    observed_at: str,
) -> dict[str, Any]:
    timing = multi_trader_artifact_timing_evidence(
        value=value,
        preflight=preflight,
        observed_at=observed_at,
        blocker_prefix="multi_trader_smoke_performance_artifact",
    )
    timing["evidence"]["present"] = value is not None
    return timing


def multi_trader_artifact_timing_evidence(
    *,
    value: Any,
    preflight: dict[str, Any],
    observed_at: str,
    blocker_prefix: str,
) -> dict[str, Any]:
    if value is None:
        return {
            "blockers": [],
            "evidence": {
                "present": False,
                "prepared_at": str(preflight.get("prepared_at") or ""),
                "observed_at": observed_at,
                "artifacts": [],
                "missing_exported_at_paths": [],
                "invalid_exported_at_paths": [],
                "before_preflight_paths": [],
                "after_observed_paths": [],
            },
        }
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        return {
            "blockers": [f"{blocker_prefix}s_invalid"],
            "evidence": {
                "present": True,
                "prepared_at": str(preflight.get("prepared_at") or ""),
                "observed_at": observed_at,
                "artifacts": [],
                "missing_exported_at_paths": [],
                "invalid_exported_at_paths": [],
                "before_preflight_paths": [],
                "after_observed_paths": [],
            },
        }
    blockers: list[str] = []
    artifacts: list[dict[str, Any]] = []
    preflight_prepared_at = str(preflight.get("prepared_at") or "")
    prepared_dt = parse_iso_datetime(preflight_prepared_at) if preflight_prepared_at else None
    observed_dt = parse_iso_datetime(observed_at) if observed_at else None
    missing_exported_at_paths: list[str] = []
    invalid_exported_at_paths: list[str] = []
    before_preflight_paths: list[str] = []
    after_observed_paths: list[str] = []
    for artifact in value:
        path = str(artifact.get("path") or "")
        exported_at = str(artifact.get("exported_at") or "")
        artifacts.append({"path": path, "exported_at": exported_at})
        if not exported_at:
            if preflight.get("present"):
                missing_exported_at_paths.append(path)
            continue
        exported_dt = parse_iso_datetime(exported_at)
        if exported_dt is None:
            invalid_exported_at_paths.append(path)
            continue
        if prepared_dt is not None:
            try:
                if exported_dt < prepared_dt:
                    before_preflight_paths.append(path)
            except TypeError:
                invalid_exported_at_paths.append(path)
        if observed_dt is not None:
            try:
                if observed_dt < exported_dt:
                    after_observed_paths.append(path)
            except TypeError:
                invalid_exported_at_paths.append(path)
    if missing_exported_at_paths:
        blockers.append(f"{blocker_prefix}_exported_at_missing")
    if invalid_exported_at_paths:
        blockers.append(f"{blocker_prefix}_exported_at_invalid")
    if before_preflight_paths:
        blockers.append(f"{blocker_prefix}_before_preflight")
    if after_observed_paths:
        blockers.append(f"{blocker_prefix}_after_observed")
    return {
        "blockers": sorted(set(blockers)),
        "evidence": {
            "present": True,
            "prepared_at": preflight_prepared_at,
            "observed_at": observed_at,
            "artifacts": artifacts,
            "missing_exported_at_paths": sorted(set(missing_exported_at_paths)),
            "invalid_exported_at_paths": sorted(set(invalid_exported_at_paths)),
            "before_preflight_paths": sorted(set(before_preflight_paths)),
            "after_observed_paths": sorted(set(after_observed_paths)),
        },
    }


def multi_trader_runtime_health_timing_evidence(
    runtime_health: Any,
    preflight: dict[str, Any],
    observed_at: str,
) -> dict[str, Any]:
    if not isinstance(runtime_health, dict) or not runtime_health.get("path"):
        return {
            "blockers": [],
            "evidence": {
                "present": False,
                "path": "",
                "generated_at": "",
                "prepared_at": str(preflight.get("prepared_at") or ""),
                "observed_at": observed_at,
            },
        }

    path = str(runtime_health.get("path") or "")
    generated_at = str(runtime_health.get("generated_at") or "")
    prepared_at = str(preflight.get("prepared_at") or "")
    blockers: list[str] = []
    generated_dt = parse_iso_datetime(generated_at) if generated_at else None
    prepared_dt = parse_iso_datetime(prepared_at) if prepared_at else None
    observed_dt = parse_iso_datetime(observed_at) if observed_at else None

    if not generated_at:
        blockers.append("multi_trader_smoke_runtime_health_generated_at_missing")
    elif generated_dt is None:
        blockers.append("multi_trader_smoke_runtime_health_generated_at_invalid")
    else:
        if prepared_dt is not None:
            try:
                if generated_dt < prepared_dt:
                    blockers.append("multi_trader_smoke_runtime_health_before_preflight")
            except TypeError:
                blockers.append("multi_trader_smoke_runtime_health_generated_at_invalid")
        if observed_dt is not None:
            try:
                if observed_dt < generated_dt:
                    blockers.append("multi_trader_smoke_runtime_health_after_observed")
            except TypeError:
                blockers.append("multi_trader_smoke_runtime_health_generated_at_invalid")

    return {
        "blockers": sorted(set(blockers)),
        "evidence": {
            "present": True,
            "path": path,
            "generated_at": generated_at,
            "prepared_at": prepared_at,
            "observed_at": observed_at,
        },
    }


def multi_trader_workflow_timing_evidence(
    workflows: dict[str, Any],
    preflight: dict[str, Any],
    observed_at: str,
) -> dict[str, Any]:
    prepared_at = str(preflight.get("prepared_at") or "")
    prepared_dt = parse_iso_datetime(prepared_at) if prepared_at else None
    final_observed_dt = parse_iso_datetime(observed_at) if observed_at else None
    observed_at_by_workflow: dict[str, str] = {}
    missing_workflows: list[str] = []
    invalid_workflows: list[str] = []
    before_preflight_workflows: list[str] = []
    after_observed_workflows: list[str] = []

    for workflow, value in sorted(workflows.items()):
        if not isinstance(value, dict) or value.get("passed") is not True:
            continue
        workflow_observed_at = str(value.get("observed_at") or "")
        observed_at_by_workflow[workflow] = workflow_observed_at
        if not workflow_observed_at:
            missing_workflows.append(workflow)
            continue
        workflow_dt = parse_iso_datetime(workflow_observed_at)
        if workflow_dt is None:
            invalid_workflows.append(workflow)
            continue
        if prepared_dt is not None:
            try:
                if workflow_dt < prepared_dt:
                    before_preflight_workflows.append(workflow)
            except TypeError:
                invalid_workflows.append(workflow)
        if final_observed_dt is not None:
            try:
                if final_observed_dt < workflow_dt:
                    after_observed_workflows.append(workflow)
            except TypeError:
                invalid_workflows.append(workflow)

    blockers: list[str] = []
    if missing_workflows:
        blockers.append("multi_trader_smoke_workflow_observed_at_missing")
    if invalid_workflows:
        blockers.append("multi_trader_smoke_workflow_observed_at_invalid")
    if before_preflight_workflows:
        blockers.append("multi_trader_smoke_workflow_before_preflight")
    if after_observed_workflows:
        blockers.append("multi_trader_smoke_workflow_after_observed")
    return {
        "blockers": sorted(set(blockers)),
        "evidence": {
            "prepared_at": prepared_at,
            "observed_at": observed_at,
            "observed_at_by_workflow": observed_at_by_workflow,
            "missing_workflows": sorted(set(missing_workflows)),
            "invalid_workflows": sorted(set(invalid_workflows)),
            "before_preflight_workflows": sorted(set(before_preflight_workflows)),
            "after_observed_workflows": sorted(set(after_observed_workflows)),
        },
    }


def url_origin(value: str) -> str:
    parsed = urlparse(value)
    try:
        port = parsed.port
    except ValueError:
        return ""
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return ""
    host = parsed.hostname.lower()
    if port is None:
        return f"{parsed.scheme}://{host}"
    return f"{parsed.scheme}://{host}:{port}"


def client_symbol_status_evidence(value: Any, watchlist: list[str]) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    if not isinstance(value, dict):
        return {}, ["multi_trader_smoke_symbol_statuses_missing"]
    allowed_statuses = {"loading", "warm", "live", "closed", "degraded"}
    evidence: dict[str, Any] = {}
    missing_symbols = [symbol for symbol in watchlist if symbol not in value]
    if missing_symbols:
        blockers.append("multi_trader_smoke_symbol_statuses_missing_watchlist_symbols")
    for symbol, status in value.items():
        if not is_canonical_symbol(symbol):
            blockers.append("multi_trader_smoke_symbol_status_symbol_invalid")
            continue
        if symbol not in watchlist:
            blockers.append("multi_trader_smoke_symbol_status_unwatched_symbol")
        if not isinstance(status, dict):
            blockers.append("multi_trader_smoke_symbol_status_invalid")
            continue
        state = str(status.get("status") or "")
        requested = str(status.get("requested_trade_date") or "")
        effective = str(status.get("effective_trade_date") or "")
        source_dates = status.get("source_dates")
        degraded_reasons = status.get("degraded_reasons")
        evidence[symbol] = {
            "status": state,
            "snapshot_loaded": status.get("snapshot_loaded") is True,
            "requested_trade_date": requested,
            "effective_trade_date": effective,
            "source_dates": dict(source_dates) if isinstance(source_dates, dict) else {},
            "degraded_reasons": list(degraded_reasons) if isinstance(degraded_reasons, list) else [],
        }
        if state not in allowed_statuses:
            blockers.append("multi_trader_smoke_symbol_status_invalid")
        if status.get("snapshot_loaded") is not True and state != "loading":
            blockers.append("multi_trader_smoke_symbol_status_snapshot_missing")
        if requested and not is_yyyymmdd(requested):
            blockers.append("multi_trader_smoke_symbol_status_requested_date_invalid")
        if effective and not is_yyyymmdd(effective):
            blockers.append("multi_trader_smoke_symbol_status_effective_date_invalid")
        if state == "closed":
            if not requested or not effective or requested == effective:
                blockers.append("multi_trader_smoke_symbol_status_closed_date_evidence_missing")
            if not isinstance(source_dates, dict) or not source_dates:
                blockers.append("multi_trader_smoke_symbol_status_closed_source_dates_missing")
        if state == "degraded" and not degraded_reasons:
            blockers.append("multi_trader_smoke_symbol_status_degraded_reason_missing")
    return evidence, blockers


def workflow_passed(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, dict):
        return value.get("passed") is True
    return False


def workflow_evidence_blockers(workflows: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    cold_query = workflows.get("cold_query")
    if isinstance(cold_query, dict) and cold_query.get("passed") is True:
        if not is_canonical_symbol(cold_query.get("symbol")):
            blockers.append("multi_trader_smoke_cold_query_symbol_invalid")
        if cold_query.get("loading_observed") is not True:
            blockers.append("multi_trader_smoke_cold_query_loading_missing")
        if cold_query.get("snapshot_visible") is not True:
            blockers.append("multi_trader_smoke_cold_query_snapshot_missing")

    add_to_watchlist = workflows.get("add_to_watchlist")
    if isinstance(add_to_watchlist, dict) and add_to_watchlist.get("passed") is True:
        if not is_canonical_symbol(add_to_watchlist.get("symbol")):
            blockers.append("multi_trader_smoke_add_watchlist_symbol_invalid")
        if add_to_watchlist.get("persisted") is not True:
            blockers.append("multi_trader_smoke_add_watchlist_persistence_missing")

    refresh_recovery = workflows.get("refresh_recovery")
    if isinstance(refresh_recovery, dict) and refresh_recovery.get("passed") is True:
        if refresh_recovery.get("browser_refreshed") is not True:
            blockers.append("multi_trader_smoke_refresh_browser_not_refreshed")
        if refresh_recovery.get("watchlist_restored") is not True:
            blockers.append("multi_trader_smoke_refresh_watchlist_not_restored")
        if refresh_recovery.get("snapshots_visible") is not True:
            blockers.append("multi_trader_smoke_refresh_snapshots_not_visible")

    redis_clear_recovery = workflows.get("redis_clear_recovery")
    if isinstance(redis_clear_recovery, dict) and redis_clear_recovery.get("passed") is True:
        if not is_canonical_symbol(redis_clear_recovery.get("symbol")):
            blockers.append("multi_trader_smoke_redis_clear_symbol_invalid")
        if redis_clear_recovery.get("cache_cleared") is not True:
            blockers.append("multi_trader_smoke_redis_clear_action_missing")
        if redis_clear_recovery.get("snapshot_rebuilt") is not True:
            blockers.append("multi_trader_smoke_redis_clear_snapshot_not_rebuilt")

    process_restart_recovery = workflows.get("process_restart_recovery")
    if isinstance(process_restart_recovery, dict) and process_restart_recovery.get("passed") is True:
        if process_restart_recovery.get("backend_restarted") is not True:
            blockers.append("multi_trader_smoke_process_restart_action_missing")
        if process_restart_recovery.get("first_screen_restored") is not True:
            blockers.append("multi_trader_smoke_process_restart_first_screen_missing")

    closed_market = workflows.get("closed_market_effective_date")
    if isinstance(closed_market, dict) and closed_market.get("passed") is True:
        if closed_market.get("source_dates_visible") is not True:
            blockers.append("multi_trader_smoke_closed_market_source_dates_missing")
        requested_date = str(closed_market.get("requested_trade_date") or "")
        effective_date = str(closed_market.get("effective_trade_date") or "")
        if not requested_date:
            blockers.append("multi_trader_smoke_requested_date_missing")
        elif not is_yyyymmdd(requested_date):
            blockers.append("multi_trader_smoke_requested_date_invalid")
        if not effective_date:
            blockers.append("multi_trader_smoke_effective_date_missing")
        elif not is_yyyymmdd(effective_date):
            blockers.append("multi_trader_smoke_effective_date_invalid")
        if requested_date and effective_date and requested_date == effective_date and closed_market.get("expected_closed_market") is True:
            blockers.append("multi_trader_smoke_closed_market_dates_not_distinct")
    return blockers


def is_canonical_symbol(value: Any) -> bool:
    return isinstance(value, str) and valid_terminal_symbol(value)


def evaluate_runtime_health(runtime_health: dict[str, Any]) -> dict[str, Any]:
    blockers: list[str] = []
    evidence: dict[str, Any] = {
        "runtime_health_present": True,
        "schema_version": runtime_health.get("schema_version"),
    }
    if runtime_health.get("schema_version") != 1:
        blockers.append("runtime_health_schema_invalid")
    generated_at = str(runtime_health.get("generated_at") or "")
    evidence["generated_at"] = generated_at
    if not generated_at:
        blockers.append("runtime_health_generated_at_missing")
    elif not is_iso_datetime(generated_at):
        blockers.append("runtime_health_generated_at_invalid")
    runtime_trade_date = str(runtime_health.get("trade_date") or "")
    evidence["trade_date"] = runtime_trade_date
    if not is_yyyymmdd(runtime_trade_date):
        blockers.append("runtime_health_trade_date_invalid")
    topics = runtime_health.get("topics")
    evidence["topics"] = dict(topics) if isinstance(topics, dict) else {}
    if not topics:
        blockers.append("runtime_health_missing_topics")
    else:
        runtime_topics_to_validate = (
            *REQUIRED_RUNTIME_TOPICS,
            *(topic for topic in OPTIONAL_RUNTIME_TOPICS if topic in topics),
        )
        missing_runtime_topics = [topic for topic in REQUIRED_RUNTIME_TOPICS if topic not in topics]
        invalid_topic_shapes = [
            topic for topic in runtime_topics_to_validate if topic in topics and not isinstance(topics.get(topic), dict)
        ]
        missing_committed_offset_topics = [
            topic
            for topic in runtime_topics_to_validate
            if isinstance(topics.get(topic), dict) and "committed_offset" not in topics[topic]
        ]
        invalid_committed_offset_topics = [
            topic
            for topic in runtime_topics_to_validate
            if isinstance(topics.get(topic), dict)
            and "committed_offset" in topics[topic]
            and not non_negative_integer(topics[topic].get("committed_offset"))
        ]
        missing_lag_topics = [
            topic for topic in runtime_topics_to_validate if isinstance(topics.get(topic), dict) and "lag" not in topics[topic]
        ]
        invalid_lag_topics = [
            topic
            for topic in runtime_topics_to_validate
            if isinstance(topics.get(topic), dict)
            and "lag" in topics[topic]
            and not non_negative_integer(topics[topic].get("lag"))
        ]
        evidence["optional_runtime_topics_present"] = [topic for topic in OPTIONAL_RUNTIME_TOPICS if topic in topics]
        evidence["missing_runtime_topics"] = missing_runtime_topics
        evidence["invalid_runtime_topic_shapes"] = invalid_topic_shapes
        evidence["missing_runtime_topic_committed_offsets"] = missing_committed_offset_topics
        evidence["invalid_runtime_topic_committed_offsets"] = invalid_committed_offset_topics
        evidence["missing_runtime_topic_lag"] = missing_lag_topics
        evidence["invalid_runtime_topic_lag"] = invalid_lag_topics
        if missing_runtime_topics:
            blockers.append("runtime_health_missing_required_topics")
        if invalid_topic_shapes:
            blockers.append("runtime_health_topic_shape_invalid")
        if missing_committed_offset_topics:
            blockers.append("runtime_health_topic_committed_offset_missing")
        if invalid_committed_offset_topics:
            blockers.append("runtime_health_topic_committed_offset_invalid")
        if missing_lag_topics:
            blockers.append("runtime_health_topic_lag_missing")
        if invalid_lag_topics:
            blockers.append("runtime_health_topic_lag_invalid")
        if any(
            isinstance(topics.get(topic), dict)
            and non_negative_integer(topics[topic].get("lag"))
            and int(topics[topic].get("lag")) > 0
            for topic in runtime_topics_to_validate
        ):
            blockers.append("runtime_health_kafka_lag_present")
    supervisor = runtime_health.get("supervisor") or {}
    evidence["supervisor"] = dict(supervisor) if isinstance(supervisor, dict) else {}
    if not isinstance(supervisor, dict):
        blockers.append("runtime_health_missing_supervisor")
    else:
        invalid_supervisor_counters = [
            field for field in ("ticks", "ingested_events", "processed_events") if not non_negative_integer(supervisor.get(field))
        ]
        started_at = supervisor.get("started_at")
        last_tick_at = supervisor.get("last_tick_at")
        stopped_at = supervisor.get("stopped_at")
        stop_reason = supervisor.get("stop_reason")
        evidence["supervisor"]["invalid_counters"] = invalid_supervisor_counters
        evidence["supervisor"]["started_at"] = started_at
        evidence["supervisor"]["last_tick_at"] = last_tick_at
        evidence["supervisor"]["stopped_at"] = stopped_at
        evidence["supervisor"]["stop_reason"] = stop_reason
        if invalid_supervisor_counters:
            blockers.append("runtime_health_supervisor_counter_invalid")
        if non_negative_int_value(supervisor.get("ticks")) <= 0:
            blockers.append("runtime_health_supervisor_no_ticks")
        if non_negative_int_value(supervisor.get("ingested_events")) <= 0:
            blockers.append("runtime_health_supervisor_no_ingested_events")
        if non_negative_int_value(supervisor.get("processed_events")) <= 0:
            blockers.append("runtime_health_supervisor_no_processed_events")
        if not isinstance(started_at, str) or not started_at.strip():
            blockers.append("runtime_health_supervisor_started_at_missing")
        elif not is_iso_datetime(started_at):
            blockers.append("runtime_health_supervisor_started_at_invalid")
        elif is_iso_datetime(generated_at) and iso_datetime_is_before(generated_at, started_at):
            blockers.append("runtime_health_supervisor_started_after_generated_at")
        if not isinstance(last_tick_at, str) or not last_tick_at.strip():
            blockers.append("runtime_health_supervisor_last_tick_at_missing")
        elif not is_iso_datetime(last_tick_at):
            blockers.append("runtime_health_supervisor_last_tick_at_invalid")
        elif is_iso_datetime(generated_at) and iso_datetime_is_before(generated_at, last_tick_at):
            blockers.append("runtime_health_supervisor_last_tick_after_generated_at")
        if is_iso_datetime(started_at) and is_iso_datetime(last_tick_at) and iso_datetime_is_before(last_tick_at, started_at):
            blockers.append("runtime_health_supervisor_last_tick_before_start")
        if stopped_at not in {None, ""} and not (isinstance(stopped_at, str) and is_iso_datetime(stopped_at)):
            blockers.append("runtime_health_supervisor_stopped_at_invalid")
        if runtime_health.get("running") is True and isinstance(stopped_at, str) and stopped_at.strip():
            blockers.append("runtime_health_supervisor_stopped_while_running")
        if stop_reason not in {None, "", "running", "finished", "max_ticks", "stop_event"} and not (
            isinstance(stop_reason, str) and stop_reason in {"signal:SIGINT", "signal:SIGTERM"}
        ):
            blockers.append("runtime_health_supervisor_stop_reason_invalid")
    queues = runtime_health.get("queues")
    raw_callback_backlog = queues.get("raw_callback_backlog") if isinstance(queues, dict) else None
    raw_callback_rejected = queues.get("raw_callback_rejected") if isinstance(queues, dict) else None
    raw_callback_rejection_path = queues.get("raw_callback_rejection_path") if isinstance(queues, dict) else None
    raw_consumer_dead_letter_path = queues.get("raw_consumer_dead_letter_path") if isinstance(queues, dict) else None
    evidence["raw_callback_backlog"] = non_negative_int_value(raw_callback_backlog)
    evidence["raw_callback_rejected"] = non_negative_int_value(raw_callback_rejected)
    evidence["raw_callback_rejection_path"] = raw_callback_rejection_path if isinstance(raw_callback_rejection_path, str) else ""
    evidence["raw_consumer_dead_letter_path"] = raw_consumer_dead_letter_path if isinstance(raw_consumer_dead_letter_path, str) else ""
    if not isinstance(queues, dict):
        blockers.append("runtime_health_queues_missing")
    if isinstance(queues, dict) and not non_negative_integer(raw_callback_backlog):
        blockers.append("runtime_health_callback_backlog_invalid")
    if evidence["raw_callback_backlog"] > 0:
        blockers.append("runtime_health_callback_backlog_present")
    if isinstance(queues, dict) and not non_negative_integer(raw_callback_rejected):
        blockers.append("runtime_health_callback_rejections_invalid")
    if evidence["raw_callback_rejected"] > 0:
        blockers.append("runtime_health_callback_rejections_present")
    if isinstance(queues, dict) and (
        not isinstance(raw_callback_rejection_path, str)
        or not raw_callback_rejection_path.strip()
        or not raw_callback_rejection_path.endswith("callback-rejections.jsonl")
    ):
        blockers.append("runtime_health_callback_rejection_path_invalid")
    if isinstance(queues, dict) and (
        not isinstance(raw_consumer_dead_letter_path, str)
        or not raw_consumer_dead_letter_path.strip()
        or not raw_consumer_dead_letter_path.endswith("raw-consumer-dead-letters.jsonl")
    ):
        blockers.append("runtime_health_raw_consumer_dead_letter_path_invalid")
    workers = runtime_health.get("workers") or {}
    ingest_dead_letters = worker_dead_letter_count(workers.get("ingest")) if isinstance(workers, dict) else 0
    raw_consumer_dead_letters = worker_dead_letter_count(workers.get("raw_consumer")) if isinstance(workers, dict) else 0
    ingest_processed = worker_processed_count(workers, "ingest") if isinstance(workers, dict) else 0
    raw_consumer_processed = worker_processed_count(workers, "raw_consumer") if isinstance(workers, dict) else 0
    invalid_worker_processed = [
        worker
        for worker in ("ingest", "raw_consumer")
        if not isinstance(workers, dict)
        or not isinstance(workers.get(worker), dict)
        or not non_negative_integer((workers.get(worker) or {}).get("processed"))
    ]
    evidence["worker_dead_letters"] = {
        "ingest": ingest_dead_letters,
        "raw_consumer": raw_consumer_dead_letters,
    }
    evidence["worker_processed"] = {
        "ingest": ingest_processed,
        "raw_consumer": raw_consumer_processed,
    }
    evidence["invalid_worker_processed"] = invalid_worker_processed
    if invalid_worker_processed:
        blockers.append("runtime_health_worker_processed_invalid")
    if ingest_dead_letters > 0 or raw_consumer_dead_letters > 0:
        blockers.append("runtime_health_worker_dead_letters_present")
    if evidence["worker_processed"]["ingest"] <= 0:
        blockers.append("runtime_health_ingest_worker_no_processed_events")
    if evidence["worker_processed"]["raw_consumer"] <= 0:
        blockers.append("runtime_health_raw_consumer_no_processed_events")
    producer = runtime_health.get("producer") or {}
    producer_dead_letters = producer.get("dead_letters") if isinstance(producer, dict) else None
    producer_publish_attempts = producer.get("publish_attempts") if isinstance(producer, dict) else None
    producer_spooled_records = producer.get("spooled_records") if isinstance(producer, dict) else None
    producer_quarantined_spool_records = producer.get("quarantined_spool_records") if isinstance(producer, dict) else None
    producer_spool_path = producer.get("spool_path") if isinstance(producer, dict) else None
    producer_spool_quarantine_path = producer.get("spool_quarantine_path") if isinstance(producer, dict) else None
    evidence["producer_dead_letters"] = non_negative_int_value(producer_dead_letters)
    evidence["producer_publish_attempts"] = non_negative_int_value(producer_publish_attempts)
    evidence["producer_spooled_records"] = non_negative_int_value(producer_spooled_records)
    evidence["producer_quarantined_spool_records"] = non_negative_int_value(producer_quarantined_spool_records)
    evidence["producer_spool_path"] = producer_spool_path if isinstance(producer_spool_path, str) else ""
    evidence["producer_spool_quarantine_path"] = (
        producer_spool_quarantine_path if isinstance(producer_spool_quarantine_path, str) else ""
    )
    if isinstance(producer, dict) and not non_negative_integer(producer_dead_letters):
        blockers.append("runtime_health_producer_dead_letters_invalid")
    if isinstance(producer, dict) and not non_negative_integer(producer_publish_attempts):
        blockers.append("runtime_health_producer_publish_attempts_invalid")
    if isinstance(producer, dict) and not non_negative_integer(producer_spooled_records):
        blockers.append("runtime_health_producer_spooled_records_invalid")
    if isinstance(producer, dict) and not non_negative_integer(producer_quarantined_spool_records):
        blockers.append("runtime_health_producer_quarantined_spool_records_invalid")
    if isinstance(producer, dict) and (
        not isinstance(producer_spool_path, str)
        or not producer_spool_path.strip()
        or not producer_spool_path.endswith("publish-failures.jsonl")
    ):
        blockers.append("runtime_health_producer_spool_path_invalid")
    if isinstance(producer, dict) and (
        not isinstance(producer_spool_quarantine_path, str)
        or not producer_spool_quarantine_path.strip()
        or not producer_spool_quarantine_path.endswith("publish-failures.jsonl.quarantine")
    ):
        blockers.append("runtime_health_producer_spool_quarantine_path_invalid")
    if evidence["producer_dead_letters"] > 0:
        blockers.append("runtime_health_producer_dead_letters_present")
    if evidence["producer_spooled_records"] > 0:
        blockers.append("runtime_health_producer_spooled_records_present")
    if evidence["producer_quarantined_spool_records"] > 0:
        blockers.append("runtime_health_producer_quarantined_spool_records_present")
    if evidence["producer_publish_attempts"] <= 0:
        blockers.append("runtime_health_producer_no_publish_attempts")
    expected_publish_attempts = ingest_processed + raw_consumer_processed
    evidence["producer_expected_min_publish_attempts"] = expected_publish_attempts
    if (
        non_negative_integer(producer_publish_attempts)
        and producer_publish_attempts < expected_publish_attempts
    ):
        blockers.append("runtime_health_producer_publish_attempts_below_worker_activity")
    redis = runtime_health.get("redis") if isinstance(runtime_health.get("redis"), dict) else {}
    redis_write_stats = redis.get("write_stats") if isinstance(redis, dict) else None
    evidence["redis_write_stats"] = dict(redis_write_stats) if isinstance(redis_write_stats, dict) else {}
    if not isinstance(redis_write_stats, dict):
        blockers.append("runtime_health_redis_write_stats_missing")
    else:
        redis_writes = redis_write_stats.get("writes")
        redis_failures = redis_write_stats.get("failures")
        redis_last_latency_ms = redis_write_stats.get("last_latency_ms")
        redis_max_latency_ms = redis_write_stats.get("max_latency_ms")
        if not non_negative_integer(redis_writes):
            blockers.append("runtime_health_redis_write_count_invalid")
        if not non_negative_integer(redis_failures):
            blockers.append("runtime_health_redis_write_failures_invalid")
        if not non_negative_number(redis_last_latency_ms):
            blockers.append("runtime_health_redis_last_latency_invalid")
        if not non_negative_number(redis_max_latency_ms):
            blockers.append("runtime_health_redis_max_latency_invalid")
        if (
            non_negative_number(redis_last_latency_ms)
            and non_negative_number(redis_max_latency_ms)
            and float(redis_max_latency_ms) < float(redis_last_latency_ms)
        ):
            blockers.append("runtime_health_redis_latency_order_invalid")
        if non_negative_integer(redis_failures) and redis_failures > 0:
            blockers.append("runtime_health_redis_write_failures_present")
    subscribed_symbols: list[str] = []
    subscription = runtime_health.get("subscription")
    evidence["subscription"] = dict(subscription) if isinstance(subscription, dict) else {}
    if not isinstance(subscription, dict):
        blockers.append("runtime_health_missing_subscription")
    else:
        subscription_symbols = symbol_list_evidence(subscription.get("subscribed_symbols"))
        subscribed_symbols = subscription_symbols["symbols"]
        evidence["subscription"]["subscribed_symbols"] = subscribed_symbols
        evidence["subscription"]["invalid_symbols"] = subscription_symbols["invalid_symbols"]
        evidence["subscription"]["duplicate_symbols"] = subscription_symbols["duplicate_symbols"]
        if subscription_symbols["shape_invalid"]:
            blockers.append("runtime_health_subscription_symbols_invalid")
        if subscription_symbols["invalid_symbols"]:
            blockers.append("runtime_health_subscription_symbol_format_invalid")
        if subscription_symbols["duplicate_symbols"]:
            blockers.append("runtime_health_subscription_symbols_duplicate")
        if subscription.get("running") is not True:
            blockers.append("runtime_health_subscription_not_running")
        if not subscribed_symbols:
            blockers.append("runtime_health_subscription_symbols_empty")
    symbol_runtime = runtime_health.get("symbol_runtime")
    evidence["symbol_runtime"] = dict(symbol_runtime) if isinstance(symbol_runtime, dict) else {}
    symbol_runtime_manager = runtime_health.get("symbol_runtime_manager")
    evidence["symbol_runtime_manager"] = (
        dict(symbol_runtime_manager) if isinstance(symbol_runtime_manager, dict) else {}
    )
    if not isinstance(symbol_runtime_manager, dict):
        blockers.append("runtime_health_symbol_runtime_manager_missing")
    else:
        runtime_count = symbol_runtime_manager.get("runtime_count")
        total_ref_count = symbol_runtime_manager.get("total_ref_count")
        active_hydrations = symbol_runtime_manager.get("active_hydrations")
        max_concurrent_hydrations = symbol_runtime_manager.get("max_concurrent_hydrations")
        capacity_rejections = symbol_runtime_manager.get("capacity_rejections")
        state_sink_failures = symbol_runtime_manager.get("state_sink_failures")
        snapshot_sink_failures = symbol_runtime_manager.get("snapshot_sink_failures")
        hydrating_symbols = symbol_list_evidence(symbol_runtime_manager.get("hydrating_symbols"))
        realtime_attached_symbols = symbol_list_evidence(
            symbol_runtime_manager.get("realtime_attached_symbols")
        )
        state_sink_failure_symbols = symbol_list_evidence(
            symbol_runtime_manager.get("state_sink_failure_symbols")
        )
        snapshot_sink_failure_symbols = symbol_list_evidence(
            symbol_runtime_manager.get("snapshot_sink_failure_symbols")
        )
        state_counts = symbol_runtime_manager.get("state_counts")
        manager_metric_invalid = False
        if not non_negative_integer(runtime_count):
            manager_metric_invalid = True
        if not non_negative_integer(total_ref_count):
            manager_metric_invalid = True
        if not non_negative_integer(active_hydrations):
            manager_metric_invalid = True
        if not non_negative_integer(max_concurrent_hydrations) or int(max_concurrent_hydrations or 0) <= 0:
            manager_metric_invalid = True
        if not non_negative_integer(capacity_rejections):
            manager_metric_invalid = True
        if not non_negative_integer(state_sink_failures):
            manager_metric_invalid = True
        if not non_negative_integer(snapshot_sink_failures):
            manager_metric_invalid = True
        if (
            non_negative_integer(active_hydrations)
            and non_negative_integer(max_concurrent_hydrations)
            and int(active_hydrations or 0) > int(max_concurrent_hydrations or 0)
        ):
            manager_metric_invalid = True
        if not isinstance(state_counts, dict) or any(
            not isinstance(state, str) or not non_negative_integer(count)
            for state, count in (state_counts or {}).items()
        ):
            manager_metric_invalid = True
        if hydrating_symbols["shape_invalid"] or hydrating_symbols["invalid_symbols"]:
            manager_metric_invalid = True
        if hydrating_symbols["duplicate_symbols"]:
            manager_metric_invalid = True
        if realtime_attached_symbols["shape_invalid"] or realtime_attached_symbols["invalid_symbols"]:
            manager_metric_invalid = True
        if realtime_attached_symbols["duplicate_symbols"]:
            manager_metric_invalid = True
        if state_sink_failure_symbols["shape_invalid"] or state_sink_failure_symbols["invalid_symbols"]:
            manager_metric_invalid = True
        if state_sink_failure_symbols["duplicate_symbols"]:
            manager_metric_invalid = True
        if snapshot_sink_failure_symbols["shape_invalid"] or snapshot_sink_failure_symbols["invalid_symbols"]:
            manager_metric_invalid = True
        if snapshot_sink_failure_symbols["duplicate_symbols"]:
            manager_metric_invalid = True
        evidence["symbol_runtime_manager"]["hydrating_symbols"] = hydrating_symbols["symbols"]
        evidence["symbol_runtime_manager"]["realtime_attached_symbols"] = realtime_attached_symbols["symbols"]
        evidence["symbol_runtime_manager"]["state_sink_failures"] = int(state_sink_failures or 0) if non_negative_integer(state_sink_failures) else 0
        evidence["symbol_runtime_manager"]["last_state_sink_error"] = str(
            symbol_runtime_manager.get("last_state_sink_error") or ""
        )
        evidence["symbol_runtime_manager"]["state_sink_failure_symbols"] = state_sink_failure_symbols["symbols"]
        evidence["symbol_runtime_manager"]["snapshot_sink_failures"] = (
            int(snapshot_sink_failures or 0) if non_negative_integer(snapshot_sink_failures) else 0
        )
        evidence["symbol_runtime_manager"]["last_snapshot_sink_error"] = str(
            symbol_runtime_manager.get("last_snapshot_sink_error") or ""
        )
        evidence["symbol_runtime_manager"]["snapshot_sink_failure_symbols"] = snapshot_sink_failure_symbols["symbols"]
        evidence["symbol_runtime_manager"]["invalid_hydrating_symbols"] = hydrating_symbols["invalid_symbols"]
        evidence["symbol_runtime_manager"]["invalid_realtime_attached_symbols"] = realtime_attached_symbols[
            "invalid_symbols"
        ]
        evidence["symbol_runtime_manager"]["invalid_state_sink_failure_symbols"] = state_sink_failure_symbols[
            "invalid_symbols"
        ]
        evidence["symbol_runtime_manager"]["invalid_snapshot_sink_failure_symbols"] = snapshot_sink_failure_symbols[
            "invalid_symbols"
        ]
        evidence["symbol_runtime_manager"]["duplicate_hydrating_symbols"] = hydrating_symbols["duplicate_symbols"]
        evidence["symbol_runtime_manager"]["duplicate_realtime_attached_symbols"] = realtime_attached_symbols[
            "duplicate_symbols"
        ]
        evidence["symbol_runtime_manager"]["duplicate_state_sink_failure_symbols"] = state_sink_failure_symbols[
            "duplicate_symbols"
        ]
        evidence["symbol_runtime_manager"]["duplicate_snapshot_sink_failure_symbols"] = snapshot_sink_failure_symbols[
            "duplicate_symbols"
        ]
        if manager_metric_invalid:
            blockers.append("runtime_health_symbol_runtime_manager_metric_invalid")
        if non_negative_integer(capacity_rejections) and int(capacity_rejections or 0) > 0:
            blockers.append("runtime_health_symbol_runtime_manager_capacity_rejections_present")
        if non_negative_integer(state_sink_failures) and int(state_sink_failures or 0) > 0:
            blockers.append("runtime_health_symbol_runtime_manager_state_sink_failures_present")
        if non_negative_integer(snapshot_sink_failures) and int(snapshot_sink_failures or 0) > 0:
            blockers.append("runtime_health_symbol_runtime_manager_snapshot_sink_failures_present")
    if not isinstance(symbol_runtime, dict):
        blockers.append("runtime_health_symbol_runtime_missing")
    else:
        missing_symbol_runtime = [symbol for symbol in subscribed_symbols if symbol not in symbol_runtime]
        invalid_symbol_runtime_symbols = [
            str(symbol) for symbol in symbol_runtime if not isinstance(symbol, str) or not valid_terminal_symbol(symbol)
        ]
        mismatched_symbol_runtime_symbols = []
        degraded_symbol_runtime = []
        invalid_hydration_metrics = []
        failed_hydration_symbols = []
        capacity_rejected_symbols = []
        symbol_runtime_state_counts = {state: 0 for state in SYMBOL_RUNTIME_STATES}
        symbol_runtime_ref_count_total = 0
        symbol_runtime_ref_counts_valid = True
        symbol_runtime_hydrating_symbols = []
        symbol_runtime_realtime_attached_symbols = []
        for symbol, runtime in symbol_runtime.items():
            if not isinstance(runtime, dict):
                invalid_hydration_metrics.append(str(symbol))
                continue
            symbol_name = str(symbol)
            if runtime.get("symbol") != symbol_name:
                mismatched_symbol_runtime_symbols.append(symbol_name)
            state = runtime.get("state")
            if state not in SYMBOL_RUNTIME_STATES:
                invalid_hydration_metrics.append(symbol_name)
            else:
                symbol_runtime_state_counts[state] += 1
            ref_count = runtime.get("ref_count")
            if not non_negative_integer(ref_count):
                invalid_hydration_metrics.append(symbol_name)
                symbol_runtime_ref_counts_valid = False
            else:
                symbol_runtime_ref_count_total += int(ref_count)
            if state == "HYDRATING":
                symbol_runtime_hydrating_symbols.append(symbol_name)
            if runtime.get("realtime_attached") is True:
                symbol_runtime_realtime_attached_symbols.append(symbol_name)
            elif runtime.get("realtime_attached") is not False:
                invalid_hydration_metrics.append(symbol_name)
            if state == "DEGRADED":
                degraded_symbol_runtime.append(str(symbol))
            if not non_negative_integer(runtime.get("hydrate_count")):
                invalid_hydration_metrics.append(str(symbol))
            if not non_negative_integer(runtime.get("hydration_failures")):
                invalid_hydration_metrics.append(str(symbol))
            elif int(runtime.get("hydration_failures") or 0) > 0:
                failed_hydration_symbols.append(str(symbol))
            if not non_negative_number(runtime.get("last_hydration_latency_ms")):
                invalid_hydration_metrics.append(str(symbol))
            if not non_negative_number(runtime.get("max_hydration_latency_ms")):
                invalid_hydration_metrics.append(str(symbol))
            max_concurrent_hydrations = runtime.get("max_concurrent_hydrations")
            if not non_negative_integer(max_concurrent_hydrations) or int(max_concurrent_hydrations) <= 0:
                invalid_hydration_metrics.append(str(symbol))
            if not non_negative_integer(runtime.get("capacity_rejections")):
                invalid_hydration_metrics.append(str(symbol))
            elif int(runtime.get("capacity_rejections") or 0) > 0:
                capacity_rejected_symbols.append(str(symbol))
        evidence["missing_symbol_runtime"] = missing_symbol_runtime
        evidence["invalid_symbol_runtime_symbols"] = sorted(set(invalid_symbol_runtime_symbols))
        evidence["mismatched_symbol_runtime_symbols"] = sorted(set(mismatched_symbol_runtime_symbols))
        evidence["degraded_symbol_runtime"] = sorted(set(degraded_symbol_runtime))
        evidence["invalid_symbol_runtime_hydration_metrics"] = sorted(set(invalid_hydration_metrics))
        evidence["failed_symbol_runtime_hydrations"] = sorted(set(failed_hydration_symbols))
        evidence["capacity_rejected_symbol_runtime"] = sorted(set(capacity_rejected_symbols))
        evidence["symbol_runtime_summary"] = {
            "runtime_count": len(symbol_runtime),
            "state_counts": symbol_runtime_state_counts,
            "total_ref_count": symbol_runtime_ref_count_total,
            "hydrating_symbols": sorted(symbol_runtime_hydrating_symbols),
            "realtime_attached_symbols": sorted(symbol_runtime_realtime_attached_symbols),
        }
        if missing_symbol_runtime:
            blockers.append("runtime_health_symbol_runtime_missing_subscribed_symbols")
        if invalid_symbol_runtime_symbols:
            blockers.append("runtime_health_symbol_runtime_symbol_format_invalid")
        if mismatched_symbol_runtime_symbols:
            blockers.append("runtime_health_symbol_runtime_symbol_mismatch")
        if degraded_symbol_runtime:
            blockers.append("runtime_health_symbol_runtime_degraded")
        if invalid_hydration_metrics:
            blockers.append("runtime_health_symbol_runtime_hydration_metric_invalid")
        if failed_hydration_symbols:
            blockers.append("runtime_health_symbol_runtime_hydration_failures_present")
        if capacity_rejected_symbols:
            blockers.append("runtime_health_symbol_runtime_capacity_rejections_present")
        if isinstance(symbol_runtime_manager, dict):
            manager_runtime_count = symbol_runtime_manager.get("runtime_count")
            manager_total_ref_count = symbol_runtime_manager.get("total_ref_count")
            manager_active_hydrations = symbol_runtime_manager.get("active_hydrations")
            manager_state_counts = symbol_runtime_manager.get("state_counts")
            manager_hydrating_symbols = symbol_list_evidence(symbol_runtime_manager.get("hydrating_symbols"))["symbols"]
            manager_realtime_attached_symbols = symbol_list_evidence(
                symbol_runtime_manager.get("realtime_attached_symbols")
            )["symbols"]
            manager_state_counts_normalized = {
                state: manager_state_counts.get(state, 0) for state in SYMBOL_RUNTIME_STATES
            } if isinstance(manager_state_counts, dict) else {}
            if non_negative_integer(manager_runtime_count) and int(manager_runtime_count) != len(symbol_runtime):
                blockers.append("runtime_health_symbol_runtime_manager_runtime_count_mismatch")
            if (
                symbol_runtime_ref_counts_valid
                and non_negative_integer(manager_total_ref_count)
                and int(manager_total_ref_count) != symbol_runtime_ref_count_total
            ):
                blockers.append("runtime_health_symbol_runtime_manager_ref_count_mismatch")
            if (
                non_negative_integer(manager_active_hydrations)
                and int(manager_active_hydrations) != len(symbol_runtime_hydrating_symbols)
            ):
                blockers.append("runtime_health_symbol_runtime_manager_active_hydrations_mismatch")
            if manager_state_counts_normalized and manager_state_counts_normalized != symbol_runtime_state_counts:
                blockers.append("runtime_health_symbol_runtime_manager_state_counts_mismatch")
            if sorted(manager_hydrating_symbols) != sorted(symbol_runtime_hydrating_symbols):
                blockers.append("runtime_health_symbol_runtime_manager_hydrating_symbols_mismatch")
            if sorted(manager_realtime_attached_symbols) != sorted(symbol_runtime_realtime_attached_symbols):
                blockers.append("runtime_health_symbol_runtime_manager_realtime_attached_symbols_mismatch")
    redis_snapshot = runtime_health.get("redis_snapshot")
    evidence["redis_snapshot"] = dict(redis_snapshot) if isinstance(redis_snapshot, dict) else {}
    if not isinstance(redis_snapshot, dict):
        blockers.append("runtime_health_missing_redis_snapshot_probe")
    else:
        checked_symbol_evidence = symbol_list_evidence(redis_snapshot.get("checked_symbols"))
        present_symbol_evidence = symbol_list_evidence(redis_snapshot.get("present_symbols"))
        missing_symbol_evidence = symbol_list_evidence(redis_snapshot.get("missing_symbols"))
        checked_symbols = checked_symbol_evidence["symbols"]
        present_symbols = present_symbol_evidence["symbols"]
        missing_snapshot_symbols = missing_symbol_evidence["symbols"]
        redis_snapshot_trade_date = str(redis_snapshot.get("trade_date") or "")
        missing_checked_subscribed_symbols = [
            symbol for symbol in subscribed_symbols if symbol not in checked_symbols
        ]
        missing_present_subscribed_symbols = [
            symbol for symbol in subscribed_symbols if symbol not in present_symbols
        ]
        redis_invalid_symbol_fields = [
            field
            for field, value in (
                ("checked_symbols", checked_symbol_evidence),
                ("present_symbols", present_symbol_evidence),
                ("missing_symbols", missing_symbol_evidence),
            )
            if value["shape_invalid"] or value["invalid_symbols"]
        ]
        redis_duplicate_symbol_fields = [
            field
            for field, value in (
                ("checked_symbols", checked_symbol_evidence),
                ("present_symbols", present_symbol_evidence),
                ("missing_symbols", missing_symbol_evidence),
            )
            if value["duplicate_symbols"]
        ]
        present_missing_overlap = sorted(set(present_symbols) & set(missing_snapshot_symbols))
        unresolved_checked_symbols = sorted(
            set(checked_symbols) - set(present_symbols) - set(missing_snapshot_symbols)
        )
        unchecked_result_symbols = sorted((set(present_symbols) | set(missing_snapshot_symbols)) - set(checked_symbols))
        evidence["redis_snapshot"]["checked_symbols"] = checked_symbols
        evidence["redis_snapshot"]["present_symbols"] = present_symbols
        evidence["redis_snapshot"]["missing_symbols"] = missing_snapshot_symbols
        evidence["redis_snapshot"]["trade_date"] = redis_snapshot_trade_date
        evidence["redis_snapshot"]["missing_checked_subscribed_symbols"] = missing_checked_subscribed_symbols
        evidence["redis_snapshot"]["missing_present_subscribed_symbols"] = missing_present_subscribed_symbols
        evidence["redis_snapshot"]["invalid_symbol_fields"] = redis_invalid_symbol_fields
        evidence["redis_snapshot"]["duplicate_symbol_fields"] = redis_duplicate_symbol_fields
        evidence["redis_snapshot"]["present_missing_overlap"] = present_missing_overlap
        evidence["redis_snapshot"]["unresolved_checked_symbols"] = unresolved_checked_symbols
        evidence["redis_snapshot"]["unchecked_result_symbols"] = unchecked_result_symbols
        redis_key_family_evidence, redis_key_family_blockers = redis_snapshot_key_family_evidence(
            redis_snapshot,
            subscribed_symbols,
            generated_at,
        )
        evidence["redis_snapshot"]["required_key_families"] = redis_key_family_evidence["required_key_families"]
        evidence["redis_snapshot"]["key_family_coverage"] = redis_key_family_evidence["key_family_coverage"]
        blockers.extend(redis_key_family_blockers)
        if redis_invalid_symbol_fields:
            blockers.append("runtime_health_redis_snapshot_symbol_list_invalid")
        if redis_duplicate_symbol_fields:
            blockers.append("runtime_health_redis_snapshot_symbol_list_duplicate")
        if present_missing_overlap:
            blockers.append("runtime_health_redis_snapshot_symbol_status_conflict")
        if unresolved_checked_symbols:
            blockers.append("runtime_health_redis_snapshot_checked_symbols_unresolved")
        if unchecked_result_symbols:
            blockers.append("runtime_health_redis_snapshot_result_symbols_unchecked")
        if not checked_symbols:
            blockers.append("runtime_health_redis_snapshot_probe_empty")
        if missing_snapshot_symbols:
            blockers.append("runtime_health_redis_snapshot_missing_symbols")
        if is_yyyymmdd(runtime_trade_date) and redis_snapshot_trade_date != runtime_trade_date:
            blockers.append("runtime_health_redis_snapshot_trade_date_mismatch")
        if missing_checked_subscribed_symbols:
            blockers.append("runtime_health_redis_snapshot_probe_missing_subscribed_symbols")
        if missing_present_subscribed_symbols:
            blockers.append("runtime_health_redis_snapshot_subscribed_symbols_not_present")
    gateway_websocket = runtime_health.get("gateway_websocket")
    evidence["gateway_websocket"] = dict(gateway_websocket) if isinstance(gateway_websocket, dict) else {}
    if not isinstance(gateway_websocket, dict):
        blockers.append("runtime_health_missing_gateway_websocket")
    else:
        host = gateway_websocket.get("host")
        port = gateway_websocket.get("port")
        evidence["gateway_websocket"]["host"] = host
        evidence["gateway_websocket"]["port"] = port
        if not isinstance(host, str) or not host.strip():
            blockers.append("runtime_health_gateway_host_invalid")
        elif is_loopback_host(host):
            blockers.append("runtime_health_gateway_host_loopback")
        if not non_negative_integer(port) or int(port) <= 0:
            blockers.append("runtime_health_gateway_port_invalid")
        if gateway_websocket.get("path") != GATEWAY_WEBSOCKET_PATH:
            blockers.append("runtime_health_gateway_websocket_path_mismatch")
        if gateway_websocket.get("request_schema_version") != SCHEMA_VERSION:
            blockers.append("runtime_health_gateway_request_schema_version_mismatch")
        if gateway_websocket.get("accepted_protocol") != TERMINAL_MESSAGE_PROTOCOL:
            blockers.append("runtime_health_gateway_protocol_mismatch")
        if gateway_websocket.get("running") is not True:
            blockers.append("runtime_health_gateway_websocket_not_running")
    gateway_activity = runtime_health.get("gateway_activity")
    evidence["gateway_activity"] = dict(gateway_activity) if isinstance(gateway_activity, dict) else {}
    if not isinstance(gateway_activity, dict):
        blockers.append("runtime_health_missing_gateway_activity")
    else:
        invalid_gateway_counters = [
            field
            for field in (
                "processed_records_consumed",
                "shadow_processed_records_drained",
                "direct_runtime_messages_emitted",
                "terminal_messages_emitted",
                "terminal_messages_delivered",
            )
            if not non_negative_integer(gateway_activity.get(field))
        ]
        processed_records_consumed = non_negative_int_value(gateway_activity.get("processed_records_consumed"))
        shadow_processed_records_drained = non_negative_int_value(gateway_activity.get("shadow_processed_records_drained"))
        direct_runtime_messages_emitted = non_negative_int_value(gateway_activity.get("direct_runtime_messages_emitted"))
        terminal_messages_emitted = non_negative_int_value(gateway_activity.get("terminal_messages_emitted"))
        terminal_messages_delivered = non_negative_int_value(gateway_activity.get("terminal_messages_delivered"))
        expected_max_terminal_messages_emitted = processed_records_consumed + direct_runtime_messages_emitted
        delivered_symbol_evidence = symbol_list_evidence(gateway_activity.get("delivered_terminal_symbols"))
        delivered_terminal_symbols = delivered_symbol_evidence["symbols"]
        missing_delivered_subscribed_symbols = [
            symbol for symbol in subscribed_symbols if symbol not in delivered_terminal_symbols
        ]
        delivered_unsubscribed_symbols = sorted(set(delivered_terminal_symbols) - set(subscribed_symbols))
        last_terminal_message_delivered_at = gateway_activity.get("last_terminal_message_delivered_at")
        client_queue = gateway_activity.get("client_queue")
        client_queue_evidence, client_queue_blockers = gateway_client_queue_evidence(client_queue)
        evidence["gateway_activity"]["processed_records_consumed"] = processed_records_consumed
        evidence["gateway_activity"]["shadow_processed_records_drained"] = shadow_processed_records_drained
        evidence["gateway_activity"]["direct_runtime_messages_emitted"] = direct_runtime_messages_emitted
        evidence["gateway_activity"]["terminal_messages_emitted"] = terminal_messages_emitted
        evidence["gateway_activity"]["terminal_messages_delivered"] = terminal_messages_delivered
        evidence["gateway_activity"]["expected_max_terminal_messages_emitted"] = expected_max_terminal_messages_emitted
        evidence["gateway_activity"]["invalid_counters"] = invalid_gateway_counters
        evidence["gateway_activity"]["delivered_terminal_symbols"] = delivered_terminal_symbols
        evidence["gateway_activity"]["invalid_delivered_terminal_symbols"] = delivered_symbol_evidence["invalid_symbols"]
        evidence["gateway_activity"]["duplicate_delivered_terminal_symbols"] = delivered_symbol_evidence["duplicate_symbols"]
        evidence["gateway_activity"]["missing_delivered_subscribed_symbols"] = missing_delivered_subscribed_symbols
        evidence["gateway_activity"]["delivered_unsubscribed_symbols"] = delivered_unsubscribed_symbols
        evidence["gateway_activity"]["last_terminal_message_delivered_at"] = last_terminal_message_delivered_at
        evidence["gateway_activity"]["client_queue"] = client_queue_evidence
        blockers.extend(client_queue_blockers)
        if invalid_gateway_counters:
            blockers.append("runtime_health_gateway_activity_counter_invalid")
        if processed_records_consumed > raw_consumer_processed:
            blockers.append("runtime_health_gateway_processed_consumed_exceeds_raw_consumer_processed")
        if shadow_processed_records_drained > raw_consumer_processed:
            blockers.append("runtime_health_gateway_shadow_drained_exceeds_raw_consumer_processed")
        if direct_runtime_messages_emitted > raw_consumer_processed:
            blockers.append("runtime_health_gateway_direct_emitted_exceeds_raw_consumer_processed")
        if processed_records_consumed <= 0 and direct_runtime_messages_emitted <= 0:
            blockers.append("runtime_health_gateway_no_terminal_message_source")
        if terminal_messages_emitted <= 0:
            blockers.append("runtime_health_gateway_no_terminal_messages_emitted")
        if terminal_messages_emitted > expected_max_terminal_messages_emitted:
            blockers.append("runtime_health_gateway_emitted_count_exceeds_sources")
        if terminal_messages_delivered <= 0:
            blockers.append("runtime_health_gateway_no_terminal_messages_delivered")
        if terminal_messages_delivered > terminal_messages_emitted:
            blockers.append("runtime_health_gateway_delivery_count_exceeds_emitted")
        if len(delivered_terminal_symbols) > terminal_messages_delivered:
            blockers.append("runtime_health_gateway_delivered_symbol_count_exceeds_deliveries")
        if delivered_symbol_evidence["shape_invalid"]:
            blockers.append("runtime_health_gateway_delivered_symbols_invalid")
        if delivered_symbol_evidence["invalid_symbols"]:
            blockers.append("runtime_health_gateway_delivered_symbol_format_invalid")
        if delivered_symbol_evidence["duplicate_symbols"]:
            blockers.append("runtime_health_gateway_delivered_symbols_duplicate")
        if missing_delivered_subscribed_symbols:
            blockers.append("runtime_health_gateway_delivery_missing_subscribed_symbols")
        if delivered_unsubscribed_symbols:
            blockers.append("runtime_health_gateway_delivered_unsubscribed_symbols")
        if terminal_messages_delivered > 0 and (
            not isinstance(last_terminal_message_delivered_at, str)
            or not last_terminal_message_delivered_at.strip()
        ):
            blockers.append("runtime_health_gateway_terminal_delivery_timestamp_missing")
        elif (
            terminal_messages_delivered > 0
            and isinstance(last_terminal_message_delivered_at, str)
            and not is_iso_datetime(last_terminal_message_delivered_at)
        ):
            blockers.append("runtime_health_gateway_terminal_delivery_timestamp_invalid")
        elif (
            terminal_messages_delivered > 0
            and is_iso_datetime(generated_at)
            and isinstance(last_terminal_message_delivered_at, str)
            and is_iso_datetime(last_terminal_message_delivered_at)
            and iso_datetime_is_before(generated_at, last_terminal_message_delivered_at)
        ):
            blockers.append("runtime_health_gateway_terminal_delivery_after_generated_at")
    performance_samples = runtime_health.get("performance_samples")
    performance_evidence, performance_blockers = runtime_health_performance_samples_evidence(performance_samples)
    evidence["performance_samples"] = performance_evidence
    blockers.extend(performance_blockers)
    kafka_activity_coverage = kafka_activity_offset_coverage(
        topics=topics,
        ingest_processed=ingest_processed,
        raw_consumer_processed=raw_consumer_processed,
        gateway_processed_records_consumed=shadow_processed_records_drained if isinstance(gateway_activity, dict) else 0,
    )
    evidence["kafka_activity_coverage"] = kafka_activity_coverage
    if kafka_activity_coverage["raw_topic_committed_offset_below_ingest_processed"]:
        blockers.append("runtime_health_raw_topic_committed_offset_below_ingest_processed")
    if kafka_activity_coverage["processed_topic_committed_offset_below_raw_consumer_processed"]:
        blockers.append("runtime_health_processed_topic_committed_offset_below_raw_consumer_processed")
    if kafka_activity_coverage["processed_topic_committed_offset_below_gateway_consumed"]:
        blockers.append("runtime_health_processed_topic_committed_offset_below_gateway_consumed")
    health = runtime_health.get("health")
    evidence["component_health_present"] = bool(health)
    if not health:
        blockers.append("runtime_health_missing_component_health")
    else:
        redis_status = {}
        for component in ("octopus", "gateway"):
            status = (health.get(component) or {}).get("redis")
            redis_status[component] = status
            if status != "connected":
                blockers.append(f"runtime_health_{component}_redis_not_connected")
        collector_health = health.get("collector") or {}
        degraded_symbols = degraded_freshness_symbols(collector_health)
        freshness_coverage = freshness_coverage_for_subscribed_symbols(
            collector_health=collector_health,
            subscribed_symbols=(evidence.get("subscription") or {}).get("subscribed_symbols") or [],
            generated_at=generated_at,
        )
        evidence["redis_status"] = redis_status
        evidence["degraded_freshness_symbols"] = degraded_symbols
        evidence["freshness_missing_symbols"] = freshness_coverage["missing_symbols"]
        evidence["freshness_unsubscribed_symbols"] = freshness_coverage["unsubscribed_symbols"]
        evidence["freshness_missing_latest_event_symbols"] = freshness_coverage["missing_latest_event_symbols"]
        evidence["freshness_invalid_latest_event_symbols"] = freshness_coverage["invalid_latest_event_symbols"]
        evidence["freshness_future_latest_event_symbols"] = freshness_coverage["future_latest_event_symbols"]
        if degraded_symbols:
            blockers.append("runtime_health_symbol_freshness_degraded")
        if freshness_coverage["missing_symbols"]:
            blockers.append("runtime_health_symbol_freshness_missing")
        if freshness_coverage["unsubscribed_symbols"]:
            blockers.append("runtime_health_symbol_freshness_not_subscribed")
        if freshness_coverage["missing_latest_event_symbols"]:
            blockers.append("runtime_health_symbol_freshness_latest_event_missing")
        if freshness_coverage["invalid_latest_event_symbols"]:
            blockers.append("runtime_health_symbol_freshness_latest_event_invalid")
        if freshness_coverage["future_latest_event_symbols"]:
            blockers.append("runtime_health_symbol_freshness_latest_event_after_generated_at")
    return {
        "schema_version": 1,
        "passed": not blockers,
        "blockers": blockers,
        "evidence": evidence,
    }


def evaluate_evidence_bundle(paths: EvidenceBundlePaths) -> dict[str, Any]:
    blockers: list[str] = []
    evidence: dict[str, Any] = {}

    reports = load_shadow_run_report_directory(paths.shadow_reports_directory)
    default_policy_readiness = evaluate_cutover_readiness(reports)
    default_policy_accepted_report_ids = {
        str(report_id) for report_id in default_policy_readiness.get("accepted_report_ids", [])
    }
    invalid_shadow_reports = validate_shadow_run_reports(reports)
    passing_report_ids = {
        str(report.get("session_id"))
        for report in reports
        if report.get("passed") is True and report.get("session_id")
    }
    evidence["shadow_report_count"] = len(reports)
    evidence["accepted_shadow_reports"] = sorted(passing_report_ids)
    evidence["default_policy_accepted_shadow_reports"] = sorted(default_policy_accepted_report_ids)
    evidence["default_policy_shadow_readiness_passed"] = default_policy_readiness.get("passed") is True
    evidence["invalid_shadow_reports"] = invalid_shadow_reports
    shadow_source_file_audits = shadow_run_source_file_audits(
        reports=reports,
        base_directory=paths.shadow_reports_directory,
    )
    evidence["shadow_source_file_audits"] = shadow_source_file_audits
    shadow_report_trading_dates = sorted(
        {
            str(report.get("trading_date"))
            for report in reports
            if report.get("session_id") in passing_report_ids and is_yyyymmdd(str(report.get("trading_date") or ""))
        }
    )
    evidence["shadow_report_trading_dates"] = shadow_report_trading_dates
    if not reports:
        blockers.append("missing_shadow_run_reports")
    elif not any(report.get("passed") is True for report in reports):
        blockers.append("no_passing_shadow_run_report")
    elif default_policy_readiness.get("passed") is not True:
        blockers.append("shadow_run_reports_fail_default_cutover_policy")
    if invalid_shadow_reports:
        blockers.append("shadow_run_report_schema_invalid")
    if any(audit.get("errors") for audit in shadow_source_file_audits):
        blockers.append("shadow_run_evidence_source_files_invalid")
    if len(shadow_report_trading_dates) > 1:
        blockers.append("shadow_run_reports_multiple_trading_dates")

    runtime_config = load_required_json(paths.runtime_config_path, "missing_runtime_config", blockers)
    evidence["runtime_config_present"] = runtime_config is not None
    runtime_config_result: dict[str, Any] | None = None
    if runtime_config is not None:
        runtime_config_result = evaluate_runtime_config_artifact(runtime_config)
        evidence["runtime_config"] = runtime_config_result["evidence"]
        if runtime_config_result.get("passed") is not True:
            blockers.append("runtime_config_artifact_not_passed")
        blockers.extend(runtime_config_result.get("blockers") or [])

    manifest_paths = sorted(Path(paths.manifest_directory).glob("*.manifest.json"))
    manifests = [load_json(path) for path in manifest_paths]
    manifest_data_types = sorted({str(manifest.get("data_type")) for manifest in manifests if manifest.get("data_type")})
    missing_manifest_types = [
        data_type for data_type in REQUIRED_HISTORICAL_MANIFEST_TYPES if data_type not in manifest_data_types
    ]
    invalid_manifests = validate_historical_manifests(manifests)
    evidence["manifest_count"] = len(manifests)
    evidence["manifest_paths"] = [str(path) for path in manifest_paths]
    evidence["manifest_data_types"] = manifest_data_types
    evidence["missing_manifest_data_types"] = missing_manifest_types
    evidence["invalid_historical_manifests"] = invalid_manifests
    if not manifests:
        blockers.append("missing_historical_manifests")
    else:
        if missing_manifest_types:
            blockers.append("historical_manifest_coverage_incomplete")
        if invalid_manifests:
            blockers.append("historical_manifest_schema_invalid")
        if any((manifest.get("quality_checks") or {}).get("passed") is not True for manifest in manifests):
            blockers.append("historical_manifest_quality_failed")

    runtime_health = load_required_json(paths.runtime_health_path, "missing_runtime_health", blockers)
    evidence["runtime_health_present"] = runtime_health is not None
    if runtime_health is not None:
        runtime_result = evaluate_runtime_health(runtime_health)
        evidence["runtime_health"] = runtime_result["evidence"]
        runtime_trade_date = str((runtime_result["evidence"] or {}).get("trade_date") or "")
        evidence["runtime_trade_date"] = runtime_trade_date
        config_trade_date = str(((runtime_config_result or {}).get("evidence") or {}).get("trade_date") or "")
        evidence["runtime_config_trade_date"] = config_trade_date
        if config_trade_date and runtime_trade_date and config_trade_date != runtime_trade_date:
            blockers.append("runtime_config_trade_date_mismatch")
        gateway_config = ((runtime_config_result or {}).get("evidence") or {}).get("gateway") or {}
        gateway_health = (runtime_health or {}).get("gateway_websocket") or {}
        evidence["runtime_config_gateway_path"] = gateway_config.get("path") if isinstance(gateway_config, dict) else None
        evidence["runtime_config_gateway_port"] = gateway_config.get("port") if isinstance(gateway_config, dict) else None
        if isinstance(gateway_config, dict) and isinstance(gateway_health, dict):
            if gateway_config.get("path") != gateway_health.get("path"):
                blockers.append("runtime_config_gateway_path_mismatch")
            if gateway_config.get("port") != gateway_health.get("port"):
                blockers.append("runtime_config_gateway_port_mismatch")
        if shadow_report_trading_dates and runtime_trade_date != shadow_report_trading_dates[0]:
            blockers.append("runtime_trade_date_mismatch")
        manifest_date_mismatches = manifest_date_range_mismatches(
            manifests=manifests,
            trading_date=shadow_report_trading_dates[0] if shadow_report_trading_dates else runtime_trade_date,
        )
        subscribed_symbols = ((runtime_result["evidence"] or {}).get("subscription") or {}).get("subscribed_symbols") or []
        manifest_symbol_mismatches = manifest_symbol_coverage_mismatches(
            manifests=manifests,
            subscribed_symbols=[str(symbol) for symbol in subscribed_symbols],
        )
        shadow_comparison_symbol_coverage = shadow_comparison_symbol_coverage_for_runtime(
            reports=reports,
            accepted_report_ids=default_policy_accepted_report_ids,
            subscribed_symbols=[str(symbol) for symbol in subscribed_symbols],
        )
        delivered_shadow_coverage = delivered_symbol_shadow_coverage(
            runtime_evidence=runtime_result["evidence"],
            shadow_comparison_symbols=shadow_comparison_symbol_coverage["comparison_symbols"],
        )
        runtime_shadow_window = runtime_shadow_window_coverage(
            reports=reports,
            accepted_report_ids=default_policy_accepted_report_ids,
            runtime_evidence=runtime_result["evidence"],
        )
        evidence["manifest_date_range_mismatches"] = manifest_date_mismatches
        evidence["manifest_symbol_coverage_mismatches"] = manifest_symbol_mismatches
        evidence["shadow_comparison_symbols"] = shadow_comparison_symbol_coverage["comparison_symbols"]
        evidence["shadow_comparison_missing_subscribed_symbols"] = shadow_comparison_symbol_coverage["missing_symbols"]
        evidence["delivered_symbols_without_shadow_comparison"] = delivered_shadow_coverage["missing_symbols"]
        evidence["runtime_shadow_window"] = runtime_shadow_window
        if manifest_date_mismatches:
            blockers.append("historical_manifest_date_range_mismatch")
        if manifest_symbol_mismatches:
            blockers.append("historical_manifest_symbol_coverage_mismatch")
        if shadow_comparison_symbol_coverage["missing_symbols"]:
            blockers.append("shadow_run_comparison_missing_subscribed_symbols")
        if delivered_shadow_coverage["missing_symbols"]:
            blockers.append("runtime_gateway_delivery_without_shadow_comparison")
        if runtime_shadow_window["started_after_report_start"]:
            blockers.append("runtime_health_started_after_shadow_report_start")
        if runtime_shadow_window["last_tick_before_report_finish"]:
            blockers.append("runtime_health_last_tick_before_shadow_report_finish")
        if runtime_shadow_window["generated_before_report_finish"]:
            blockers.append("runtime_health_generated_before_shadow_report_finish")
        blockers.extend(runtime_result["blockers"])

    readiness = load_required_json(paths.readiness_path, "missing_cutover_readiness", blockers)
    evidence["cutover_readiness_passed"] = bool(readiness and readiness.get("passed") is True)
    readiness_accepted_report_ids = [
        str(report_id)
        for report_id in ((readiness or {}).get("accepted_report_ids") or [])
    ]
    missing_readiness_report_ids = [
        report_id for report_id in readiness_accepted_report_ids if report_id not in passing_report_ids
    ]
    default_policy_rejected_readiness_report_ids = [
        report_id for report_id in readiness_accepted_report_ids if report_id not in default_policy_accepted_report_ids
    ]
    evidence["readiness_accepted_report_ids"] = readiness_accepted_report_ids
    evidence["missing_readiness_report_ids"] = missing_readiness_report_ids
    evidence["default_policy_rejected_readiness_report_ids"] = default_policy_rejected_readiness_report_ids
    if readiness is not None:
        readiness_errors = validate_cutover_readiness_artifact(readiness)
        evidence["invalid_cutover_readiness_errors"] = readiness_errors
        if readiness_errors:
            blockers.append("cutover_readiness_schema_invalid")
    if readiness is not None and readiness.get("passed") is not True:
        blockers.append("cutover_readiness_failed")
    if readiness is not None and readiness.get("passed") is True:
        if not readiness_accepted_report_ids:
            blockers.append("cutover_readiness_missing_accepted_reports")
        if missing_readiness_report_ids:
            blockers.append("cutover_readiness_accepted_reports_not_in_bundle")
        if default_policy_rejected_readiness_report_ids:
            blockers.append("cutover_readiness_accepted_reports_fail_default_policy")

    frontend = load_required_json(paths.frontend_deployment_path, "missing_frontend_deployment", blockers)
    evidence["frontend_schema_version"] = (frontend or {}).get("schema_version")
    evidence["frontend_artifact_passed"] = (frontend or {}).get("passed") is True
    evidence["frontend_default_v2_deployed"] = bool(frontend and frontend.get("frontend_default_v2_deployed") is True)
    if frontend is not None and frontend.get("schema_version") != 1:
        blockers.append("frontend_deployment_schema_invalid")
    if frontend is not None and frontend.get("passed") is not True:
        blockers.append("frontend_deployment_artifact_not_passed")
    if frontend is not None and frontend.get("frontend_default_v2_deployed") is not True:
        blockers.append("frontend_default_v2_not_deployed")
    frontend_readiness = ((frontend or {}).get("deployed_env") or {}).get("VITE_MARKET_CUTOVER_READINESS")
    frontend_mode = ((frontend or {}).get("deployed_env") or {}).get("VITE_MARKET_DATA_MODE")
    frontend_protocol = ((frontend or {}).get("deployed_env") or {}).get("VITE_MARKET_PROTOCOL")
    frontend_live_url = ((frontend or {}).get("deployed_env") or {}).get("VITE_MARKET_WS_URL")
    frontend_expected_live_url = (frontend or {}).get("expected_live_url")
    frontend_verified_at = (frontend or {}).get("verified_at")
    evidence["frontend_deployed_readiness_present"] = isinstance(frontend_readiness, dict)
    evidence["frontend_mode"] = frontend_mode
    evidence["frontend_protocol"] = frontend_protocol
    evidence["frontend_live_url"] = frontend_live_url
    evidence["frontend_expected_live_url"] = frontend_expected_live_url
    evidence["frontend_verified_at"] = frontend_verified_at
    if frontend is not None and frontend.get("frontend_default_v2_deployed") is True:
        if not isinstance(frontend_verified_at, str) or not frontend_verified_at.strip():
            blockers.append("frontend_deployment_verified_at_missing")
        elif not is_iso_datetime(frontend_verified_at):
            blockers.append("frontend_deployment_verified_at_invalid")
        else:
            early_frontend_report_ids = frontend_verified_before_shadow_report_finish(
                reports=reports,
                accepted_report_ids=default_policy_accepted_report_ids,
                frontend_verified_at=frontend_verified_at,
            )
            evidence["frontend_verified_before_shadow_report_finish"] = early_frontend_report_ids
            if early_frontend_report_ids:
                blockers.append("frontend_deployment_verified_before_shadow_report_finish")
        if frontend_mode not in {"auto", "live"}:
            blockers.append("frontend_deployment_data_mode_not_live_or_auto")
        if frontend_protocol != FRONTEND_PROTOCOL:
            blockers.append("frontend_deployment_protocol_mismatch")
        frontend_live_url_errors = validate_frontend_live_url(frontend_live_url)
        evidence["frontend_live_url_errors"] = frontend_live_url_errors
        blockers.extend(frontend_live_url_errors)
        if not isinstance(frontend_live_url, str) or not frontend_live_url.strip():
            blockers.append("frontend_deployment_live_url_missing")
        elif frontend_expected_live_url != frontend_live_url:
            blockers.append("frontend_deployment_live_url_mismatch")
        blockers.extend(
            validate_frontend_live_url_targets_gateway(
                frontend_live_url=frontend_live_url,
                runtime_health=runtime_health,
                evidence=evidence,
            )
        )
        if not isinstance(frontend_readiness, dict):
            blockers.append("frontend_deployment_readiness_missing")
        elif readiness is not None and frontend_readiness != readiness:
            blockers.append("frontend_deployment_readiness_mismatch")

    decommission = load_required_json(paths.legacy_decommission_path, "missing_legacy_decommission", blockers)
    evidence["legacy_decommission_passed"] = bool(decommission and decommission.get("passed") is True)
    if decommission is not None and decommission.get("passed") is not True:
        blockers.append("legacy_decommission_failed")
    if decommission is not None and decommission.get("passed") is True:
        blockers.extend(validate_legacy_decommission_artifact(decommission=decommission, evidence=evidence))
        decommission_observed_at = ((decommission.get("observation") or {}).get("observed_at")) if isinstance(decommission.get("observation"), dict) else ""
        if (
            isinstance(frontend_verified_at, str)
            and isinstance(decommission_observed_at, str)
            and is_iso_datetime(frontend_verified_at)
            and is_iso_datetime(decommission_observed_at)
            and iso_datetime_is_before(decommission_observed_at, frontend_verified_at)
        ):
            blockers.append("legacy_decommission_observed_before_frontend_verified")

    retirement = load_required_json(paths.legacy_retirement_path, "missing_legacy_retirement", blockers)
    evidence["legacy_retired"] = bool(retirement and retirement.get("legacy_retired") is True)
    if retirement is not None and retirement.get("legacy_retired") is not True:
        blockers.append("legacy_retirement_failed")
    if retirement is not None and retirement.get("legacy_retired") is True:
        blockers.extend(
            validate_legacy_retirement_artifact(
                retirement=retirement,
                readiness=readiness,
                frontend=frontend,
                decommission=decommission,
                evidence=evidence,
            )
        )

    evidence["multi_trader_smoke_present"] = paths.multi_trader_smoke_path is not None
    smoke = None
    if paths.multi_trader_smoke_path is not None:
        smoke = load_required_json(
            paths.multi_trader_smoke_path,
            "missing_multi_trader_smoke",
            blockers,
        )
    evidence["multi_trader_smoke_schema_version"] = (smoke or {}).get("schema_version")
    evidence["multi_trader_smoke_passed"] = bool(smoke and smoke.get("passed") is True)
    evidence["multi_trader_smoke_blockers"] = list((smoke or {}).get("blockers") or [])
    evidence["multi_trader_smoke_client_count"] = (smoke or {}).get("client_count")
    evidence["multi_trader_smoke_watchlist_overlap"] = list((smoke or {}).get("watchlist_overlap") or [])
    if smoke is not None:
        if smoke.get("schema_version") != 1:
            blockers.append("multi_trader_smoke_schema_invalid")
        if smoke.get("passed") is not True:
            blockers.append("multi_trader_smoke_blocked")
    preflight = None
    evidence["multi_trader_smoke_preflight_present"] = paths.multi_trader_smoke_preflight_path is not None
    if paths.multi_trader_smoke_path is not None:
        if paths.multi_trader_smoke_preflight_path is None:
            blockers.append("missing_multi_trader_smoke_preflight")
        else:
            preflight = load_required_json(
                paths.multi_trader_smoke_preflight_path,
                "missing_multi_trader_smoke_preflight",
                blockers,
            )
    evidence["multi_trader_smoke_preflight_schema_version"] = (preflight or {}).get("schema_version")
    evidence["multi_trader_smoke_preflight_passed"] = bool(preflight and preflight.get("passed") is True)
    evidence["multi_trader_smoke_preflight_blockers"] = list((preflight or {}).get("blockers") or [])
    evidence["multi_trader_smoke_preflight_page_url"] = (preflight or {}).get("page_url")
    evidence["multi_trader_smoke_preflight_gateway_url"] = (preflight or {}).get("gateway_url")
    if preflight is not None:
        blockers.extend(
            validate_multi_trader_smoke_preflight(
                preflight=preflight,
                frontend_live_url=frontend_live_url,
            )
        )
    smoke_manifest = None
    evidence["multi_trader_smoke_manifest_present"] = paths.multi_trader_smoke_manifest_path is not None
    if paths.multi_trader_smoke_path is not None:
        if paths.multi_trader_smoke_manifest_path is None:
            blockers.append("missing_multi_trader_smoke_manifest")
        else:
            smoke_manifest = load_required_json(
                paths.multi_trader_smoke_manifest_path,
                "missing_multi_trader_smoke_manifest",
                blockers,
            )
    evidence["multi_trader_smoke_manifest_schema_version"] = (smoke_manifest or {}).get("schema_version")
    evidence["multi_trader_smoke_manifest_file_count"] = (smoke_manifest or {}).get("file_count")
    if smoke_manifest is not None:
        blockers.extend(
            validate_multi_trader_smoke_manifest(
                manifest=smoke_manifest,
                manifest_path=paths.multi_trader_smoke_manifest_path,
                smoke_path=paths.multi_trader_smoke_path,
                preflight_path=paths.multi_trader_smoke_preflight_path,
                evidence=evidence,
            )
        )
    package_metadata = None
    evidence["multi_trader_smoke_package_present"] = paths.multi_trader_smoke_package_path is not None
    evidence["multi_trader_smoke_package_metadata_present"] = paths.multi_trader_smoke_package_metadata_path is not None
    if paths.multi_trader_smoke_package_path is not None or paths.multi_trader_smoke_package_metadata_path is not None:
        if paths.multi_trader_smoke_path is None:
            blockers.append("missing_multi_trader_smoke")
        if paths.multi_trader_smoke_preflight_path is None:
            blockers.append("missing_multi_trader_smoke_preflight")
        if paths.multi_trader_smoke_manifest_path is None:
            blockers.append("missing_multi_trader_smoke_manifest")
        if paths.multi_trader_smoke_package_path is None:
            blockers.append("missing_multi_trader_smoke_package")
        if paths.multi_trader_smoke_package_metadata_path is None:
            blockers.append("missing_multi_trader_smoke_package_metadata")
        else:
            package_metadata = load_required_json(
                paths.multi_trader_smoke_package_metadata_path,
                "missing_multi_trader_smoke_package_metadata",
                blockers,
            )
    evidence["multi_trader_smoke_package_schema_version"] = (package_metadata or {}).get("schema_version")
    evidence["multi_trader_smoke_package_file_count"] = (package_metadata or {}).get("file_count")
    if package_metadata is not None:
        blockers.extend(
            validate_multi_trader_smoke_package(
                metadata=package_metadata,
                package_path=paths.multi_trader_smoke_package_path,
                smoke_path=paths.multi_trader_smoke_path,
                preflight_path=paths.multi_trader_smoke_preflight_path,
                manifest_path=paths.multi_trader_smoke_manifest_path,
                smoke=smoke,
                evidence=evidence,
            )
        )

    return {
        "schema_version": 1,
        "passed": not blockers,
        "blockers": blockers,
        "evidence": evidence,
        "paths": {
            "shadow_reports_directory": str(paths.shadow_reports_directory),
            "manifest_directory": str(paths.manifest_directory),
            "runtime_config_path": str(paths.runtime_config_path),
            "runtime_health_path": str(paths.runtime_health_path),
            "readiness_path": str(paths.readiness_path),
            "frontend_deployment_path": str(paths.frontend_deployment_path),
            "legacy_decommission_path": str(paths.legacy_decommission_path),
            "legacy_retirement_path": str(paths.legacy_retirement_path),
            "multi_trader_smoke_path": (
                str(paths.multi_trader_smoke_path) if paths.multi_trader_smoke_path is not None else None
            ),
            "multi_trader_smoke_preflight_path": (
                str(paths.multi_trader_smoke_preflight_path)
                if paths.multi_trader_smoke_preflight_path is not None
                else None
            ),
            "multi_trader_smoke_manifest_path": (
                str(paths.multi_trader_smoke_manifest_path)
                if paths.multi_trader_smoke_manifest_path is not None
                else None
            ),
            "multi_trader_smoke_package_path": (
                str(paths.multi_trader_smoke_package_path)
                if paths.multi_trader_smoke_package_path is not None
                else None
            ),
            "multi_trader_smoke_package_metadata_path": (
                str(paths.multi_trader_smoke_package_metadata_path)
                if paths.multi_trader_smoke_package_metadata_path is not None
                else None
            ),
        },
    }


def validate_multi_trader_smoke_manifest(
    *,
    manifest: dict[str, Any],
    manifest_path: Path | None,
    smoke_path: Path | None,
    preflight_path: Path | None,
    evidence: dict[str, Any],
) -> list[str]:
    blockers: list[str] = []
    if manifest.get("schema_version") != 1:
        blockers.append("multi_trader_smoke_manifest_schema_invalid")
    files = manifest.get("files")
    if not isinstance(files, list):
        evidence["multi_trader_smoke_manifest_files"] = []
        return sorted(set(blockers + ["multi_trader_smoke_manifest_files_invalid"]))
    file_entries = [entry for entry in files if isinstance(entry, dict)]
    evidence["multi_trader_smoke_manifest_files"] = [
        str(entry.get("path") or "") for entry in file_entries if isinstance(entry.get("path"), str)
    ]
    if len(file_entries) != len(files):
        blockers.append("multi_trader_smoke_manifest_files_invalid")
    if manifest.get("file_count") != len(files):
        blockers.append("multi_trader_smoke_manifest_file_count_mismatch")
    if manifest_path is not None:
        manifest_name = Path(manifest_path).name
        if manifest_name in evidence["multi_trader_smoke_manifest_files"]:
            blockers.append("multi_trader_smoke_manifest_self_references")
    for path, blocker_prefix in [
        (smoke_path, "multi_trader_smoke_manifest_smoke"),
        (preflight_path, "multi_trader_smoke_manifest_preflight"),
    ]:
        if path is None:
            continue
        entry = manifest_entry_for_path(file_entries, path)
        if entry is None:
            blockers.append(f"{blocker_prefix}_missing")
            continue
        if not manifest_entry_matches_file(entry, path):
            blockers.append(f"{blocker_prefix}_hash_mismatch")
    return sorted(set(blockers))


def manifest_entry_for_path(entries: list[dict[str, Any]], path: Path) -> dict[str, Any] | None:
    path = Path(path)
    candidates = {path.name, path.as_posix()}
    for entry in entries:
        entry_path = entry.get("path")
        if not isinstance(entry_path, str):
            continue
        if entry_path in candidates or entry_path.endswith(f"/{path.name}"):
            return entry
    return None


def manifest_entry_matches_file(entry: dict[str, Any], path: Path) -> bool:
    path = Path(path)
    try:
        data = path.read_bytes()
    except OSError:
        return False
    return entry.get("bytes") == len(data) and entry.get("sha256") == hashlib.sha256(data).hexdigest()


def validate_multi_trader_smoke_package(
    *,
    metadata: dict[str, Any],
    package_path: Path | None,
    smoke_path: Path | None = None,
    preflight_path: Path | None = None,
    manifest_path: Path | None = None,
    smoke: dict[str, Any] | None = None,
    evidence: dict[str, Any],
) -> list[str]:
    blockers: list[str] = []
    if metadata.get("schema_version") != 1:
        blockers.append("multi_trader_smoke_package_schema_invalid")
    files = metadata.get("files")
    if not isinstance(files, list) or not all(isinstance(item, str) and item for item in files):
        evidence["multi_trader_smoke_package_files"] = []
        blockers.append("multi_trader_smoke_package_files_invalid")
    else:
        evidence["multi_trader_smoke_package_files"] = list(files)
        if metadata.get("file_count") != len(files):
            blockers.append("multi_trader_smoke_package_file_count_mismatch")
    if package_path is None:
        return sorted(set(blockers))
    package = Path(package_path)
    try:
        data = package.read_bytes()
    except OSError:
        return sorted(set(blockers + ["missing_multi_trader_smoke_package"]))
    evidence["multi_trader_smoke_package_bytes"] = len(data)
    evidence["multi_trader_smoke_package_sha256"] = hashlib.sha256(data).hexdigest()
    if metadata.get("bytes") != len(data):
        blockers.append("multi_trader_smoke_package_bytes_mismatch")
    if metadata.get("sha256") != hashlib.sha256(data).hexdigest():
        blockers.append("multi_trader_smoke_package_hash_mismatch")
    import_manifest = None
    package_run_manifest = None
    package_preflight = None
    package_service_preflight = None
    try:
        with zipfile.ZipFile(package) as archive:
            zip_names = sorted(archive.namelist())
            blockers.extend(
                validate_multi_trader_smoke_package_artifact_hashes(
                    archive=archive,
                    artifact_paths={
                        "multi-trader-smoke-evidence.json": smoke_path,
                        "lan-preflight.json": preflight_path,
                        "smoke-run-manifest.json": manifest_path,
                    },
                    evidence=evidence,
                )
            )
            if "smoke-run-manifest.json" in zip_names:
                try:
                    package_run_manifest = json.loads(archive.read("smoke-run-manifest.json").decode("utf-8"))
                except (KeyError, UnicodeDecodeError, json.JSONDecodeError):
                    blockers.append("multi_trader_smoke_package_manifest_invalid")
            if "smoke-import-manifest.json" in zip_names:
                try:
                    import_manifest = json.loads(archive.read("smoke-import-manifest.json").decode("utf-8"))
                except (KeyError, UnicodeDecodeError, json.JSONDecodeError):
                    blockers.append("multi_trader_smoke_package_import_manifest_invalid")
            if "lan-preflight.json" in zip_names:
                try:
                    package_preflight = json.loads(archive.read("lan-preflight.json").decode("utf-8"))
                except (KeyError, UnicodeDecodeError, json.JSONDecodeError):
                    blockers.append("multi_trader_smoke_package_preflight_invalid")
            if "service-preflight.json" in zip_names:
                try:
                    package_service_preflight = json.loads(archive.read("service-preflight.json").decode("utf-8"))
                except (KeyError, UnicodeDecodeError, json.JSONDecodeError):
                    blockers.append("multi_trader_smoke_package_service_preflight_invalid")
    except zipfile.BadZipFile:
        evidence["multi_trader_smoke_package_zip_files"] = []
        return sorted(set(blockers + ["multi_trader_smoke_package_zip_invalid"]))
    evidence["multi_trader_smoke_package_zip_files"] = zip_names
    if isinstance(files, list) and all(isinstance(item, str) and item for item in files) and zip_names != sorted(files):
        blockers.append("multi_trader_smoke_package_zip_files_mismatch")
    if "smoke-import-manifest.json" not in zip_names:
        blockers.append("multi_trader_smoke_package_import_manifest_missing")
    elif import_manifest is not None:
        blockers.extend(validate_multi_trader_smoke_import_manifest(import_manifest, evidence, zip_names))
    if "smoke-run-manifest.json" not in zip_names:
        blockers.append("multi_trader_smoke_package_manifest_missing")
    elif package_run_manifest is not None:
        blockers.extend(
            validate_multi_trader_smoke_package_run_manifest(
                package_run_manifest,
                evidence,
                package,
                zip_names,
            )
        )
    blockers.extend(
        validate_multi_trader_smoke_package_preflight(
            package_preflight=package_preflight,
            package_service_preflight=package_service_preflight,
            smoke_observed_at=str((smoke or {}).get("observed_at") or ""),
            zip_names=zip_names,
            evidence=evidence,
        )
    )
    return sorted(set(blockers))


def validate_multi_trader_smoke_package_artifact_hashes(
    *,
    archive: zipfile.ZipFile,
    artifact_paths: dict[str, Path | None],
    evidence: dict[str, Any],
) -> list[str]:
    blockers: list[str] = []
    for zip_name, artifact_path in artifact_paths.items():
        evidence_key = zip_name.replace("-", "_").replace(".", "_")
        if artifact_path is None:
            continue
        try:
            package_bytes = archive.read(zip_name)
        except KeyError:
            blockers.append(f"multi_trader_smoke_package_{evidence_key}_missing")
            continue
        try:
            artifact_bytes = Path(artifact_path).read_bytes()
        except OSError:
            continue
        package_hash = hashlib.sha256(package_bytes).hexdigest()
        artifact_hash = hashlib.sha256(artifact_bytes).hexdigest()
        evidence[f"multi_trader_smoke_package_{evidence_key}_sha256"] = package_hash
        if package_hash != artifact_hash:
            blockers.append(f"multi_trader_smoke_package_{evidence_key}_hash_mismatch")
    return blockers


def validate_multi_trader_smoke_package_run_manifest(
    manifest: Any,
    evidence: dict[str, Any],
    package_path: Path,
    zip_names: list[str],
) -> list[str]:
    blockers: list[str] = []
    if not isinstance(manifest, dict):
        return ["multi_trader_smoke_package_manifest_invalid"]
    evidence["multi_trader_smoke_package_manifest_schema_version"] = manifest.get("schema_version")
    if manifest.get("schema_version") != 1:
        blockers.append("multi_trader_smoke_package_manifest_schema_invalid")
    files = manifest.get("files")
    if not isinstance(files, list):
        return ["multi_trader_smoke_package_manifest_files_invalid"]
    file_entries = [entry for entry in files if isinstance(entry, dict)]
    if len(file_entries) != len(files):
        blockers.append("multi_trader_smoke_package_manifest_files_invalid")
    if manifest.get("file_count") != len(files):
        blockers.append("multi_trader_smoke_package_manifest_file_count_mismatch")
    missing_paths: list[str] = []
    hash_mismatch_paths: list[str] = []
    invalid_paths: list[str] = []
    manifest_paths: list[str] = []
    with zipfile.ZipFile(package_path) as archive:
        for entry in file_entries:
            path = entry.get("path")
            if (
                not isinstance(path, str)
                or not path
                or path.startswith("/")
                or any(part in {"", ".", ".."} for part in Path(path).parts)
            ):
                invalid_paths.append(str(path or ""))
                continue
            manifest_paths.append(path)
            try:
                data = archive.read(path)
            except KeyError:
                missing_paths.append(path)
                continue
            if entry.get("bytes") != len(data) or entry.get("sha256") != hashlib.sha256(data).hexdigest():
                hash_mismatch_paths.append(path)
    unmanifested_paths = sorted(set(zip_names) - set(manifest_paths) - {"smoke-run-manifest.json"})
    evidence["multi_trader_smoke_package_manifest_missing_paths"] = missing_paths
    evidence["multi_trader_smoke_package_manifest_hash_mismatch_paths"] = hash_mismatch_paths
    evidence["multi_trader_smoke_package_manifest_invalid_paths"] = invalid_paths
    duplicate_paths = sorted({path for path in manifest_paths if manifest_paths.count(path) > 1})
    evidence["multi_trader_smoke_package_manifest_duplicate_paths"] = duplicate_paths
    evidence["multi_trader_smoke_package_manifest_unmanifested_paths"] = unmanifested_paths
    if missing_paths:
        blockers.append("multi_trader_smoke_package_manifest_file_missing")
    if hash_mismatch_paths:
        blockers.append("multi_trader_smoke_package_manifest_file_hash_mismatch")
    if invalid_paths:
        blockers.append("multi_trader_smoke_package_manifest_files_invalid")
    if duplicate_paths:
        blockers.append("multi_trader_smoke_package_manifest_duplicate_files")
    if unmanifested_paths:
        blockers.append("multi_trader_smoke_package_manifest_unmanifested_files")
    return blockers


def validate_multi_trader_smoke_package_preflight(
    *,
    package_preflight: Any,
    package_service_preflight: Any,
    smoke_observed_at: str,
    zip_names: list[str],
    evidence: dict[str, Any],
) -> list[str]:
    blockers: list[str] = []
    evidence["multi_trader_smoke_package_preflight_present"] = "lan-preflight.json" in zip_names
    evidence["multi_trader_smoke_package_service_preflight_present"] = "service-preflight.json" in zip_names
    if "lan-preflight.json" not in zip_names:
        blockers.append("multi_trader_smoke_package_preflight_missing")
    elif not isinstance(package_preflight, dict):
        blockers.append("multi_trader_smoke_package_preflight_invalid")
    else:
        preflight_blockers = validate_multi_trader_smoke_preflight(preflight=package_preflight, frontend_live_url=None)
        evidence["multi_trader_smoke_package_preflight_blockers"] = preflight_blockers
        if preflight_blockers:
            blockers.append("multi_trader_smoke_package_preflight_not_passed")

    if "service-preflight.json" not in zip_names:
        blockers.append("multi_trader_smoke_package_service_preflight_missing")
    elif not isinstance(package_service_preflight, dict):
        blockers.append("multi_trader_smoke_package_service_preflight_invalid")
    else:
        service_blockers = validate_multi_trader_smoke_service_checks(package_service_preflight)
        evidence["multi_trader_smoke_package_service_preflight_blockers"] = service_blockers
        if service_blockers:
            blockers.append("multi_trader_smoke_package_service_preflight_not_passed")

    if isinstance(package_preflight, dict) and isinstance(package_service_preflight, dict):
        if package_preflight.get("service_checks") != package_service_preflight:
            blockers.append("multi_trader_smoke_package_service_preflight_mismatch")
        package_service_timing = multi_trader_service_preflight_timing_evidence(
            multi_trader_smoke_preflight_evidence(package_preflight)["evidence"],
            smoke_observed_at,
        )
        evidence["multi_trader_smoke_package_service_preflight_timing"] = package_service_timing["evidence"]
        blockers.extend(
            blocker.replace("multi_trader_smoke_", "multi_trader_smoke_package_", 1)
            for blocker in package_service_timing["blockers"]
        )
    return sorted(set(blockers))


def validate_multi_trader_smoke_import_manifest(manifest: Any, evidence: dict[str, Any], zip_names: list[str]) -> list[str]:
    blockers: list[str] = []
    if not isinstance(manifest, dict):
        return ["multi_trader_smoke_package_import_manifest_invalid"]
    evidence["multi_trader_smoke_import_manifest_schema_version"] = manifest.get("schema_version")
    evidence["multi_trader_smoke_import_manifest_imported_count"] = manifest.get("imported_count")
    evidence["multi_trader_smoke_import_manifest_skipped_count"] = manifest.get("skipped_count")
    if manifest.get("schema_version") != 1:
        blockers.append("multi_trader_smoke_package_import_manifest_schema_invalid")
    imported = manifest.get("imported")
    skipped = manifest.get("skipped")
    runs = manifest.get("runs")
    if not isinstance(imported, list) or not all(isinstance(item, dict) for item in imported):
        blockers.append("multi_trader_smoke_package_import_manifest_imported_invalid")
    elif not imported:
        blockers.append("multi_trader_smoke_package_import_manifest_imported_empty")
    if not isinstance(skipped, list) or not all(isinstance(item, dict) for item in skipped):
        blockers.append("multi_trader_smoke_package_import_manifest_skipped_invalid")
    if not isinstance(runs, list) or not runs or not all(isinstance(item, dict) for item in runs):
        blockers.append("multi_trader_smoke_package_import_manifest_runs_invalid")
    if isinstance(imported, list) and manifest.get("imported_count") != len(imported):
        blockers.append("multi_trader_smoke_package_import_manifest_imported_count_mismatch")
    if isinstance(skipped, list) and manifest.get("skipped_count") != len(skipped):
        blockers.append("multi_trader_smoke_package_import_manifest_skipped_count_mismatch")
    if isinstance(imported, list):
        imported_output_paths: list[str] = []
        invalid_output_paths: list[str] = []
        kind_output_mismatches: list[str] = []
        invalid_kinds: list[str] = []
        invalid_input_paths: list[str] = []
        for item in imported:
            if not isinstance(item, dict):
                continue
            kind = item.get("kind")
            input_path = item.get("input_path")
            output_path = item.get("output_path")
            if kind not in {"client", "performance"}:
                invalid_kinds.append(str(kind))
            if not isinstance(input_path, str) or not input_path.strip():
                invalid_input_paths.append(str(input_path))
            relative_output = smoke_import_manifest_zip_output_relative(output_path)
            if relative_output is None:
                invalid_output_paths.append(str(output_path))
                continue
            expected_prefix = "clients/" if kind == "client" else "performance/"
            if kind in {"client", "performance"} and not relative_output.startswith(expected_prefix):
                kind_output_mismatches.append(relative_output)
            imported_output_paths.append(relative_output)
        duplicate_outputs = sorted({path for path in imported_output_paths if imported_output_paths.count(path) > 1})
        missing_outputs = [
            output_path for output_path in imported_output_paths if not import_output_path_in_zip(output_path, zip_names)
        ]
        evidence["multi_trader_smoke_import_manifest_output_paths"] = imported_output_paths
        evidence["multi_trader_smoke_import_manifest_invalid_kinds"] = invalid_kinds
        evidence["multi_trader_smoke_import_manifest_invalid_input_paths"] = invalid_input_paths
        evidence["multi_trader_smoke_import_manifest_invalid_output_paths"] = invalid_output_paths
        evidence["multi_trader_smoke_import_manifest_kind_output_mismatches"] = kind_output_mismatches
        evidence["multi_trader_smoke_import_manifest_duplicate_output_paths"] = duplicate_outputs
        evidence["multi_trader_smoke_import_manifest_missing_output_paths"] = missing_outputs
        if invalid_kinds:
            blockers.append("multi_trader_smoke_package_import_manifest_kind_invalid")
        if invalid_input_paths:
            blockers.append("multi_trader_smoke_package_import_manifest_input_path_invalid")
        if invalid_output_paths:
            blockers.append("multi_trader_smoke_package_import_manifest_output_path_invalid")
        if kind_output_mismatches:
            blockers.append("multi_trader_smoke_package_import_manifest_kind_output_mismatch")
        if duplicate_outputs:
            blockers.append("multi_trader_smoke_package_import_manifest_duplicate_output")
        if missing_outputs:
            blockers.append("multi_trader_smoke_package_import_manifest_output_missing")
        smoke_artifact_paths = smoke_package_frontend_artifact_paths(zip_names)
        missing_import_coverage = sorted(set(smoke_artifact_paths) - set(imported_output_paths))
        evidence["multi_trader_smoke_import_manifest_frontend_artifact_paths"] = smoke_artifact_paths
        evidence["multi_trader_smoke_import_manifest_missing_frontend_artifact_paths"] = missing_import_coverage
        if missing_import_coverage:
            blockers.append("multi_trader_smoke_package_import_manifest_coverage_missing")
    if isinstance(runs, list) and all(isinstance(item, dict) for item in runs):
        flattened_imported: list[Any] = []
        flattened_skipped: list[Any] = []
        run_shape_invalid = False
        run_count_mismatch = False
        for run in runs:
            run_imported = run.get("imported")
            run_skipped = run.get("skipped")
            if not isinstance(run_imported, list) or not all(isinstance(item, dict) for item in run_imported):
                run_shape_invalid = True
            else:
                flattened_imported.extend(run_imported)
                if run.get("imported_count") != len(run_imported):
                    run_count_mismatch = True
            if not isinstance(run_skipped, list) or not all(isinstance(item, dict) for item in run_skipped):
                run_shape_invalid = True
            else:
                flattened_skipped.extend(run_skipped)
                if run.get("skipped_count") != len(run_skipped):
                    run_count_mismatch = True
        if run_shape_invalid:
            blockers.append("multi_trader_smoke_package_import_manifest_run_shape_invalid")
        if run_count_mismatch:
            blockers.append("multi_trader_smoke_package_import_manifest_run_count_mismatch")
        if isinstance(imported, list) and flattened_imported != imported:
            blockers.append("multi_trader_smoke_package_import_manifest_imported_run_mismatch")
        if isinstance(skipped, list) and flattened_skipped != skipped:
            blockers.append("multi_trader_smoke_package_import_manifest_skipped_run_mismatch")
    return blockers


def import_output_path_in_zip(output_path: str, zip_names: list[str]) -> bool:
    normalized = smoke_import_manifest_zip_output_relative(output_path)
    return normalized is not None and normalized in zip_names


def smoke_package_frontend_artifact_paths(zip_names: list[str]) -> list[str]:
    return sorted(
        name
        for name in zip_names
        if name.endswith(".json")
        and not any(part in {"", ".", ".."} for part in Path(name).parts)
        and (name.startswith("clients/") or name.startswith("performance/"))
    )


def smoke_import_manifest_zip_output_relative(output_path: Any) -> str | None:
    if not isinstance(output_path, str) or not output_path.strip():
        return None
    normalized = output_path.strip().replace("\\", "/")
    path = Path(normalized)
    if path.is_absolute():
        return None
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        return None
    relative = path.as_posix()
    if not relative.endswith(".json"):
        return None
    if not (relative.startswith("clients/") or relative.startswith("performance/")):
        return None
    return relative


def validate_multi_trader_smoke_preflight(*, preflight: dict[str, Any], frontend_live_url: Any) -> list[str]:
    blockers: list[str] = []
    if preflight.get("schema_version") != 1:
        blockers.append("multi_trader_smoke_preflight_schema_invalid")
    if preflight.get("passed") is not True:
        blockers.append("multi_trader_smoke_preflight_blocked")
    preflight_blockers = preflight.get("blockers")
    if not isinstance(preflight_blockers, list) or preflight_blockers:
        blockers.append("multi_trader_smoke_preflight_blockers_present")
    prepared_at = preflight.get("prepared_at")
    if not isinstance(prepared_at, str) or not prepared_at:
        blockers.append("multi_trader_smoke_preflight_prepared_at_missing")
    elif not is_iso_datetime(prepared_at):
        blockers.append("multi_trader_smoke_preflight_prepared_at_invalid")
    page_url = preflight.get("page_url")
    gateway_url = preflight.get("gateway_url")
    if not isinstance(page_url, str) or not page_url.strip():
        blockers.append("multi_trader_smoke_preflight_page_url_missing")
    else:
        parsed_page = urlparse(page_url)
        if parsed_page.scheme not in {"http", "https"} or not parsed_page.netloc:
            blockers.append("multi_trader_smoke_preflight_page_url_invalid")
        if is_loopback_host(parsed_page.hostname) or parsed_page.hostname in {"0.0.0.0", "::"}:
            blockers.append("multi_trader_smoke_preflight_page_url_not_client_routable")
    gateway_errors = validate_frontend_live_url(gateway_url)
    if gateway_errors:
        blockers.append("multi_trader_smoke_preflight_gateway_url_invalid")
        blockers.extend(f"multi_trader_smoke_preflight_{error}" for error in gateway_errors)
    if isinstance(frontend_live_url, str) and frontend_live_url.strip() and gateway_url != frontend_live_url:
        blockers.append("multi_trader_smoke_preflight_gateway_url_mismatch")
    service_checks = preflight.get("service_checks")
    blockers.extend(validate_multi_trader_smoke_service_checks(service_checks))
    blockers.extend(
        validate_multi_trader_smoke_service_check_targets(
            service_checks=service_checks,
            page_url=page_url,
            gateway_url=gateway_url,
        )
    )
    return sorted(set(blockers))


def validate_multi_trader_smoke_service_checks(service_checks: Any) -> list[str]:
    blockers: list[str] = []
    if not isinstance(service_checks, dict):
        return ["multi_trader_smoke_service_preflight_missing"]
    if service_checks.get("schema_version") != 1:
        blockers.append("multi_trader_smoke_service_preflight_schema_invalid")
    if service_checks.get("passed") is not True:
        blockers.append("multi_trader_smoke_service_preflight_blocked")
    checked_at = service_checks.get("checked_at")
    if not isinstance(checked_at, str) or not checked_at:
        blockers.append("multi_trader_smoke_service_preflight_checked_at_missing")
    elif not is_iso_datetime(checked_at):
        blockers.append("multi_trader_smoke_service_preflight_checked_at_invalid")
    service_blockers = service_checks.get("blockers")
    if not isinstance(service_blockers, list) or service_blockers:
        blockers.append("multi_trader_smoke_service_preflight_blockers_present")
    checks = service_checks.get("checks")
    if not isinstance(checks, dict):
        blockers.append("multi_trader_smoke_service_preflight_checks_invalid")
        return sorted(set(blockers))
    for name in ("frontend", "gateway"):
        check = checks.get(name)
        if not isinstance(check, dict):
            blockers.append(f"multi_trader_smoke_{name}_service_check_missing")
            continue
        if check.get("reachable") is not True:
            blockers.append(f"multi_trader_smoke_{name}_service_unreachable")
        expected_probe = "frontend_http" if name == "frontend" else "gateway_websocket"
        if check.get("probe_kind") != expected_probe:
            blockers.append(f"multi_trader_smoke_{name}_service_probe_kind_invalid")
        if name == "frontend" and not isinstance(check.get("status_code"), int):
            blockers.append("multi_trader_smoke_frontend_service_status_missing")
        if name == "gateway" and check.get("websocket_handshake") is not True:
            blockers.append("multi_trader_smoke_gateway_service_websocket_handshake_missing")
    return sorted(set(blockers))


def validate_multi_trader_smoke_service_check_targets(
    *,
    service_checks: Any,
    page_url: Any,
    gateway_url: Any,
) -> list[str]:
    if not isinstance(service_checks, dict):
        return []
    checks = service_checks.get("checks")
    if not isinstance(checks, dict):
        return []
    blockers: list[str] = []
    expected_urls = {
        "frontend": page_url if isinstance(page_url, str) else "",
        "gateway": gateway_url if isinstance(gateway_url, str) else "",
    }
    for name, expected_url in expected_urls.items():
        check = checks.get(name)
        if not isinstance(check, dict):
            continue
        checked_url = check.get("url")
        if isinstance(expected_url, str) and expected_url.strip() and checked_url != expected_url:
            blockers.append(f"multi_trader_smoke_{name}_service_check_url_mismatch")
    return sorted(set(blockers))


def validate_frontend_live_url_targets_gateway(
    *,
    frontend_live_url: Any,
    runtime_health: dict[str, Any] | None,
    evidence: dict[str, Any],
) -> list[str]:
    blockers = []
    if not isinstance(frontend_live_url, str) or not frontend_live_url.strip():
        return blockers
    gateway_websocket = (runtime_health or {}).get("gateway_websocket") or {}
    if not isinstance(gateway_websocket, dict):
        return blockers
    parsed = urlparse(frontend_live_url)
    try:
        parsed_port = parsed.port
    except ValueError:
        parsed_port = None
        blockers.append("frontend_deployment_live_url_invalid")
    try:
        gateway_port = int(gateway_websocket.get("port") or 0)
    except (TypeError, ValueError):
        gateway_port = 0
    evidence["frontend_live_url_scheme"] = parsed.scheme
    evidence["frontend_live_url_path"] = parsed.path
    evidence["frontend_live_url_port"] = parsed_port
    evidence["runtime_gateway_path"] = gateway_websocket.get("path")
    evidence["runtime_gateway_port"] = gateway_websocket.get("port")
    if parsed.scheme not in {"ws", "wss"} or not parsed.netloc:
        blockers.append("frontend_deployment_live_url_invalid")
    if is_loopback_host(parsed.hostname):
        blockers.append("frontend_live_url_loopback_host")
    if parsed.path != gateway_websocket.get("path"):
        blockers.append("frontend_live_url_gateway_path_mismatch")
    if parsed_port is None:
        blockers.append("frontend_live_url_gateway_port_missing")
    elif parsed_port != gateway_port:
        blockers.append("frontend_live_url_gateway_port_mismatch")
    return blockers


def is_loopback_host(hostname: Any) -> bool:
    if not isinstance(hostname, str):
        return False
    normalized = hostname.strip().lower()
    return normalized in {"localhost", "127.0.0.1", "::1"} or normalized.startswith("127.")


def manifest_date_range_mismatches(
    *,
    manifests: list[dict[str, Any]],
    trading_date: str,
) -> list[str]:
    if not is_yyyymmdd(trading_date):
        return []
    mismatches = []
    for manifest in manifests:
        data_type = str(manifest.get("data_type") or "")
        date_range = manifest.get("date_range") or {}
        if not isinstance(date_range, dict):
            continue
        start = str(date_range.get("start") or "")
        end = str(date_range.get("end") or "")
        if is_yyyymmdd(start) and is_yyyymmdd(end) and not (start <= trading_date <= end):
            mismatches.append(data_type or f"manifest-{len(mismatches) + 1}")
    return mismatches


def manifest_symbol_coverage_mismatches(
    *,
    manifests: list[dict[str, Any]],
    subscribed_symbols: list[str],
) -> dict[str, list[str]]:
    if not subscribed_symbols:
        return {}
    mismatches = {}
    for manifest in manifests:
        data_type = str(manifest.get("data_type") or "")
        if data_type not in SYMBOL_SCOPED_HISTORICAL_MANIFEST_TYPES:
            continue
        symbols = manifest.get("symbols")
        if not isinstance(symbols, list):
            mismatches[data_type] = list(subscribed_symbols)
            continue
        covered_symbols = {str(symbol) for symbol in symbols}
        missing_symbols = [symbol for symbol in subscribed_symbols if symbol not in covered_symbols]
        if missing_symbols:
            mismatches[data_type] = missing_symbols
    return mismatches


def shadow_comparison_symbol_coverage_for_runtime(
    *,
    reports: list[dict[str, Any]],
    accepted_report_ids: set[str],
    subscribed_symbols: list[str],
) -> dict[str, list[str]]:
    comparison_symbols: set[str] = set()
    if subscribed_symbols:
        for report in reports:
            report_id = str(report.get("session_id") or "")
            if report_id not in accepted_report_ids:
                continue
            symbols = ((report.get("comparison") or {}).get("symbols") or {})
            if isinstance(symbols, dict):
                comparison_symbols.update(str(symbol) for symbol in symbols)
    missing_symbols = [symbol for symbol in subscribed_symbols if symbol not in comparison_symbols]
    return {
        "comparison_symbols": sorted(comparison_symbols),
        "missing_symbols": missing_symbols,
    }


def delivered_symbol_shadow_coverage(
    *,
    runtime_evidence: dict[str, Any],
    shadow_comparison_symbols: list[str],
) -> dict[str, list[str]]:
    gateway_activity = runtime_evidence.get("gateway_activity") or {}
    delivered_symbols = gateway_activity.get("delivered_terminal_symbols") if isinstance(gateway_activity, dict) else []
    if not isinstance(delivered_symbols, list):
        return {"missing_symbols": []}
    comparison_symbol_set = set(shadow_comparison_symbols)
    missing_symbols = [
        symbol
        for symbol in delivered_symbols
        if isinstance(symbol, str) and valid_terminal_symbol(symbol) and symbol not in comparison_symbol_set
    ]
    return {"missing_symbols": missing_symbols}


def kafka_activity_offset_coverage(
    *,
    topics: Any,
    ingest_processed: int,
    raw_consumer_processed: int,
    gateway_processed_records_consumed: int,
) -> dict[str, Any]:
    raw_committed_offset = runtime_topic_committed_offset(topics, RAW_TOPIC)
    processed_committed_offset = runtime_topic_committed_offset(topics, PROCESSED_TOPIC)
    return {
        "raw_topic_committed_offset": raw_committed_offset,
        "processed_topic_committed_offset": processed_committed_offset,
        "ingest_processed": ingest_processed,
        "raw_consumer_processed": raw_consumer_processed,
        "gateway_processed_records_consumed": gateway_processed_records_consumed,
        "raw_topic_committed_offset_below_ingest_processed": (
            raw_committed_offset is not None and raw_committed_offset < ingest_processed
        ),
        "processed_topic_committed_offset_below_raw_consumer_processed": (
            processed_committed_offset is not None and processed_committed_offset < raw_consumer_processed
        ),
        "processed_topic_committed_offset_below_gateway_consumed": (
            processed_committed_offset is not None
            and processed_committed_offset < gateway_processed_records_consumed
        ),
    }


def runtime_topic_committed_offset(topics: Any, topic: str) -> int | None:
    if not isinstance(topics, dict) or not isinstance(topics.get(topic), dict):
        return None
    committed_offset = topics[topic].get("committed_offset")
    return int(committed_offset) if non_negative_integer(committed_offset) else None


def gateway_client_queue_evidence(value: Any) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    if not isinstance(value, dict):
        return {}, ["runtime_health_gateway_client_queue_missing"]
    counter_fields = (
        "connected_clients",
        "observed_client_count",
        "observed_declared_client_count",
        "max_connected_clients",
        "client_queue_max_size",
        "total_current_backlog",
        "max_current_backlog",
        "enqueued",
        "coalesced",
        "dropped",
        "alerts_enqueued",
        "alert_overflow",
        "alert_dropped",
        "critical_overflow",
    )
    invalid_counters = [field for field in counter_fields if not non_negative_integer(value.get(field))]
    current_backlog_by_client = value.get("current_backlog_by_client")
    invalid_backlog_clients: list[str] = []
    if not isinstance(current_backlog_by_client, dict):
        invalid_backlog_clients.append("<current_backlog_by_client>")
        current_backlog_by_client = {}
    else:
        invalid_backlog_clients = [
            str(client_id)
            for client_id, backlog in current_backlog_by_client.items()
            if not isinstance(client_id, str) or not non_negative_integer(backlog)
        ]
    connected_clients = non_negative_int_value(value.get("connected_clients"))
    total_current_backlog = non_negative_int_value(value.get("total_current_backlog"))
    max_current_backlog = non_negative_int_value(value.get("max_current_backlog"))
    observed_client_count = non_negative_int_value(value.get("observed_client_count"))
    observed_declared_client_count = non_negative_int_value(value.get("observed_declared_client_count"))
    max_connected_clients = non_negative_int_value(value.get("max_connected_clients"))
    client_queue_max_size = non_negative_int_value(value.get("client_queue_max_size"))
    dropped = non_negative_int_value(value.get("dropped"))
    alert_overflow = non_negative_int_value(value.get("alert_overflow"))
    alert_dropped = non_negative_int_value(value.get("alert_dropped"))
    critical_overflow = non_negative_int_value(value.get("critical_overflow"))
    observed_backlogs = [
        int(backlog) for backlog in current_backlog_by_client.values() if non_negative_integer(backlog)
    ]
    observed_total_backlog = sum(observed_backlogs)
    observed_max_backlog = max(observed_backlogs, default=0)
    observed_client_ids = value.get("observed_client_ids")
    observed_declared_client_ids = value.get("observed_declared_client_ids")
    invalid_observed_client_ids = not isinstance(observed_client_ids, list) or any(
        not normalized_client_id(client_id) for client_id in observed_client_ids or []
    )
    invalid_declared_client_ids = not isinstance(observed_declared_client_ids, list) or any(
        not normalized_client_id(client_id) for client_id in observed_declared_client_ids or []
    )
    evidence = {
        "connected_clients": connected_clients,
        "observed_client_count": observed_client_count,
        "observed_client_ids": list(observed_client_ids) if isinstance(observed_client_ids, list) else [],
        "observed_declared_client_count": observed_declared_client_count,
        "observed_declared_client_ids": (
            list(observed_declared_client_ids) if isinstance(observed_declared_client_ids, list) else []
        ),
        "max_connected_clients": max_connected_clients,
        "client_queue_max_size": client_queue_max_size,
        "current_backlog_by_client": dict(current_backlog_by_client),
        "total_current_backlog": total_current_backlog,
        "max_current_backlog": max_current_backlog,
        "enqueued": non_negative_int_value(value.get("enqueued")),
        "coalesced": non_negative_int_value(value.get("coalesced")),
        "dropped": dropped,
        "alerts_enqueued": non_negative_int_value(value.get("alerts_enqueued")),
        "alert_overflow": alert_overflow,
        "alert_dropped": alert_dropped,
        "critical_overflow": critical_overflow,
        "noncritical_drops_present": dropped > 0,
        "alert_overflow_present": alert_overflow > 0,
        "alert_drops_present": alert_dropped > 0,
        "critical_overflow_present": critical_overflow > 0,
        "invalid_counters": invalid_counters,
        "invalid_backlog_clients": invalid_backlog_clients,
        "invalid_observed_client_ids": invalid_observed_client_ids,
        "invalid_declared_client_ids": invalid_declared_client_ids,
        "observed_total_backlog": observed_total_backlog,
        "observed_max_backlog": observed_max_backlog,
    }
    if invalid_counters:
        blockers.append("runtime_health_gateway_client_queue_counter_invalid")
    if invalid_backlog_clients:
        blockers.append("runtime_health_gateway_client_queue_backlog_invalid")
    if invalid_observed_client_ids or invalid_declared_client_ids:
        blockers.append("runtime_health_gateway_client_queue_client_ids_invalid")
    if non_negative_integer(value.get("observed_client_count")) and isinstance(observed_client_ids, list):
        if int(value.get("observed_client_count") or 0) != len({client_id for client_id in observed_client_ids if normalized_client_id(client_id)}):
            blockers.append("runtime_health_gateway_client_queue_client_ids_invalid")
    if non_negative_integer(value.get("observed_declared_client_count")) and isinstance(observed_declared_client_ids, list):
        if int(value.get("observed_declared_client_count") or 0) != len({client_id for client_id in observed_declared_client_ids if normalized_client_id(client_id)}):
            blockers.append("runtime_health_gateway_client_queue_client_ids_invalid")
    if client_queue_max_size <= 0:
        blockers.append("runtime_health_gateway_client_queue_size_invalid")
    if connected_clients != len(current_backlog_by_client):
        blockers.append("runtime_health_gateway_client_queue_client_count_mismatch")
    if total_current_backlog != observed_total_backlog:
        blockers.append("runtime_health_gateway_client_queue_total_backlog_mismatch")
    if max_current_backlog != observed_max_backlog:
        blockers.append("runtime_health_gateway_client_queue_max_backlog_mismatch")
    if client_queue_max_size > 0 and max_current_backlog > client_queue_max_size:
        blockers.append("runtime_health_gateway_client_queue_backlog_exceeds_size")
    if alert_dropped > 0:
        blockers.append("runtime_health_gateway_alert_drops_present")
    if critical_overflow > 0:
        blockers.append("runtime_health_gateway_critical_overflow_present")
    return evidence, blockers


def normalized_client_id(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip()) and value == value.strip()


def runtime_health_performance_samples_evidence(value: Any) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    if not isinstance(value, dict):
        return {}, ["runtime_health_performance_samples_missing"]
    subscribe_values = value.get("subscribe_snapshot_ms")
    if not isinstance(subscribe_values, list):
        return {
            "sample_counts": {},
            "invalid_sample_keys": ["subscribe_snapshot_ms"],
        }, ["runtime_health_performance_samples_invalid"]
    invalid_values = [
        index
        for index, sample in enumerate(subscribe_values)
        if not non_negative_number(sample)
    ]
    if invalid_values:
        blockers.append("runtime_health_performance_samples_invalid")
    numeric_values = [float(sample) for sample in subscribe_values if non_negative_number(sample)]
    subscribe_snapshot_p95_ms = percentile(numeric_values, 0.95) if numeric_values else 0.0
    if not numeric_values:
        blockers.append("runtime_health_performance_samples_empty")
    elif subscribe_snapshot_p95_ms > 200:
        blockers.append("runtime_health_subscribe_snapshot_p95_exceeded")
    evidence = {
        "sample_counts": {"subscribe_snapshot_ms": len(subscribe_values)},
        "subscribe_snapshot_p95_ms": subscribe_snapshot_p95_ms,
        "invalid_sample_keys": [],
        "invalid_sample_indexes": {"subscribe_snapshot_ms": invalid_values},
    }
    return evidence, blockers


def runtime_shadow_window_coverage(
    *,
    reports: list[dict[str, Any]],
    accepted_report_ids: set[str],
    runtime_evidence: dict[str, Any],
) -> dict[str, list[str]]:
    supervisor = runtime_evidence.get("supervisor") or {}
    runtime_started_at = supervisor.get("started_at") if isinstance(supervisor, dict) else None
    runtime_last_tick_at = supervisor.get("last_tick_at") if isinstance(supervisor, dict) else None
    runtime_generated_at = runtime_evidence.get("generated_at")
    started_after_report_start: list[str] = []
    last_tick_before_report_finish: list[str] = []
    generated_before_report_finish: list[str] = []

    for report in reports:
        report_id = str(report.get("session_id") or "")
        if report_id not in accepted_report_ids:
            continue
        report_started_at = report.get("started_at")
        report_finished_at = report.get("finished_at")
        if (
            isinstance(runtime_started_at, str)
            and isinstance(report_started_at, str)
            and is_iso_datetime(runtime_started_at)
            and is_iso_datetime(report_started_at)
            and iso_datetime_is_before(report_started_at, runtime_started_at)
        ):
            started_after_report_start.append(report_id)
        if (
            isinstance(runtime_last_tick_at, str)
            and isinstance(report_finished_at, str)
            and is_iso_datetime(runtime_last_tick_at)
            and is_iso_datetime(report_finished_at)
            and iso_datetime_is_before(runtime_last_tick_at, report_finished_at)
        ):
            last_tick_before_report_finish.append(report_id)
        if (
            isinstance(runtime_generated_at, str)
            and isinstance(report_finished_at, str)
            and is_iso_datetime(runtime_generated_at)
            and is_iso_datetime(report_finished_at)
            and iso_datetime_is_before(runtime_generated_at, report_finished_at)
        ):
            generated_before_report_finish.append(report_id)

    return {
        "started_after_report_start": started_after_report_start,
        "last_tick_before_report_finish": last_tick_before_report_finish,
        "generated_before_report_finish": generated_before_report_finish,
    }


def frontend_verified_before_shadow_report_finish(
    *,
    reports: list[dict[str, Any]],
    accepted_report_ids: set[str],
    frontend_verified_at: str,
) -> list[str]:
    early_report_ids = []
    for report in reports:
        report_id = str(report.get("session_id") or "")
        if report_id not in accepted_report_ids:
            continue
        report_finished_at = report.get("finished_at")
        if (
            isinstance(report_finished_at, str)
            and is_iso_datetime(report_finished_at)
            and iso_datetime_is_before(frontend_verified_at, report_finished_at)
        ):
            early_report_ids.append(report_id)
    return early_report_ids


def validate_cutover_readiness_artifact(readiness: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if readiness.get("schema_version") != 1:
        errors.append("schema_version_invalid")
    if readiness.get("passed") is not True:
        errors.append("passed_not_true")
    if readiness.get("frontend_default_v2_allowed") is not True:
        errors.append("frontend_default_v2_allowed_not_true")
    if readiness.get("legacy_retirement_allowed") is not True:
        errors.append("legacy_retirement_allowed_not_true")
    if readiness.get("blockers") != []:
        errors.append("blockers_not_empty")
    if readiness.get("legacy_retirement_blockers") != []:
        errors.append("legacy_retirement_blockers_not_empty")
    if not non_negative_integer(readiness.get("report_count")) or int(readiness.get("report_count") or 0) <= 0:
        errors.append("report_count_invalid")

    accepted_report_ids = readiness.get("accepted_report_ids")
    if not isinstance(accepted_report_ids, list) or any(
        not isinstance(report_id, str) or not report_id.strip() for report_id in accepted_report_ids
    ):
        errors.append("accepted_report_ids_invalid")
    elif len(accepted_report_ids) != len(set(accepted_report_ids)):
        errors.append("accepted_report_ids_duplicate")

    if not isinstance(readiness.get("rejected_reports"), list):
        errors.append("rejected_reports_invalid")

    policy = readiness.get("policy")
    if not isinstance(policy, dict):
        errors.append("policy_missing")
    else:
        default_policy = CutoverPolicy()
        expected_policy = {
            "min_parallel_session_count": default_policy.min_parallel_session_count,
            "min_session_duration_seconds": default_policy.min_session_duration_seconds,
            "min_stream_coverage_ratio": default_policy.min_stream_coverage_ratio,
            "require_non_empty_streams": default_policy.require_non_empty_streams,
            "require_no_failed_symbols": default_policy.require_no_failed_symbols,
            "allow_legacy_retirement": default_policy.allow_legacy_retirement,
        }
        for field, expected in expected_policy.items():
            if policy.get(field) != expected:
                errors.append(f"policy_{field}_mismatch")
    return errors


def validate_shadow_run_reports(reports: list[dict[str, Any]]) -> list[dict[str, Any]]:
    invalid = []
    for index, report in enumerate(reports):
        errors = shadow_run_report_errors(report)
        if errors:
            invalid.append(
                {
                    "index": index,
                    "session_id": str(report.get("session_id") or ""),
                    "errors": errors,
                }
            )
    return invalid


def shadow_run_report_errors(report: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if report.get("schema_version") != 1:
        errors.append("schema_version_invalid")
    for field in ("session_id",):
        if not isinstance(report.get(field), str) or not report.get(field):
            errors.append(f"{field}_missing")
    errors.extend(shadow_run_report_timing_errors(report))
    if not is_yyyymmdd(str(report.get("trading_date") or "")):
        errors.append("trading_date_invalid")
    for field in ("legacy_event_count", "v2_event_count"):
        if not non_negative_integer(report.get(field)):
            errors.append(f"{field}_invalid")
    for field in ("legacy_source_coverage_seconds", "v2_source_coverage_seconds"):
        if not non_negative_number(report.get(field)):
            errors.append(f"{field}_invalid")
    errors.extend(shadow_run_evidence_source_errors(report))

    comparison = report.get("comparison")
    if not isinstance(comparison, dict):
        errors.append("comparison_missing")
    else:
        if comparison.get("passed") is not True:
            errors.append("comparison_not_passed")
        if not isinstance(comparison.get("failed_symbols"), list):
            errors.append("comparison_failed_symbols_invalid")
        else:
            failed_symbols = comparison["failed_symbols"]
            if any(not isinstance(symbol, str) or not valid_terminal_symbol(symbol) for symbol in failed_symbols):
                errors.append("comparison_failed_symbols_format_invalid")
            if len(failed_symbols) != len(set(failed_symbols)):
                errors.append("comparison_failed_symbols_duplicate")
        if not non_negative_integer(comparison.get("missing_symbol_count")):
            errors.append("comparison_missing_symbol_count_invalid")
        symbols = comparison.get("symbols")
        if not isinstance(symbols, dict) or not symbols:
            errors.append("comparison_symbols_missing")
        else:
            legacy_symbol_count_total = 0
            v2_symbol_count_total = 0
            failed_symbol_set = set()
            missing_symbol_count = 0
            for symbol, symbol_result in symbols.items():
                if not isinstance(symbol, str) or not valid_terminal_symbol(symbol):
                    errors.append(f"comparison_symbol_format_invalid:{symbol}")
                if not isinstance(symbol_result, dict):
                    errors.append(f"comparison_symbol_invalid:{symbol}")
                    continue
                for field in (
                    "legacy_count",
                    "v2_count",
                    "count_delta_ratio",
                    "duplicate_ratio",
                    "out_of_order_ratio",
                    "max_latency_delta_ms",
                    "max_stale_gap_seconds",
                    "legacy_source_coverage_seconds",
                    "v2_source_coverage_seconds",
                    "missing",
                    "passed",
                ):
                    if field not in symbol_result:
                        errors.append(f"comparison_symbol_field_missing:{symbol}:{field}")
                if not non_negative_integer(symbol_result.get("legacy_count")):
                    errors.append(f"comparison_symbol_legacy_count_invalid:{symbol}")
                else:
                    legacy_symbol_count_total += int(symbol_result["legacy_count"])
                if not non_negative_integer(symbol_result.get("v2_count")):
                    errors.append(f"comparison_symbol_v2_count_invalid:{symbol}")
                else:
                    v2_symbol_count_total += int(symbol_result["v2_count"])
                if "missing" in symbol_result and not isinstance(symbol_result.get("missing"), bool):
                    errors.append(f"comparison_symbol_missing_invalid:{symbol}")
                for field in ("count_delta_ratio", "duplicate_ratio", "out_of_order_ratio"):
                    if not bounded_ratio(symbol_result.get(field)):
                        errors.append(f"comparison_symbol_{field}_invalid:{symbol}")
                for field in (
                    "max_latency_delta_ms",
                    "max_stale_gap_seconds",
                    "legacy_source_coverage_seconds",
                    "v2_source_coverage_seconds",
                ):
                    if not non_negative_number(symbol_result.get(field)):
                        errors.append(f"comparison_symbol_{field}_invalid:{symbol}")
                if symbol_result.get("missing") is True:
                    missing_symbol_count += 1
                    if symbol_result.get("passed") is True:
                        errors.append(f"comparison_symbol_missing_marked_passed:{symbol}")
                if symbol_result.get("passed") is not True:
                    failed_symbol_set.add(str(symbol))
            if non_negative_integer(report.get("legacy_event_count")) and legacy_symbol_count_total != report.get("legacy_event_count"):
                errors.append("legacy_event_count_mismatch")
            if non_negative_integer(report.get("v2_event_count")) and v2_symbol_count_total != report.get("v2_event_count"):
                errors.append("v2_event_count_mismatch")
            if isinstance(comparison.get("failed_symbols"), list) and sorted(map(str, comparison["failed_symbols"])) != sorted(failed_symbol_set):
                errors.append("comparison_failed_symbols_mismatch")
            if non_negative_integer(comparison.get("missing_symbol_count")) and comparison.get("missing_symbol_count") != missing_symbol_count:
                errors.append("comparison_missing_symbol_count_mismatch")
        thresholds = comparison.get("thresholds")
        if not isinstance(thresholds, dict):
            errors.append("comparison_thresholds_missing")
        else:
            for field in ("max_event_count_delta_ratio", "max_duplicate_ratio", "max_out_of_order_ratio"):
                if not bounded_ratio(thresholds.get(field)):
                    errors.append(f"comparison_threshold_invalid:{field}")
            if not non_negative_integer(thresholds.get("max_missing_symbol_count")):
                errors.append("comparison_threshold_invalid:max_missing_symbol_count")
            for field in ("max_latency_delta_ms", "max_stale_gap_seconds"):
                if not non_negative_number(thresholds.get(field)):
                    errors.append(f"comparison_threshold_invalid:{field}")

    performance = report.get("performance")
    if not isinstance(performance, dict):
        errors.append("performance_missing")
    else:
        if performance.get("passed") is not True:
            errors.append("performance_not_passed")
        if not isinstance(performance.get("missing_sample_keys"), list):
            errors.append("performance_missing_sample_keys_invalid")
        if not isinstance(performance.get("insufficient_sample_keys"), list):
            errors.append("performance_insufficient_sample_keys_invalid")
        if not non_negative_integer(performance.get("min_samples_per_key")) or int(performance.get("min_samples_per_key") or 0) <= 0:
            errors.append("performance_min_samples_per_key_invalid")
        sample_counts = performance.get("sample_counts")
        metrics = performance.get("metrics")
        required_metrics = (
            "collector_to_kafka",
            "processed_to_gateway",
            "gateway_to_frontend",
            "subscribe_snapshot",
            "frontend_store_update",
        )
        required_sample_keys = (
            "collector_to_kafka_ms",
            "processed_to_gateway_ms",
            "gateway_to_frontend_ms",
            "subscribe_snapshot_ms",
            "frontend_store_update_ms",
        )
        if not isinstance(sample_counts, dict):
            errors.append("performance_sample_counts_missing")
        else:
            for sample_key in required_sample_keys:
                if not non_negative_integer(sample_counts.get(sample_key)):
                    errors.append(f"performance_sample_count_missing:{sample_key}")
        missing_sample_keys = performance.get("missing_sample_keys")
        insufficient_sample_keys = performance.get("insufficient_sample_keys")
        min_samples_per_key = performance.get("min_samples_per_key")
        if (
            isinstance(sample_counts, dict)
            and isinstance(missing_sample_keys, list)
            and isinstance(insufficient_sample_keys, list)
            and non_negative_integer(min_samples_per_key)
            and int(min_samples_per_key) > 0
        ):
            expected_missing_sample_keys = [
                key for key in required_sample_keys if non_negative_integer(sample_counts.get(key)) and int(sample_counts.get(key)) == 0
            ]
            expected_insufficient_sample_keys = [
                key
                for key in required_sample_keys
                if non_negative_integer(sample_counts.get(key))
                and int(sample_counts.get(key)) > 0
                and int(sample_counts.get(key)) < int(min_samples_per_key)
            ]
            if sorted(map(str, missing_sample_keys)) != sorted(expected_missing_sample_keys):
                errors.append("performance_missing_sample_keys_mismatch")
            if sorted(map(str, insufficient_sample_keys)) != sorted(expected_insufficient_sample_keys):
                errors.append("performance_insufficient_sample_keys_mismatch")
        if not isinstance(metrics, dict):
            errors.append("performance_metrics_missing")
        else:
            for metric in required_metrics:
                metric_value = metrics.get(metric)
                if not isinstance(metric_value, dict):
                    errors.append(f"performance_metric_missing:{metric}")
                    continue
                if not non_negative_number(metric_value.get("p95_ms")):
                    errors.append(f"performance_metric_p95_missing:{metric}")
                if not non_negative_number(metric_value.get("p95_target_ms")):
                    errors.append(f"performance_metric_p95_target_missing:{metric}")
            collector_metric = metrics.get("collector_to_kafka") if isinstance(metrics, dict) else {}
            if isinstance(collector_metric, dict):
                if not non_negative_number(collector_metric.get("p99_ms")):
                    errors.append("performance_metric_p99_missing:collector_to_kafka")
                if not non_negative_number(collector_metric.get("p99_target_ms")):
                    errors.append("performance_metric_p99_target_missing:collector_to_kafka")
            if performance.get("passed") is True and performance_metrics_have_failures(metrics):
                errors.append("performance_passed_with_metric_failures")
    return errors


def shadow_run_evidence_source_errors(report: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    source = report.get("evidence_source")
    if not isinstance(source, dict):
        return ["evidence_source_missing"]
    if source.get("schema_version") != 1:
        errors.append("evidence_source_schema_version_invalid")
    if source.get("kind") != "file_backed_shadow_run":
        errors.append("evidence_source_kind_invalid")
    files = source.get("files")
    if not isinstance(files, dict):
        errors.append("evidence_source_files_missing")
    else:
        for field in ("metadata", "legacy_events", "v2_events", "performance_samples"):
            if not isinstance(files.get(field), str) or not files.get(field).strip():
                errors.append(f"evidence_source_file_missing:{field}")
    if source.get("legacy_event_count") != report.get("legacy_event_count"):
        errors.append("evidence_source_legacy_event_count_mismatch")
    if source.get("v2_event_count") != report.get("v2_event_count"):
        errors.append("evidence_source_v2_event_count_mismatch")
    source_sample_counts = source.get("performance_sample_counts")
    report_sample_counts = (report.get("performance") or {}).get("sample_counts")
    if not isinstance(source_sample_counts, dict):
        errors.append("evidence_source_performance_sample_counts_missing")
    elif isinstance(report_sample_counts, dict):
        normalized_source_counts = {
            str(key): value for key, value in source_sample_counts.items() if non_negative_integer(value)
        }
        normalized_report_counts = {
            str(key): value for key, value in report_sample_counts.items() if non_negative_integer(value)
        }
        if normalized_source_counts != normalized_report_counts:
            errors.append("evidence_source_performance_sample_counts_mismatch")
        if len(normalized_source_counts) != len(source_sample_counts):
            errors.append("evidence_source_performance_sample_counts_invalid")
    return errors


def shadow_run_source_file_audits(
    *,
    reports: list[dict[str, Any]],
    base_directory: str | Path,
) -> list[dict[str, Any]]:
    audits: list[dict[str, Any]] = []
    for report in reports:
        report_id = str(report.get("session_id") or "")
        source = report.get("evidence_source")
        if not isinstance(source, dict):
            audits.append({"session_id": report_id, "errors": ["evidence_source_missing"]})
            continue
        files = source.get("files")
        if not isinstance(files, dict):
            audits.append({"session_id": report_id, "errors": ["evidence_source_files_missing"]})
            continue
        errors: list[str] = []
        resolved_files: dict[str, str] = {}
        for field in ("metadata", "legacy_events", "v2_events", "performance_samples"):
            path = resolve_shadow_source_path(files.get(field), base_directory=base_directory)
            resolved_files[field] = str(path) if path is not None else ""
            if path is None:
                errors.append(f"evidence_source_file_missing:{field}")
            elif not path.exists():
                errors.append(f"evidence_source_file_not_found:{field}")

        loaded_shadow_source: dict[str, Any] | None = None
        all_source_files_exist = all(resolved_files.get(field) and Path(resolved_files[field]).exists() for field in (
            "metadata",
            "legacy_events",
            "v2_events",
            "performance_samples",
        ))
        if all_source_files_exist:
            try:
                loaded_shadow_source = load_shadow_run_files(
                    ShadowRunFiles(
                        metadata_path=Path(resolved_files["metadata"]),
                        legacy_events_path=Path(resolved_files["legacy_events"]),
                        v2_events_path=Path(resolved_files["v2_events"]),
                        performance_samples_path=Path(resolved_files["performance_samples"]),
                    )
                )
            except (json.JSONDecodeError, ValueError):
                errors.append("evidence_source_files_parse_invalid")
            else:
                metadata = loaded_shadow_source["metadata"]
                if metadata.get("session_id") != report.get("session_id"):
                    errors.append("evidence_source_metadata_session_id_mismatch")
                if metadata.get("trading_date") != report.get("trading_date"):
                    errors.append("evidence_source_metadata_trading_date_mismatch")

        observed_legacy_count = len(loaded_shadow_source["legacy_events"]) if loaded_shadow_source is not None else None
        observed_v2_count = len(loaded_shadow_source["v2_events"]) if loaded_shadow_source is not None else None
        observed_sample_counts = (
            {key: len(values) for key, values in loaded_shadow_source["performance_samples"].items()}
            if loaded_shadow_source is not None
            else None
        )
        if observed_legacy_count is not None and observed_legacy_count != report.get("legacy_event_count"):
            errors.append("evidence_source_legacy_event_file_count_mismatch")
        if observed_v2_count is not None and observed_v2_count != report.get("v2_event_count"):
            errors.append("evidence_source_v2_event_file_count_mismatch")
        source_sample_counts = source.get("performance_sample_counts")
        report_sample_counts = (report.get("performance") or {}).get("sample_counts")
        if observed_sample_counts is not None:
            if isinstance(source_sample_counts, dict) and observed_sample_counts != source_sample_counts:
                errors.append("evidence_source_performance_file_count_mismatch")
            if isinstance(report_sample_counts, dict) and observed_sample_counts != report_sample_counts:
                errors.append("evidence_source_performance_report_count_mismatch")

        audits.append(
            {
                "session_id": report_id,
                "files": resolved_files,
                "observed_legacy_event_count": observed_legacy_count,
                "observed_v2_event_count": observed_v2_count,
                "observed_performance_sample_counts": observed_sample_counts,
                "errors": errors,
            }
        )
    return audits


def resolve_shadow_source_path(value: Any, *, base_directory: str | Path) -> Path | None:
    if not isinstance(value, str) or not value.strip():
        return None
    path = Path(value)
    if path.is_absolute():
        return path
    return Path(base_directory) / path


def shadow_run_report_timing_errors(report: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    started_at = report.get("started_at")
    finished_at = report.get("finished_at")
    duration_seconds = report.get("duration_seconds")

    if not isinstance(started_at, str) or not started_at:
        errors.append("started_at_missing")
    elif not is_iso_datetime(started_at):
        errors.append("started_at_invalid")

    if not isinstance(finished_at, str) or not finished_at:
        errors.append("finished_at_missing")
    elif not is_iso_datetime(finished_at):
        errors.append("finished_at_invalid")

    if not positive_number(duration_seconds):
        errors.append("duration_seconds_invalid")

    window_seconds = shadow_run_window_seconds(started_at, finished_at)
    if window_seconds is not None:
        if window_seconds < 0:
            errors.append("finished_at_before_started_at")
        elif positive_number(duration_seconds) and abs(float(duration_seconds) - window_seconds) > 0.001:
            errors.append("duration_seconds_mismatch")

    return errors


def performance_metrics_have_failures(metrics: dict[str, Any]) -> bool:
    for metric_name, metric_value in metrics.items():
        if not isinstance(metric_value, dict):
            continue
        p95 = metric_value.get("p95_ms")
        p95_target = metric_value.get("p95_target_ms")
        if non_negative_number(p95) and non_negative_number(p95_target) and float(p95) > float(p95_target):
            return True
        if metric_name == "collector_to_kafka":
            p99 = metric_value.get("p99_ms")
            p99_target = metric_value.get("p99_target_ms")
            if non_negative_number(p99) and non_negative_number(p99_target) and float(p99) > float(p99_target):
                return True
    return False


def shadow_run_window_seconds(started_at: Any, finished_at: Any) -> float | None:
    if not isinstance(started_at, str) or not isinstance(finished_at, str):
        return None
    started = parse_iso_datetime(started_at)
    finished = parse_iso_datetime(finished_at)
    if started is None or finished is None:
        return None
    try:
        return (finished - started).total_seconds()
    except TypeError:
        return None


def validate_historical_manifests(manifests: list[dict[str, Any]]) -> list[dict[str, Any]]:
    invalid = []
    for index, manifest in enumerate(manifests):
        data_type = str(manifest.get("data_type") or "")
        errors = historical_manifest_errors(manifest)
        if errors:
            invalid.append(
                {
                    "index": index,
                    "data_type": data_type,
                    "errors": errors,
                }
            )
    return invalid


def historical_manifest_errors(manifest: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    data_type = manifest.get("data_type")
    if manifest.get("schema_version") != 1:
        errors.append("schema_version_invalid")
    if data_type not in REQUIRED_HISTORICAL_MANIFEST_TYPES:
        errors.append("data_type_invalid")
    else:
        expected_source_data_type = source_data_type_for_manifest(str(data_type))
        if manifest.get("source_data_type") != expected_source_data_type:
            errors.append("source_data_type_mismatch")
    if not isinstance(manifest.get("table"), str) or not manifest.get("table"):
        errors.append("table_missing")

    date_range = manifest.get("date_range")
    if not isinstance(date_range, dict):
        errors.append("date_range_missing")
    else:
        start = str(date_range.get("start") or "")
        end = str(date_range.get("end") or "")
        if not is_yyyymmdd(start) or not is_yyyymmdd(end) or start > end:
            errors.append("date_range_invalid")

    if not isinstance(manifest.get("symbol_count"), int) or manifest.get("symbol_count") < 0:
        errors.append("symbol_count_invalid")
    symbols = manifest.get("symbols")
    if not isinstance(symbols, list) or any(not isinstance(symbol, str) or not symbol for symbol in symbols):
        errors.append("symbols_invalid")
    elif any(not valid_terminal_symbol(symbol) for symbol in symbols):
        errors.append("symbols_format_invalid")
    elif len(symbols) != len(set(symbols)):
        errors.append("symbols_duplicate")
    elif isinstance(manifest.get("symbol_count"), int) and manifest.get("symbol_count") != len(set(symbols)):
        errors.append("symbol_count_mismatch")
    row_count = manifest.get("row_count")
    if not isinstance(row_count, int) or isinstance(row_count, bool) or row_count < 0:
        errors.append("row_count_invalid")
    failed_items = manifest.get("failed_items")
    if not isinstance(failed_items, list):
        errors.append("failed_items_invalid")
    if not isinstance(manifest.get("code_version"), str) or not manifest.get("code_version"):
        errors.append("code_version_missing")
    timestamp_values: dict[str, str] = {}
    for field in ("started_at", "finished_at"):
        value = manifest.get(field)
        if not isinstance(value, str) or not value:
            errors.append(f"{field}_missing")
        elif not is_iso_datetime(value):
            errors.append(f"{field}_invalid")
        else:
            timestamp_values[field] = value
    if set(timestamp_values) == {"started_at", "finished_at"} and iso_datetime_is_before(
        timestamp_values["finished_at"],
        timestamp_values["started_at"],
    ):
        errors.append("finished_at_before_started_at")

    quality_checks = manifest.get("quality_checks")
    if not isinstance(quality_checks, dict):
        errors.append("quality_checks_missing")
    else:
        required_quality_fields = {
            "missing_required_columns",
            "duplicate_primary_keys",
            "invalid_symbol_rows",
            "invalid_date_rows",
            "negative_value_count",
            "empty_output",
            "passed",
            "failed_items",
        }
        if required_quality_fields - set(quality_checks):
            errors.append("quality_checks_required_fields_missing")
        if not isinstance(quality_checks.get("passed"), bool):
            errors.append("quality_checks_passed_invalid")
        quality_failed_items = quality_checks.get("failed_items")
        if not isinstance(quality_failed_items, list):
            errors.append("quality_checks_failed_items_invalid")
        elif any(not isinstance(item, str) or not item for item in quality_failed_items):
            errors.append("quality_checks_failed_items_invalid")
        elif isinstance(failed_items, list):
            if quality_failed_items != failed_items:
                errors.append("quality_checks_failed_items_mismatch")
            if quality_checks.get("passed") is True and failed_items:
                errors.append("failed_items_present_when_quality_passed")
            if quality_checks.get("passed") is False and not failed_items:
                errors.append("failed_items_missing_when_quality_failed")
        if isinstance(failed_items, list) and any(not isinstance(item, str) or not item for item in failed_items):
            errors.append("failed_items_invalid")
        for field in (
            "missing_required_columns",
            "duplicate_primary_keys",
            "invalid_symbol_rows",
            "invalid_date_rows",
        ):
            if field in quality_checks and not isinstance(quality_checks.get(field), list):
                errors.append(f"quality_checks_{field}_invalid")
        if "negative_value_count" in quality_checks and (
            not isinstance(quality_checks.get("negative_value_count"), int)
            or isinstance(quality_checks.get("negative_value_count"), bool)
            or quality_checks.get("negative_value_count") < 0
        ):
            errors.append("quality_checks_negative_value_count_invalid")
        if "empty_output" in quality_checks and not isinstance(quality_checks.get("empty_output"), bool):
            errors.append("quality_checks_empty_output_invalid")
        if non_negative_integer(row_count) and isinstance(quality_checks.get("empty_output"), bool):
            if row_count == 0 and quality_checks.get("empty_output") is not True:
                errors.append("row_count_empty_output_mismatch")
            if row_count > 0 and quality_checks.get("empty_output") is True:
                errors.append("row_count_empty_output_mismatch")
            if row_count == 0 and quality_checks.get("passed") is True:
                errors.append("row_count_empty_when_quality_passed")
        quality_failure_evidence = {
            "missing_required_columns": "missing_required_columns",
            "duplicate_primary_keys": "duplicate_primary_keys",
            "invalid_symbol_rows": "invalid_symbol_format",
            "invalid_date_rows": "invalid_date_format",
        }
        if isinstance(quality_failed_items, list):
            quality_failed_item_set = set(quality_failed_items)
            for field, failed_item in quality_failure_evidence.items():
                value = quality_checks.get(field)
                if isinstance(value, list) and value and failed_item not in quality_failed_item_set:
                    errors.append(f"quality_checks_{field}_failed_item_missing")
            negative_value_count = quality_checks.get("negative_value_count")
            if non_negative_integer(negative_value_count) and int(negative_value_count) > 0 and "negative_values" not in quality_failed_item_set:
                errors.append("quality_checks_negative_values_failed_item_missing")
            if quality_checks.get("empty_output") is True and "empty_output" not in quality_failed_item_set:
                errors.append("quality_checks_empty_output_failed_item_missing")
    return errors


def is_yyyymmdd(value: str) -> bool:
    return len(value) == 8 and value.isdigit()


def is_iso_datetime(value: str) -> bool:
    return parse_iso_datetime(value) is not None


def parse_iso_datetime(value: str) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    normalized = value.strip()
    if "T" not in normalized:
        return None
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def iso_datetime_is_before(left: str, right: str) -> bool:
    left_dt = parse_iso_datetime(left)
    right_dt = parse_iso_datetime(right)
    if left_dt is None or right_dt is None:
        return False
    try:
        return left_dt < right_dt
    except TypeError:
        return False


def positive_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0


def non_negative_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 0


def bounded_ratio(value: Any) -> bool:
    return non_negative_number(value) and 0 <= float(value) <= 1


def non_negative_integer(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def non_negative_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 0


def non_negative_int_value(value: Any) -> int:
    return value if non_negative_integer(value) else 0


def worker_processed_count(workers: dict[str, Any], worker: str) -> int:
    snapshot = workers.get(worker)
    if not isinstance(snapshot, dict):
        return 0
    return non_negative_int_value(snapshot.get("processed"))


def topic_count_evidence(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {
            "keys": [],
            "values": {},
            "invalid_topics": ["<observation>"],
        }
    keys = [str(key) for key in value]
    values = {
        str(key): raw_value for key, raw_value in value.items() if non_negative_integer(raw_value)
    }
    invalid_topics = [
        str(key) for key, raw_value in value.items() if not non_negative_integer(raw_value)
    ]
    return {
        "keys": keys,
        "values": values,
        "invalid_topics": invalid_topics,
    }


def symbol_list_evidence(value: Any) -> dict[str, Any]:
    if not isinstance(value, list):
        return {
            "symbols": [],
            "shape_invalid": True,
            "invalid_symbols": [],
            "duplicate_symbols": [],
        }
    symbols = [symbol for symbol in value if isinstance(symbol, str) and symbol]
    invalid_symbols = [str(symbol) for symbol in value if not isinstance(symbol, str) or not valid_terminal_symbol(symbol)]
    duplicate_symbols = sorted({symbol for symbol in symbols if symbols.count(symbol) > 1})
    return {
        "symbols": symbols,
        "shape_invalid": False,
        "invalid_symbols": invalid_symbols,
        "duplicate_symbols": duplicate_symbols,
    }


def redis_snapshot_key_family_evidence(
    redis_snapshot: dict[str, Any],
    subscribed_symbols: list[str],
    generated_at: str = "",
) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    expected_families = list(REDIS_RUNTIME_SNAPSHOT_KEY_FAMILIES)
    required_key_families = redis_snapshot.get("required_key_families")
    if not isinstance(required_key_families, list):
        observed_families: list[str] = []
        invalid_required_families = ["<required_key_families>"]
        duplicate_required_families: list[str] = []
    else:
        observed_families = [family for family in required_key_families if isinstance(family, str) and family]
        invalid_required_families = [
            str(family)
            for family in required_key_families
            if not isinstance(family, str) or family not in expected_families
        ]
        duplicate_required_families = sorted({family for family in observed_families if observed_families.count(family) > 1})
    missing_required_families = [family for family in expected_families if family not in observed_families]
    unexpected_required_families = [family for family in observed_families if family not in expected_families]
    if invalid_required_families or duplicate_required_families or missing_required_families or unexpected_required_families:
        blockers.append("runtime_health_redis_snapshot_required_key_families_invalid")

    coverage = redis_snapshot.get("key_family_coverage")
    family_evidence: dict[str, Any] = {}
    if not isinstance(coverage, dict):
        blockers.append("runtime_health_redis_snapshot_key_family_coverage_missing")
        coverage = {}
    missing_coverage_families = [family for family in expected_families if family not in coverage]
    unexpected_coverage_families = [str(family) for family in coverage if family not in expected_families]
    invalid_coverage_families = [
        family for family in expected_families if family in coverage and not isinstance(coverage.get(family), dict)
    ]
    if missing_coverage_families:
        blockers.append("runtime_health_redis_snapshot_key_family_coverage_missing")
    if unexpected_coverage_families or invalid_coverage_families:
        blockers.append("runtime_health_redis_snapshot_key_family_coverage_invalid")

    for family in expected_families:
        entry = coverage.get(family)
        if not isinstance(entry, dict):
            family_evidence[family] = {
                "checked_symbols": [],
                "present_symbols": [],
                "missing_symbols": [],
                "missing_checked_subscribed_symbols": list(subscribed_symbols),
                "missing_present_subscribed_symbols": list(subscribed_symbols),
            }
            continue
        checked_symbol_evidence = symbol_list_evidence(entry.get("checked_symbols"))
        present_symbol_evidence = symbol_list_evidence(entry.get("present_symbols"))
        missing_symbol_evidence = symbol_list_evidence(entry.get("missing_symbols"))
        checked_symbols = checked_symbol_evidence["symbols"]
        present_symbols = present_symbol_evidence["symbols"]
        missing_symbols = missing_symbol_evidence["symbols"]
        invalid_symbol_fields = [
            field
            for field, value in (
                ("checked_symbols", checked_symbol_evidence),
                ("present_symbols", present_symbol_evidence),
                ("missing_symbols", missing_symbol_evidence),
            )
            if value["shape_invalid"] or value["invalid_symbols"]
        ]
        duplicate_symbol_fields = [
            field
            for field, value in (
                ("checked_symbols", checked_symbol_evidence),
                ("present_symbols", present_symbol_evidence),
                ("missing_symbols", missing_symbol_evidence),
            )
            if value["duplicate_symbols"]
        ]
        present_missing_overlap = sorted(set(present_symbols) & set(missing_symbols))
        unresolved_checked_symbols = sorted(set(checked_symbols) - set(present_symbols) - set(missing_symbols))
        unchecked_result_symbols = sorted((set(present_symbols) | set(missing_symbols)) - set(checked_symbols))
        missing_checked_subscribed_symbols = [symbol for symbol in subscribed_symbols if symbol not in checked_symbols]
        missing_present_subscribed_symbols = [symbol for symbol in subscribed_symbols if symbol not in present_symbols]
        raw_updated_at_by_symbol = entry.get("updated_at_by_symbol")
        raw_missing_updated_at_symbols = entry.get("missing_updated_at_symbols")
        updated_at_by_symbol = raw_updated_at_by_symbol if isinstance(raw_updated_at_by_symbol, dict) else {}
        missing_updated_at_evidence = symbol_list_evidence(raw_missing_updated_at_symbols)
        missing_updated_at_symbols = missing_updated_at_evidence["symbols"]
        invalid_updated_at_symbols = [
            str(symbol)
            for symbol, updated_at in updated_at_by_symbol.items()
            if not isinstance(symbol, str)
            or not valid_terminal_symbol(symbol)
            or not isinstance(updated_at, str)
            or not is_iso_datetime(updated_at)
        ]
        future_updated_at_symbols = [
            str(symbol)
            for symbol, updated_at in updated_at_by_symbol.items()
            if (
                isinstance(symbol, str)
                and valid_terminal_symbol(symbol)
                and isinstance(updated_at, str)
                and is_iso_datetime(updated_at)
                and is_iso_datetime(generated_at)
                and iso_datetime_is_before(generated_at, updated_at)
            )
        ]
        missing_updated_at_subscribed_symbols = [
            symbol for symbol in subscribed_symbols if symbol not in updated_at_by_symbol or symbol in missing_updated_at_symbols
        ]
        raw_ttl_seconds_by_symbol = entry.get("ttl_seconds_by_symbol")
        raw_missing_ttl_symbols = entry.get("missing_ttl_symbols")
        ttl_seconds_by_symbol = raw_ttl_seconds_by_symbol if isinstance(raw_ttl_seconds_by_symbol, dict) else {}
        missing_ttl_evidence = symbol_list_evidence(raw_missing_ttl_symbols)
        missing_ttl_symbols = missing_ttl_evidence["symbols"]
        raw_contract_missing_by_symbol = entry.get("contract_missing_by_symbol")
        contract_missing_by_symbol = (
            raw_contract_missing_by_symbol if isinstance(raw_contract_missing_by_symbol, dict) else {}
        )
        invalid_contract_missing_symbols = [
            str(symbol)
            for symbol, fields in contract_missing_by_symbol.items()
            if not isinstance(symbol, str)
            or not valid_terminal_symbol(symbol)
            or not isinstance(fields, list)
            or any(not isinstance(field, str) or not field.strip() for field in fields)
        ]
        contract_missing_symbols = [
            symbol
            for symbol, fields in contract_missing_by_symbol.items()
            if isinstance(symbol, str)
            and valid_terminal_symbol(symbol)
            and isinstance(fields, list)
            and any(isinstance(field, str) and field.strip() for field in fields)
        ]
        invalid_ttl_symbols = [
            str(symbol)
            for symbol, ttl_seconds in ttl_seconds_by_symbol.items()
            if not isinstance(symbol, str)
            or not valid_terminal_symbol(symbol)
            or not non_negative_integer(ttl_seconds)
            or int(ttl_seconds) <= 0
        ]
        missing_ttl_subscribed_symbols = [
            symbol for symbol in subscribed_symbols if symbol not in ttl_seconds_by_symbol or symbol in missing_ttl_symbols
        ]
        history_participants_invalid: list[str] = []
        history_participants_missing: list[str] = []
        history_missing_keys: dict[str, Any] = {}
        if family == "ccass_history":
            participants_by_symbol = entry.get("participants_by_symbol")
            raw_missing_keys = entry.get("missing_keys")
            if not isinstance(participants_by_symbol, dict):
                history_participants_invalid.append("<participants_by_symbol>")
                participants_by_symbol = {}
            if not isinstance(raw_missing_keys, dict):
                history_participants_invalid.append("<missing_keys>")
                raw_missing_keys = {}
            for symbol in subscribed_symbols:
                participant_ids = participants_by_symbol.get(symbol)
                if (
                    not isinstance(participant_ids, list)
                    or not participant_ids
                    or any(not isinstance(participant_id, str) or not participant_id.strip() for participant_id in participant_ids)
                ):
                    history_participants_missing.append(symbol)
            history_missing_keys = {
                str(symbol): keys
                for symbol, keys in raw_missing_keys.items()
                if isinstance(keys, list) and keys
            }
        family_evidence[family] = {
            "checked_symbols": checked_symbols,
            "present_symbols": present_symbols,
            "missing_symbols": missing_symbols,
            "invalid_symbol_fields": invalid_symbol_fields,
            "duplicate_symbol_fields": duplicate_symbol_fields,
            "present_missing_overlap": present_missing_overlap,
            "unresolved_checked_symbols": unresolved_checked_symbols,
            "unchecked_result_symbols": unchecked_result_symbols,
            "missing_checked_subscribed_symbols": missing_checked_subscribed_symbols,
            "missing_present_subscribed_symbols": missing_present_subscribed_symbols,
            "updated_at_by_symbol": dict(updated_at_by_symbol),
            "missing_updated_at_symbols": missing_updated_at_symbols,
            "missing_updated_at_subscribed_symbols": missing_updated_at_subscribed_symbols,
            "invalid_updated_at_symbols": invalid_updated_at_symbols,
            "future_updated_at_symbols": future_updated_at_symbols,
            "ttl_seconds_by_symbol": dict(ttl_seconds_by_symbol),
            "missing_ttl_symbols": missing_ttl_symbols,
            "missing_ttl_subscribed_symbols": missing_ttl_subscribed_symbols,
            "invalid_ttl_symbols": invalid_ttl_symbols,
            "contract_missing_by_symbol": dict(contract_missing_by_symbol),
            "invalid_contract_missing_symbols": invalid_contract_missing_symbols,
            "contract_missing_symbols": contract_missing_symbols,
        }
        if family == "ccass_history":
            family_evidence[family]["history_participants_invalid"] = history_participants_invalid
            family_evidence[family]["history_participants_missing"] = history_participants_missing
            family_evidence[family]["history_missing_keys"] = history_missing_keys
            if history_participants_invalid:
                blockers.append("runtime_health_redis_snapshot_history_participants_invalid")
            if history_participants_missing:
                blockers.append("runtime_health_redis_snapshot_history_participants_missing")
            if history_missing_keys:
                blockers.append("runtime_health_redis_snapshot_history_keys_missing")
        if invalid_symbol_fields:
            blockers.append("runtime_health_redis_snapshot_key_family_symbol_list_invalid")
        if duplicate_symbol_fields:
            blockers.append("runtime_health_redis_snapshot_key_family_symbol_list_duplicate")
        if present_missing_overlap:
            blockers.append("runtime_health_redis_snapshot_key_family_symbol_status_conflict")
        if unresolved_checked_symbols:
            blockers.append("runtime_health_redis_snapshot_key_family_checked_symbols_unresolved")
        if unchecked_result_symbols:
            blockers.append("runtime_health_redis_snapshot_key_family_result_symbols_unchecked")
        if not checked_symbols:
            blockers.append("runtime_health_redis_snapshot_key_family_probe_empty")
        if missing_symbols:
            blockers.append("runtime_health_redis_snapshot_key_family_missing_symbols")
        if missing_checked_subscribed_symbols:
            blockers.append("runtime_health_redis_snapshot_key_family_probe_missing_subscribed_symbols")
        if missing_present_subscribed_symbols:
            blockers.append("runtime_health_redis_snapshot_key_family_subscribed_symbols_not_present")
        if not isinstance(raw_updated_at_by_symbol, dict) or missing_updated_at_evidence["shape_invalid"]:
            blockers.append("runtime_health_redis_snapshot_key_family_updated_at_missing")
        if missing_updated_at_evidence["invalid_symbols"]:
            blockers.append("runtime_health_redis_snapshot_key_family_updated_at_symbol_invalid")
        if missing_updated_at_evidence["duplicate_symbols"]:
            blockers.append("runtime_health_redis_snapshot_key_family_updated_at_symbols_duplicate")
        if missing_updated_at_subscribed_symbols:
            blockers.append("runtime_health_redis_snapshot_key_family_updated_at_missing")
        if invalid_updated_at_symbols:
            blockers.append("runtime_health_redis_snapshot_key_family_updated_at_invalid")
        if future_updated_at_symbols:
            blockers.append("runtime_health_redis_snapshot_key_family_updated_at_after_generated_at")
        if not isinstance(raw_ttl_seconds_by_symbol, dict) or missing_ttl_evidence["shape_invalid"]:
            blockers.append("runtime_health_redis_snapshot_key_family_ttl_missing")
        if missing_ttl_evidence["invalid_symbols"]:
            blockers.append("runtime_health_redis_snapshot_key_family_ttl_symbol_invalid")
        if missing_ttl_evidence["duplicate_symbols"]:
            blockers.append("runtime_health_redis_snapshot_key_family_ttl_symbols_duplicate")
        if missing_ttl_subscribed_symbols:
            blockers.append("runtime_health_redis_snapshot_key_family_ttl_missing")
        if invalid_ttl_symbols:
            blockers.append("runtime_health_redis_snapshot_key_family_ttl_invalid")
        if not isinstance(raw_contract_missing_by_symbol, dict):
            blockers.append("runtime_health_redis_snapshot_key_family_contract_evidence_missing")
        if invalid_contract_missing_symbols:
            blockers.append("runtime_health_redis_snapshot_key_family_contract_evidence_invalid")
        if contract_missing_symbols:
            blockers.append("runtime_health_redis_snapshot_key_family_contract_invalid")

    return (
        {
            "required_key_families": {
                "expected": expected_families,
                "observed": observed_families,
                "invalid": invalid_required_families,
                "duplicates": duplicate_required_families,
                "missing": missing_required_families,
                "unexpected": unexpected_required_families,
            },
            "key_family_coverage": {
                "missing_families": missing_coverage_families,
                "unexpected_families": unexpected_coverage_families,
                "invalid_families": invalid_coverage_families,
                "families": family_evidence,
            },
        },
        sorted(set(blockers)),
    )


def valid_terminal_symbol(symbol: str) -> bool:
    return len(symbol) == 8 and symbol[:5].isdigit() and symbol[5:] == ".HK"


def validate_legacy_decommission_artifact(
    *,
    decommission: dict[str, Any],
    evidence: dict[str, Any],
) -> list[str]:
    blockers: list[str] = []
    if decommission.get("schema_version") != 1:
        blockers.append("legacy_decommission_schema_invalid")
    observation = decommission.get("observation")
    evidence["legacy_decommission_observation_present"] = isinstance(observation, dict)
    if not isinstance(observation, dict):
        blockers.append("legacy_decommission_observation_missing")
        return blockers

    expected_old_topics = [str(topic) for topic in (observation.get("expected_old_topics") or [])]
    missing_expected_topics = [
        topic for topic in DEFAULT_LEGACY_TOPIC_NAMES if topic not in expected_old_topics
    ]
    old_topic_consumers = observation.get("old_topic_consumers") or {}
    old_topic_lag = observation.get("old_topic_lag") or {}
    consumer_counts = topic_count_evidence(old_topic_consumers)
    lag_counts = topic_count_evidence(old_topic_lag)
    observed_at = str(observation.get("observed_at") or "")
    legacy_websocket_enabled = observation.get("legacy_websocket_enabled")
    evidence["legacy_decommission_expected_old_topics"] = expected_old_topics
    evidence["legacy_decommission_missing_default_old_topics"] = missing_expected_topics
    evidence["legacy_decommission_observed_at"] = observed_at
    evidence["legacy_decommission_legacy_websocket_enabled"] = legacy_websocket_enabled
    if decommission.get("legacy_websocket_disabled") is not True:
        blockers.append("legacy_decommission_websocket_flag_not_disabled")
    if decommission.get("old_topic_consumers_disabled") is not True:
        blockers.append("legacy_decommission_consumers_flag_not_disabled")
    if decommission.get("no_legacy_consumers_observed") is not True:
        blockers.append("legacy_decommission_no_consumers_flag_not_true")
    if not observed_at:
        blockers.append("legacy_decommission_observed_at_missing")
    elif not is_iso_datetime(observed_at):
        blockers.append("legacy_decommission_observed_at_invalid")
    if legacy_websocket_enabled is not False:
        blockers.append("legacy_decommission_websocket_observed_enabled")
    if missing_expected_topics:
        blockers.append("legacy_decommission_default_topic_coverage_incomplete")
    invalid_default_consumer_topics = [
        topic for topic in DEFAULT_LEGACY_TOPIC_NAMES if topic in consumer_counts["invalid_topics"]
    ]
    invalid_default_lag_topics = [
        topic for topic in DEFAULT_LEGACY_TOPIC_NAMES if topic in lag_counts["invalid_topics"]
    ]
    evidence["legacy_decommission_invalid_default_consumer_topics"] = invalid_default_consumer_topics
    evidence["legacy_decommission_invalid_default_lag_topics"] = invalid_default_lag_topics
    if invalid_default_consumer_topics:
        blockers.append("legacy_decommission_default_topic_consumers_invalid")
    if invalid_default_lag_topics:
        blockers.append("legacy_decommission_default_topic_lag_invalid")
    if any(consumer_counts["values"].get(topic, -1) != 0 for topic in DEFAULT_LEGACY_TOPIC_NAMES):
        blockers.append("legacy_decommission_default_topic_consumers_not_zero")
    if any(lag_counts["values"].get(topic, -1) != 0 for topic in DEFAULT_LEGACY_TOPIC_NAMES):
        blockers.append("legacy_decommission_default_topic_lag_not_zero")
    return blockers


def validate_legacy_retirement_artifact(
    *,
    retirement: dict[str, Any],
    readiness: dict[str, Any] | None,
    frontend: dict[str, Any] | None,
    decommission: dict[str, Any] | None,
    evidence: dict[str, Any],
) -> list[str]:
    blockers: list[str] = []

    if retirement.get("schema_version") != 1:
        blockers.append("legacy_retirement_schema_invalid")
    if retirement.get("passed") is not True:
        blockers.append("legacy_retirement_artifact_not_passed")

    retirement_readiness = retirement.get("cutover_readiness")
    evidence["legacy_retirement_readiness_present"] = isinstance(retirement_readiness, dict)
    if not isinstance(retirement_readiness, dict):
        blockers.append("legacy_retirement_readiness_missing")
    elif readiness is not None:
        expected_readiness = {
            "frontend_default_v2_allowed": readiness.get("frontend_default_v2_allowed") is True,
            "legacy_retirement_allowed": readiness.get("legacy_retirement_allowed") is True,
            "accepted_report_ids": [str(report_id) for report_id in (readiness.get("accepted_report_ids") or [])],
        }
        observed_readiness = {
            "frontend_default_v2_allowed": retirement_readiness.get("frontend_default_v2_allowed") is True,
            "legacy_retirement_allowed": retirement_readiness.get("legacy_retirement_allowed") is True,
            "accepted_report_ids": [
                str(report_id) for report_id in (retirement_readiness.get("accepted_report_ids") or [])
            ],
        }
        if observed_readiness != expected_readiness:
            blockers.append("legacy_retirement_readiness_mismatch")

    retirement_evidence = retirement.get("evidence")
    evidence["legacy_retirement_execution_evidence_present"] = isinstance(retirement_evidence, dict)
    if not isinstance(retirement_evidence, dict):
        blockers.append("legacy_retirement_execution_evidence_missing")
        return blockers

    if frontend is not None and (
        retirement_evidence.get("frontend_default_v2_deployed") is True
    ) != (frontend.get("frontend_default_v2_deployed") is True):
        blockers.append("legacy_retirement_frontend_mismatch")

    if decommission is not None:
        expected_decommission_evidence = {
            "legacy_websocket_disabled": decommission.get("legacy_websocket_disabled") is True,
            "old_topic_consumers_disabled": decommission.get("old_topic_consumers_disabled") is True,
            "no_legacy_consumers_observed": decommission.get("no_legacy_consumers_observed") is True,
        }
        observed_decommission_evidence = {
            field: retirement_evidence.get(field) is True for field in expected_decommission_evidence
        }
        if observed_decommission_evidence != expected_decommission_evidence:
            blockers.append("legacy_retirement_decommission_mismatch")
        decommission_observed_at = ((decommission.get("observation") or {}).get("observed_at")) if isinstance(decommission.get("observation"), dict) else ""
        evidence["legacy_retirement_decommission_observed_at"] = decommission_observed_at

    if retirement_evidence.get("rollback_window_completed") is not True:
        blockers.append("legacy_retirement_rollback_window_not_completed")
    rollback_window_started_at = str(retirement_evidence.get("rollback_window_started_at") or "")
    rollback_window_completed_at = str(retirement_evidence.get("rollback_window_completed_at") or "")
    operator_approved_at = str(retirement_evidence.get("operator_approved_at") or "")
    evidence["legacy_retirement_rollback_window_started_at"] = rollback_window_started_at
    evidence["legacy_retirement_rollback_window_completed_at"] = rollback_window_completed_at
    evidence["legacy_retirement_operator_approved_at"] = operator_approved_at
    if not rollback_window_started_at:
        blockers.append("legacy_retirement_rollback_window_started_at_missing")
    elif not is_iso_datetime(rollback_window_started_at):
        blockers.append("legacy_retirement_rollback_window_started_at_invalid")
    if not rollback_window_completed_at:
        blockers.append("legacy_retirement_rollback_window_completed_at_missing")
    elif not is_iso_datetime(rollback_window_completed_at):
        blockers.append("legacy_retirement_rollback_window_completed_at_invalid")
    if (
        is_iso_datetime(rollback_window_started_at)
        and is_iso_datetime(rollback_window_completed_at)
        and iso_datetime_is_before(rollback_window_completed_at, rollback_window_started_at)
    ):
        blockers.append("legacy_retirement_rollback_window_completed_before_start")
    if retirement_evidence.get("operator_approved") is not True:
        blockers.append("legacy_retirement_operator_approval_missing")
    if not operator_approved_at:
        blockers.append("legacy_retirement_operator_approved_at_missing")
    elif not is_iso_datetime(operator_approved_at):
        blockers.append("legacy_retirement_operator_approved_at_invalid")
    if (
        is_iso_datetime(rollback_window_completed_at)
        and is_iso_datetime(operator_approved_at)
        and iso_datetime_is_before(operator_approved_at, rollback_window_completed_at)
    ):
        blockers.append("legacy_retirement_operator_approved_before_rollback_window_completed")
    if (
        decommission is not None
        and isinstance(decommission_observed_at, str)
        and is_iso_datetime(decommission_observed_at)
        and is_iso_datetime(operator_approved_at)
        and iso_datetime_is_before(operator_approved_at, decommission_observed_at)
    ):
        blockers.append("legacy_retirement_operator_approved_before_decommission_observed")

    return blockers


def blockers_for_report(report: dict[str, Any], policy: CutoverPolicy) -> list[str]:
    blockers: list[str] = []
    comparison = report.get("comparison") or {}
    performance = report.get("performance") or {}

    timing_errors = shadow_run_report_timing_errors(report)
    blockers.extend(f"shadow_run_{error}" for error in timing_errors)
    if report.get("passed") is not True:
        blockers.append("shadow_run_report_failed")
    if comparison.get("passed") is not True:
        blockers.append("stream_comparison_failed")
    if performance.get("passed") is not True:
        blockers.append("performance_sla_failed")
    if float(report.get("duration_seconds") or 0) < policy.min_session_duration_seconds:
        blockers.append("session_duration_below_minimum")
    duration_seconds = float(report.get("duration_seconds") or 0)
    required_coverage_seconds = duration_seconds * policy.min_stream_coverage_ratio
    if duration_seconds > 0:
        if float(report.get("legacy_source_coverage_seconds") or 0) < required_coverage_seconds:
            blockers.append("legacy_stream_coverage_below_minimum")
        if float(report.get("v2_source_coverage_seconds") or 0) < required_coverage_seconds:
            blockers.append("v2_stream_coverage_below_minimum")
        for symbol, symbol_result in (comparison.get("symbols") or {}).items():
            if float((symbol_result or {}).get("legacy_source_coverage_seconds") or 0) < required_coverage_seconds:
                blockers.append(f"legacy_symbol_coverage_below_minimum:{symbol}")
            if float((symbol_result or {}).get("v2_source_coverage_seconds") or 0) < required_coverage_seconds:
                blockers.append(f"v2_symbol_coverage_below_minimum:{symbol}")
    if policy.require_non_empty_streams:
        if int(report.get("legacy_event_count") or 0) <= 0:
            blockers.append("legacy_stream_empty")
        if int(report.get("v2_event_count") or 0) <= 0:
            blockers.append("v2_stream_empty")
    if policy.require_no_failed_symbols:
        if comparison.get("failed_symbols"):
            blockers.append("failed_symbols_present")
        if int(comparison.get("missing_symbol_count") or 0) > 0:
            blockers.append("missing_symbols_present")

    return blockers


def load_cutover_reports(paths: list[str | Path]) -> list[dict[str, Any]]:
    reports: list[dict[str, Any]] = []
    for path in paths:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        if isinstance(raw, list):
            reports.extend(raw)
        else:
            reports.append(raw)
    return reports


def load_json(path: str | Path) -> dict[str, Any]:
    decoded = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(decoded, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return decoded


def load_required_json(path: str | Path, missing_blocker: str, blockers: list[str]) -> dict[str, Any] | None:
    try:
        return load_json(path)
    except FileNotFoundError:
        blockers.append(missing_blocker)
        return None


def worker_dead_letter_count(worker: Any) -> int:
    if not isinstance(worker, dict):
        return 0
    dead_letters = worker.get("dead_letters") or []
    return len(dead_letters) if isinstance(dead_letters, list) else 0


def degraded_freshness_symbols(collector_health: dict[str, Any]) -> list[str]:
    freshness = collector_health.get("symbol_freshness") or {}
    if not isinstance(freshness, dict):
        return []
    return [
        str(symbol)
        for symbol, value in freshness.items()
        if isinstance(value, dict) and value.get("degraded") is True
    ]


def freshness_coverage_for_subscribed_symbols(
    *,
    collector_health: dict[str, Any],
    subscribed_symbols: list[str],
    generated_at: str = "",
) -> dict[str, list[str]]:
    freshness = collector_health.get("symbol_freshness") or {}
    if not isinstance(freshness, dict):
        freshness = {}
    missing_symbols = []
    unsubscribed_symbols = []
    missing_latest_event_symbols = []
    invalid_latest_event_symbols = []
    future_latest_event_symbols = []
    generated_at_valid = bool(generated_at and is_iso_datetime(generated_at))
    for symbol in subscribed_symbols:
        state = freshness.get(symbol)
        if not isinstance(state, dict):
            missing_symbols.append(symbol)
            continue
        if state.get("subscribed") is not True:
            unsubscribed_symbols.append(symbol)
        latest_event_at = state.get("latest_event_at")
        if not latest_event_at:
            missing_latest_event_symbols.append(symbol)
        elif not isinstance(latest_event_at, str) or not is_iso_datetime(latest_event_at):
            invalid_latest_event_symbols.append(symbol)
        elif generated_at_valid and iso_datetime_is_before(generated_at, latest_event_at):
            future_latest_event_symbols.append(symbol)
    return {
        "missing_symbols": missing_symbols,
        "unsubscribed_symbols": unsubscribed_symbols,
        "missing_latest_event_symbols": missing_latest_event_symbols,
        "invalid_latest_event_symbols": invalid_latest_event_symbols,
        "future_latest_event_symbols": future_latest_event_symbols,
    }


def save_shadow_run_report(report: dict[str, Any], directory: str | Path) -> Path:
    target_dir = Path(directory)
    target_dir.mkdir(parents=True, exist_ok=True)
    session_id = safe_path_part(str(report.get("session_id") or "session"))
    trading_date = safe_path_part(str(report.get("trading_date") or "unknown-date"))
    path = target_dir / f"{trading_date}.{session_id}.shadow-run.json"
    write_json(path, report)
    return path


def load_shadow_run_report_directory(directory: str | Path) -> list[dict[str, Any]]:
    paths = sorted(Path(directory).glob("*.shadow-run.json"))
    return load_cutover_reports(paths)


def write_cutover_readiness(
    *,
    reports_directory: str | Path,
    output_path: str | Path,
    policy: CutoverPolicy | None = None,
) -> dict[str, Any]:
    reports = load_shadow_run_report_directory(reports_directory)
    readiness = evaluate_cutover_readiness(reports, policy=policy)
    write_json(output_path, readiness)
    return readiness


def frontend_cutover_env(
    readiness: dict[str, Any],
    *,
    live_url: str,
    mode: str = "auto",
) -> dict[str, str]:
    return {
        "VITE_MARKET_DATA_MODE": mode,
        "VITE_MARKET_WS_URL": live_url,
        "VITE_MARKET_PROTOCOL": FRONTEND_PROTOCOL,
        "VITE_MARKET_CUTOVER_READINESS": json.dumps(readiness, separators=(",", ":"), ensure_ascii=False),
    }


def write_frontend_cutover_env(
    readiness: dict[str, Any],
    path: str | Path,
    *,
    live_url: str,
    mode: str = "auto",
) -> dict[str, str]:
    env = frontend_cutover_env(readiness, live_url=live_url, mode=mode)
    lines = [f"{key}={value}" for key, value in env.items()]
    Path(path).write_text("\n".join(lines) + "\n", encoding="utf-8")
    return env


def load_env_file(path: str | Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in Path(path).read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def write_frontend_deployment_evidence(
    *,
    env_path: str | Path,
    expected_live_url: str,
    output_path: str | Path,
    verified_at: str | None = None,
) -> dict[str, Any]:
    result = evaluate_frontend_deployment(
        FrontendDeploymentEvidence(
            expected_live_url=expected_live_url,
            deployed_env=load_env_file(env_path),
            verified_at=verified_at or now_iso(),
        )
    )
    write_json(output_path, result)
    return result


def load_legacy_decommission_observation(path: str | Path) -> LegacyDecommissionObservation:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("legacy decommission observation must be a JSON object")
    return LegacyDecommissionObservation(
        legacy_websocket_enabled=bool(raw.get("legacy_websocket_enabled", True)),
        old_topic_consumers=int_record(raw.get("old_topic_consumers")),
        old_topic_lag=int_record(raw.get("old_topic_lag")),
        observed_at=str(raw.get("observed_at") or ""),
    )


def write_legacy_decommission_evidence(
    *,
    observation_path: str | Path,
    output_path: str | Path,
    expected_old_topics: list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    result = evaluate_legacy_decommission(
        load_legacy_decommission_observation(observation_path),
        expected_old_topics=expected_old_topics,
    )
    write_json(output_path, result)
    return result


def legacy_retirement_evidence_from_artifacts(
    *,
    frontend_deployment: dict[str, Any],
    legacy_decommission: dict[str, Any],
    rollback_window_completed: bool,
    rollback_window_started_at: str,
    rollback_window_completed_at: str,
    operator_approved: bool,
    operator_approved_at: str,
    notes: str = "",
) -> LegacyRetirementEvidence:
    return LegacyRetirementEvidence(
        frontend_default_v2_deployed=frontend_deployment.get("frontend_default_v2_deployed") is True,
        legacy_websocket_disabled=legacy_decommission.get("legacy_websocket_disabled") is True,
        old_topic_consumers_disabled=legacy_decommission.get("old_topic_consumers_disabled") is True,
        no_legacy_consumers_observed=legacy_decommission.get("no_legacy_consumers_observed") is True,
        rollback_window_completed=rollback_window_completed,
        rollback_window_started_at=rollback_window_started_at,
        rollback_window_completed_at=rollback_window_completed_at,
        operator_approved=operator_approved,
        operator_approved_at=operator_approved_at,
        notes=notes,
    )


def write_legacy_retirement_from_artifacts(
    *,
    readiness_path: str | Path,
    frontend_deployment_path: str | Path,
    legacy_decommission_path: str | Path,
    rollback_window_completed: bool,
    rollback_window_started_at: str,
    rollback_window_completed_at: str,
    operator_approved: bool,
    operator_approved_at: str,
    output_path: str | Path,
    notes: str = "",
) -> dict[str, Any]:
    readiness = json.loads(Path(readiness_path).read_text(encoding="utf-8"))
    frontend_deployment = json.loads(Path(frontend_deployment_path).read_text(encoding="utf-8"))
    legacy_decommission = json.loads(Path(legacy_decommission_path).read_text(encoding="utf-8"))
    evidence = legacy_retirement_evidence_from_artifacts(
        frontend_deployment=frontend_deployment,
        legacy_decommission=legacy_decommission,
        rollback_window_completed=rollback_window_completed,
        rollback_window_started_at=rollback_window_started_at,
        rollback_window_completed_at=rollback_window_completed_at,
        operator_approved=operator_approved,
        operator_approved_at=operator_approved_at,
        notes=notes,
    )
    return write_legacy_retirement_evidence(
        readiness=readiness,
        evidence=evidence,
        output_path=output_path,
    )


def write_evidence_bundle_verification(
    *,
    paths: EvidenceBundlePaths,
    output_path: str | Path,
) -> dict[str, Any]:
    result = evaluate_evidence_bundle(paths)
    write_json(output_path, result)
    return result


def write_runtime_health_verification(
    *,
    runtime_health_path: str | Path,
    output_path: str | Path,
) -> dict[str, Any]:
    result = evaluate_runtime_health(load_json(runtime_health_path))
    write_json(output_path, result)
    return result


def write_multi_trader_smoke_evidence(
    *,
    observation_path: str | Path,
    output_path: str | Path,
) -> dict[str, Any]:
    result = evaluate_multi_trader_smoke(load_json(observation_path))
    write_json(output_path, result)
    return result


def write_cutover_artifacts(
    *,
    report: dict[str, Any],
    reports_directory: str | Path,
    readiness_path: str | Path,
    frontend_env_path: str | Path,
    live_url: str,
    policy: CutoverPolicy | None = None,
) -> CutoverArtifactPaths:
    report_path = save_shadow_run_report(report, reports_directory)
    readiness = write_cutover_readiness(
        reports_directory=reports_directory,
        output_path=readiness_path,
        policy=policy,
    )
    write_frontend_cutover_env(readiness, frontend_env_path, live_url=live_url)
    return CutoverArtifactPaths(
        report_path=report_path,
        readiness_path=Path(readiness_path),
        frontend_env_path=Path(frontend_env_path),
    )


def write_legacy_retirement_evidence(
    *,
    readiness: dict[str, Any],
    evidence: LegacyRetirementEvidence,
    output_path: str | Path,
) -> dict[str, Any]:
    result = evaluate_legacy_retirement(readiness, evidence)
    write_json(output_path, result)
    return result


def write_json(path: str | Path, value: dict[str, Any]) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def safe_path_part(value: str) -> str:
    cleaned = "".join(character if character.isalnum() or character in {"-", "_"} else "-" for character in value)
    return cleaned.strip("-") or "unknown"


def parse_readiness(value: str) -> dict[str, Any] | None:
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError:
        return None
    return decoded if isinstance(decoded, dict) else None


def int_record(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {
        str(key): raw_value
        for key, raw_value in value.items()
    }
