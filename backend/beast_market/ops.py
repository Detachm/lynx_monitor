from __future__ import annotations

import json
import math
import hashlib
import ipaddress
import os
import shlex
import socket
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .adapters import EventBus, FileBackedSpool, validate_snapshot_key_inputs
from .contracts import REDIS_RUNTIME_SNAPSHOT_KEY_TEMPLATES
from .cutover import (
    CutoverPolicy,
    GATEWAY_WEBSOCKET_PATH,
    client_gateway_url_blockers,
    client_page_url_blockers,
    client_symbol_status_evidence,
    evaluate_multi_trader_smoke,
    evaluate_runtime_health,
    is_iso_datetime,
    is_yyyymmdd,
    is_loopback_host,
    multi_trader_service_preflight_timing_evidence,
    multi_trader_smoke_preflight_evidence,
    normalized_client_id,
    symbol_list_evidence,
    validate_multi_trader_smoke_preflight,
    write_cutover_artifacts,
    write_multi_trader_smoke_evidence,
    workflow_evidence_blockers,
)
from .mammoth_api import MammothAPI, REQUIRED_HISTORICAL_MANIFEST_TYPES, save_manifest
from .runtime_state import RuntimeStateClearResult, clear_runtime_state_files
from .shadow_run import FileBackedShadowRunRecorder, LegacyShadowTelemetryAdapter, build_shadow_run_report_from_files, shadow_run_file_paths


def repository_root() -> Path:
    return Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class ShadowRunCutoverResult:
    report: dict[str, Any]
    report_path: Path
    readiness_path: Path
    frontend_env_path: Path


@dataclass(frozen=True)
class HistoricalManifestGenerationResult:
    manifests: list[dict[str, Any]]
    manifest_paths: list[Path]

    @property
    def passed(self) -> bool:
        return all((manifest.get("quality_checks") or {}).get("passed") is True for manifest in self.manifests)

    @property
    def failed_data_types(self) -> list[str]:
        return [
            str(manifest.get("data_type"))
            for manifest in self.manifests
            if (manifest.get("quality_checks") or {}).get("passed") is not True
        ]


@dataclass(frozen=True)
class LegacyTelemetryImportResult:
    imported_count: int
    stream_directory: Path
    legacy_events_path: Path


@dataclass(frozen=True)
class FrontendPerformanceImportResult:
    imported_count: int
    stream_directory: Path
    performance_samples_path: Path


@dataclass(frozen=True)
class RuntimeCacheClearResult:
    dry_run: bool
    confirmed: bool
    keys: list[str]
    deleted_keys: list[str]


@dataclass(frozen=True)
class KafkaSpoolReplayResult:
    dry_run: bool
    confirmed: bool
    spool_path: Path
    quarantine_path: Path
    replayed_count: int
    failed_count: int
    remaining_count: int
    quarantined_count: int
    deleted_spool: bool
    errors: list[str]


@dataclass(frozen=True)
class MultiTraderSmokeObservationBuildResult:
    observation: dict[str, Any]
    output_path: Path


@dataclass(frozen=True)
class MultiTraderSmokeWorkflowTemplateResult:
    workflows: dict[str, Any]
    output_path: Path


@dataclass(frozen=True)
class MultiTraderSmokeWorkflowRecordResult:
    workflows: dict[str, Any]
    output_path: Path
    workflow: str


@dataclass(frozen=True)
class MultiTraderSmokeReadinessResult:
    ready: bool
    root_path: Path
    summary: dict[str, Any]


@dataclass(frozen=True)
class MultiTraderSmokePreparationResult:
    preflight: dict[str, Any]
    root_path: Path
    workflows_path: Path
    preflight_path: Path


@dataclass(frozen=True)
class MultiTraderSmokeServiceCheckResult:
    passed: bool
    root_path: Path
    preflight_path: Path
    output_path: Path
    preflight: dict[str, Any]
    service_checks: dict[str, Any]


@dataclass(frozen=True)
class MultiTraderSmokeFinalizeResult:
    evidence: dict[str, Any]
    root_path: Path
    observation_path: Path
    evidence_path: Path
    manifest_path: Path


@dataclass(frozen=True)
class MultiTraderSmokePackageResult:
    package_path: Path
    metadata_path: Path
    sha256: str
    byte_count: int
    file_count: int
    files: list[str]


@dataclass(frozen=True)
class MultiTraderSmokeArtifactImportResult:
    kind: str
    input_path: Path
    output_path: Path
    manifest_path: Path | None = None


@dataclass(frozen=True)
class MultiTraderSmokeArtifactBatchImportResult:
    imported: list[MultiTraderSmokeArtifactImportResult]
    skipped: list[dict[str, str]]
    manifest_path: Path


MULTI_TRADER_SMOKE_REQUIRED_WORKFLOWS = [
    "cold_query",
    "add_to_watchlist",
    "refresh_recovery",
    "redis_clear_recovery",
    "process_restart_recovery",
    "closed_market_effective_date",
]


def generate_required_historical_manifests(
    *,
    silver_root: str | Path,
    manifest_root: str | Path,
    start_date: str,
    end_date: str,
    symbols: list[str],
    code_version: str,
) -> HistoricalManifestGenerationResult:
    mammoth = MammothAPI(silver_root)
    manifests: list[dict[str, Any]] = []
    manifest_paths: list[Path] = []
    for data_type in REQUIRED_HISTORICAL_MANIFEST_TYPES:
        manifest = mammoth.build_manifest(
            data_type=data_type,
            start_date=start_date,
            end_date=end_date,
            symbols=symbols,
            code_version=code_version,
        )
        path = save_manifest(manifest, manifest_root)
        manifests.append(manifest)
        manifest_paths.append(path)
    return HistoricalManifestGenerationResult(manifests=manifests, manifest_paths=manifest_paths)


def clear_runtime_cache(
    *,
    redis_client: Any | None = None,
    trade_date: str,
    symbols: list[str],
    dry_run: bool = True,
    confirm: bool = False,
) -> RuntimeCacheClearResult:
    """Clear only dashboard runtime-cache keys for explicit date/symbol scopes."""

    if not dry_run and not confirm:
        raise ValueError("clear-runtime-cache requires --confirm when not running as dry-run")
    keys = runtime_cache_keys(
        redis_client=redis_client,
        trade_date=trade_date,
        symbols=symbols,
        include_unmatched_history_pattern=dry_run,
    )
    deleted_keys: list[str] = []
    if not dry_run:
        if redis_client is None:
            raise ValueError("redis_client is required to delete runtime cache keys")
        delete = getattr(redis_client, "delete", None)
        if not callable(delete):
            raise ValueError("redis_client must expose delete(*keys) or delete(key)")
        for key in keys:
            delete(key)
            deleted_keys.append(key)
    return RuntimeCacheClearResult(
        dry_run=dry_run,
        confirmed=confirm,
        keys=keys,
        deleted_keys=deleted_keys,
    )


def runtime_cache_keys(
    *,
    redis_client: Any | None = None,
    trade_date: str,
    symbols: list[str],
    include_unmatched_history_pattern: bool = True,
) -> list[str]:
    keys: list[str] = []
    for symbol in symbols:
        validate_snapshot_key_inputs(trade_date, symbol)
        for family in ("terminal_snapshot", "terminal_minute", "terminal_alerts", "terminal_queue", "terminal_state", "ccass_holding"):
            keys.append(REDIS_RUNTIME_SNAPSHOT_KEY_TEMPLATES[family].format(trade_date=trade_date, symbol=symbol))
        history_pattern = f"ccass:history:{symbol}:*"
        history_keys = matching_redis_keys(redis_client, history_pattern)
        if history_keys:
            keys.extend(history_keys)
        elif include_unmatched_history_pattern:
            keys.append(history_pattern)
    return sorted(dict.fromkeys(keys))


def clear_runtime_state(
    *,
    runtime_state_root: str | Path,
    trade_date: str,
    symbols: list[str],
    dry_run: bool = True,
    confirm: bool = False,
    include_callback_rejections: bool = False,
    include_dead_letters: bool = False,
) -> RuntimeStateClearResult:
    return clear_runtime_state_files(
        runtime_state_root,
        trade_date=trade_date,
        symbols=symbols,
        dry_run=dry_run,
        confirm=confirm,
        include_callback_rejections=include_callback_rejections,
        include_dead_letters=include_dead_letters,
    )


def replay_kafka_spool(
    *,
    spool_path: str | Path,
    event_bus: EventBus,
    dry_run: bool = True,
    confirm: bool = False,
) -> KafkaSpoolReplayResult:
    if not dry_run and not confirm:
        raise ValueError("replay-kafka-spool requires --confirm when not running as dry-run")
    spool = FileBackedSpool(spool_path)
    if dry_run:
        return KafkaSpoolReplayResult(
            dry_run=True,
            confirmed=confirm,
            spool_path=Path(spool_path),
            quarantine_path=spool.quarantine_path,
            replayed_count=0,
            failed_count=0,
            remaining_count=len(spool.records),
            quarantined_count=spool.quarantined_records,
            deleted_spool=False,
            errors=[],
        )

    replayed_count = 0
    records = list(spool.records)
    for index, record in enumerate(records):
        try:
            event_bus.publish(record.topic, record.key, record.value)
            replayed_count += 1
        except Exception as error:
            remaining = records[index:]
            spool.replace(remaining)
            return KafkaSpoolReplayResult(
                dry_run=False,
                confirmed=confirm,
                spool_path=Path(spool_path),
                quarantine_path=spool.quarantine_path,
                replayed_count=replayed_count,
                failed_count=1,
                remaining_count=len(remaining),
                quarantined_count=spool.quarantined_records,
                deleted_spool=False,
                errors=[f"{record.topic}/{record.key}: {error}"],
            )

    drained = spool.drain()
    return KafkaSpoolReplayResult(
        dry_run=False,
        confirmed=confirm,
        spool_path=Path(spool_path),
        quarantine_path=spool.quarantine_path,
        replayed_count=len(drained),
        failed_count=0,
        remaining_count=0,
        quarantined_count=spool.quarantined_records,
        deleted_spool=True,
        errors=[],
    )


def build_multi_trader_smoke_observation(
    *,
    clients_path: str | Path,
    workflows_path: str | Path,
    runtime_health_path: str | Path,
    output_path: str | Path,
    observed_at: str,
    performance_samples_path: str | Path | None = None,
    metrics_path: str | Path | None = None,
    preflight_path: str | Path | None = None,
) -> MultiTraderSmokeObservationBuildResult:
    observation = build_multi_trader_smoke_observation_payload(
        clients_path=clients_path,
        workflows_path=workflows_path,
        runtime_health_path=runtime_health_path,
        performance_samples_path=performance_samples_path,
        metrics_path=metrics_path,
        preflight_path=preflight_path,
        observed_at=observed_at,
    )

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(observation, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
    return MultiTraderSmokeObservationBuildResult(observation=observation, output_path=path)


def build_multi_trader_smoke_workflows_template(
    *,
    output_path: str | Path,
    cold_query_symbol: str = "",
    redis_clear_symbol: str = "",
    add_to_watchlist_symbol: str = "",
    requested_trade_date: str = "",
    effective_trade_date: str = "",
) -> MultiTraderSmokeWorkflowTemplateResult:
    workflows = {
        "cold_query": {
            "passed": False,
            "symbol": cold_query_symbol,
            "loading_observed": False,
            "snapshot_visible": False,
        },
        "add_to_watchlist": {
            "passed": False,
            "symbol": add_to_watchlist_symbol,
            "persisted": False,
        },
        "refresh_recovery": {
            "passed": False,
            "browser_refreshed": False,
            "watchlist_restored": False,
            "snapshots_visible": False,
        },
        "redis_clear_recovery": {
            "passed": False,
            "symbol": redis_clear_symbol,
            "cache_cleared": False,
            "snapshot_rebuilt": False,
        },
        "process_restart_recovery": {
            "passed": False,
            "backend_restarted": False,
            "first_screen_restored": False,
        },
        "closed_market_effective_date": {
            "passed": False,
            "expected_closed_market": True,
            "requested_trade_date": requested_trade_date,
            "effective_trade_date": effective_trade_date,
            "source_dates_visible": False,
        },
    }
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"schema_version": 1, "workflows": workflows}
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
    return MultiTraderSmokeWorkflowTemplateResult(workflows=workflows, output_path=path)


def record_multi_trader_smoke_workflow(
    *,
    workflows_path: str | Path,
    workflow: str,
    symbol: str = "",
    requested_trade_date: str = "",
    effective_trade_date: str = "",
    observed_at: str = "",
    notes: str = "",
) -> MultiTraderSmokeWorkflowRecordResult:
    path = Path(workflows_path)
    payload = load_json_file(path)
    if not isinstance(payload, dict):
        raise ValueError("workflows_path must contain a JSON object")
    workflows = payload.get("workflows")
    if not isinstance(workflows, dict):
        raise ValueError("workflows_path must contain a workflows object")
    if workflow not in workflows or not isinstance(workflows[workflow], dict):
        raise ValueError(f"unknown smoke workflow: {workflow}")

    entry = dict(workflows[workflow])
    if workflow == "cold_query":
        entry.update({"loading_observed": True, "snapshot_visible": True})
        if symbol:
            entry["symbol"] = canonical_smoke_symbol(symbol)
    elif workflow == "add_to_watchlist":
        entry["persisted"] = True
        if symbol:
            entry["symbol"] = canonical_smoke_symbol(symbol)
    elif workflow == "refresh_recovery":
        entry.update({"browser_refreshed": True, "watchlist_restored": True, "snapshots_visible": True})
    elif workflow == "redis_clear_recovery":
        entry["cache_cleared"] = True
        entry["snapshot_rebuilt"] = True
        if symbol:
            entry["symbol"] = canonical_smoke_symbol(symbol)
    elif workflow == "process_restart_recovery":
        entry["backend_restarted"] = True
        entry["first_screen_restored"] = True
    elif workflow == "closed_market_effective_date":
        entry["source_dates_visible"] = True
        if requested_trade_date:
            entry["requested_trade_date"] = requested_trade_date
        if effective_trade_date:
            entry["effective_trade_date"] = effective_trade_date
    else:
        raise ValueError(f"unsupported smoke workflow: {workflow}")

    entry["passed"] = True
    if observed_at:
        if not is_iso_datetime(observed_at):
            raise ValueError("observed_at must be an ISO datetime")
        entry["observed_at"] = observed_at
    if notes:
        entry["notes"] = notes
    entry_blockers = multi_trader_smoke_workflow_entry_blockers(workflow, entry)
    if entry_blockers:
        raise ValueError(
            "cannot record incomplete smoke workflow evidence: "
            + ", ".join(sorted(entry_blockers))
        )
    workflows[workflow] = entry
    payload["workflows"] = workflows
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
    return MultiTraderSmokeWorkflowRecordResult(workflows=workflows, output_path=path, workflow=workflow)


def prepare_multi_trader_smoke(
    *,
    root_path: str | Path,
    lan_host: str,
    frontend_port: int | str = 5173,
    gateway_port: int | str = 9020,
    silver_root: str | Path | None = None,
    cold_query_symbol: str = "",
    redis_clear_symbol: str = "",
    add_to_watchlist_symbol: str = "",
    requested_trade_date: str = "",
    effective_trade_date: str = "",
    require_local_lan_host: bool = False,
    local_addresses: list[str] | None = None,
    auto_root_base_path: str | Path = "artifacts/multi-trader-smoke",
) -> MultiTraderSmokePreparationResult:
    root = resolve_multi_trader_smoke_root_path(root_path, auto_root_base_path=auto_root_base_path)
    clients_dir = root / "clients"
    performance_dir = root / "performance"
    scripts_dir = root / "scripts"
    clients_dir.mkdir(parents=True, exist_ok=True)
    performance_dir.mkdir(parents=True, exist_ok=True)
    scripts_dir.mkdir(parents=True, exist_ok=True)
    workflows_path = root / "workflows.json"
    canonical_cold_query_symbol = canonical_smoke_symbol(cold_query_symbol)
    canonical_redis_clear_symbol = canonical_smoke_symbol(redis_clear_symbol)
    canonical_add_to_watchlist_symbol = canonical_smoke_symbol(add_to_watchlist_symbol)
    workflow_result = build_multi_trader_smoke_workflows_template(
        output_path=workflows_path,
        cold_query_symbol=canonical_cold_query_symbol,
        redis_clear_symbol=canonical_redis_clear_symbol,
        add_to_watchlist_symbol=canonical_add_to_watchlist_symbol,
        requested_trade_date=requested_trade_date,
        effective_trade_date=effective_trade_date,
    )

    detected_local_addresses = detect_local_ip_addresses() if local_addresses is None else sorted(set(local_addresses))
    host = lan_host.strip()
    auto_detected_host = False
    if host.lower() == "auto":
        if detected_local_addresses:
            host = detected_local_addresses[0]
            auto_detected_host = True
        else:
            host = ""
    resolved_frontend_port, auto_frontend_port = resolve_smoke_port(frontend_port, default_port=5173)
    resolved_gateway_port, auto_gateway_port = resolve_smoke_port(
        gateway_port,
        default_port=9020,
        reserved_ports={resolved_frontend_port},
    )
    url_host = host_for_url(host)
    page_url = f"http://{url_host}:{resolved_frontend_port}/" if host else ""
    gateway_url = f"ws://{url_host}:{resolved_gateway_port}{GATEWAY_WEBSOCKET_PATH}" if host else ""
    local_address_evidence = local_lan_host_evidence(
        host,
        local_addresses=detected_local_addresses,
        require_local_lan_host=require_local_lan_host,
    )
    blockers = multi_trader_smoke_preflight_blockers(
        lan_host=host,
        frontend_port=resolved_frontend_port,
        gateway_port=resolved_gateway_port,
        page_url=page_url,
        gateway_url=gateway_url,
        local_address_evidence=local_address_evidence,
    )
    if lan_host.strip().lower() == "auto" and not host:
        blockers.append("multi_trader_smoke_lan_host_auto_detect_failed")
    runtime_symbols = multi_trader_smoke_runtime_symbols(
        canonical_cold_query_symbol,
        canonical_redis_clear_symbol,
        canonical_add_to_watchlist_symbol,
    )
    frontend_initial_symbols = multi_trader_smoke_frontend_initial_symbols(
        runtime_symbols=runtime_symbols,
        cold_query_symbol=canonical_cold_query_symbol,
        add_to_watchlist_symbol=canonical_add_to_watchlist_symbol,
        fallback_symbol=canonical_redis_clear_symbol,
    )
    backend_command = multi_trader_smoke_backend_command(
        gateway_port=resolved_gateway_port,
        runtime_health_path=root / "runtime-health-verification.json",
        requested_trade_date=requested_trade_date,
        symbols=runtime_symbols,
        silver_root=silver_root,
    )
    frontend_env = multi_trader_smoke_frontend_env(
        gateway_url=gateway_url,
        symbols=frontend_initial_symbols,
    )
    frontend_command = multi_trader_smoke_frontend_command(
        frontend_port=resolved_frontend_port,
        package_command="npm run dev",
        env=frontend_env,
    )
    service_preflight_command = (
        "PYTHONPATH=backend python -m beast_market.ops_cli "
        f"verify-multi-trader-smoke-services --root-path {shlex.quote(str(root))}"
    )
    inspect_next_action_command = (
        "PYTHONPATH=backend python -m beast_market.ops_cli "
        f"inspect-multi-trader-smoke --root-path {shlex.quote(str(root))} --next-action"
    )
    verify_handoff_command = (
        "PYTHONPATH=backend python -m beast_market.ops_cli "
        f"inspect-multi-trader-smoke --root-path {shlex.quote(str(root))} --require-package-ready"
    )
    finalize_package_command = (
        "PYTHONPATH=backend python -m beast_market.ops_cli "
        f"finalize-multi-trader-smoke --root-path {shlex.quote(str(root))} "
        '--observed-at "$(date --iso-8601=seconds)" --package'
    )
    redis_clear_dry_run_command = multi_trader_smoke_redis_clear_command(
        trade_date=requested_trade_date,
        symbol=canonical_redis_clear_symbol,
        dry_run=True,
    )
    redis_clear_confirm_command = multi_trader_smoke_redis_clear_command(
        trade_date=requested_trade_date,
        symbol=canonical_redis_clear_symbol,
        dry_run=False,
    )
    repo_root = repository_root()
    backend_script = write_multi_trader_smoke_command_script(
        scripts_dir / "start-backend.sh",
        command=backend_command,
        repo_root=repo_root,
        port_check=resolved_gateway_port,
        port_check_label="Gateway",
    )
    restart_backend_script = write_multi_trader_smoke_restart_backend_script(
        scripts_dir / "restart-backend.sh",
        backend_script=backend_script,
        runtime_health_path=root / "runtime-health-verification.json",
        gateway_port=resolved_gateway_port,
        repo_root=repo_root,
    )
    frontend_script = write_multi_trader_smoke_command_script(
        scripts_dir / "start-frontend.sh",
        command=frontend_command,
        repo_root=repo_root,
        port_check=resolved_frontend_port,
        port_check_label="frontend",
    )
    service_preflight_script = write_multi_trader_smoke_command_script(
        scripts_dir / "verify-services.sh",
        command=service_preflight_command,
        repo_root=repo_root,
    )
    inspect_next_action_script = write_multi_trader_smoke_command_script(
        scripts_dir / "inspect-next-action.sh",
        command=inspect_next_action_command,
        repo_root=repo_root,
    )
    verify_handoff_script = write_multi_trader_smoke_command_script(
        scripts_dir / "verify-handoff.sh",
        command=verify_handoff_command,
        repo_root=repo_root,
    )
    finalize_package_script = write_multi_trader_smoke_command_script(
        scripts_dir / "finalize-package.sh",
        command=finalize_package_command,
        repo_root=repo_root,
    )
    import_artifacts_script = write_multi_trader_smoke_import_script(
        scripts_dir / "import-artifacts.sh",
        root_path=root,
        repo_root=repo_root,
    )
    record_workflow_script = write_multi_trader_smoke_record_workflow_script(
        scripts_dir / "record-workflow.sh",
        workflows_path=workflows_path,
        repo_root=repo_root,
    )
    preflight = {
        "schema_version": 1,
        "prepared_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "passed": not blockers,
        "blockers": blockers,
        "warnings": local_address_evidence["warnings"],
        "requested_root_path": str(root_path),
        "auto_created_root_path": str(root_path).strip().lower() == "auto",
        "lan_host": host,
        "requested_lan_host": lan_host.strip(),
        "auto_detected_lan_host": auto_detected_host,
        "local_lan_host": local_address_evidence,
        "requested_frontend_port": str(frontend_port),
        "requested_gateway_port": str(gateway_port),
        "frontend_port": resolved_frontend_port,
        "gateway_port": resolved_gateway_port,
        "silver_root": str(silver_root) if silver_root is not None else "",
        "auto_selected_frontend_port": auto_frontend_port,
        "auto_selected_gateway_port": auto_gateway_port,
        "page_url": page_url,
        "gateway_url": gateway_url,
        "runtime_symbols": runtime_symbols,
        "frontend_initial_symbols": frontend_initial_symbols,
        "artifact_paths": {
            "root": str(root),
            "clients": str(clients_dir),
            "performance": str(performance_dir),
            "scripts": str(scripts_dir),
            "readme": str(root / "README.md"),
            "client_instructions": str(root / "CLIENT_INSTRUCTIONS.md"),
            "workflows": str(workflows_path),
            "service_preflight": str(root / "service-preflight.json"),
            "runtime_health": str(root / "runtime-health-verification.json"),
            "observation": str(root / "multi-trader-smoke-observation.json"),
            "evidence": str(root / "multi-trader-smoke-evidence.json"),
            "import_manifest": str(root / "smoke-import-manifest.json"),
            "run_manifest": str(root / "smoke-run-manifest.json"),
            "package": str(root / "multi-trader-smoke-evidence.zip"),
            "package_metadata": str(root / "smoke-run-package.json"),
        },
        "commands": {
            "backend": backend_command,
            "backend_script": str(backend_script.resolve()),
            "restart_backend_script": str(restart_backend_script.resolve()),
            "frontend": frontend_command,
            "frontend_script": str(frontend_script.resolve()),
            "frontend_pnpm": multi_trader_smoke_frontend_command(
                frontend_port=resolved_frontend_port,
                package_command="corepack pnpm dev",
                env=frontend_env,
            ),
            "frontend_npm": frontend_command,
            "service_preflight": service_preflight_command,
            "service_preflight_script": str(service_preflight_script.resolve()),
            "inspect_next_action": inspect_next_action_command,
            "inspect_next_action_script": str(inspect_next_action_script.resolve()),
            "verify_handoff": verify_handoff_command,
            "verify_handoff_script": str(verify_handoff_script.resolve()),
            "finalize_package": finalize_package_command,
            "finalize_package_script": str(finalize_package_script.resolve()),
            "redis_clear_dry_run": redis_clear_dry_run_command,
            "redis_clear_confirm": redis_clear_confirm_command,
            "import_artifacts": (
                "PYTHONPATH=backend python -m beast_market.ops_cli "
                f"import-multi-trader-smoke-artifacts --root-path {shlex.quote(str(root))} --input-path <downloads-dir>"
            ),
            "import_artifacts_script": str(import_artifacts_script.resolve()),
            "record_workflow": (
                "PYTHONPATH=backend python -m beast_market.ops_cli "
                f"record-multi-trader-smoke-workflow --workflows-path {shlex.quote(str(workflows_path))} "
                '--workflow <workflow> --observed-at "$(date --iso-8601=seconds)"'
            ),
            "record_workflow_script": str(record_workflow_script.resolve()),
            "client_url": page_url,
        },
        "workflows": workflow_result.workflows,
    }
    write_multi_trader_smoke_readme(root / "README.md", preflight=preflight)
    write_multi_trader_smoke_client_instructions(root / "CLIENT_INSTRUCTIONS.md", preflight=preflight)
    preflight_path = root / "lan-preflight.json"
    preflight_path.write_text(json.dumps(preflight, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
    return MultiTraderSmokePreparationResult(
        preflight=preflight,
        root_path=root,
        workflows_path=workflows_path,
        preflight_path=preflight_path,
    )


def write_multi_trader_smoke_readme(path: Path, *, preflight: dict[str, Any]) -> Path:
    commands = preflight.get("commands") if isinstance(preflight.get("commands"), dict) else {}
    paths = preflight.get("artifact_paths") if isinstance(preflight.get("artifact_paths"), dict) else {}
    workflows = preflight.get("workflows") if isinstance(preflight.get("workflows"), dict) else {}
    workflow_names = ", ".join(workflows.keys()) if workflows else "cold_query, add_to_watchlist, refresh_recovery, redis_clear_recovery, process_restart_recovery, closed_market_effective_date"
    record_script = str(commands.get("record_workflow_script", ""))
    record_command = shlex.quote(record_script) if record_script else ""
    restart_backend_script = str(commands.get("restart_backend_script", ""))
    restart_backend_command = shlex.quote(restart_backend_script) if restart_backend_script else ""
    import_artifacts_script = str(commands.get("import_artifacts_script", ""))
    import_artifacts_command = shlex.quote(import_artifacts_script) if import_artifacts_script else ""
    backend_command = quote_command_path(commands.get("backend_script"))
    frontend_command = quote_command_path(commands.get("frontend_script"))
    service_preflight_command = quote_command_path(commands.get("service_preflight_script"))
    inspect_next_action_command = quote_command_path(commands.get("inspect_next_action_script"))
    verify_handoff_command = quote_command_path(commands.get("verify_handoff_script"))
    finalize_package_script_command = quote_command_path(commands.get("finalize_package_script"))
    cold_query_symbol = workflow_symbol(workflows, "cold_query")
    add_to_watchlist_symbol = workflow_symbol(workflows, "add_to_watchlist")
    redis_clear_symbol = workflow_symbol(workflows, "redis_clear_recovery")
    closed_market = workflows.get("closed_market_effective_date") if isinstance(workflows.get("closed_market_effective_date"), dict) else {}
    requested_trade_date = str(closed_market.get("requested_trade_date") or "")
    effective_trade_date = str(closed_market.get("effective_trade_date") or "")
    content = "\n".join(
        [
            "# LAN Multi-Trader Smoke Run",
            "",
            f"- Prepared at: {preflight.get('prepared_at', '')}",
            f"- Passed preflight: {preflight.get('passed') is True}",
            f"- Page URL: {preflight.get('page_url', '')}",
            f"- Gateway URL: {preflight.get('gateway_url', '')}",
            f"- Runtime symbols: {', '.join(preflight.get('runtime_symbols') or [])}",
            f"- Initial frontend watchlist symbols: {', '.join(preflight.get('frontend_initial_symbols') or [])}",
            "",
            "## Scripts",
            "",
            f"- Backend: `{backend_command}`",
            f"- Restart backend: `{restart_backend_command}`",
            f"- Frontend: `{frontend_command}`",
            f"- Verify services: `{service_preflight_command}`",
            f"- Import browser artifacts: `{import_artifacts_command} <downloads-file-or-dir>`",
            f"- Record workflow: `{record_command} <workflow> [extra args...]`",
            f"- Inspect next action: `{inspect_next_action_command}`",
            f"- Verify handoff gate: `{verify_handoff_command}`",
            f"- Finalize/package: `{finalize_package_script_command}`",
            f"- Client instructions: `{paths.get('client_instructions', '')}`",
            "",
            "## Operator Flow",
            "",
            "1. Start backend and frontend scripts in separate terminals.",
            "2. Run the verify services script before handing the URL to clients.",
            "3. Run the inspect next action script repeatedly during the smoke.",
            "4. Import client and performance JSON exports with the import script.",
            "5. Record each observed workflow with the record workflow script.",
            "6. For process restart recovery, run the restart backend script and wait for clients to restore.",
            "7. Run the finalize/package script.",
            "8. Run the verify handoff gate script before submitting the zip.",
            "",
            "## Workflow Names",
            "",
            workflow_names,
            "",
            "## Workflow Recording",
            "",
            f"- Cold query: `{record_command} cold_query --symbol {cold_query_symbol or '<symbol>'}`",
            f"- Add to watchlist: `{record_command} add_to_watchlist --symbol {add_to_watchlist_symbol or '<symbol>'}`",
            f"- Refresh recovery: `{record_command} refresh_recovery`",
            f"- Redis clear recovery: `{record_command} redis_clear_recovery --symbol {redis_clear_symbol or '<symbol>'}`",
            f"- Process restart recovery: `{record_command} process_restart_recovery`",
            f"- Closed-market effective date: `{record_command} closed_market_effective_date --requested-trade-date {requested_trade_date or '<requested-date>'} --effective-trade-date {effective_trade_date or '<effective-date>'}`",
            "",
            "## Redis Clear Recovery",
            "",
            "Run the dry-run first and verify the scoped keys before confirming deletion:",
            "",
            f"```bash\n{commands.get('redis_clear_dry_run', '')}\n```",
            "",
            "Then run the confirmed clear for this smoke symbol/date only:",
            "",
            f"```bash\n{commands.get('redis_clear_confirm', '')}\n```",
            "",
            f"After the client observes the rebuilt snapshot, record: `{record_command} redis_clear_recovery --symbol {redis_clear_symbol or '<symbol>'}`",
            "",
            "## Process Restart Recovery",
            "",
            "Restart only this smoke backend, then wait for client first screens to restore:",
            "",
            f"```bash\n{restart_backend_command}\n```",
            "",
            f"After clients observe restored first-screen state, record: `{record_command} process_restart_recovery`",
            "",
            "## Expected Artifacts",
            "",
            f"- Clients: `{paths.get('clients', '')}`",
            f"- Performance: `{paths.get('performance', '')}`",
            f"- Workflows: `{paths.get('workflows', '')}`",
            f"- Service preflight: `{paths.get('service_preflight', '')}`",
            f"- Runtime health: `{paths.get('runtime_health', '')}`",
            f"- Observation: `{paths.get('observation', '')}`",
            f"- Evidence: `{paths.get('evidence', '')}`",
            f"- Import manifest: `{paths.get('import_manifest', '')}`",
            f"- Run manifest: `{paths.get('run_manifest', '')}`",
            f"- Package: `{paths.get('package', '')}`",
            f"- Package metadata: `{paths.get('package_metadata', '')}`",
            "",
            "The run is complete only after the verify handoff gate succeeds and the evidence zip is present.",
            "",
        ]
    )
    path.write_text(content, encoding="utf-8")
    return path


def workflow_symbol(workflows: dict[str, Any], workflow: str) -> str:
    entry = workflows.get(workflow)
    if not isinstance(entry, dict):
        return ""
    return str(entry.get("symbol") or "")


def quote_command_path(value: Any) -> str:
    text = str(value or "").strip()
    return shlex.quote(text) if text else ""


def write_multi_trader_smoke_client_instructions(path: Path, *, preflight: dict[str, Any]) -> Path:
    runtime_symbols = preflight.get("runtime_symbols") if isinstance(preflight.get("runtime_symbols"), list) else []
    workflows = preflight.get("workflows") if isinstance(preflight.get("workflows"), dict) else {}
    frontend_initial_symbols = (
        preflight.get("frontend_initial_symbols") if isinstance(preflight.get("frontend_initial_symbols"), list) else []
    )
    primary_symbol = frontend_initial_symbols[0] if frontend_initial_symbols else (runtime_symbols[0] if runtime_symbols else "00700.HK")
    cold_query_symbol = workflow_symbol(workflows, "cold_query") or next(
        (symbol for symbol in runtime_symbols if symbol != primary_symbol),
        primary_symbol,
    )
    add_to_watchlist_symbol = workflow_symbol(workflows, "add_to_watchlist") or next(
        (symbol for symbol in runtime_symbols if symbol not in {primary_symbol, cold_query_symbol}),
        cold_query_symbol,
    )
    redis_clear_symbol = workflow_symbol(workflows, "redis_clear_recovery") or primary_symbol
    content = "\n".join(
        [
            "# Client Smoke Instructions",
            "",
            f"Open: {preflight.get('page_url', '')}",
            "",
            "Use two different LAN client machines for the final smoke. Browser profiles are acceptable only for a local rehearsal.",
            "Confirm the smoke machine id shown in the toolbar is different on each client before exporting artifacts.",
            "",
            "Suggested watchlists:",
            "",
            f"- Client A: start with `{primary_symbol}`, then cold query `{cold_query_symbol}`.",
            f"- Client B: start with `{primary_symbol}`, then add `{add_to_watchlist_symbol}` to watchlist.",
            "",
            "Required client actions:",
            "",
            "1. Open the prepared LAN URL above.",
            f"2. Confirm the initial watchlist contains `{primary_symbol}` and wait until its snapshot is visible.",
            f"3. Client A searches/subscribes `{cold_query_symbol}` and waits for loading to become a snapshot.",
            f"4. Client B searches/subscribes `{add_to_watchlist_symbol}` and keeps it in the watchlist.",
            "5. Confirm the page is in live mode and connected.",
            "6. Refresh the browser and confirm the watchlist restores.",
            f"7. After the operator clears Redis for `{redis_clear_symbol}`, resubscribe or refresh and confirm the snapshot rebuilds.",
            "8. Export both smoke JSON files from the dashboard smoke controls: client smoke JSON and performance smoke JSON.",
            "9. Keep this browser tab open and connected until the backend operator confirms runtime health has observed this machine id.",
            "10. Send the exported files back to the backend operator for import.",
            "",
            "Do not export from a loopback URL such as 127.0.0.1 or localhost.",
            "",
        ]
    )
    path.write_text(content, encoding="utf-8")
    return path


def write_multi_trader_smoke_command_script(
    path: Path,
    *,
    command: str,
    repo_root: Path,
    port_check: int | None = None,
    port_check_label: str = "service",
) -> Path:
    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        f"cd {shlex.quote(str(repo_root))}",
    ]
    if port_check is not None:
        lines.extend(
            [
                f"if ! python - {int(port_check)} <<'PY'",
                "import socket",
                "import sys",
                "port = int(sys.argv[1])",
                "sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)",
                "sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)",
                "try:",
                "    sock.bind(('0.0.0.0', port))",
                "except OSError:",
                "    raise SystemExit(1)",
                "finally:",
                "    sock.close()",
                "PY",
                "then",
                f"  echo \"prepared {port_check_label} port {int(port_check)} is already in use; stop the existing process or rerun prepare-multi-trader-smoke with another port\" >&2",
                "  exit 1",
                "fi",
            ]
        )
    lines.extend([command, ""])
    content = "\n".join(lines)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)
    return path


def write_multi_trader_smoke_restart_backend_script(
    path: Path,
    *,
    backend_script: Path,
    runtime_health_path: Path,
    gateway_port: int,
    repo_root: Path,
) -> Path:
    content = "\n".join(
        [
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            f"cd {shlex.quote(str(repo_root))}",
            f"port={int(gateway_port)}",
            f"runtime_health_path={shlex.quote(str(runtime_health_path))}",
            f"backend_script={shlex.quote(str(backend_script.resolve()))}",
            "python - \"$port\" \"$runtime_health_path\" <<'PY'",
            "import os",
            "import signal",
            "import sys",
            "import time",
            "",
            "port = int(sys.argv[1])",
            "runtime_health_path = sys.argv[2]",
            "",
            "def listener_inodes(target_port):",
            "    inodes = set()",
            "    for path in ('/proc/net/tcp', '/proc/net/tcp6'):",
            "        try:",
            "            lines = open(path, encoding='utf-8').read().splitlines()[1:]",
            "        except OSError:",
            "            continue",
            "        for line in lines:",
            "            fields = line.split()",
            "            if len(fields) < 10 or fields[3] != '0A':",
            "                continue",
            "            local_address = fields[1]",
            "            local_port = int(local_address.rsplit(':', 1)[1], 16)",
            "            if local_port == target_port:",
            "                inodes.add(fields[9])",
            "    return inodes",
            "",
            "def listener_pids(target_port):",
            "    inodes = listener_inodes(target_port)",
            "    pids = []",
            "    if not inodes:",
            "        return pids",
            "    for name in os.listdir('/proc'):",
            "        if not name.isdigit():",
            "            continue",
            "        fd_dir = f'/proc/{name}/fd'",
            "        try:",
            "            fds = os.listdir(fd_dir)",
            "        except OSError:",
            "            continue",
            "        for fd in fds:",
            "            try:",
            "                target = os.readlink(f'{fd_dir}/{fd}')",
            "            except OSError:",
            "                continue",
            "            if target.startswith('socket:[') and target[8:-1] in inodes:",
            "                pids.append(int(name))",
            "                break",
            "    return sorted(set(pids))",
            "",
            "matches = []",
            "for pid in listener_pids(port):",
            "    try:",
            "        raw = open(f'/proc/{pid}/cmdline', 'rb').read().replace(b'\\0', b' ').decode(errors='replace')",
            "    except OSError:",
            "        continue",
            "    if 'real_data_runner' in raw and runtime_health_path in raw:",
            "        matches.append(pid)",
            "",
            "if not matches:",
            "    raise SystemExit(f'no matching smoke backend process found on port {port} for {runtime_health_path}')",
            "",
            "for pid in matches:",
            "    os.kill(pid, signal.SIGTERM)",
            "",
            "deadline = time.time() + 10",
            "while time.time() < deadline:",
            "    if not any(pid in listener_pids(port) for pid in matches):",
            "        break",
            "    time.sleep(0.2)",
            "else:",
            "    for pid in matches:",
            "        try:",
            "            os.kill(pid, signal.SIGKILL)",
            "        except ProcessLookupError:",
            "            pass",
            "PY",
            'exec "$backend_script"',
            "",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)
    return path


def write_multi_trader_smoke_record_workflow_script(path: Path, *, workflows_path: Path, repo_root: Path) -> Path:
    content = "\n".join(
        [
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            'if [ "$#" -lt 1 ]; then',
            '  echo "usage: $0 <workflow> [record-workflow-extra-args...]" >&2',
            "  exit 2",
            "fi",
            'workflow="$1"',
            "shift",
            f"cd {shlex.quote(str(repo_root))}",
            "PYTHONPATH=backend python -m beast_market.ops_cli record-multi-trader-smoke-workflow "
            f"--workflows-path {shlex.quote(str(workflows_path))} --workflow \"$workflow\" "
            '--observed-at "$(date --iso-8601=seconds)" "$@"',
            "",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)
    return path


def write_multi_trader_smoke_import_script(path: Path, *, root_path: Path, repo_root: Path) -> Path:
    content = "\n".join(
        [
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            'if [ "$#" -lt 1 ]; then',
            '  echo "usage: $0 <downloads-file-or-dir> [auto|client|performance]" >&2',
            "  exit 2",
            "fi",
            f"cd {shlex.quote(str(repo_root))}",
            'kind="${2:-auto}"',
            "PYTHONPATH=backend python -m beast_market.ops_cli import-multi-trader-smoke-artifacts "
            f"--root-path {shlex.quote(str(root_path))} --input-path \"$1\" --kind \"$kind\"",
            "",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)
    return path


def resolve_multi_trader_smoke_root_path(
    root_path: str | Path,
    *,
    auto_root_base_path: str | Path = "artifacts/multi-trader-smoke",
) -> Path:
    raw = str(root_path).strip()
    if raw.lower() != "auto":
        return Path(root_path).expanduser().resolve()
    base = Path(auto_root_base_path)
    if not base.is_absolute():
        base = repository_root() / base
    stamp = datetime.now().astimezone().strftime("%Y%m%d-%H%M%S")
    candidate = base / stamp
    if not candidate.exists():
        return candidate
    for index in range(2, 1000):
        next_candidate = base / f"{stamp}-{index}"
        if not next_candidate.exists():
            return next_candidate
    raise ValueError(f"could not allocate unique smoke root under {base}")


def resolve_smoke_port(
    value: int | str,
    *,
    default_port: int,
    reserved_ports: set[int] | None = None,
) -> tuple[int, bool]:
    reserved = reserved_ports or set()
    if isinstance(value, str) and value.strip().lower() == "auto":
        port = default_port
        while port in reserved or not port_bind_available(port):
            port += 1
            if port > 65535:
                raise ValueError("could not allocate free smoke port")
        return port, port != default_port
    try:
        return int(value), False
    except (TypeError, ValueError) as error:
        raise ValueError(f"invalid smoke port: {value}") from error


def multi_trader_smoke_runtime_symbols(*symbols: str) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        value = str(symbol or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def multi_trader_smoke_frontend_initial_symbols(
    *,
    runtime_symbols: list[str],
    cold_query_symbol: str = "",
    add_to_watchlist_symbol: str = "",
    fallback_symbol: str = "",
) -> list[str]:
    excluded = {symbol for symbol in [cold_query_symbol, add_to_watchlist_symbol] if symbol}
    initial = [symbol for symbol in runtime_symbols if symbol and symbol not in excluded]
    if initial:
        return initial
    if fallback_symbol:
        return [fallback_symbol]
    if runtime_symbols:
        return [runtime_symbols[0]]
    return ["00700.HK"]


def canonical_smoke_symbol(value: str) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    if "." not in text and text.isdigit():
        text = f"{text.zfill(5)}.HK"
    if not (len(text) == 8 and text[:5].isdigit() and text[5:] == ".HK"):
        raise ValueError(f"smoke symbol must use canonical HK format 00700.HK: {value}")
    return text


def multi_trader_smoke_frontend_env(*, gateway_url: str, symbols: list[str]) -> dict[str, str]:
    return {
        "VITE_MARKET_DATA_MODE": "live",
        "VITE_MARKET_WS_URL": gateway_url,
        "VITE_MARKET_PROTOCOL": "terminal-message-v1",
        "VITE_MARKET_SYMBOLS": ",".join(symbols),
    }


def multi_trader_smoke_redis_clear_command(
    *,
    trade_date: str,
    symbol: str,
    redis_url: str = "redis://127.0.0.1:6379/0",
    dry_run: bool,
) -> str:
    date = trade_date if trade_date else "<trade-date>"
    target_symbol = symbol if symbol else "<symbol>"
    parts = [
        "PYTHONPATH=backend",
        "python",
        "-m",
        "beast_market.ops_cli",
        "clear-runtime-cache",
        "--trade-date",
        date,
        "--symbols",
        target_symbol,
        "--redis-url",
        redis_url,
    ]
    parts.append("--dry-run" if dry_run else "--confirm")
    return " ".join(shlex.quote(part) for part in parts)


def multi_trader_smoke_frontend_command(
    *,
    frontend_port: int,
    package_command: str,
    env: dict[str, str],
) -> str:
    env_prefix = " ".join(f"{key}={shlex.quote(value)}" for key, value in env.items())
    return f"cd market-terminal && {env_prefix} {package_command} -- --port {frontend_port} --strictPort"


def multi_trader_smoke_backend_command(
    *,
    gateway_port: int,
    runtime_health_path: Path,
    requested_trade_date: str,
    symbols: list[str],
    silver_root: str | Path | None = None,
) -> str:
    parts = [
        "PYTHONPATH=backend",
        "python",
        "-m",
        "backend.tools.real_data_runner",
    ]
    if requested_trade_date:
        parts.extend(["--trade-date", requested_trade_date])
    if symbols:
        parts.extend(["--symbols", ",".join(symbols)])
    if silver_root is not None and str(silver_root).strip():
        parts.extend(["--silver-root", str(silver_root)])
    parts.extend(
        [
            "--host",
            "0.0.0.0",
            "--port",
            str(gateway_port),
            "--runtime-health-path",
            str(runtime_health_path),
        ]
    )
    return " ".join(shlex.quote(part) for part in parts)


SERVICE_CHECK_BLOCKERS = {
    "multi_trader_smoke_frontend_service_unreachable",
    "multi_trader_smoke_gateway_service_unreachable",
    "multi_trader_smoke_service_preflight_invalid",
}


def verify_multi_trader_smoke_services(
    *,
    root_path: str | Path,
    timeout_seconds: float = 1.0,
) -> MultiTraderSmokeServiceCheckResult:
    root = Path(root_path)
    preflight_path = root / "lan-preflight.json"
    output_path = root / "service-preflight.json"
    preflight = load_json_file(preflight_path)
    if not isinstance(preflight, dict):
        raise ValueError("lan-preflight.json must contain a JSON object")

    service_checks = build_multi_trader_smoke_service_checks(preflight, timeout_seconds=timeout_seconds)
    existing_blockers = [
        str(blocker)
        for blocker in preflight.get("blockers", [])
        if str(blocker) not in SERVICE_CHECK_BLOCKERS
    ]
    blockers = sorted(set(existing_blockers + list(service_checks["blockers"])))
    preflight["service_checks"] = service_checks
    preflight["blockers"] = blockers
    preflight["passed"] = not blockers

    output_path.write_text(
        json.dumps(service_checks, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    preflight_path.write_text(
        json.dumps(preflight, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return MultiTraderSmokeServiceCheckResult(
        passed=preflight["passed"],
        root_path=root,
        preflight_path=preflight_path,
        output_path=output_path,
        preflight=preflight,
        service_checks=service_checks,
    )


def build_multi_trader_smoke_service_checks(
    preflight: dict[str, Any],
    *,
    timeout_seconds: float,
) -> dict[str, Any]:
    checks: dict[str, Any] = {}
    blockers: list[str] = []
    for name, url_key, probe_kind, blocker in (
        ("frontend", "page_url", "frontend_http", "multi_trader_smoke_frontend_service_unreachable"),
        ("gateway", "gateway_url", "gateway_websocket", "multi_trader_smoke_gateway_service_unreachable"),
    ):
        probe = probe_service_url(preflight.get(url_key), timeout_seconds=timeout_seconds, probe_kind=probe_kind)
        checks[name] = probe
        if probe.get("reachable") is not True:
            blockers.append(blocker)
    return {
        "schema_version": 1,
        "checked_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "passed": not blockers,
        "blockers": sorted(set(blockers)),
        "timeout_seconds": timeout_seconds,
        "checks": checks,
    }


def probe_service_url(url: Any, *, timeout_seconds: float, probe_kind: str) -> dict[str, Any]:
    raw_url = str(url or "").strip()
    parsed = urlparse(raw_url)
    host = parsed.hostname or ""
    try:
        port = parsed.port
    except ValueError:
        port = None
    if not raw_url or not host or port is None:
        return {
            "url": raw_url,
            "host": host,
            "port": port,
            "probe_kind": probe_kind,
            "reachable": False,
            "error": "invalid_url",
        }
    if probe_kind == "frontend_http":
        return probe_http_service(parsed, host, port, timeout_seconds=timeout_seconds, url=raw_url)
    if probe_kind == "gateway_websocket":
        return probe_websocket_service(parsed, host, port, timeout_seconds=timeout_seconds, url=raw_url)
    return {
        "url": raw_url,
        "host": host,
        "port": port,
        "probe_kind": probe_kind,
        "reachable": False,
        "error": "unsupported_probe_kind",
    }


def probe_http_service(parsed_url: Any, host: str, port: int, *, timeout_seconds: float, url: str = "") -> dict[str, Any]:
    path = parsed_url.path or "/"
    if parsed_url.query:
        path = f"{path}?{parsed_url.query}"
    request = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {host}:{port}\r\n"
        "Connection: close\r\n"
        "\r\n"
    ).encode("ascii")
    try:
        with socket.create_connection((host, port), timeout=timeout_seconds) as connection:
            connection.settimeout(timeout_seconds)
            connection.sendall(request)
            response = connection.recv(256).decode("iso-8859-1", errors="replace")
    except OSError as error:
        return {
            "url": url,
            "host": host,
            "port": port,
            "probe_kind": "frontend_http",
            "reachable": False,
            "error": str(error),
        }
    status_code = parse_http_status_code(response)
    return {
        "url": url,
        "host": host,
        "port": port,
        "probe_kind": "frontend_http",
        "reachable": status_code is not None and 200 <= status_code < 400,
        "status_code": status_code,
        "error": "" if status_code is not None and 200 <= status_code < 400 else "unexpected_http_status",
    }


def probe_websocket_service(parsed_url: Any, host: str, port: int, *, timeout_seconds: float, url: str = "") -> dict[str, Any]:
    if parsed_url.scheme != "ws":
        return {
            "url": url,
            "host": host,
            "port": port,
            "probe_kind": "gateway_websocket",
            "reachable": False,
            "websocket_handshake": False,
            "error": "unsupported_websocket_scheme",
        }
    path = parsed_url.path or "/"
    if parsed_url.query:
        path = f"{path}?{parsed_url.query}"
    request = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {host}:{port}\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        "\r\n"
    ).encode("ascii")
    try:
        with socket.create_connection((host, port), timeout=timeout_seconds) as connection:
            connection.settimeout(timeout_seconds)
            connection.sendall(request)
            response = connection.recv(512).decode("iso-8859-1", errors="replace")
    except OSError as error:
        return {
            "url": url,
            "host": host,
            "port": port,
            "probe_kind": "gateway_websocket",
            "reachable": False,
            "websocket_handshake": False,
            "error": str(error),
        }
    status_code = parse_http_status_code(response)
    handshake = status_code == 101 and "upgrade: websocket" in response.lower()
    return {
        "url": url,
        "host": host,
        "port": port,
        "probe_kind": "gateway_websocket",
        "reachable": handshake,
        "websocket_handshake": handshake,
        "status_code": status_code,
        "error": "" if handshake else "websocket_handshake_failed",
    }


def parse_http_status_code(response: str) -> int | None:
    first_line = response.splitlines()[0] if response.splitlines() else ""
    parts = first_line.split()
    if len(parts) < 2 or not parts[0].startswith("HTTP/"):
        return None
    try:
        return int(parts[1])
    except ValueError:
        return None


def probe_tcp_service(host: str, port: int, *, timeout_seconds: float, url: str = "") -> dict[str, Any]:
    try:
        with socket.create_connection((host, port), timeout=timeout_seconds):
            return {
                "url": url,
                "host": host,
                "port": port,
                "reachable": True,
                "error": "",
            }
    except OSError as error:
        return {
            "url": url,
            "host": host,
            "port": port,
            "reachable": False,
            "error": str(error),
        }


def finalize_multi_trader_smoke(
    *,
    root_path: str | Path,
    observed_at: str,
) -> MultiTraderSmokeFinalizeResult:
    root = Path(root_path)
    clients_path = root / "clients"
    performance_path = root / "performance"
    workflows_path = root / "workflows.json"
    preflight_path = root / "lan-preflight.json"
    service_preflight_path = root / "service-preflight.json"
    runtime_health_path = root / "runtime-health-verification.json"
    observation_path = root / "multi-trader-smoke-observation.json"
    evidence_path = root / "multi-trader-smoke-evidence.json"
    manifest_path = root / "smoke-run-manifest.json"
    if not observed_at:
        return failed_multi_trader_smoke_finalize_result(
            root_path=root,
            observation_path=observation_path,
            evidence_path=evidence_path,
            manifest_path=manifest_path,
            observed_at=observed_at,
            blockers=["multi_trader_smoke_observed_at_missing"],
        )
    if not is_iso_datetime(observed_at):
        return failed_multi_trader_smoke_finalize_result(
            root_path=root,
            observation_path=observation_path,
            evidence_path=evidence_path,
            manifest_path=manifest_path,
            observed_at=observed_at,
            blockers=["multi_trader_smoke_observed_at_invalid"],
        )
    precheck = multi_trader_smoke_finalize_precheck(
        clients_path=clients_path,
        performance_path=performance_path,
        workflows_path=workflows_path,
        runtime_health_path=runtime_health_path,
        preflight_path=preflight_path,
        service_preflight_path=service_preflight_path,
    )
    if precheck["blockers"]:
        return failed_multi_trader_smoke_finalize_result(
            root_path=root,
            observation_path=observation_path,
            evidence_path=evidence_path,
            manifest_path=manifest_path,
            observed_at=observed_at,
            blockers=precheck["blockers"],
            missing_paths=precheck["missing_paths"],
            invalid_paths=precheck["invalid_paths"],
        )
    try:
        build_multi_trader_smoke_observation(
            clients_path=clients_path,
            performance_samples_path=multi_trader_smoke_performance_path(clients_path, performance_path),
            workflows_path=workflows_path,
            runtime_health_path=runtime_health_path,
            preflight_path=preflight_path,
            observed_at=observed_at,
            output_path=observation_path,
        )
    except (OSError, ValueError, json.JSONDecodeError) as error:
        return failed_multi_trader_smoke_finalize_result(
            root_path=root,
            observation_path=observation_path,
            evidence_path=evidence_path,
            manifest_path=manifest_path,
            observed_at=observed_at,
            blockers=["multi_trader_smoke_finalize_failed"],
            errors=[str(error)],
        )
    evidence = write_multi_trader_smoke_evidence(
        observation_path=observation_path,
        output_path=evidence_path,
    )
    return MultiTraderSmokeFinalizeResult(
        evidence=evidence,
        root_path=root,
        observation_path=observation_path,
        evidence_path=evidence_path,
        manifest_path=write_multi_trader_smoke_manifest(root),
    )


def multi_trader_smoke_finalize_precheck(
    *,
    clients_path: Path,
    performance_path: Path,
    workflows_path: Path,
    runtime_health_path: Path,
    preflight_path: Path,
    service_preflight_path: Path,
) -> dict[str, list[str]]:
    blockers: list[str] = []
    missing_paths: list[str] = []
    invalid_paths: list[str] = []
    required_files = [
        ("multi_trader_smoke_workflows_missing", workflows_path),
        ("multi_trader_smoke_runtime_health_missing", runtime_health_path),
        ("multi_trader_smoke_preflight_missing", preflight_path),
        ("multi_trader_smoke_service_preflight_missing", service_preflight_path),
    ]
    for blocker, path in required_files:
        if not path.is_file():
            blockers.append(blocker)
            missing_paths.append(str(path))
        elif not json_artifact_parseable(path):
            blockers.append(blocker.replace("_missing", "_invalid"))
            invalid_paths.append(str(path))
    if not json_artifacts_present(clients_path):
        blockers.append("multi_trader_smoke_clients_missing")
        missing_paths.append(str(clients_path))
    else:
        for path in resolve_artifact_paths(clients_path):
            if not json_artifact_parseable(path):
                blockers.append("multi_trader_smoke_clients_invalid")
                invalid_paths.append(str(path))
                continue
            try:
                validate_multi_trader_smoke_client_artifact(load_json_file(path))
            except ValueError:
                blockers.append("multi_trader_smoke_clients_invalid")
                invalid_paths.append(str(path))
    if json_artifacts_present(performance_path):
        for path in resolve_artifact_paths(performance_path):
            if not json_artifact_parseable(path):
                blockers.append("multi_trader_smoke_performance_invalid")
                invalid_paths.append(str(path))
                continue
            try:
                validate_multi_trader_smoke_performance_artifact(load_json_file(path))
            except ValueError:
                blockers.append("multi_trader_smoke_performance_invalid")
                invalid_paths.append(str(path))
    if preflight_path.is_file() and service_preflight_path.is_file():
        try:
            preflight_payload = load_json_file(preflight_path)
            service_payload = load_json_file(service_preflight_path)
            if not isinstance(preflight_payload, dict) or not isinstance(service_payload, dict):
                blockers.append("multi_trader_smoke_service_preflight_invalid")
                invalid_paths.append(str(service_preflight_path))
            elif preflight_payload.get("service_checks") != service_payload:
                blockers.append("multi_trader_smoke_service_preflight_mismatch")
                invalid_paths.append(str(service_preflight_path))
        except (OSError, json.JSONDecodeError):
            blockers.append("multi_trader_smoke_service_preflight_invalid")
            invalid_paths.append(str(service_preflight_path))
    return {
        "blockers": sorted(set(blockers)),
        "missing_paths": missing_paths,
        "invalid_paths": invalid_paths,
    }


def inspect_multi_trader_smoke_readiness(*, root_path: str | Path) -> MultiTraderSmokeReadinessResult:
    root = Path(root_path)
    clients_path = root / "clients"
    performance_path = root / "performance"
    workflows_path = root / "workflows.json"
    preflight_path = root / "lan-preflight.json"
    service_preflight_path = root / "service-preflight.json"
    runtime_health_path = root / "runtime-health-verification.json"
    precheck = multi_trader_smoke_finalize_precheck(
        clients_path=clients_path,
        performance_path=performance_path,
        workflows_path=workflows_path,
        runtime_health_path=runtime_health_path,
        preflight_path=preflight_path,
        service_preflight_path=service_preflight_path,
    )
    workflow_summary = multi_trader_smoke_workflow_readiness(workflows_path)
    preflight_summary = multi_trader_smoke_preflight_readiness(preflight_path)
    runtime_health_summary = multi_trader_smoke_runtime_health_readiness(runtime_health_path)
    package_summary = multi_trader_smoke_package_readiness(root)
    blockers = sorted(set(precheck["blockers"] + workflow_summary["blockers"] + runtime_health_summary["blockers"]))
    smoke_preview: dict[str, Any] | None = None
    if not precheck["missing_paths"] and not precheck["invalid_paths"]:
        try:
            preview_observation = build_multi_trader_smoke_observation_payload(
                clients_path=clients_path,
                workflows_path=workflows_path,
                runtime_health_path=runtime_health_path,
                performance_samples_path=multi_trader_smoke_performance_path(clients_path, performance_path),
                preflight_path=preflight_path,
                observed_at=datetime.now().astimezone().isoformat(),
            )
            smoke_preview = evaluate_multi_trader_smoke(preview_observation)
            gateway_client_activity = (
                smoke_preview.get("gateway_client_activity")
                if isinstance(smoke_preview.get("gateway_client_activity"), dict)
                else {}
            )
            if gateway_client_activity:
                runtime_health_summary["observed_declared_client_ids"] = list(
                    gateway_client_activity.get("observed_declared_client_ids") or []
                )
                runtime_health_summary["missing_declared_client_machines"] = list(
                    gateway_client_activity.get("missing_declared_client_machines") or []
                )
                runtime_health_summary["required_client_count"] = gateway_client_activity.get("required_client_count")
            blockers = sorted(set(blockers + list(smoke_preview.get("blockers") or [])))
        except (OSError, ValueError, json.JSONDecodeError) as error:
            smoke_preview = {
                "passed": False,
                "blockers": ["multi_trader_smoke_inspect_preview_failed"],
                "error": str(error),
            }
            blockers = sorted(set(blockers + ["multi_trader_smoke_inspect_preview_failed"]))
    summary: dict[str, Any] = {
        "schema_version": 1,
        "root_path": str(root),
        "ready": not blockers,
        "blockers": blockers,
        "missing_paths": precheck["missing_paths"],
        "invalid_paths": precheck["invalid_paths"],
        "artifact_counts": {
            "clients": len(resolve_artifact_paths(clients_path)) if json_artifacts_present(clients_path) else 0,
            "performance": len(resolve_artifact_paths(performance_path)) if json_artifacts_present(performance_path) else 0,
        },
        "workflows": workflow_summary,
        "preflight_readiness": preflight_summary,
        "runtime_health_readiness": runtime_health_summary,
        "package_readiness": package_summary,
        "paths": {
            "clients": str(clients_path),
            "performance": str(performance_path),
            "workflows": str(workflows_path),
            "preflight": str(preflight_path),
            "service_preflight": str(root / "service-preflight.json"),
            "runtime_health": str(runtime_health_path),
            "observation": str(root / "multi-trader-smoke-observation.json"),
            "evidence": str(root / "multi-trader-smoke-evidence.json"),
            "manifest": str(root / "smoke-run-manifest.json"),
            "package": str(root / "multi-trader-smoke-evidence.zip"),
            "package_metadata": str(root / "smoke-run-package.json"),
        },
    }
    if smoke_preview is not None:
        summary["smoke_preview"] = smoke_preview
    summary["next_actions"] = multi_trader_smoke_next_actions(summary)
    return MultiTraderSmokeReadinessResult(ready=not blockers, root_path=root, summary=summary)


def multi_trader_smoke_next_actions(summary: dict[str, Any]) -> list[dict[str, str]]:
    root_path = str(summary.get("root_path") or "")
    blockers = set(summary.get("blockers") or [])
    missing_paths = set(summary.get("missing_paths") or [])
    paths = summary.get("paths") if isinstance(summary.get("paths"), dict) else {}
    workflows = summary.get("workflows") if isinstance(summary.get("workflows"), dict) else {}
    preflight = summary.get("preflight_readiness") if isinstance(summary.get("preflight_readiness"), dict) else {}
    preflight_commands = preflight.get("commands") if isinstance(preflight.get("commands"), dict) else {}
    preflight_artifact_paths = preflight.get("artifact_paths") if isinstance(preflight.get("artifact_paths"), dict) else {}
    client_url = str(preflight_commands.get("client_url") or preflight.get("page_url") or "")
    client_instructions_path = str(preflight_artifact_paths.get("client_instructions") or "")
    backend_script = str(preflight_commands.get("backend_script") or "")
    frontend_script = str(preflight_commands.get("frontend_script") or "")
    service_preflight_script = str(preflight_commands.get("service_preflight_script") or "")
    ports = preflight.get("ports") if isinstance(preflight.get("ports"), dict) else {}
    gateway_port = ports.get("gateway") if isinstance(ports.get("gateway"), dict) else {}
    record_workflow_script = str(preflight_commands.get("record_workflow_script") or "")
    import_artifacts_script = str(preflight_commands.get("import_artifacts_script") or "")
    finalize_package_script = str(preflight_commands.get("finalize_package_script") or "")
    verify_handoff_script = str(preflight_commands.get("verify_handoff_script") or "")
    package = summary.get("package_readiness") if isinstance(summary.get("package_readiness"), dict) else {}
    package_blockers = set(package.get("blockers") or []) if isinstance(package, dict) else set()
    actions: list[dict[str, str]] = []

    if "multi_trader_smoke_preflight_missing" in blockers or str(paths.get("preflight") or "") in missing_paths:
        actions.append(
            {
                "stage": "prepare",
                "reason": "multi_trader_smoke_preflight_missing",
                "command": "PYTHONPATH=backend python -m beast_market.ops_cli prepare-multi-trader-smoke --root-path auto --lan-host auto --require-local-lan-host",
            }
        )
        return actions

    if "multi_trader_smoke_runtime_health_missing" in blockers:
        runtime_command = str(
            backend_script
            or preflight_commands.get("backend")
            or "start backend.tools.real_data_runner with --runtime-health-path from lan-preflight.json"
        )
        if preflight.get("service_checks_passed") is True and gateway_port.get("bind_available") is False:
            listeners = gateway_port.get("listeners") if isinstance(gateway_port.get("listeners"), list) else []
            listener_summary = ""
            if listeners:
                listener_summary = "; observed listener(s): " + ", ".join(
                    f"pid={listener.get('pid')} {listener.get('command')}".strip()
                    for listener in listeners
                    if isinstance(listener, dict)
                )
            runtime_command = (
                "prepared Gateway service is reachable but this smoke runtime health file is missing; "
                f"stop the process using port {gateway_port.get('port')} and run {runtime_command}, "
                "or restart the existing service with this smoke directory's --runtime-health-path"
                f"{listener_summary}"
            )
        actions.append(
            {
                "stage": "runtime_health",
                "reason": "multi_trader_smoke_runtime_health_missing",
                "command": runtime_command,
            }
        )

    if preflight.get("service_checks_passed") is not True:
        actions.append(
            {
                "stage": "frontend",
                "reason": "multi_trader_smoke_frontend_service_not_ready",
                "command": str(
                    frontend_script
                    or preflight_commands.get("frontend")
                    or "cd market-terminal && npm run dev"
                ),
            }
        )
        actions.append(
            {
                "stage": "service_preflight",
                "reason": "multi_trader_smoke_service_preflight_not_ready",
                "command": str(
                    service_preflight_script
                    or preflight_commands.get("service_preflight")
                    or f'PYTHONPATH=backend python -m beast_market.ops_cli verify-multi-trader-smoke-services --root-path "{root_path}"'
                ),
            }
        )

    data_quality_blockers = {
        "multi_trader_smoke_minute_bars_missing",
    }
    if blockers.intersection(data_quality_blockers):
        actions.append(
            {
                "stage": "data_quality",
                "reason": ",".join(sorted(blockers.intersection(data_quality_blockers))),
                "command": "generate or repair silver_minute_bars_v1 for every smoke runtime symbol, then restart the backend with this smoke directory's --runtime-health-path",
            }
        )

    if "multi_trader_smoke_clients_missing" in blockers:
        actions.append(
            {
                "stage": "client_activity",
                "reason": "multi_trader_smoke_clients_missing",
                "command": "open the prepared LAN client URL from at least two machines, subscribe overlapping watchlists, then export client and performance smoke JSON",
                "url": client_url,
                "instructions_path": client_instructions_path,
            }
        )
        actions.append(
            {
                "stage": "client_artifacts",
                "reason": "multi_trader_smoke_clients_missing",
                "command": (
                    f"{import_artifacts_script} <downloads-file-or-dir>"
                    if import_artifacts_script
                    else f'PYTHONPATH=backend python -m beast_market.ops_cli import-multi-trader-smoke-artifacts --root-path "{root_path}" --input-path <downloads-dir>'
                ),
                "url": client_url,
            }
        )
    elif "multi_trader_smoke_clients_invalid" in blockers:
        actions.append(
            {
                "stage": "client_artifacts",
                "reason": "multi_trader_smoke_clients_invalid",
                "command": "fix live LAN frontend state, export fresh client smoke JSON, then import again",
            }
        )

    gateway_client_activity_blockers = {
        "multi_trader_smoke_gateway_client_activity_missing",
        "multi_trader_smoke_gateway_declared_client_activity_missing",
        "multi_trader_smoke_gateway_declared_client_coverage_missing",
        "multi_trader_smoke_gateway_observed_clients_insufficient",
        "multi_trader_smoke_gateway_max_connected_clients_insufficient",
    }
    if blockers.intersection(gateway_client_activity_blockers) and not any(action["stage"] == "client_activity" for action in actions):
        actions.append(
            {
                "stage": "client_activity",
                "reason": ",".join(sorted(blockers.intersection(gateway_client_activity_blockers))),
                "command": "open the prepared LAN client URL from each exported machine_id, subscribe the smoke watchlists, then wait for runtime health to record both clients",
                "url": client_url,
                "instructions_path": client_instructions_path,
            }
        )

    if "multi_trader_smoke_performance_invalid" in blockers:
        actions.append(
            {
                "stage": "performance_artifacts",
                "reason": "multi_trader_smoke_performance_invalid",
                "command": "export fresh performance smoke JSON from each client, then import again",
            }
        )

    incomplete = workflows.get("incomplete") if isinstance(workflows.get("incomplete"), list) else []
    missing = workflows.get("missing") if isinstance(workflows.get("missing"), list) else []
    if incomplete or missing:
        actions.append(
            {
                "stage": "workflows",
                "reason": "multi_trader_smoke_workflows_incomplete",
                "command": (
                    f"{record_workflow_script} <workflow> [workflow args...]"
                    if record_workflow_script
                    else f'PYTHONPATH=backend python -m beast_market.ops_cli record-multi-trader-smoke-workflow --workflows-path "{paths.get("workflows", "<smoke-dir>/workflows.json")}" --workflow <workflow> --observed-at "$(date --iso-8601=seconds)"'
                ),
            }
        )
    elif workflows.get("evidence_blockers"):
        actions.append(
            {
                "stage": "workflows",
                "reason": "multi_trader_smoke_workflow_evidence_invalid",
                "command": "rerun the affected workflow and record it with the required symbol/date evidence",
            }
        )

    if not actions and summary.get("ready") is True and "multi_trader_smoke_evidence_missing_for_package" in package_blockers:
        actions.append(
            {
                "stage": "finalize",
                "reason": "multi_trader_smoke_evidence_missing_for_package",
                "command": (
                    finalize_package_script
                    if finalize_package_script
                    else f'PYTHONPATH=backend python -m beast_market.ops_cli finalize-multi-trader-smoke --root-path "{root_path}" --observed-at "$(date --iso-8601=seconds)" --package'
                ),
            }
        )
    elif not actions and summary.get("ready") is True and package.get("ready") is not True:
        actions.append(
            {
                "stage": "package",
                "reason": ",".join(sorted(package_blockers)) or "multi_trader_smoke_package_not_ready",
                "command": (
                    finalize_package_script
                    if finalize_package_script
                    else f'PYTHONPATH=backend python -m beast_market.ops_cli finalize-multi-trader-smoke --root-path "{root_path}" --observed-at "$(date --iso-8601=seconds)" --package'
                ),
            }
        )

    if not actions and summary.get("ready") is True and package.get("ready") is True:
        command = "submit multi-trader-smoke-evidence.zip with smoke-run-package.json and bundle evidence"
        if verify_handoff_script:
            command = f"{verify_handoff_script} && {command}"
        actions.append(
            {
                "stage": "handoff",
                "reason": "multi_trader_smoke_ready",
                "command": command,
            }
        )

    return actions


def multi_trader_smoke_preflight_readiness(preflight_path: Path) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "path": str(preflight_path),
        "present": preflight_path.is_file(),
        "parseable": False,
        "passed": False,
        "service_checks_present": False,
        "service_checks_passed": False,
        "blockers": [],
    }
    blockers: list[str] = []
    if not preflight_path.is_file():
        blockers.append("multi_trader_smoke_preflight_missing")
    elif not json_artifact_parseable(preflight_path):
        blockers.append("multi_trader_smoke_preflight_invalid")
    else:
        payload = load_json_file(preflight_path)
        if not isinstance(payload, dict):
            blockers.append("multi_trader_smoke_preflight_invalid")
        else:
            summary["parseable"] = True
            if isinstance(payload.get("page_url"), str):
                summary["page_url"] = payload["page_url"]
            if isinstance(payload.get("gateway_url"), str):
                summary["gateway_url"] = payload["gateway_url"]
            ports: dict[str, dict[str, Any]] = {}
            frontend_port = payload.get("frontend_port")
            if isinstance(frontend_port, int):
                ports["frontend"] = port_readiness(frontend_port)
            gateway_port = payload.get("gateway_port")
            if isinstance(gateway_port, int):
                ports["gateway"] = port_readiness(gateway_port)
            if ports:
                summary["ports"] = ports
            commands = payload.get("commands")
            if isinstance(commands, dict):
                summary["commands"] = {str(key): str(value) for key, value in commands.items() if isinstance(value, str)}
            artifact_paths = payload.get("artifact_paths")
            if isinstance(artifact_paths, dict):
                summary["artifact_paths"] = {
                    str(key): str(value) for key, value in artifact_paths.items() if isinstance(value, str)
                }
            service_checks = payload.get("service_checks")
            summary["passed"] = payload.get("passed") is True
            summary["service_checks_present"] = isinstance(service_checks, dict)
            summary["service_checks_passed"] = isinstance(service_checks, dict) and service_checks.get("passed") is True
            blockers.extend(validate_multi_trader_smoke_preflight(preflight=payload, frontend_live_url=None))
    summary["blockers"] = sorted(set(blockers))
    return summary


def port_bind_available(port: int) -> bool:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind(("0.0.0.0", port))
    except OSError:
        return False
    finally:
        sock.close()
    return True


def port_readiness(port: int) -> dict[str, Any]:
    bind_available = port_bind_available(port)
    summary: dict[str, Any] = {"port": port, "bind_available": bind_available}
    if not bind_available:
        listeners = tcp_listeners_for_port(port)
        if listeners:
            summary["listeners"] = listeners
    return summary


def tcp_listeners_for_port(port: int) -> list[dict[str, Any]]:
    inodes = tcp_listener_inodes_for_port(port)
    if not inodes:
        return []
    listeners: list[dict[str, Any]] = []
    proc_root = Path("/proc")
    for proc_dir in proc_root.iterdir():
        if not proc_dir.name.isdigit():
            continue
        fd_dir = proc_dir / "fd"
        if not fd_dir.is_dir():
            continue
        try:
            fd_paths = list(fd_dir.iterdir())
        except OSError:
            continue
        matched = False
        for fd_path in fd_paths:
            try:
                target = os.readlink(fd_path)
            except OSError:
                continue
            if target.startswith("socket:[") and target.removeprefix("socket:[").removesuffix("]") in inodes:
                matched = True
                break
        if not matched:
            continue
        command = ""
        try:
            command = (proc_dir / "comm").read_text(encoding="utf-8").strip()
        except OSError:
            pass
        listeners.append({"pid": int(proc_dir.name), "command": command})
    return sorted(listeners, key=lambda item: int(item["pid"]))


def tcp_listener_inodes_for_port(port: int) -> set[str]:
    inodes: set[str] = set()
    for path in (Path("/proc/net/tcp"), Path("/proc/net/tcp6")):
        try:
            lines = path.read_text(encoding="utf-8").splitlines()[1:]
        except OSError:
            continue
        for line in lines:
            fields = line.split()
            if len(fields) < 10 or fields[3] != "0A":
                continue
            local_address = fields[1]
            _, _, port_hex = local_address.rpartition(":")
            try:
                local_port = int(port_hex, 16)
            except ValueError:
                continue
            if local_port == port:
                inodes.add(fields[9])
    return inodes


def multi_trader_smoke_runtime_health_readiness(runtime_health_path: Path) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "path": str(runtime_health_path),
        "present": runtime_health_path.is_file(),
        "parseable": False,
        "passed": False,
        "blockers": [],
    }
    blockers: list[str] = []
    if not runtime_health_path.is_file():
        blockers.append("multi_trader_smoke_runtime_health_missing")
    elif not json_artifact_parseable(runtime_health_path):
        blockers.append("multi_trader_smoke_runtime_health_invalid")
    else:
        payload = load_json_file(runtime_health_path)
        if not isinstance(payload, dict):
            blockers.append("multi_trader_smoke_runtime_health_invalid")
        else:
            summary["parseable"] = True
            summary["passed"] = payload.get("passed") is True
            summary["generated_at"] = str(payload.get("generated_at") or "")
            payload_blockers = payload.get("blockers")
            if isinstance(payload_blockers, list):
                summary["runtime_blockers"] = [str(blocker) for blocker in payload_blockers]
                blockers.extend(str(blocker) for blocker in payload_blockers)
            if payload.get("passed") is not True:
                blockers.append("multi_trader_smoke_runtime_health_not_passed")
            evidence = payload.get("evidence") if isinstance(payload.get("evidence"), dict) else payload
            gateway_activity = evidence.get("gateway_activity") if isinstance(evidence, dict) else {}
            client_queue = gateway_activity.get("client_queue") if isinstance(gateway_activity, dict) else {}
            if isinstance(client_queue, dict):
                observed_client_count = int(client_queue.get("observed_client_count") or 0)
                observed_declared_client_count = int(client_queue.get("observed_declared_client_count") or 0)
                max_connected_clients = int(client_queue.get("max_connected_clients") or 0)
                summary["observed_client_count"] = observed_client_count
                summary["observed_declared_client_count"] = observed_declared_client_count
                summary["max_connected_clients"] = max_connected_clients
                if observed_client_count < 2:
                    blockers.append("real_data_runner_insufficient_observed_clients")
                if observed_declared_client_count < 2:
                    blockers.append("real_data_runner_insufficient_declared_clients")
                if max_connected_clients < 2:
                    blockers.append("real_data_runner_max_connected_clients_insufficient")
            performance_samples = evidence.get("performance_samples") if isinstance(evidence, dict) else {}
            subscribe_samples = (
                performance_samples.get("subscribe_snapshot_ms")
                if isinstance(performance_samples, dict)
                else None
            )
            subscribe_snapshot_sample_count = len(subscribe_samples) if isinstance(subscribe_samples, list) else 0
            summary["subscribe_snapshot_sample_count"] = subscribe_snapshot_sample_count
            if isinstance(performance_samples, dict) and subscribe_snapshot_sample_count < 1:
                blockers.append("real_data_runner_subscribe_snapshot_samples_missing")
            symbol_runtime = evidence.get("symbol_runtime") if isinstance(evidence, dict) else {}
            if isinstance(symbol_runtime, dict):
                missing_minute_symbols: list[str] = []
                degraded_symbols: dict[str, list[str]] = {}
                for symbol, state in symbol_runtime.items():
                    if not isinstance(state, dict):
                        continue
                    freshness = state.get("freshness") if isinstance(state.get("freshness"), dict) else {}
                    source_dates = freshness.get("source_dates") if isinstance(freshness.get("source_dates"), dict) else {}
                    has_source_dates = isinstance(freshness.get("source_dates"), dict)
                    degraded_reasons = state.get("degraded_reasons")
                    reasons = [str(reason) for reason in degraded_reasons] if isinstance(degraded_reasons, list) else []
                    if "missing_minute_bars" in reasons or (
                        has_source_dates and not str(source_dates.get("minute_bars") or "")
                    ):
                        missing_minute_symbols.append(str(symbol))
                        degraded_symbols[str(symbol)] = reasons
                if missing_minute_symbols:
                    summary["missing_minute_bar_symbols"] = sorted(set(missing_minute_symbols))
                    summary["symbol_degraded_reasons"] = degraded_symbols
                    blockers.append("multi_trader_smoke_minute_bars_missing")
            if blockers:
                summary["passed"] = False
    summary["blockers"] = sorted(set(blockers))
    return summary


def multi_trader_smoke_package_readiness(root_path: Path) -> dict[str, Any]:
    evidence_path = root_path / "multi-trader-smoke-evidence.json"
    import_manifest_path = root_path / "smoke-import-manifest.json"
    blockers: list[str] = []
    evidence: dict[str, Any] = {
        "path": str(evidence_path),
        "present": evidence_path.is_file(),
        "passed": False,
    }
    smoke_observed_at = ""
    if not evidence_path.is_file():
        blockers.append("multi_trader_smoke_evidence_missing_for_package")
    elif not json_artifact_parseable(evidence_path):
        blockers.append("multi_trader_smoke_evidence_invalid_for_package")
    else:
        payload = load_json_file(evidence_path)
        evidence["passed"] = isinstance(payload, dict) and payload.get("passed") is True
        smoke_observed_at = str(payload.get("observed_at") or "") if isinstance(payload, dict) else ""
        evidence["observed_at"] = smoke_observed_at
        if evidence["passed"] is not True:
            blockers.append("multi_trader_smoke_evidence_not_passed_for_package")

    import_manifest: dict[str, Any] = {
        "path": str(import_manifest_path),
        "present": import_manifest_path.is_file(),
        "valid": False,
    }
    if not import_manifest_path.is_file():
        blockers.append("multi_trader_smoke_import_manifest_missing_for_package")
    elif not json_artifact_parseable(import_manifest_path):
        blockers.append("multi_trader_smoke_import_manifest_invalid_for_package")
    else:
        try:
            validate_smoke_import_manifest(load_json_file(import_manifest_path), root_path=root_path)
            import_manifest["valid"] = True
        except ValueError as error:
            import_manifest["error"] = str(error)
            blockers.append("multi_trader_smoke_import_manifest_invalid_for_package")

    preflight = multi_trader_smoke_package_preflight_readiness(root_path, smoke_observed_at=smoke_observed_at)
    blockers.extend(preflight["blockers"])

    run_manifest_path = root_path / "smoke-run-manifest.json"
    run_manifest: dict[str, Any] = {
        "path": str(run_manifest_path),
        "present": run_manifest_path.is_file(),
        "valid": False,
    }
    if not run_manifest_path.is_file():
        blockers.append("multi_trader_smoke_run_manifest_missing_for_package")
    elif not json_artifact_parseable(run_manifest_path):
        blockers.append("multi_trader_smoke_run_manifest_invalid_for_package")
    else:
        try:
            validate_smoke_run_manifest_for_package(
                root_path=root_path,
                metadata_path=root_path / "smoke-run-package.json",
            )
            run_manifest["valid"] = True
        except ValueError as error:
            run_manifest["error"] = str(error)
            blockers.append("multi_trader_smoke_run_manifest_invalid_for_package")

    return {
        "ready": not blockers,
        "blockers": sorted(set(blockers)),
        "evidence": evidence,
        "import_manifest": import_manifest,
        "preflight": preflight,
        "run_manifest": run_manifest,
    }


def multi_trader_smoke_package_preflight_readiness(root_path: Path, *, smoke_observed_at: str = "") -> dict[str, Any]:
    preflight_path = root_path / "lan-preflight.json"
    service_preflight_path = root_path / "service-preflight.json"
    summary: dict[str, Any] = {
        "preflight_path": str(preflight_path),
        "service_preflight_path": str(service_preflight_path),
        "preflight_present": preflight_path.is_file(),
        "service_preflight_present": service_preflight_path.is_file(),
        "valid": False,
        "blockers": [],
    }
    blockers: list[str] = []
    preflight_payload: Any = None
    service_payload: Any = None
    if not preflight_path.is_file():
        blockers.append("multi_trader_smoke_preflight_missing_for_package")
    elif not json_artifact_parseable(preflight_path):
        blockers.append("multi_trader_smoke_preflight_invalid_for_package")
    else:
        preflight_payload = load_json_file(preflight_path)
        if not isinstance(preflight_payload, dict):
            blockers.append("multi_trader_smoke_preflight_invalid_for_package")
        else:
            preflight_blockers = validate_multi_trader_smoke_preflight(preflight=preflight_payload, frontend_live_url=None)
            if preflight_blockers:
                blockers.append("multi_trader_smoke_preflight_not_passed_for_package")
                summary["preflight_blockers"] = preflight_blockers

    if not service_preflight_path.is_file():
        blockers.append("multi_trader_smoke_service_preflight_missing_for_package")
    elif not json_artifact_parseable(service_preflight_path):
        blockers.append("multi_trader_smoke_service_preflight_invalid_for_package")
    else:
        service_payload = load_json_file(service_preflight_path)
        if not isinstance(service_payload, dict) or service_payload.get("schema_version") != 1:
            blockers.append("multi_trader_smoke_service_preflight_invalid_for_package")
        elif service_payload.get("passed") is not True:
            blockers.append("multi_trader_smoke_service_preflight_not_passed_for_package")

    if isinstance(preflight_payload, dict) and isinstance(service_payload, dict):
        if preflight_payload.get("service_checks") != service_payload:
            blockers.append("multi_trader_smoke_service_preflight_mismatch_for_package")
        elif smoke_observed_at:
            timing = multi_trader_service_preflight_timing_evidence(
                multi_trader_smoke_preflight_evidence(preflight_payload)["evidence"],
                smoke_observed_at,
            )
            summary["service_preflight_timing"] = timing["evidence"]
            timing_blockers = [
                f"{blocker}_for_package"
                for blocker in timing["blockers"]
            ]
            if timing_blockers:
                summary["service_preflight_timing_blockers"] = timing_blockers
                blockers.extend(timing_blockers)
    summary["valid"] = not blockers
    summary["blockers"] = sorted(set(blockers))
    return summary


def build_multi_trader_smoke_observation_payload(
    *,
    clients_path: str | Path,
    workflows_path: str | Path,
    runtime_health_path: str | Path,
    observed_at: str,
    performance_samples_path: str | Path | None = None,
    metrics_path: str | Path | None = None,
    preflight_path: str | Path | None = None,
) -> dict[str, Any]:
    if not observed_at:
        raise ValueError("observed_at is required")
    if not is_iso_datetime(observed_at):
        raise ValueError("observed_at must be an ISO datetime")
    client_evidence = load_multi_trader_client_evidence(clients_path)
    clients = client_evidence["clients"]
    workflows_payload = load_json_file(workflows_path)
    runtime_health_payload = load_json_file(runtime_health_path)
    workflows = (
        workflows_payload.get("workflows")
        if isinstance(workflows_payload, dict) and isinstance(workflows_payload.get("workflows"), dict)
        else workflows_payload
    )
    if not isinstance(workflows, dict):
        raise ValueError("workflows_path must contain a JSON object or an object with workflows")

    observation: dict[str, Any] = {
        "schema_version": 1,
        "observed_at": observed_at,
        "clients": clients,
        "client_artifacts": client_evidence["artifacts"],
        "workflows": workflows,
        "runtime_health": runtime_health_for_multi_trader_smoke(runtime_health_payload, runtime_health_path),
    }
    if preflight_path is not None:
        preflight_payload = load_json_file(preflight_path)
        if not isinstance(preflight_payload, dict):
            raise ValueError("preflight_path must contain a JSON object")
        observation["preflight"] = preflight_payload
    runtime_health_performance = performance_samples_from_runtime_health(runtime_health_payload)
    runtime_health_metrics = performance_metrics_from_runtime_health(runtime_health_payload)
    if performance_samples_path is not None:
        performance_evidence = load_multi_trader_performance_evidence(performance_samples_path)
        observation["performance_samples"] = merge_subscribe_snapshot_samples(
            performance_evidence["samples"],
            runtime_health_performance,
        )
        observation["performance_artifacts"] = performance_evidence["artifacts"]
    elif runtime_health_performance["subscribe_snapshot_ms"]:
        observation["performance_samples"] = runtime_health_performance
    if runtime_health_metrics:
        observation["metrics"] = runtime_health_metrics
    if metrics_path is not None:
        metrics = load_json_file(metrics_path)
        if not isinstance(metrics, dict):
            raise ValueError("metrics_path must contain a JSON object")
        explicit_metrics = metrics.get("metrics") if isinstance(metrics.get("metrics"), dict) else metrics
        observation["metrics"] = {**runtime_health_metrics, **explicit_metrics}
    return observation


def multi_trader_smoke_workflow_readiness(workflows_path: Path) -> dict[str, Any]:
    required = MULTI_TRADER_SMOKE_REQUIRED_WORKFLOWS
    if not workflows_path.is_file() or not json_artifact_parseable(workflows_path):
        return {
            "required": required,
            "passed": [],
            "missing": required,
            "incomplete": [],
            "blockers": [],
        }
    payload = load_json_file(workflows_path)
    workflows = (
        payload.get("workflows")
        if isinstance(payload, dict) and isinstance(payload.get("workflows"), dict)
        else payload
    )
    if not isinstance(workflows, dict):
        return {
            "required": required,
            "passed": [],
            "missing": required,
            "incomplete": [],
            "blockers": ["multi_trader_smoke_workflows_invalid"],
        }
    passed = [name for name in required if isinstance(workflows.get(name), dict) and workflows[name].get("passed") is True]
    missing = [name for name in required if name not in workflows]
    incomplete = [name for name in required if name in workflows and name not in passed]
    blockers = []
    if missing:
        blockers.append("multi_trader_smoke_workflows_required_missing")
    if incomplete:
        blockers.append("multi_trader_smoke_workflows_incomplete")
    evidence_blockers_by_workflow = multi_trader_smoke_workflow_blockers_by_workflow(workflows)
    evidence_blockers = sorted(
        {
            blocker
            for workflow_blockers in evidence_blockers_by_workflow.values()
            for blocker in workflow_blockers
        }
    )
    blockers.extend(evidence_blockers)
    return {
        "required": required,
        "passed": passed,
        "missing": missing,
        "incomplete": incomplete,
        "evidence_blockers": evidence_blockers,
        "evidence_blockers_by_workflow": evidence_blockers_by_workflow,
        "blockers": blockers,
    }


def multi_trader_smoke_workflow_blockers_by_workflow(workflows: dict[str, Any]) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = {}
    for workflow in MULTI_TRADER_SMOKE_REQUIRED_WORKFLOWS:
        entry = workflows.get(workflow)
        if isinstance(entry, dict):
            blockers = multi_trader_smoke_workflow_entry_blockers(workflow, entry)
            if blockers:
                grouped[workflow] = blockers
    return grouped


def multi_trader_smoke_workflow_entry_blockers(workflow: str, entry: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    if workflow == "closed_market_effective_date":
        requested_date = str(entry.get("requested_trade_date") or "")
        effective_date = str(entry.get("effective_trade_date") or "")
        if requested_date and not is_yyyymmdd(requested_date):
            blockers.append("multi_trader_smoke_requested_date_invalid")
        if effective_date and not is_yyyymmdd(effective_date):
            blockers.append("multi_trader_smoke_effective_date_invalid")
        if requested_date and effective_date and requested_date == effective_date and entry.get("expected_closed_market") is True:
            blockers.append("multi_trader_smoke_closed_market_dates_not_distinct")
    if entry.get("passed") is not True:
        return sorted(set(blockers))
    blockers.extend(workflow_evidence_blockers({workflow: entry}))
    return sorted(set(blockers))


def failed_multi_trader_smoke_finalize_result(
    *,
    root_path: Path,
    observation_path: Path,
    evidence_path: Path,
    manifest_path: Path,
    observed_at: str,
    blockers: list[str],
    missing_paths: list[str] | None = None,
    invalid_paths: list[str] | None = None,
    errors: list[str] | None = None,
) -> MultiTraderSmokeFinalizeResult:
    evidence: dict[str, Any] = {
        "schema_version": 1,
        "passed": False,
        "blockers": blockers,
        "observed_at": observed_at,
    }
    if missing_paths:
        evidence["missing_paths"] = missing_paths
    if invalid_paths:
        evidence["invalid_paths"] = invalid_paths
    if errors:
        evidence["errors"] = errors
    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    evidence_path.write_text(json.dumps(evidence, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
    return MultiTraderSmokeFinalizeResult(
        evidence=evidence,
        root_path=root_path,
        observation_path=observation_path,
        evidence_path=evidence_path,
        manifest_path=write_multi_trader_smoke_manifest(root_path),
    )


def write_multi_trader_smoke_manifest(root_path: Path) -> Path:
    manifest_path = root_path / "smoke-run-manifest.json"
    files = []
    for path in sorted(root_path.rglob("*.json")):
        if path == manifest_path:
            continue
        if path.name == "smoke-run-package.json":
            continue
        if not path.is_file():
            continue
        data = path.read_bytes()
        files.append(
            {
                "path": path.relative_to(root_path).as_posix(),
                "bytes": len(data),
                "sha256": hashlib.sha256(data).hexdigest(),
            }
        )
    manifest = {
        "schema_version": 1,
        "root_path": str(root_path),
        "file_count": len(files),
        "files": files,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
    return manifest_path


def package_multi_trader_smoke(
    *,
    root_path: str | Path,
    output_path: str | Path | None = None,
    metadata_path: str | Path | None = None,
) -> MultiTraderSmokePackageResult:
    root = Path(root_path)
    package_path = Path(output_path) if output_path is not None else root / "multi-trader-smoke-evidence.zip"
    metadata = Path(metadata_path) if metadata_path is not None else root / "smoke-run-package.json"
    package_path.parent.mkdir(parents=True, exist_ok=True)
    metadata.parent.mkdir(parents=True, exist_ok=True)
    import_manifest_path = root / "smoke-import-manifest.json"
    if not import_manifest_path.is_file():
        raise ValueError("package-multi-trader-smoke requires smoke-import-manifest.json")
    validate_smoke_import_manifest(load_json_file(import_manifest_path), root_path=root)
    evidence_path = root / "multi-trader-smoke-evidence.json"
    if not evidence_path.is_file():
        raise ValueError("package-multi-trader-smoke requires passed multi-trader-smoke-evidence.json")
    evidence = load_json_file(evidence_path)
    if not isinstance(evidence, dict) or evidence.get("passed") is not True:
        raise ValueError("package-multi-trader-smoke requires passed multi-trader-smoke-evidence.json")
    preflight = multi_trader_smoke_package_preflight_readiness(root)
    if preflight["blockers"]:
        raise ValueError("package-multi-trader-smoke requires passed lan/service preflight")
    validate_smoke_run_manifest_for_package(root_path=root, metadata_path=metadata)

    files = multi_trader_smoke_package_files(root, metadata_path=metadata)
    with zipfile.ZipFile(package_path, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in files:
            archive.write(path, path.relative_to(root).as_posix())

    data = package_path.read_bytes()
    result = MultiTraderSmokePackageResult(
        package_path=package_path,
        metadata_path=metadata,
        sha256=hashlib.sha256(data).hexdigest(),
        byte_count=len(data),
        file_count=len(files),
        files=[path.relative_to(root).as_posix() for path in files],
    )
    metadata_payload = {
        "schema_version": 1,
        "root_path": str(root),
        "package_path": str(package_path),
        "bytes": result.byte_count,
        "sha256": result.sha256,
        "file_count": result.file_count,
        "files": result.files,
    }
    metadata.write_text(json.dumps(metadata_payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
    return result


def validate_smoke_run_manifest_for_package(*, root_path: Path, metadata_path: Path) -> None:
    manifest_path = root_path / "smoke-run-manifest.json"
    if not manifest_path.is_file():
        raise ValueError("package-multi-trader-smoke requires smoke-run-manifest.json")
    payload = load_json_file(manifest_path)
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        raise ValueError("package-multi-trader-smoke smoke-run-manifest invalid")
    files = payload.get("files")
    if not isinstance(files, list) or not all(isinstance(item, dict) for item in files):
        raise ValueError("package-multi-trader-smoke smoke-run-manifest files invalid")
    entries: dict[str, dict[str, Any]] = {}
    for item in files:
        path = item.get("path")
        if not isinstance(path, str) or not path or path.startswith("/") or ".." in Path(path).parts:
            raise ValueError("package-multi-trader-smoke smoke-run-manifest files invalid")
        if path == "smoke-run-manifest.json":
            raise ValueError("package-multi-trader-smoke smoke-run-manifest self references")
        entries[path] = item
    if payload.get("file_count") != len(files) or len(entries) != len(files):
        raise ValueError("package-multi-trader-smoke smoke-run-manifest files invalid")
    package_files = multi_trader_smoke_package_files(root_path, metadata_path=metadata_path)
    expected_paths = sorted(
        path.relative_to(root_path).as_posix()
        for path in package_files
        if path.relative_to(root_path).as_posix() != "smoke-run-manifest.json"
    )
    if sorted(entries) != expected_paths:
        raise ValueError("package-multi-trader-smoke smoke-run-manifest stale")
    for relative_path in expected_paths:
        path = root_path / relative_path
        data = path.read_bytes()
        entry = entries[relative_path]
        if entry.get("bytes") != len(data) or entry.get("sha256") != hashlib.sha256(data).hexdigest():
            raise ValueError("package-multi-trader-smoke smoke-run-manifest stale")


def import_multi_trader_smoke_artifact(
    *,
    root_path: str | Path,
    input_path: str | Path,
    kind: str = "auto",
    record_manifest: bool = True,
) -> MultiTraderSmokeArtifactImportResult:
    root = Path(root_path)
    source = Path(input_path)
    payload = load_json_file(source)
    artifact_kind = kind if kind != "auto" else detect_multi_trader_smoke_artifact_kind(payload)
    if artifact_kind not in {"client", "performance"}:
        raise ValueError("kind must be auto, client, or performance")
    if artifact_kind == "client":
        validate_multi_trader_smoke_client_artifact(payload)
        target_dir = root / "clients"
    else:
        validate_multi_trader_smoke_performance_artifact(payload)
        target_dir = root / "performance"
    target_dir.mkdir(parents=True, exist_ok=True)
    target = unique_artifact_path(target_dir / safe_json_filename(source.name, fallback=f"{artifact_kind}.json"))
    target.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
    manifest_path = None
    if record_manifest:
        manifest_path = append_smoke_import_manifest(
            root=root,
            source=source,
            imported=[MultiTraderSmokeArtifactImportResult(kind=artifact_kind, input_path=source, output_path=target)],
            skipped=[],
        )
    return MultiTraderSmokeArtifactImportResult(kind=artifact_kind, input_path=source, output_path=target, manifest_path=manifest_path)


def import_multi_trader_smoke_artifacts(
    *,
    root_path: str | Path,
    input_path: str | Path,
    kind: str = "auto",
) -> MultiTraderSmokeArtifactBatchImportResult:
    source = Path(input_path)
    root = Path(root_path)
    if source.is_dir():
        paths = sorted(path for path in source.iterdir() if path.is_file() and path.suffix == ".json")
    else:
        paths = [source]
    imported: list[MultiTraderSmokeArtifactImportResult] = []
    skipped: list[dict[str, str]] = []
    for path in paths:
        try:
            imported.append(
                import_multi_trader_smoke_artifact(
                    root_path=root_path,
                    input_path=path,
                    kind=kind,
                    record_manifest=False,
                )
            )
        except (OSError, ValueError, json.JSONDecodeError) as error:
            skipped.append({"path": str(path), "error": str(error)})
    manifest_path = append_smoke_import_manifest(root=root, source=source, imported=imported, skipped=skipped)
    return MultiTraderSmokeArtifactBatchImportResult(imported=imported, skipped=skipped, manifest_path=manifest_path)


def append_smoke_import_manifest(
    *,
    root: Path,
    source: Path,
    imported: list[MultiTraderSmokeArtifactImportResult],
    skipped: list[dict[str, str]],
) -> Path:
    manifest_path = root / "smoke-import-manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    run = {
        "source_path": str(source),
        "imported_count": len(imported),
        "skipped_count": len(skipped),
        "imported": [
            {
                "kind": item.kind,
                "input_path": str(item.input_path),
                "output_path": smoke_import_manifest_record_output_path(root=root, output_path=item.output_path),
            }
            for item in imported
        ],
        "skipped": skipped,
    }
    previous_runs = [
        smoke_import_manifest_normalized_run(root=root, run=item)
        for item in existing_smoke_import_runs(manifest_path)
    ]
    runs = previous_runs + [run]
    manifest = {
        "schema_version": 1,
        "source_path": str(source),
        "imported_count": sum(int(item.get("imported_count") or 0) for item in runs),
        "skipped_count": sum(int(item.get("skipped_count") or 0) for item in runs),
        "imported": [item for run_item in runs for item in list(run_item.get("imported") or [])],
        "skipped": [item for run_item in runs for item in list(run_item.get("skipped") or [])],
        "runs": runs,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
    return manifest_path


def smoke_import_manifest_record_output_path(*, root: Path, output_path: Path) -> str:
    try:
        return output_path.resolve(strict=False).relative_to(root.resolve(strict=False)).as_posix()
    except ValueError:
        return str(output_path)


def smoke_import_manifest_normalized_run(*, root: Path, run: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(run)
    imported = run.get("imported")
    if isinstance(imported, list):
        normalized_imported: list[Any] = []
        for item in imported:
            if not isinstance(item, dict):
                normalized_imported.append(item)
                continue
            normalized_item = dict(item)
            relative_output = smoke_import_manifest_output_relative(
                root_path=root,
                output_path=normalized_item.get("output_path"),
            )
            if relative_output is not None:
                normalized_item["output_path"] = relative_output
            normalized_imported.append(normalized_item)
        normalized["imported"] = normalized_imported
    return normalized


def validate_smoke_import_manifest(payload: Any, *, root_path: Path | None = None) -> None:
    if not isinstance(payload, dict):
        raise ValueError("package-multi-trader-smoke import manifest must be a JSON object")
    if payload.get("schema_version") != 1:
        raise ValueError("package-multi-trader-smoke import manifest schema invalid")
    imported = payload.get("imported")
    skipped = payload.get("skipped")
    runs = payload.get("runs")
    if not isinstance(imported, list) or not all(isinstance(item, dict) for item in imported):
        raise ValueError("package-multi-trader-smoke import manifest imported invalid")
    if not imported:
        raise ValueError("package-multi-trader-smoke import manifest has no imported artifacts")
    if not isinstance(skipped, list) or not all(isinstance(item, dict) for item in skipped):
        raise ValueError("package-multi-trader-smoke import manifest skipped invalid")
    if not isinstance(runs, list) or not runs or not all(isinstance(item, dict) for item in runs):
        raise ValueError("package-multi-trader-smoke import manifest runs invalid")
    if payload.get("imported_count") != len(imported):
        raise ValueError("package-multi-trader-smoke import manifest imported_count mismatch")
    if payload.get("skipped_count") != len(skipped):
        raise ValueError("package-multi-trader-smoke import manifest skipped_count mismatch")
    for run in runs:
        run_imported = run.get("imported")
        run_skipped = run.get("skipped")
        if not isinstance(run_imported, list) or not all(isinstance(item, dict) for item in run_imported):
            raise ValueError("package-multi-trader-smoke import manifest run imported invalid")
        if not isinstance(run_skipped, list) or not all(isinstance(item, dict) for item in run_skipped):
            raise ValueError("package-multi-trader-smoke import manifest run skipped invalid")
        if run.get("imported_count") != len(run_imported):
            raise ValueError("package-multi-trader-smoke import manifest run imported_count mismatch")
        if run.get("skipped_count") != len(run_skipped):
            raise ValueError("package-multi-trader-smoke import manifest run skipped_count mismatch")
    flattened_imported = [item for run in runs for item in list(run.get("imported") or [])]
    flattened_skipped = [item for run in runs for item in list(run.get("skipped") or [])]
    if flattened_imported != imported:
        raise ValueError("package-multi-trader-smoke import manifest imported run mismatch")
    if flattened_skipped != skipped:
        raise ValueError("package-multi-trader-smoke import manifest skipped run mismatch")
    normalized_outputs: list[str] = []
    for item in imported:
        kind = item.get("kind")
        input_path = item.get("input_path")
        output_path = item.get("output_path")
        if kind not in {"client", "performance"}:
            raise ValueError("package-multi-trader-smoke import manifest kind invalid")
        if not isinstance(input_path, str) or not input_path.strip():
            raise ValueError("package-multi-trader-smoke import manifest input_path invalid")
        if root_path is not None:
            relative_output = smoke_import_manifest_output_relative(
                root_path=root_path,
                output_path=output_path,
            )
            if relative_output is None:
                raise ValueError("package-multi-trader-smoke import manifest output_path invalid")
            expected_prefix = "clients/" if kind == "client" else "performance/"
            if not relative_output.startswith(expected_prefix):
                raise ValueError("package-multi-trader-smoke import manifest kind output mismatch")
            normalized_outputs.append(relative_output)
    if len(normalized_outputs) != len(set(normalized_outputs)):
        raise ValueError("package-multi-trader-smoke import manifest duplicate output")
    if root_path is not None:
        missing_outputs = [
            str(item.get("output_path"))
            for item in imported
            if not smoke_import_output_path_exists(root_path, item.get("output_path"))
        ]
        if missing_outputs:
            raise ValueError("package-multi-trader-smoke import manifest output missing")
        missing_import_coverage = smoke_import_manifest_missing_artifact_coverage(root_path, imported)
        if missing_import_coverage:
            raise ValueError("package-multi-trader-smoke import manifest coverage missing")


def smoke_import_manifest_output_relative(*, root_path: Path, output_path: Any) -> str | None:
    if not isinstance(output_path, str) or not output_path.strip():
        return None
    root_resolved = root_path.resolve()
    normalized_output = output_path.strip().replace("\\", "/")
    path = Path(normalized_output)
    if path.is_absolute():
        try:
            relative = path.resolve(strict=False).relative_to(root_resolved)
        except ValueError:
            return None
    else:
        relative = Path(normalized_output)
    if relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
        return None
    relative_posix = relative.as_posix()
    if not relative_posix.endswith(".json"):
        return None
    if not (relative_posix.startswith("clients/") or relative_posix.startswith("performance/")):
        return None
    return relative_posix


def smoke_import_manifest_missing_artifact_coverage(root_path: Path, imported: list[dict[str, Any]]) -> list[str]:
    imported_paths = smoke_import_manifest_output_relatives(root_path, imported)
    missing: list[str] = []
    for path in smoke_frontend_artifact_paths(root_path):
        relative = path.relative_to(root_path).as_posix()
        if relative not in imported_paths:
            missing.append(relative)
    return sorted(missing)


def smoke_import_manifest_output_relatives(root_path: Path, imported: list[dict[str, Any]]) -> set[str]:
    relatives: set[str] = set()
    for item in imported:
        relative = smoke_import_manifest_output_relative(root_path=root_path, output_path=item.get("output_path"))
        if relative is not None:
            relatives.add(relative)
    return relatives


def smoke_frontend_artifact_paths(root_path: Path) -> list[Path]:
    paths: list[Path] = []
    for directory, validator in (
        (root_path / "clients", validate_multi_trader_smoke_client_artifact),
        (root_path / "performance", validate_multi_trader_smoke_performance_artifact),
    ):
        if not directory.is_dir():
            continue
        for path in sorted(directory.glob("*.json")):
            if not path.is_file() or not json_artifact_parseable(path):
                continue
            try:
                validator(load_json_file(path))
            except ValueError:
                continue
            paths.append(path)
    return paths


def smoke_import_output_path_exists(root_path: Path, output_path: Any) -> bool:
    relative = smoke_import_manifest_output_relative(root_path=root_path, output_path=output_path)
    return relative is not None and (root_path / relative).is_file()


def existing_smoke_import_runs(manifest_path: Path) -> list[dict[str, Any]]:
    if not manifest_path.is_file():
        return []
    try:
        payload = load_json_file(manifest_path)
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(payload, dict):
        return []
    runs = payload.get("runs")
    if isinstance(runs, list):
        return [item for item in runs if isinstance(item, dict)]
    if isinstance(payload.get("imported"), list) or isinstance(payload.get("skipped"), list):
        return [
            {
                "source_path": str(payload.get("source_path") or ""),
                "imported_count": int(payload.get("imported_count") or 0),
                "skipped_count": int(payload.get("skipped_count") or 0),
                "imported": payload.get("imported") if isinstance(payload.get("imported"), list) else [],
                "skipped": payload.get("skipped") if isinstance(payload.get("skipped"), list) else [],
            }
        ]
    return []


def detect_multi_trader_smoke_artifact_kind(payload: Any) -> str:
    if isinstance(payload, dict) and isinstance(payload.get("clients"), list):
        return "client"
    if isinstance(payload, list):
        return "client"
    if isinstance(payload, dict) and isinstance(payload.get("performance_samples"), dict):
        return "performance"
    raise ValueError("could not detect smoke artifact kind")


def validate_multi_trader_smoke_client_artifact(payload: Any) -> None:
    if not isinstance(payload, dict):
        raise ValueError("client smoke artifact must be a JSON object")
    if payload.get("schema_version") != 1:
        raise ValueError("client smoke artifact schema_version must be 1")
    exported_at = payload.get("exported_at")
    if not isinstance(exported_at, str) or not is_iso_datetime(exported_at):
        raise ValueError("client smoke artifact exported_at must be an ISO datetime")
    value = payload.get("clients")
    if not isinstance(value, list) or not value:
        raise ValueError("client smoke artifact must contain a non-empty clients list")
    if not all(isinstance(item, dict) for item in value):
        raise ValueError("client smoke artifact clients must be JSON objects")
    blockers = semantic_client_smoke_artifact_blockers(value)
    if blockers:
        raise ValueError(f"client smoke artifact semantic validation failed: {', '.join(blockers)}")


def semantic_client_smoke_artifact_blockers(clients: list[dict[str, Any]]) -> list[str]:
    blockers: list[str] = []
    for client in clients:
        machine_id = client.get("machine_id") or client.get("host")
        if machine_id is None or machine_id == "":
            blockers.append("multi_trader_smoke_client_machine_missing")
        elif not normalized_client_id(machine_id):
            blockers.append("multi_trader_smoke_client_machine_invalid")
        if str(client.get("data_source_mode") or "").strip() != "live":
            blockers.append("multi_trader_smoke_client_not_live")
        blockers.extend(client_page_url_blockers(str(client.get("page_url") or "").strip()))
        blockers.extend(client_gateway_url_blockers(str(client.get("gateway_url") or "").strip()))
        watchlist_evidence = symbol_list_evidence(client.get("watchlist"))
        if watchlist_evidence["shape_invalid"] or watchlist_evidence["invalid_symbols"] or watchlist_evidence["duplicate_symbols"]:
            blockers.append("multi_trader_smoke_watchlist_invalid")
        _, status_blockers = client_symbol_status_evidence(
            client.get("symbol_statuses"),
            watchlist_evidence["symbols"],
        )
        blockers.extend(status_blockers)
        if client.get("connected") is not True:
            blockers.append("multi_trader_smoke_client_not_connected")
        if client.get("refresh_recovered") is not True:
            blockers.append("multi_trader_smoke_client_refresh_not_recovered")
    return sorted(set(blockers))


def validate_multi_trader_smoke_performance_artifact(payload: Any) -> None:
    if not isinstance(payload, dict):
        raise ValueError("performance smoke artifact must be a JSON object")
    if payload.get("schema_version") != 1:
        raise ValueError("performance smoke artifact schema_version must be 1")
    exported_at = payload.get("exported_at")
    if not isinstance(exported_at, str) or not is_iso_datetime(exported_at):
        raise ValueError("performance smoke artifact exported_at must be an ISO datetime")
    machine_id = payload.get("machine_id")
    if machine_id is None or machine_id == "":
        raise ValueError("performance smoke artifact machine_id is required")
    if not normalized_client_id(machine_id):
        raise ValueError("performance smoke artifact machine_id must not require trimming")
    performance_samples_for_multi_trader_smoke(payload)


def validate_multi_trader_smoke_performance_evidence_payload(payload: Any) -> None:
    if isinstance(payload, dict) and isinstance(payload.get("clients"), list):
        validate_multi_trader_smoke_client_artifact(payload)
        performance_samples_for_multi_trader_smoke(payload)
        if not performance_artifact_machine_id(payload):
            raise ValueError("performance smoke artifact machine_id is required")
        return
    validate_multi_trader_smoke_performance_artifact(payload)


def safe_json_filename(name: str, *, fallback: str) -> str:
    candidate = Path(name).name
    if not candidate or candidate in {".", ".."}:
        candidate = fallback
    candidate = "".join(char if char.isalnum() or char in {"-", "_", "."} else "-" for char in candidate)
    if not candidate.endswith(".json"):
        candidate = f"{candidate}.json"
    return candidate


def unique_artifact_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    for index in range(2, 1000):
        candidate = path.with_name(f"{stem}-{index}{suffix}")
        if not candidate.exists():
            return candidate
    raise ValueError(f"could not allocate unique artifact path under {path.parent}")


def multi_trader_smoke_package_files(root_path: Path, *, metadata_path: Path) -> list[Path]:
    metadata_resolved = metadata_path.resolve()
    files: list[Path] = []
    for path in sorted(root_path.rglob("*.json")):
        if not path.is_file():
            continue
        if path.resolve() == metadata_resolved or path.name == "smoke-run-package.json":
            continue
        files.append(path)
    return files


def json_artifact_parseable(path: Path) -> bool:
    try:
        load_json_file(path)
    except (OSError, json.JSONDecodeError):
        return False
    return True


def json_artifacts_present(path: Path) -> bool:
    return path.is_file() or (path.is_dir() and any(item.is_file() and item.suffix == ".json" for item in path.iterdir()))


def multi_trader_smoke_performance_path(clients_path: Path, performance_path: Path) -> Path | None:
    if json_artifacts_present(performance_path):
        return performance_path
    if performance_samples_present(clients_path):
        return clients_path
    return None


def performance_samples_present(path: Path) -> bool:
    if not json_artifacts_present(path):
        return False
    for artifact_path in resolve_artifact_paths(path):
        try:
            payload = load_json_file(artifact_path)
            performance_samples_for_multi_trader_smoke(payload)
        except ValueError:
            continue
        return True
    return False


def multi_trader_smoke_preflight_blockers(
    *,
    lan_host: str,
    frontend_port: int,
    gateway_port: int,
    page_url: str,
    gateway_url: str,
    local_address_evidence: dict[str, Any] | None = None,
) -> list[str]:
    blockers: list[str] = []
    normalized_host = lan_host.strip("[]")
    if not lan_host:
        blockers.append("multi_trader_smoke_lan_host_missing")
    elif is_loopback_host(normalized_host) or normalized_host in {"0.0.0.0", "::"}:
        blockers.append("multi_trader_smoke_lan_host_not_client_routable")
    if frontend_port <= 0:
        blockers.append("multi_trader_smoke_frontend_port_invalid")
    if gateway_port <= 0:
        blockers.append("multi_trader_smoke_gateway_port_invalid")
    blockers.extend(validate_smoke_page_url(page_url))
    blockers.extend(validate_smoke_gateway_url(gateway_url))
    if local_address_evidence and local_address_evidence.get("required") is True:
        if local_address_evidence.get("matches_local_address") is not True:
            blockers.append("multi_trader_smoke_lan_host_not_local")
    return sorted(set(blockers))


def local_lan_host_evidence(
    lan_host: str,
    *,
    local_addresses: list[str] | None = None,
    require_local_lan_host: bool = False,
) -> dict[str, Any]:
    addresses = sorted(set(local_addresses if local_addresses is not None else detect_local_ip_addresses()))
    host = lan_host.strip("[]")
    warnings: list[str] = []
    matches: bool | None = None
    if is_ip_literal(host):
        matches = host in addresses
        if not matches:
            warnings.append("multi_trader_smoke_lan_host_not_detected_on_local_machine")
    return {
        "required": require_local_lan_host,
        "host_is_ip": is_ip_literal(host),
        "detected_addresses": addresses,
        "matches_local_address": matches,
        "warnings": warnings,
    }


def detect_local_ip_addresses() -> list[str]:
    addresses: set[str] = set()
    try:
        hostname = socket.gethostname()
        for result in socket.getaddrinfo(hostname, None):
            address = result[4][0]
            if usable_lan_address(address):
                addresses.add(address)
    except OSError:
        pass
    for target in ("8.8.8.8", "1.1.1.1"):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                sock.connect((target, 80))
                address = sock.getsockname()[0]
                if usable_lan_address(address):
                    addresses.add(address)
        except OSError:
            continue
    return sorted(addresses)


def is_ip_literal(value: str) -> bool:
    try:
        ipaddress.ip_address(value)
    except ValueError:
        return False
    return True


def usable_lan_address(value: str) -> bool:
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        return False
    return not (address.is_loopback or address.is_unspecified or address.is_link_local)


def validate_smoke_page_url(page_url: str) -> list[str]:
    if not page_url:
        return ["multi_trader_smoke_page_url_missing"]
    parsed = urlparse(page_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return ["multi_trader_smoke_page_url_invalid"]
    if is_loopback_host(parsed.hostname) or parsed.hostname in {"0.0.0.0", "::"}:
        return ["multi_trader_smoke_page_url_not_client_routable"]
    return []


def host_for_url(host: str) -> str:
    return f"[{host}]" if ":" in host and not host.startswith("[") else host


def validate_smoke_gateway_url(gateway_url: str) -> list[str]:
    if not gateway_url:
        return ["multi_trader_smoke_gateway_url_missing"]
    blockers: list[str] = []
    parsed = urlparse(gateway_url)
    try:
        parsed_port = parsed.port
    except ValueError:
        parsed_port = None
        blockers.append("multi_trader_smoke_gateway_url_invalid")
    if parsed.scheme not in {"ws", "wss"} or not parsed.netloc:
        blockers.append("multi_trader_smoke_gateway_url_invalid")
    if is_loopback_host(parsed.hostname) or parsed.hostname in {"0.0.0.0", "::"}:
        blockers.append("multi_trader_smoke_gateway_url_not_client_routable")
    if parsed.path != GATEWAY_WEBSOCKET_PATH:
        blockers.append("multi_trader_smoke_gateway_url_path_invalid")
    if parsed_port is None:
        blockers.append("multi_trader_smoke_gateway_url_port_missing")
    return sorted(set(blockers))


def load_multi_trader_clients(path_or_paths: str | Path) -> list[dict[str, Any]]:
    return load_multi_trader_client_evidence(path_or_paths)["clients"]


def load_multi_trader_client_evidence(path_or_paths: str | Path) -> dict[str, Any]:
    clients: list[dict[str, Any]] = []
    artifacts: list[dict[str, Any]] = []
    for path in resolve_artifact_paths(path_or_paths):
        payload = load_json_file(path)
        value = payload.get("clients") if isinstance(payload, dict) else payload
        if not isinstance(value, list):
            raise ValueError("clients_path must contain a JSON list or an object with clients")
        machine_ids: list[str] = []
        for item in value:
            if not isinstance(item, dict):
                raise ValueError("clients_path clients must be JSON objects")
            clients.append(item)
            machine_id = str(item.get("machine_id") or "").strip()
            if machine_id:
                machine_ids.append(machine_id)
        if value:
            exported_at = payload.get("exported_at") if isinstance(payload, dict) else ""
            artifacts.append(
                {
                    "path": str(path),
                    "exported_at": exported_at if isinstance(exported_at, str) else "",
                    "machine_ids": sorted(set(machine_ids)),
                    "client_count": len(value),
                }
            )
    return {"clients": clients, "artifacts": artifacts}


def load_multi_trader_performance_samples(path_or_paths: str | Path) -> dict[str, list[float]]:
    return load_multi_trader_performance_evidence(path_or_paths)["samples"]


def load_multi_trader_performance_evidence(path_or_paths: str | Path) -> dict[str, Any]:
    values: list[float] = []
    artifacts: list[dict[str, Any]] = []
    for path in resolve_artifact_paths(path_or_paths):
        payload = load_json_file(path)
        validate_multi_trader_smoke_performance_evidence_payload(payload)
        samples = performance_samples_for_multi_trader_smoke(payload)["subscribe_snapshot_ms"]
        values.extend(samples)
        machine_id = performance_artifact_machine_id(payload)
        artifacts.append(
            {
                "path": str(path),
                "machine_id": machine_id,
                "exported_at": str(payload.get("exported_at") or "") if isinstance(payload, dict) else "",
                "subscribe_snapshot_count": len(samples),
            }
        )
    return {
        "samples": {"subscribe_snapshot_ms": values},
        "artifacts": artifacts,
    }


def performance_artifact_machine_id(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    machine_id = str(payload.get("machine_id") or "").strip()
    if machine_id:
        return machine_id
    clients = payload.get("clients")
    if isinstance(clients, list) and len(clients) == 1 and isinstance(clients[0], dict):
        return str(clients[0].get("machine_id") or "").strip()
    return ""


def performance_samples_from_runtime_health(payload: Any) -> dict[str, list[float]]:
    if not isinstance(payload, dict):
        return {"subscribe_snapshot_ms": []}
    if isinstance(payload.get("performance_samples"), dict):
        return performance_samples_for_multi_trader_smoke(payload["performance_samples"])
    evidence = payload.get("evidence")
    if isinstance(evidence, dict) and isinstance(evidence.get("performance_samples"), dict):
        values = evidence["performance_samples"].get("subscribe_snapshot_ms")
        if isinstance(values, list):
            return performance_samples_for_multi_trader_smoke({"subscribe_snapshot_ms": values})
    return {"subscribe_snapshot_ms": []}


def performance_metrics_from_runtime_health(payload: Any) -> dict[str, float]:
    if not isinstance(payload, dict):
        return {}
    evidence = payload.get("evidence")
    if not isinstance(evidence, dict):
        return {}
    performance_samples = evidence.get("performance_samples")
    if not isinstance(performance_samples, dict):
        return {}
    value = performance_samples.get("subscribe_snapshot_p95_ms")
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return {"warm_snapshot_p95_ms": float(value)}
    return {}


def merge_subscribe_snapshot_samples(*sample_sets: dict[str, list[float]]) -> dict[str, list[float]]:
    values: list[float] = []
    for sample_set in sample_sets:
        values.extend(sample_set.get("subscribe_snapshot_ms", []))
    return {"subscribe_snapshot_ms": values}


def resolve_artifact_paths(path_or_paths: str | Path) -> list[Path]:
    if isinstance(path_or_paths, str) and "," in path_or_paths:
        paths = [Path(item.strip()) for item in path_or_paths.split(",") if item.strip()]
    else:
        path = Path(path_or_paths)
        if path.is_dir():
            paths = sorted(item for item in path.iterdir() if item.is_file() and item.suffix == ".json")
        else:
            paths = [path]
    if not paths:
        raise ValueError("artifact path list is empty")
    return paths


def runtime_health_for_multi_trader_smoke(payload: Any, path: str | Path) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("runtime_health_path must contain a JSON object")
    performance_samples = performance_samples_from_runtime_health(payload)
    generated_at = str(payload.get("generated_at") or "")
    if "evidence" in payload and isinstance(payload.get("evidence"), dict):
        evidence = payload["evidence"]
        if not generated_at:
            generated_at = str(evidence.get("generated_at") or "")
        result = {
            "passed": payload.get("passed") is True,
            "path": str(path),
            "generated_at": generated_at,
            "symbol_runtime": evidence.get("symbol_runtime", {}),
            "symbol_runtime_manager": evidence.get("symbol_runtime_manager", {}),
            "gateway_websocket": evidence.get("gateway_websocket", {}),
        }
        if isinstance(evidence.get("gateway_activity"), dict):
            result["gateway_activity"] = evidence["gateway_activity"]
        if performance_samples["subscribe_snapshot_ms"]:
            result["performance_samples"] = performance_samples
        metrics = performance_metrics_from_runtime_health(payload)
        if metrics:
            result["performance_metrics"] = metrics
        return result
    if payload.get("schema_version") == 1 and "symbol_runtime" in payload:
        verified = evaluate_runtime_health(payload)
        result = {
            "passed": verified.get("passed") is True,
            "path": str(path),
            "generated_at": generated_at,
            "symbol_runtime": payload.get("symbol_runtime", {}),
            "symbol_runtime_manager": payload.get("symbol_runtime_manager", {}),
            "gateway_websocket": payload.get("gateway_websocket", {}),
        }
        if isinstance(payload.get("gateway_activity"), dict):
            result["gateway_activity"] = payload["gateway_activity"]
        if performance_samples["subscribe_snapshot_ms"]:
            result["performance_samples"] = performance_samples
        return result
    result = {
        "passed": payload.get("passed") is True,
        "path": str(path),
        "generated_at": generated_at,
        "symbol_runtime": payload.get("symbol_runtime", {}),
        "symbol_runtime_manager": payload.get("symbol_runtime_manager", {}),
        "gateway_websocket": payload.get("gateway_websocket", {}),
    }
    if isinstance(payload.get("gateway_activity"), dict):
        result["gateway_activity"] = payload["gateway_activity"]
    if performance_samples["subscribe_snapshot_ms"]:
        result["performance_samples"] = performance_samples
    return result


def performance_samples_for_multi_trader_smoke(payload: Any) -> dict[str, list[float]]:
    if isinstance(payload, dict) and isinstance(payload.get("subscribe_snapshot_ms"), list):
        return {
            "subscribe_snapshot_ms": [
                performance_sample_value_for_multi_trader_smoke(value)
                for value in payload["subscribe_snapshot_ms"]
            ]
        }
    if isinstance(payload, dict) and isinstance(payload.get("performance_samples"), dict):
        return performance_samples_for_multi_trader_smoke(payload["performance_samples"])
    if isinstance(payload, list):
        values = []
        for sample in payload:
            if not isinstance(sample, dict):
                continue
            key = sample.get("key")
            if key not in {"subscribe_snapshot_ms", "gateway_subscribe_snapshot_ms"}:
                continue
            value = sample.get("valueMs", sample.get("value_ms", sample.get("value")))
            values.append(performance_sample_value_for_multi_trader_smoke(value))
        return {"subscribe_snapshot_ms": values}
    raise ValueError("performance_samples_path must contain subscribe_snapshot_ms or sample objects")


def performance_sample_value_for_multi_trader_smoke(value: Any) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError("performance_samples_path contains invalid subscribe_snapshot_ms")
    numeric = float(value)
    if not math.isfinite(numeric) or numeric < 0:
        raise ValueError("performance_samples_path contains invalid subscribe_snapshot_ms")
    return numeric


def load_json_file(path: str | Path) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def matching_redis_keys(redis_client: Any | None, pattern: str) -> list[str]:
    if redis_client is None:
        return []
    values = getattr(redis_client, "values", None)
    if isinstance(values, dict):
        from fnmatch import fnmatch

        return sorted(str(key) for key in values if isinstance(key, str) and fnmatch(key, pattern))
    scan_iter = getattr(redis_client, "scan_iter", None)
    if callable(scan_iter):
        return sorted(decode_redis_key(key) for key in scan_iter(match=pattern))
    keys = getattr(redis_client, "keys", None)
    if callable(keys):
        return sorted(decode_redis_key(key) for key in keys(pattern))
    return []


def decode_redis_key(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def import_legacy_shadow_telemetry(
    *,
    input_path: str | Path,
    stream_directory: str | Path,
    session_id: str,
    trading_date: str,
    started_at: str,
    source: str = "legacy",
    reset: bool = False,
) -> LegacyTelemetryImportResult:
    recorder = FileBackedShadowRunRecorder(
        directory=stream_directory,
        session_id=session_id,
        trading_date=trading_date,
        started_at=started_at,
        reset=False,
    )
    if reset and recorder.files.legacy_events_path.exists():
        recorder.files.legacy_events_path.unlink()
    adapter = LegacyShadowTelemetryAdapter(recorder, source=source)
    imported_count = 0
    for line_number, line in enumerate(Path(input_path).read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        decoded = json.loads(line)
        if not isinstance(decoded, dict):
            raise ValueError(f"legacy telemetry line {line_number} must be a JSON object")
        adapter.record(decoded)
        imported_count += 1
    return LegacyTelemetryImportResult(
        imported_count=imported_count,
        stream_directory=Path(stream_directory),
        legacy_events_path=recorder.files.legacy_events_path,
    )


def import_frontend_performance_samples(
    *,
    input_path: str | Path,
    stream_directory: str | Path,
    session_id: str,
    trading_date: str,
    started_at: str,
) -> FrontendPerformanceImportResult:
    samples: list[float] = []
    for line_number, line in enumerate(Path(input_path).read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        decoded = json.loads(line)
        if not isinstance(decoded, dict):
            raise ValueError(f"frontend performance line {line_number} must be a JSON object")
        key = str(decoded.get("key") or "")
        if key != "frontend_store_update_ms":
            raise ValueError(f"frontend performance line {line_number} must use key frontend_store_update_ms")
        value = decoded.get("value_ms", decoded.get("valueMs"))
        if (
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(float(value))
            or float(value) < 0
        ):
            raise ValueError(f"frontend performance line {line_number} missing non-negative numeric valueMs")
        samples.append(float(value))

    recorder = FileBackedShadowRunRecorder(
        directory=stream_directory,
        session_id=session_id,
        trading_date=trading_date,
        started_at=started_at,
        reset=False,
    )
    for value in samples:
        recorder.record_performance_sample("frontend_store_update_ms", value)
    return FrontendPerformanceImportResult(
        imported_count=len(samples),
        stream_directory=Path(stream_directory),
        performance_samples_path=recorder.files.performance_samples_path,
    )


def finalize_shadow_run_cutover(
    *,
    stream_directory: str | Path,
    session_id: str,
    trading_date: str,
    finished_at: str,
    reports_directory: str | Path,
    readiness_path: str | Path,
    frontend_env_path: str | Path,
    live_url: str,
    policy: CutoverPolicy | None = None,
) -> ShadowRunCutoverResult:
    files = shadow_run_file_paths(stream_directory, trading_date=trading_date, session_id=session_id)
    report = build_shadow_run_report_from_files(files, finished_at=finished_at)
    paths = write_cutover_artifacts(
        report=report,
        reports_directory=reports_directory,
        readiness_path=readiness_path,
        frontend_env_path=frontend_env_path,
        live_url=live_url,
        policy=policy,
    )
    return ShadowRunCutoverResult(
        report=report,
        report_path=paths.report_path,
        readiness_path=paths.readiness_path,
        frontend_env_path=paths.frontend_env_path,
    )
