from __future__ import annotations

import argparse
import json
import shlex
from typing import Sequence

from .cutover import CutoverPolicy, DEFAULT_LEGACY_TOPIC_NAMES
from .cutover import (
    EvidenceBundlePaths,
    write_frontend_deployment_evidence,
    write_legacy_decommission_evidence,
    write_legacy_retirement_from_artifacts,
    write_multi_trader_smoke_evidence,
    write_evidence_bundle_verification,
    write_runtime_health_verification,
)
from .app_runtime import write_runtime_config_verification
from .ops import (
    explain_active_pool_symbol,
    build_multi_trader_smoke_observation,
    build_multi_trader_smoke_workflows_template,
    clear_runtime_cache,
    clear_runtime_state,
    finalize_multi_trader_smoke,
    finalize_shadow_run_cutover,
    generate_active_pool,
    generate_required_historical_manifests,
    import_frontend_performance_samples,
    import_legacy_shadow_telemetry,
    import_multi_trader_smoke_artifact,
    import_multi_trader_smoke_artifacts,
    inspect_multi_trader_smoke_readiness,
    package_multi_trader_smoke,
    pin_active_pool_symbol,
    prepare_multi_trader_smoke,
    record_multi_trader_smoke_workflow,
    replay_kafka_spool,
    unpin_active_pool_symbol,
    validate_redis_read_model_coverage,
    verify_multi_trader_smoke_services,
)
from .production_adapters import KafkaEventBusAdapter


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "finalize-shadow-run":
        policy = CutoverPolicy(
            min_parallel_session_count=args.min_parallel_session_count,
            min_session_duration_seconds=args.min_session_duration_seconds,
            require_non_empty_streams=not args.allow_empty_streams,
            require_no_failed_symbols=not args.allow_failed_symbols,
            allow_legacy_retirement=not args.hold_legacy_retirement,
        )
        result = finalize_shadow_run_cutover(
            stream_directory=args.stream_directory,
            session_id=args.session_id,
            trading_date=args.trading_date,
            finished_at=args.finished_at,
            reports_directory=args.reports_directory,
            readiness_path=args.readiness_path,
            frontend_env_path=args.frontend_env_path,
            live_url=args.live_url,
            policy=policy,
        )
        print(
            json.dumps(
                {
                    "passed": result.report["passed"],
                    "session_id": result.report["session_id"],
                    "trading_date": result.report["trading_date"],
                    "report_path": str(result.report_path),
                    "readiness_path": str(result.readiness_path),
                    "frontend_env_path": str(result.frontend_env_path),
                },
                sort_keys=True,
            )
        )
        return 0

    if args.command == "generate-historical-manifests":
        result = generate_required_historical_manifests(
            silver_root=args.silver_root,
            manifest_root=args.manifest_directory,
            start_date=args.start_date,
            end_date=args.end_date,
            symbols=parse_symbols(args.symbols),
            code_version=args.code_version,
        )
        print(
            json.dumps(
                {
                    "passed": result.passed,
                    "data_types": [manifest["data_type"] for manifest in result.manifests],
                    "failed_data_types": result.failed_data_types,
                    "manifest_paths": [str(path) for path in result.manifest_paths],
                },
                sort_keys=True,
            )
        )
        return 0

    if args.command == "import-legacy-telemetry":
        result = import_legacy_shadow_telemetry(
            input_path=args.input_path,
            stream_directory=args.stream_directory,
            session_id=args.session_id,
            trading_date=args.trading_date,
            started_at=args.started_at,
            source=args.source,
            reset=args.reset,
        )
        print(
            json.dumps(
                {
                    "imported_count": result.imported_count,
                    "stream_directory": str(result.stream_directory),
                    "legacy_events_path": str(result.legacy_events_path),
                },
                sort_keys=True,
            )
        )
        return 0

    if args.command == "import-frontend-performance":
        result = import_frontend_performance_samples(
            input_path=args.input_path,
            stream_directory=args.stream_directory,
            session_id=args.session_id,
            trading_date=args.trading_date,
            started_at=args.started_at,
        )
        print(
            json.dumps(
                {
                    "imported_count": result.imported_count,
                    "stream_directory": str(result.stream_directory),
                    "performance_samples_path": str(result.performance_samples_path),
                },
                sort_keys=True,
            )
        )
        return 0

    if args.command == "verify-frontend-deployment":
        result = write_frontend_deployment_evidence(
            env_path=args.env_path,
            expected_live_url=args.expected_live_url,
            output_path=args.output_path,
            verified_at=args.verified_at or None,
        )
        print(
            json.dumps(
                {
                    "passed": result["passed"],
                    "frontend_default_v2_deployed": result["frontend_default_v2_deployed"],
                    "output_path": args.output_path,
                    "blockers": result["blockers"],
                },
                sort_keys=True,
            )
        )
        return 0

    if args.command == "verify-runtime-health":
        result = write_runtime_health_verification(
            runtime_health_path=args.runtime_health_path,
            output_path=args.output_path,
        )
        print(
            json.dumps(
                {
                    "passed": result["passed"],
                    "output_path": args.output_path,
                    "blockers": result["blockers"],
                },
                sort_keys=True,
            )
        )
        return 0 if result["passed"] else 1

    if args.command == "verify-runtime-config":
        result = write_runtime_config_verification(
            config_path=args.config_path,
            output_path=args.output_path,
        )
        print(
            json.dumps(
                {
                    "passed": result["passed"],
                    "output_path": args.output_path,
                    "blockers": result["blockers"],
                },
                sort_keys=True,
            )
        )
        return 0 if result["passed"] else 1

    if args.command == "generate-active-pool":
        result = generate_active_pool(
            silver_root=args.silver_root,
            trade_date=args.trade_date,
            target_size=args.target_size,
            pinned_max_size=args.pinned_max_size,
            rank_window_days=args.rank_window_days,
            rank_metric=args.rank_metric,
            exclude_instrument_types=parse_csv(args.exclude_instrument_types),
            pinned_path=args.pinned_path or None,
        )
        print(json.dumps(result.snapshot, sort_keys=True))
        return 0

    if args.command == "explain-active-pool-symbol":
        result = explain_active_pool_symbol(
            silver_root=args.silver_root,
            trade_date=args.trade_date,
            symbol=args.symbol,
            target_size=args.target_size,
            pinned_max_size=args.pinned_max_size,
            rank_window_days=args.rank_window_days,
            rank_metric=args.rank_metric,
            exclude_instrument_types=parse_csv(args.exclude_instrument_types),
            pinned_path=args.pinned_path or None,
        )
        print(json.dumps(result.snapshot["explanation"], sort_keys=True))
        return 0

    if args.command in {"pin-active-symbol", "unpin-active-symbol"}:
        operation = pin_active_pool_symbol if args.command == "pin-active-symbol" else unpin_active_pool_symbol
        result = operation(
            silver_root=args.silver_root,
            trade_date=args.trade_date,
            symbol=args.symbol,
            pinned_path=args.pinned_path,
            target_size=args.target_size,
            pinned_max_size=args.pinned_max_size,
            rank_window_days=args.rank_window_days,
            rank_metric=args.rank_metric,
            exclude_instrument_types=parse_csv(args.exclude_instrument_types),
        )
        print(
            json.dumps(
                {
                    "operation": result.operation,
                    "symbol": result.symbol,
                    "change": result.change,
                    "pinned_path": str(result.pinned_path),
                    "snapshot": result.snapshot,
                },
                sort_keys=True,
            )
        )
        return 0

    if args.command == "validate-redis-read-model-coverage":
        redis_client = build_redis_client(args.redis_url)
        result = validate_redis_read_model_coverage(
            redis_client=redis_client,
            trade_date=args.trade_date,
            symbols=parse_symbols(args.symbols),
        )
        print(json.dumps(result, sort_keys=True))
        return 0 if result["passed"] else 1

    if args.command == "clear-runtime-cache":
        redis_client = build_redis_client(args.redis_url) if args.redis_url else None
        result = clear_runtime_cache(
            redis_client=redis_client,
            trade_date=args.trade_date,
            symbols=parse_symbols(args.symbols),
            dry_run=args.dry_run or not args.confirm,
            confirm=args.confirm,
        )
        print(
            json.dumps(
                {
                    "dry_run": result.dry_run,
                    "confirmed": result.confirmed,
                    "keys": result.keys,
                    "deleted_keys": result.deleted_keys,
                },
                sort_keys=True,
            )
        )
        return 0

    if args.command == "clear-runtime-state":
        result = clear_runtime_state(
            runtime_state_root=args.runtime_state_root,
            trade_date=args.trade_date,
            symbols=parse_symbols(args.symbols),
            dry_run=args.dry_run or not args.confirm,
            confirm=args.confirm,
            include_callback_rejections=args.include_callback_rejections,
            include_dead_letters=args.include_dead_letters,
        )
        print(
            json.dumps(
                {
                    "dry_run": result.dry_run,
                    "confirmed": result.confirmed,
                    "paths": result.paths,
                    "deleted_paths": result.deleted_paths,
                },
                sort_keys=True,
            )
        )
        return 0

    if args.command == "replay-kafka-spool":
        event_bus = NoopEventBus() if args.dry_run or not args.confirm else build_kafka_event_bus(args.kafka_bootstrap_servers)
        result = replay_kafka_spool(
            spool_path=args.spool_path,
            event_bus=event_bus,
            dry_run=args.dry_run or not args.confirm,
            confirm=args.confirm,
        )
        print(
            json.dumps(
                {
                    "dry_run": result.dry_run,
                    "confirmed": result.confirmed,
                    "spool_path": str(result.spool_path),
                    "quarantine_path": str(result.quarantine_path),
                    "replayed_count": result.replayed_count,
                    "failed_count": result.failed_count,
                    "remaining_count": result.remaining_count,
                    "quarantined_count": result.quarantined_count,
                    "deleted_spool": result.deleted_spool,
                    "errors": result.errors,
                },
                sort_keys=True,
            )
        )
        return 1 if result.failed_count else 0

    if args.command == "verify-legacy-decommission":
        result = write_legacy_decommission_evidence(
            observation_path=args.observation_path,
            output_path=args.output_path,
            expected_old_topics=parse_symbols(args.expected_old_topics),
        )
        print(
            json.dumps(
                {
                    "passed": result["passed"],
                    "legacy_websocket_disabled": result["legacy_websocket_disabled"],
                    "old_topic_consumers_disabled": result["old_topic_consumers_disabled"],
                    "no_legacy_consumers_observed": result["no_legacy_consumers_observed"],
                    "output_path": args.output_path,
                    "blockers": result["blockers"],
                },
                sort_keys=True,
            )
        )
        return 0

    if args.command == "verify-multi-trader-smoke":
        result = write_multi_trader_smoke_evidence(
            observation_path=args.observation_path,
            output_path=args.output_path,
        )
        print(
            json.dumps(
                {
                    "passed": result["passed"],
                    "output_path": args.output_path,
                    "blockers": result["blockers"],
                },
                sort_keys=True,
            )
        )
        return 0 if result["passed"] else 1

    if args.command == "build-multi-trader-smoke-observation":
        try:
            result = build_multi_trader_smoke_observation(
                clients_path=args.clients_path,
                workflows_path=args.workflows_path,
                runtime_health_path=args.runtime_health_path,
                performance_samples_path=args.performance_samples_path or None,
                metrics_path=args.metrics_path or None,
                preflight_path=args.preflight_path or None,
                observed_at=args.observed_at,
                output_path=args.output_path,
            )
        except ValueError as error:
            print(
                json.dumps(
                    {
                        "passed": False,
                        "output_path": args.output_path,
                        "error": str(error),
                    },
                    sort_keys=True,
                )
            )
            return 1
        print(
            json.dumps(
                {
                    "passed": True,
                    "output_path": str(result.output_path),
                    "client_count": len(result.observation["clients"]),
                    "workflow_count": len(result.observation["workflows"]),
                    "runtime_health_passed": result.observation["runtime_health"]["passed"],
                },
                sort_keys=True,
            )
        )
        return 0

    if args.command == "build-multi-trader-smoke-workflows-template":
        result = build_multi_trader_smoke_workflows_template(
            output_path=args.output_path,
            cold_query_symbol=args.cold_query_symbol,
            redis_clear_symbol=args.redis_clear_symbol,
            add_to_watchlist_symbol=args.add_to_watchlist_symbol,
            requested_trade_date=args.requested_trade_date,
            effective_trade_date=args.effective_trade_date,
        )
        print(
            json.dumps(
                {
                    "output_path": str(result.output_path),
                    "workflow_count": len(result.workflows),
                },
                sort_keys=True,
            )
        )
        return 0

    if args.command == "prepare-multi-trader-smoke":
        result = prepare_multi_trader_smoke(
            root_path=args.root_path,
            lan_host=args.lan_host,
            frontend_port=args.frontend_port,
            gateway_port=args.gateway_port,
            silver_root=args.silver_root or None,
            cold_query_symbol=args.cold_query_symbol,
            redis_clear_symbol=args.redis_clear_symbol,
            add_to_watchlist_symbol=args.add_to_watchlist_symbol,
            requested_trade_date=args.requested_trade_date,
            effective_trade_date=args.effective_trade_date,
            require_local_lan_host=args.require_local_lan_host,
        )
        if args.print_env:
            print(f"export SMOKE_DIR={shlex.quote(str(result.root_path))}")
            print(f"export SMOKE_PAGE_URL={shlex.quote(str(result.preflight['page_url']))}")
            print(f"export SMOKE_GATEWAY_URL={shlex.quote(str(result.preflight['gateway_url']))}")
            print(f"export SMOKE_PREFLIGHT_PATH={shlex.quote(str(result.preflight_path))}")
            print(f"export SMOKE_CLIENT_INSTRUCTIONS_PATH={shlex.quote(str(result.preflight['artifact_paths']['client_instructions']))}")
            print(f"export SMOKE_WORKFLOWS_PATH={shlex.quote(str(result.preflight['artifact_paths']['workflows']))}")
            print(f"export SMOKE_SERVICE_PREFLIGHT_PATH={shlex.quote(str(result.preflight['artifact_paths']['service_preflight']))}")
            print(f"export SMOKE_RUNTIME_HEALTH_PATH={shlex.quote(str(result.preflight['artifact_paths']['runtime_health']))}")
            print(f"export SMOKE_OBSERVATION_PATH={shlex.quote(str(result.preflight['artifact_paths']['observation']))}")
            print(f"export SMOKE_EVIDENCE_PATH={shlex.quote(str(result.preflight['artifact_paths']['evidence']))}")
            print(f"export SMOKE_IMPORT_MANIFEST_PATH={shlex.quote(str(result.preflight['artifact_paths']['import_manifest']))}")
            print(f"export SMOKE_RUN_MANIFEST_PATH={shlex.quote(str(result.preflight['artifact_paths']['run_manifest']))}")
            print(f"export SMOKE_PACKAGE_PATH={shlex.quote(str(result.preflight['artifact_paths']['package']))}")
            print(f"export SMOKE_PACKAGE_METADATA_PATH={shlex.quote(str(result.preflight['artifact_paths']['package_metadata']))}")
            if result.preflight.get("silver_root"):
                print(f"export SMOKE_SILVER_ROOT={shlex.quote(str(result.preflight['silver_root']))}")
            print(f"export SMOKE_BACKEND_COMMAND={shlex.quote(str(result.preflight['commands']['backend']))}")
            print(f"export SMOKE_FRONTEND_COMMAND={shlex.quote(str(result.preflight['commands']['frontend']))}")
            print(f"export SMOKE_SERVICE_PREFLIGHT_COMMAND={shlex.quote(str(result.preflight['commands']['service_preflight']))}")
            print(f"export SMOKE_BACKEND_SCRIPT={shlex.quote(str(result.preflight['commands']['backend_script']))}")
            print(f"export SMOKE_RESTART_BACKEND_SCRIPT={shlex.quote(str(result.preflight['commands']['restart_backend_script']))}")
            print(f"export SMOKE_FRONTEND_SCRIPT={shlex.quote(str(result.preflight['commands']['frontend_script']))}")
            print(f"export SMOKE_SERVICE_PREFLIGHT_SCRIPT={shlex.quote(str(result.preflight['commands']['service_preflight_script']))}")
            print(f"export SMOKE_INSPECT_NEXT_ACTION_SCRIPT={shlex.quote(str(result.preflight['commands']['inspect_next_action_script']))}")
            print(f"export SMOKE_VERIFY_HANDOFF_SCRIPT={shlex.quote(str(result.preflight['commands']['verify_handoff_script']))}")
            print(f"export SMOKE_FINALIZE_PACKAGE_SCRIPT={shlex.quote(str(result.preflight['commands']['finalize_package_script']))}")
            print(f"export SMOKE_IMPORT_ARTIFACTS_SCRIPT={shlex.quote(str(result.preflight['commands']['import_artifacts_script']))}")
            print(f"export SMOKE_RECORD_WORKFLOW_SCRIPT={shlex.quote(str(result.preflight['commands']['record_workflow_script']))}")
            if result.preflight["passed"] is not True:
                print(f"# prepare-multi-trader-smoke failed: {','.join(result.preflight['blockers'])}")
                print("false")
            return 0 if result.preflight["passed"] else 1
        print(
            json.dumps(
                {
                    "passed": result.preflight["passed"],
                    "root_path": str(result.root_path),
                    "preflight_path": str(result.preflight_path),
                    "workflows_path": str(result.workflows_path),
                    "page_url": result.preflight["page_url"],
                    "gateway_url": result.preflight["gateway_url"],
                    "frontend_port": result.preflight["frontend_port"],
                    "gateway_port": result.preflight["gateway_port"],
                    "requested_frontend_port": result.preflight["requested_frontend_port"],
                    "requested_gateway_port": result.preflight["requested_gateway_port"],
                    "auto_selected_frontend_port": result.preflight["auto_selected_frontend_port"],
                    "auto_selected_gateway_port": result.preflight["auto_selected_gateway_port"],
                    "local_lan_host": result.preflight["local_lan_host"],
                    "runtime_symbols": result.preflight["runtime_symbols"],
                    "frontend_initial_symbols": result.preflight["frontend_initial_symbols"],
                    "artifact_paths": result.preflight["artifact_paths"],
                    "commands": result.preflight["commands"],
                    "blockers": result.preflight["blockers"],
                    "warnings": result.preflight["warnings"],
                },
                sort_keys=True,
            )
        )
        return 0 if result.preflight["passed"] else 1

    if args.command == "verify-multi-trader-smoke-services":
        result = verify_multi_trader_smoke_services(
            root_path=args.root_path,
            timeout_seconds=args.timeout_seconds,
        )
        print(
            json.dumps(
                {
                    "passed": result.passed,
                    "service_preflight_path": str(result.output_path),
                    "preflight_path": str(result.preflight_path),
                    "blockers": result.preflight["blockers"],
                    "service_blockers": result.service_checks["blockers"],
                    "checks": result.service_checks["checks"],
                },
                sort_keys=True,
            )
        )
        return 0 if result.passed else 1

    if args.command == "record-multi-trader-smoke-workflow":
        try:
            result = record_multi_trader_smoke_workflow(
                workflows_path=args.workflows_path,
                workflow=args.workflow,
                symbol=args.symbol,
                requested_trade_date=args.requested_trade_date,
                effective_trade_date=args.effective_trade_date,
                observed_at=args.observed_at,
                notes=args.notes,
            )
        except ValueError as error:
            print(
                json.dumps(
                    {
                        "passed": False,
                        "workflows_path": args.workflows_path,
                        "workflow": args.workflow,
                        "error": str(error),
                    },
                    sort_keys=True,
                )
            )
            return 1
        workflow_entry = result.workflows[result.workflow]
        print(
            json.dumps(
                {
                    "workflow": result.workflow,
                    "passed": workflow_entry.get("passed") is True,
                    "workflows_path": str(result.output_path),
                    "entry": workflow_entry,
                },
                sort_keys=True,
            )
        )
        return 0

    if args.command == "import-multi-trader-smoke-artifact":
        result = import_multi_trader_smoke_artifact(
            root_path=args.root_path,
            input_path=args.input_path,
            kind=args.kind,
        )
        print(
            json.dumps(
                {
                    "kind": result.kind,
                    "input_path": str(result.input_path),
                    "output_path": str(result.output_path),
                    "manifest_path": str(result.manifest_path) if result.manifest_path is not None else None,
                },
                sort_keys=True,
            )
        )
        return 0

    if args.command == "import-multi-trader-smoke-artifacts":
        result = import_multi_trader_smoke_artifacts(
            root_path=args.root_path,
            input_path=args.input_path,
            kind=args.kind,
        )
        print(
            json.dumps(
                {
                    "imported_count": len(result.imported),
                    "skipped_count": len(result.skipped),
                    "manifest_path": str(result.manifest_path),
                    "imported": [
                        {
                            "kind": item.kind,
                            "input_path": str(item.input_path),
                            "output_path": str(item.output_path),
                        }
                        for item in result.imported
                    ],
                    "skipped": result.skipped,
                },
                sort_keys=True,
            )
        )
        return 0 if result.imported else 1

    if args.command == "inspect-multi-trader-smoke":
        result = inspect_multi_trader_smoke_readiness(root_path=args.root_path)
        package_ready = bool((result.summary.get("package_readiness") or {}).get("ready"))
        if args.next_action:
            next_actions = result.summary.get("next_actions") if isinstance(result.summary.get("next_actions"), list) else []
            print(
                json.dumps(
                    {
                        "ready": result.ready,
                        "package_ready": package_ready,
                        "blockers": result.summary.get("blockers") or [],
                        "preflight_readiness": result.summary.get("preflight_readiness") or {},
                        "runtime_health_readiness": result.summary.get("runtime_health_readiness") or {},
                        "next_action": next_actions[0] if next_actions else None,
                    },
                    sort_keys=True,
                )
            )
        else:
            print(json.dumps(result.summary, sort_keys=True))
        return 0 if result.ready and (package_ready or not args.require_package_ready) else 1

    if args.command == "finalize-multi-trader-smoke":
        result = finalize_multi_trader_smoke(
            root_path=args.root_path,
            observed_at=args.observed_at,
        )
        package_result = None
        if args.package:
            try:
                package_result = package_multi_trader_smoke(
                    root_path=args.root_path,
                    output_path=args.package_output_path or None,
                    metadata_path=args.package_metadata_path or None,
                )
            except ValueError as error:
                package_readiness = inspect_multi_trader_smoke_readiness(root_path=args.root_path).summary[
                    "package_readiness"
                ]
                print(
                    json.dumps(
                        {
                            "passed": False,
                            "root_path": str(result.root_path),
                            "observation_path": str(result.observation_path),
                            "evidence_path": str(result.evidence_path),
                            "manifest_path": str(result.manifest_path),
                            "blockers": result.evidence["blockers"],
                            "package_error": str(error),
                            "package_readiness": package_readiness,
                        },
                        sort_keys=True,
                    )
                )
                return 1
        summary = {
            "passed": result.evidence["passed"],
            "root_path": str(result.root_path),
            "observation_path": str(result.observation_path),
            "evidence_path": str(result.evidence_path),
            "manifest_path": str(result.manifest_path),
            "blockers": result.evidence["blockers"],
        }
        if package_result is not None:
            summary.update(
                {
                    "package_path": str(package_result.package_path),
                    "package_metadata_path": str(package_result.metadata_path),
                    "package_sha256": package_result.sha256,
                    "package_bytes": package_result.byte_count,
                    "package_file_count": package_result.file_count,
                }
            )
        print(
            json.dumps(
                summary,
                sort_keys=True,
            )
        )
        return 0 if result.evidence["passed"] else 1

    if args.command == "package-multi-trader-smoke":
        try:
            result = package_multi_trader_smoke(
                root_path=args.root_path,
                output_path=args.output_path or None,
                metadata_path=args.metadata_path or None,
            )
        except ValueError as error:
            print(json.dumps({"passed": False, "error": str(error)}, sort_keys=True))
            return 1
        print(
            json.dumps(
                {
                    "package_path": str(result.package_path),
                    "metadata_path": str(result.metadata_path),
                    "sha256": result.sha256,
                    "bytes": result.byte_count,
                    "file_count": result.file_count,
                    "files": result.files,
                },
                sort_keys=True,
            )
        )
        return 0

    if args.command == "finalize-legacy-retirement":
        result = write_legacy_retirement_from_artifacts(
            readiness_path=args.readiness_path,
            frontend_deployment_path=args.frontend_deployment_path,
            legacy_decommission_path=args.legacy_decommission_path,
            rollback_window_completed=args.rollback_window_completed,
            rollback_window_started_at=args.rollback_window_started_at,
            rollback_window_completed_at=args.rollback_window_completed_at,
            operator_approved=args.operator_approved,
            operator_approved_at=args.operator_approved_at,
            output_path=args.output_path,
            notes=args.notes,
        )
        print(
            json.dumps(
                {
                    "passed": result["passed"],
                    "legacy_retired": result["legacy_retired"],
                    "output_path": args.output_path,
                    "blockers": result["blockers"],
                },
                sort_keys=True,
            )
        )
        return 0

    if args.command == "verify-evidence-bundle":
        result = write_evidence_bundle_verification(
            paths=EvidenceBundlePaths(
                shadow_reports_directory=args.shadow_reports_directory,
                manifest_directory=args.manifest_directory,
                runtime_config_path=args.runtime_config_path,
                runtime_health_path=args.runtime_health_path,
                readiness_path=args.readiness_path,
                frontend_deployment_path=args.frontend_deployment_path,
                legacy_decommission_path=args.legacy_decommission_path,
                legacy_retirement_path=args.legacy_retirement_path,
                multi_trader_smoke_path=args.multi_trader_smoke_path or None,
                multi_trader_smoke_preflight_path=args.multi_trader_smoke_preflight_path or None,
                multi_trader_smoke_manifest_path=args.multi_trader_smoke_manifest_path or None,
                multi_trader_smoke_package_path=args.multi_trader_smoke_package_path or None,
                multi_trader_smoke_package_metadata_path=args.multi_trader_smoke_package_metadata_path or None,
            ),
            output_path=args.output_path,
        )
        print(
            json.dumps(
                {
                    "passed": result["passed"],
                    "output_path": args.output_path,
                    "blockers": result["blockers"],
                },
                sort_keys=True,
            )
        )
        return 0 if result["passed"] else 1

    parser.error(f"unsupported command: {args.command}")
    return 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m beast_market.ops_cli")
    subparsers = parser.add_subparsers(dest="command", required=True)

    finalize = subparsers.add_parser(
        "finalize-shadow-run",
        help="Build a shadow-run report from NDJSON streams and write cutover artifacts.",
    )
    finalize.add_argument("--stream-directory", required=True)
    finalize.add_argument("--session-id", required=True)
    finalize.add_argument("--trading-date", required=True)
    finalize.add_argument("--finished-at", required=True)
    finalize.add_argument("--reports-directory", required=True)
    finalize.add_argument("--readiness-path", required=True)
    finalize.add_argument("--frontend-env-path", required=True)
    finalize.add_argument("--live-url", required=True)
    finalize.add_argument("--min-parallel-session-count", type=int, default=1)
    finalize.add_argument("--min-session-duration-seconds", type=float, default=4 * 60 * 60)
    finalize.add_argument("--allow-empty-streams", action="store_true")
    finalize.add_argument("--allow-failed-symbols", action="store_true")
    finalize.add_argument("--hold-legacy-retirement", action="store_true")

    manifests = subparsers.add_parser(
        "generate-historical-manifests",
        help="Generate required Mammoth silver historical manifests for production evidence.",
    )
    manifests.add_argument("--silver-root", required=True)
    manifests.add_argument("--manifest-directory", required=True)
    manifests.add_argument("--start-date", required=True)
    manifests.add_argument("--end-date", required=True)
    manifests.add_argument("--symbols", default="")
    manifests.add_argument("--code-version", required=True)

    legacy_import = subparsers.add_parser(
        "import-legacy-telemetry",
        help="Normalize legacy WebSocket/terminal NDJSON telemetry into shadow-run legacy event streams.",
    )
    legacy_import.add_argument("--input-path", required=True)
    legacy_import.add_argument("--stream-directory", required=True)
    legacy_import.add_argument("--session-id", required=True)
    legacy_import.add_argument("--trading-date", required=True)
    legacy_import.add_argument("--started-at", required=True)
    legacy_import.add_argument("--source", default="legacy")
    legacy_import.add_argument("--reset", action="store_true")

    frontend_perf = subparsers.add_parser(
        "import-frontend-performance",
        help="Import frontend performance NDJSON samples into a shadow-run performance stream.",
    )
    frontend_perf.add_argument("--input-path", required=True)
    frontend_perf.add_argument("--stream-directory", required=True)
    frontend_perf.add_argument("--session-id", required=True)
    frontend_perf.add_argument("--trading-date", required=True)
    frontend_perf.add_argument("--started-at", required=True)

    frontend = subparsers.add_parser(
        "verify-frontend-deployment",
        help="Verify a deployed frontend env selects Gateway v2 and write evidence JSON.",
    )
    frontend.add_argument("--env-path", required=True)
    frontend.add_argument("--expected-live-url", required=True)
    frontend.add_argument("--output-path", required=True)
    frontend.add_argument("--verified-at", default="")

    runtime_health = subparsers.add_parser(
        "verify-runtime-health",
        help="Verify runtime health evidence for Kafka lag, Redis, freshness, and dead letters.",
    )
    runtime_health.add_argument("--runtime-health-path", required=True)
    runtime_health.add_argument("--output-path", required=True)

    runtime_config = subparsers.add_parser(
        "verify-runtime-config",
        help="Verify production runtime config for Gateway, Kafka, Redis, queues, freshness, and client injection evidence.",
    )
    runtime_config.add_argument("--config-path", required=True)
    runtime_config.add_argument("--output-path", required=True)

    active_pool = subparsers.add_parser(
        "generate-active-pool",
        help="Generate today's active symbol pool from silver daily bars.",
    )
    add_active_pool_arguments(active_pool)
    active_pool.add_argument("--pinned-path", default="")

    explain_pool = subparsers.add_parser(
        "explain-active-pool-symbol",
        help="Explain why one symbol is active, pinned, temporary, excluded, or below cutoff.",
    )
    add_active_pool_arguments(explain_pool)
    explain_pool.add_argument("--symbol", required=True)
    explain_pool.add_argument("--pinned-path", default="")

    pin_pool = subparsers.add_parser(
        "pin-active-symbol",
        help="Manually add a symbol to the query-pinned active pool file.",
    )
    add_active_pool_arguments(pin_pool)
    pin_pool.add_argument("--symbol", required=True)
    pin_pool.add_argument("--pinned-path", required=True)

    unpin_pool = subparsers.add_parser(
        "unpin-active-symbol",
        help="Manually remove a symbol from the query-pinned active pool file.",
    )
    add_active_pool_arguments(unpin_pool)
    unpin_pool.add_argument("--symbol", required=True)
    unpin_pool.add_argument("--pinned-path", required=True)

    redis_coverage = subparsers.add_parser(
        "validate-redis-read-model-coverage",
        help="Validate Redis read-model keys for a date and symbol set.",
    )
    redis_coverage.add_argument("--redis-url", required=True)
    redis_coverage.add_argument("--trade-date", required=True)
    redis_coverage.add_argument("--symbols", required=True)

    runtime_cache = subparsers.add_parser(
        "clear-runtime-cache",
        help="Safely clear dashboard Redis runtime cache for explicit date and symbol scopes.",
    )
    runtime_cache.add_argument("--trade-date", required=True)
    runtime_cache.add_argument("--symbols", required=True)
    runtime_cache.add_argument("--redis-url", default="")
    runtime_cache.add_argument("--dry-run", action="store_true")
    runtime_cache.add_argument("--confirm", action="store_true")

    runtime_state = subparsers.add_parser(
        "clear-runtime-state",
        help="Explicitly clear local JSONL runtime state for date and symbol scopes.",
    )
    runtime_state.add_argument("--runtime-state-root", default="artifacts/runtime-state")
    runtime_state.add_argument("--trade-date", required=True)
    runtime_state.add_argument("--symbols", required=True)
    runtime_state.add_argument("--include-callback-rejections", action="store_true")
    runtime_state.add_argument("--include-dead-letters", action="store_true")
    runtime_state.add_argument("--dry-run", action="store_true")
    runtime_state.add_argument("--confirm", action="store_true")

    replay_spool = subparsers.add_parser(
        "replay-kafka-spool",
        help="Replay persistent Kafka publish-failure spool records. Dry-run is default; real replay requires --confirm.",
    )
    replay_spool.add_argument("--spool-path", required=True)
    replay_spool.add_argument("--kafka-bootstrap-servers", required=True)
    replay_spool.add_argument("--dry-run", action="store_true")
    replay_spool.add_argument("--confirm", action="store_true")

    legacy = subparsers.add_parser(
        "verify-legacy-decommission",
        help="Verify legacy websocket and old topic consumers are disabled from an observation JSON.",
    )
    legacy.add_argument("--observation-path", required=True)
    legacy.add_argument("--output-path", required=True)
    legacy.add_argument("--expected-old-topics", default=",".join(DEFAULT_LEGACY_TOPIC_NAMES))

    smoke = subparsers.add_parser(
        "verify-multi-trader-smoke",
        help="Verify Phase 6 LAN multi-trader smoke observation evidence.",
    )
    smoke.add_argument("--observation-path", required=True)
    smoke.add_argument("--output-path", required=True)

    build_smoke = subparsers.add_parser(
        "build-multi-trader-smoke-observation",
        help="Build a Phase 6 LAN multi-trader smoke observation JSON from collected artifacts.",
    )
    build_smoke.add_argument("--clients-path", required=True)
    build_smoke.add_argument("--workflows-path", required=True)
    build_smoke.add_argument("--runtime-health-path", required=True)
    build_smoke.add_argument("--performance-samples-path", default="")
    build_smoke.add_argument("--metrics-path", default="")
    build_smoke.add_argument("--preflight-path", default="")
    build_smoke.add_argument("--observed-at", required=True)
    build_smoke.add_argument("--output-path", required=True)

    workflow_template = subparsers.add_parser(
        "build-multi-trader-smoke-workflows-template",
        help="Write a standard Phase 6 LAN multi-trader workflow evidence template.",
    )
    workflow_template.add_argument("--output-path", required=True)
    workflow_template.add_argument("--cold-query-symbol", default="")
    workflow_template.add_argument("--redis-clear-symbol", default="")
    workflow_template.add_argument("--add-to-watchlist-symbol", default="")
    workflow_template.add_argument("--requested-trade-date", default="")
    workflow_template.add_argument("--effective-trade-date", default="")

    prepare_smoke = subparsers.add_parser(
        "prepare-multi-trader-smoke",
        help="Create Phase 6 LAN smoke directories, workflow template, and preflight evidence.",
    )
    prepare_smoke.add_argument("--root-path", required=True, help="Smoke artifact directory, or 'auto' for artifacts/multi-trader-smoke/<timestamp>.")
    prepare_smoke.add_argument("--lan-host", required=True, help="Backend LAN IP/host, or 'auto' to use a detected local LAN IP.")
    prepare_smoke.add_argument("--frontend-port", default="5173", help="Frontend port number, or 'auto' to choose the first free port from 5173.")
    prepare_smoke.add_argument("--gateway-port", default="9020", help="Gateway port number, or 'auto' to choose the first free port from 9020.")
    prepare_smoke.add_argument("--silver-root", default="", help="CSV silver root to pass to the generated real-data backend command.")
    prepare_smoke.add_argument("--cold-query-symbol", default="")
    prepare_smoke.add_argument("--redis-clear-symbol", default="")
    prepare_smoke.add_argument("--add-to-watchlist-symbol", default="")
    prepare_smoke.add_argument("--requested-trade-date", default="")
    prepare_smoke.add_argument("--effective-trade-date", default="")
    prepare_smoke.add_argument("--require-local-lan-host", action="store_true")
    prepare_smoke.add_argument(
        "--print-env",
        action="store_true",
        help="Print shell export lines for SMOKE_DIR and prepared URLs instead of JSON.",
    )

    verify_smoke_services = subparsers.add_parser(
        "verify-multi-trader-smoke-services",
        help="Check that prepared LAN smoke frontend and Gateway ports are reachable from the backend host.",
    )
    verify_smoke_services.add_argument("--root-path", required=True)
    verify_smoke_services.add_argument("--timeout-seconds", type=float, default=1.0)

    record_workflow = subparsers.add_parser(
        "record-multi-trader-smoke-workflow",
        help="Mark one observed Phase 6 workflow complete in workflows.json without hand-editing JSON.",
    )
    record_workflow.add_argument("--workflows-path", required=True)
    record_workflow.add_argument(
        "--workflow",
        required=True,
        choices=[
            "cold_query",
            "add_to_watchlist",
            "refresh_recovery",
            "redis_clear_recovery",
            "process_restart_recovery",
            "closed_market_effective_date",
        ],
    )
    record_workflow.add_argument("--symbol", default="")
    record_workflow.add_argument("--requested-trade-date", default="")
    record_workflow.add_argument("--effective-trade-date", default="")
    record_workflow.add_argument("--observed-at", default="")
    record_workflow.add_argument("--notes", default="")

    import_smoke_artifact = subparsers.add_parser(
        "import-multi-trader-smoke-artifact",
        help="Import a browser-exported smoke JSON into the prepared clients/ or performance/ directory.",
    )
    import_smoke_artifact.add_argument("--root-path", required=True)
    import_smoke_artifact.add_argument("--input-path", required=True)
    import_smoke_artifact.add_argument("--kind", choices=["auto", "client", "performance"], default="auto")

    import_smoke_artifacts = subparsers.add_parser(
        "import-multi-trader-smoke-artifacts",
        help="Import all recognizable browser-exported smoke JSON files from a file or directory.",
    )
    import_smoke_artifacts.add_argument("--root-path", required=True)
    import_smoke_artifacts.add_argument("--input-path", required=True)
    import_smoke_artifacts.add_argument("--kind", choices=["auto", "client", "performance"], default="auto")

    inspect_smoke = subparsers.add_parser(
        "inspect-multi-trader-smoke",
        help="Check collected Phase 6 smoke artifacts before finalizing.",
    )
    inspect_smoke.add_argument("--root-path", required=True)
    inspect_smoke.add_argument(
        "--require-package-ready",
        action="store_true",
        help="Also return non-zero when final package handoff requirements are not yet satisfied.",
    )
    inspect_smoke.add_argument(
        "--next-action",
        action="store_true",
        help="Print only the next operator action instead of the full readiness summary.",
    )

    finalize_smoke = subparsers.add_parser(
        "finalize-multi-trader-smoke",
        help="Build and verify Phase 6 LAN smoke artifacts from a prepared smoke directory.",
    )
    finalize_smoke.add_argument("--root-path", required=True)
    finalize_smoke.add_argument("--observed-at", required=True)
    finalize_smoke.add_argument("--package", action="store_true")
    finalize_smoke.add_argument("--package-output-path", default="")
    finalize_smoke.add_argument("--package-metadata-path", default="")

    package_smoke = subparsers.add_parser(
        "package-multi-trader-smoke",
        help="Package Phase 6 LAN smoke JSON evidence into a zip and write package metadata.",
    )
    package_smoke.add_argument("--root-path", required=True)
    package_smoke.add_argument("--output-path", default="")
    package_smoke.add_argument("--metadata-path", default="")

    retirement = subparsers.add_parser(
        "finalize-legacy-retirement",
        help="Combine readiness, frontend deployment, and legacy decommission evidence into final retirement evidence.",
    )
    retirement.add_argument("--readiness-path", required=True)
    retirement.add_argument("--frontend-deployment-path", required=True)
    retirement.add_argument("--legacy-decommission-path", required=True)
    retirement.add_argument("--output-path", required=True)
    retirement.add_argument("--rollback-window-completed", action="store_true")
    retirement.add_argument("--rollback-window-started-at", required=True)
    retirement.add_argument("--rollback-window-completed-at", required=True)
    retirement.add_argument("--operator-approved", action="store_true")
    retirement.add_argument("--operator-approved-at", required=True)
    retirement.add_argument("--notes", default="")

    bundle = subparsers.add_parser(
        "verify-evidence-bundle",
        help="Verify the full production evidence bundle needed for final cutover and retirement.",
    )
    bundle.add_argument("--shadow-reports-directory", required=True)
    bundle.add_argument("--manifest-directory", required=True)
    bundle.add_argument("--runtime-config-path", required=True)
    bundle.add_argument("--runtime-health-path", required=True)
    bundle.add_argument("--readiness-path", required=True)
    bundle.add_argument("--frontend-deployment-path", required=True)
    bundle.add_argument("--legacy-decommission-path", required=True)
    bundle.add_argument("--legacy-retirement-path", required=True)
    bundle.add_argument("--multi-trader-smoke-path", default="")
    bundle.add_argument("--multi-trader-smoke-preflight-path", default="")
    bundle.add_argument("--multi-trader-smoke-manifest-path", default="")
    bundle.add_argument("--multi-trader-smoke-package-path", default="")
    bundle.add_argument("--multi-trader-smoke-package-metadata-path", default="")
    bundle.add_argument("--output-path", required=True)
    return parser


def parse_symbols(raw: str) -> list[str]:
    return [symbol.strip() for symbol in raw.split(",") if symbol.strip()]


def parse_csv(raw: str) -> list[str]:
    return [value.strip() for value in raw.split(",") if value.strip()]


def add_active_pool_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--silver-root", required=True)
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--target-size", type=int, default=200)
    parser.add_argument("--pinned-max-size", type=int, default=100)
    parser.add_argument("--rank-window-days", type=int, default=5)
    parser.add_argument("--rank-metric", choices=["avg_turnover", "avg_volume"], default="avg_turnover")
    parser.add_argument("--exclude-instrument-types", default="ETF,WARRANT,CBBC,FUND,BOND,DERIVATIVE")


def build_redis_client(redis_url: str):
    try:
        import redis
    except ImportError as error:
        raise RuntimeError("redis package is required when --redis-url is supplied") from error
    return redis.Redis.from_url(redis_url)


def build_kafka_event_bus(bootstrap_servers: str):
    try:
        from confluent_kafka import Producer
    except ImportError as error:
        raise RuntimeError("confluent-kafka package is required for replay-kafka-spool") from error
    return KafkaEventBusAdapter(Producer({"bootstrap.servers": bootstrap_servers}))


class NoopEventBus:
    def publish(self, topic: str, key: str, value: dict) -> None:
        return None

    def read(self, topic: str) -> list[dict]:
        return []

    def lag(self, topic: str, committed_offset: int = 0) -> int:
        return 0

    def commit(self, topic: str, offset: int) -> None:
        return None

    def committed_offset(self, topic: str) -> int:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
