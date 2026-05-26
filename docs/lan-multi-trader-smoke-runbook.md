# LAN Multi-Trader Smoke Runbook

Status: Phase 6 execution runbook for `BEAST_MARKET_TERMINAL_BACKEND_UPGRADE_PLAN.md`.

This runbook proves that the terminal can be used by multiple traders on different LAN machines, with different watchlists, shared hot symbol state, closed-market effective-date evidence, and verifiable runtime health.

## Required Machines

- Backend host: runs the Beast backend, Gateway, and frontend dev/preview server.
- Client A: opens the frontend from another LAN machine.
- Client B: opens the frontend from a second LAN machine or a second physical trader workstation.

The smoke is not complete if both client artifacts come from the same `machine_id`.
Client artifacts must be exported after this run's `lan-preflight.json.prepared_at` and before the final `observed_at` used by `finalize-multi-trader-smoke`; stale browser downloads from an earlier run are rejected.

## Artifact Directory

Use one directory per smoke run. The simplest path is to let preparation create the timestamped directory and export the shell variables used by later commands:

```bash
eval "$(PYTHONPATH=backend python -m beast_market.ops_cli prepare-multi-trader-smoke \
  --root-path auto \
  --lan-host auto \
  --frontend-port auto \
  --gateway-port auto \
  --silver-root /home/hliu/thousand/artifacts/xtquant-silver/20260526-smoke \
  --require-local-lan-host \
  --cold-query-symbol 00005.HK \
  --redis-clear-symbol 00700.HK \
  --add-to-watchlist-symbol 00939.HK \
  --requested-trade-date 20260525 \
  --effective-trade-date 20260522 \
  --print-env)"
```

`--print-env` prints `SMOKE_DIR`, `SMOKE_PAGE_URL`, `SMOKE_GATEWAY_URL`, every run-local artifact path, command variables, and script-path variables as shell exports while preserving the normal pass/fail exit code. The artifact path exports include `SMOKE_PREFLIGHT_PATH`, `SMOKE_CLIENT_INSTRUCTIONS_PATH`, `SMOKE_WORKFLOWS_PATH`, `SMOKE_SERVICE_PREFLIGHT_PATH`, `SMOKE_RUNTIME_HEALTH_PATH`, `SMOKE_OBSERVATION_PATH`, `SMOKE_EVIDENCE_PATH`, `SMOKE_IMPORT_MANIFEST_PATH`, `SMOKE_RUN_MANIFEST_PATH`, `SMOKE_PACKAGE_PATH`, and `SMOKE_PACKAGE_METADATA_PATH`. If preflight fails, the printed shell snippet ends with `false`, so the `eval` form also returns non-zero instead of silently continuing. If you prefer JSON output, omit `--print-env` and copy `root_path` from the returned JSON before running later commands.

Use two distinct smoke intents when needed:

- Today/live data debugging: request the current trading date and use the freshly exported silver root. This proves the real frontend, Gateway, native 1m bars, CCASS evidence, and browser smoke export path.
- Closed-market acceptance: prepare a run whose `--requested-trade-date` is not an available trading day and whose `--effective-trade-date` is the latest effective trading day. This is the run that can honestly pass the `closed_market_effective_date` workflow.

Do not mark `closed_market_effective_date` passed in a same-date live smoke. The final Phase 6 package must either use a closed-market prepared run or carry separate, explicit closed-market workflow evidence from a matching preflight.

The generated script variables are `$SMOKE_BACKEND_SCRIPT`, `$SMOKE_RESTART_BACKEND_SCRIPT`, `$SMOKE_FRONTEND_SCRIPT`, `$SMOKE_SERVICE_PREFLIGHT_SCRIPT`, `$SMOKE_IMPORT_ARTIFACTS_SCRIPT`, `$SMOKE_RECORD_WORKFLOW_SCRIPT`, `$SMOKE_INSPECT_NEXT_ACTION_SCRIPT`, `$SMOKE_VERIFY_HANDOFF_SCRIPT`, and `$SMOKE_FINALIZE_PACKAGE_SCRIPT`.

`prepare-multi-trader-smoke` creates `clients/`, `performance/`, `workflows.json`, `README.md`, `CLIENT_INSTRUCTIONS.md`, and `lan-preflight.json`. The generated preflight records all expected artifact paths, including later import manifest, run manifest, smoke evidence zip, and package metadata paths, plus the exact backend, frontend, service-preflight, and client URL commands for the run. The generated `$SMOKE_DIR/README.md` summarizes the prepared URLs, helper scripts, workflow names, and expected artifacts for that specific run. The generated `$SMOKE_DIR/CLIENT_INSTRUCTIONS.md` is the file to send to client machines; it keeps their work to opening the LAN URL, confirming distinct smoke machine ids, subscribing assigned watchlists, refreshing, exporting the client/performance smoke JSON files, and keeping the tab open until runtime health has observed that machine id. Browser profiles are acceptable for local rehearsal only; the final Phase 6 smoke needs two real LAN client machines. `--root-path auto` creates `artifacts/multi-trader-smoke/<timestamp>` under the repository root and records both the requested and actual root paths. `--lan-host auto` chooses a detected local LAN IP and records the selected value; if detection fails, the preflight is blocked with `multi_trader_smoke_lan_host_auto_detect_failed`. `--frontend-port auto` and `--gateway-port auto` choose the first free ports starting from 5173 and 9020 respectively, recording both requested and resolved ports in `lan-preflight.json`; use explicit port numbers when the URL must stay fixed. You can still pass an explicit root directory, LAN IP, DNS name, frontend port, Gateway port, or CSV silver root when auto-detection chooses the wrong path or interface. Passing `--silver-root` makes the generated backend script start `real_data_runner` against that CSV silver directory instead of the default Mammoth patch path, which is required when the smoke is validating a freshly exported `silver_minute_bars_v1` dataset. Short numeric workflow symbols are normalized before the workflow template and runner command are written, and invalid symbols are rejected during preparation. The generated backend command includes the requested trade date, workflow symbols, and explicit silver root when supplied, so the runner does not silently fall back to today's date, its default preload symbols, or stale Mammoth patch data during closed-market or cold-query smoke runs. It fails before testing if the supplied host is loopback or wildcard rather than a client-routable LAN address. With `--require-local-lan-host`, an IP literal must also match one of the backend machine's detected local addresses.
The generated `lan-preflight.json.prepared_at` is the lower bound for all freshness checks. Do not hand-create a preflight file without it; missing or malformed `prepared_at` fails inspect/finalize/package/bundle gates.

Expected final files:

- `$SMOKE_DIR/lan-preflight.json`
- `$SMOKE_DIR/CLIENT_INSTRUCTIONS.md`
- `$SMOKE_DIR/service-preflight.json`
- `$SMOKE_DIR/workflows.json`
- `$SMOKE_DIR/clients/*.json`
- `$SMOKE_DIR/performance/*.json`
- `$SMOKE_DIR/smoke-import-manifest.json`
- `$SMOKE_DIR/runtime-health-verification.json`
- `$SMOKE_DIR/multi-trader-smoke-observation.json`
- `$SMOKE_DIR/multi-trader-smoke-evidence.json`
- `$SMOKE_DIR/smoke-run-manifest.json`
- `$SMOKE_DIR/multi-trader-smoke-evidence.zip`
- `$SMOKE_DIR/smoke-run-package.json`

## Start Services

Before starting a full supervised app runtime, verify the runtime config artifact as a hard gate:

```bash
PYTHONPATH=backend python -m beast_market.ops_cli verify-runtime-config \
  --config-path artifacts/runtime-config.json \
  --output-path artifacts/runtime-config-verification.json
```

This command exits non-zero when config evidence fails, including deprecated fixed-turnover big-trade fields, loopback Gateway hosts, unsafe topic names, missing production clients, or secret-like keys. Do not continue a full runtime smoke from a failed config verification.
The runtime config loader enforces the same verifier, so a failed artifact should be fixed rather than loaded through another code path.

Backend Gateway must bind to LAN, not loopback. Run this in its own terminal:

```bash
"$SMOKE_BACKEND_SCRIPT"
```

`SMOKE_BACKEND_SCRIPT` wraps the exact backend command emitted in `$SMOKE_DIR/lan-preflight.json.commands.backend`, including the requested trade date, workflow symbols, LAN bind host, Gateway port, and runtime-health output path. It first changes into the repository root, so it can be launched from any shell directory.
Before starting, the generated script checks that the prepared Gateway port can bind on `0.0.0.0`; if the port is already occupied, stop the old process or rerun `prepare-multi-trader-smoke` with another `--gateway-port`.
The runner validates startup arguments before reading data: `--trade-date` must be a valid `YYYYMMDD`, `--symbols` must contain at least one valid HK symbol, short numeric symbols such as `700` are normalized to `00700.HK`, `--port` must be in `1..65535`, `--path` must be `/ws`, and `--runtime-health-interval-seconds` must be greater than `0`.
Big trade detection in this runner uses `--big-trade-volume-baseline-ratio` only, and the value must be greater than `0`. There is no fixed turnover threshold knob in the real-data runner; passing `--big-trade-turnover-threshold` fails argument parsing, and supplying the old field through runtime config is treated as deprecated evidence and fails config verification.

Frontend must also bind to LAN. Run this in a second terminal:

```bash
"$SMOKE_FRONTEND_SCRIPT"
```

`package.json` already sets Vite to `--host 0.0.0.0 --strictPort` for `dev` and `preview`. The generated smoke command also passes `--port <prepared frontend_port> --strictPort`; before starting, the generated script checks that this prepared frontend port can bind on `0.0.0.0`. If the prepared port is occupied, fix the old process or choose another frontend port and rerun `prepare-multi-trader-smoke` instead of letting Vite silently switch to a different LAN URL. If `pnpm` is installed through Corepack, `corepack pnpm dev -- --port <port> --strictPort` is equivalent; `npm run dev -- --port <port> --strictPort` is the fallback path used by the generated preflight commands on machines without pnpm.

The generated frontend script also sets `VITE_MARKET_DATA_MODE=live`, `VITE_MARKET_WS_URL=<prepared gateway_url>`, `VITE_MARKET_PROTOCOL=terminal-message-v1`, and `VITE_MARKET_SYMBOLS=<initial frontend watchlist symbols>`. Cold-query and add-to-watchlist workflow symbols should be available to the backend runtime but excluded from the initial frontend watchlist, so those workflows can be observed honestly. Do not start Vite by hand without these values during Phase 6; otherwise browser artifacts may connect to an older Gateway on the default port and fail the preflight URL cross-check.

Before handing the URL to traders, run one local browser smoke where possible:

1. Open the prepared page URL in Chrome or another real browser.
2. Confirm the page shows the smoke symbols, requested/effective dates, and CCASS source dates.
3. Export both client and performance smoke JSON.
4. Import those files with `$SMOKE_IMPORT_ARTIFACTS_SCRIPT <downloads-dir>`.
5. Run `$SMOKE_INSPECT_NEXT_ACTION_SCRIPT`.

This local browser smoke is not a substitute for the two-machine LAN smoke, but it proves that the generated frontend command is using the prepared Gateway URL and that exported artifacts have the expected `data_source_mode: live`, non-loopback page URL, and matching Gateway URL before other traders spend time testing.

Client machines should open:

```text
$SMOKE_PAGE_URL
```

The frontend derives its Gateway URL from the page hostname, so a LAN page URL should connect to:

```text
$SMOKE_GATEWAY_URL
```

After both services are started, run a backend-host service probe before handing the URL to client machines:

```bash
PYTHONPATH=backend python -m beast_market.ops_cli verify-multi-trader-smoke-services \
  --root-path "$SMOKE_DIR"
```

Or use the prepared command:

```bash
"$SMOKE_SERVICE_PREFLIGHT_SCRIPT"
```

This writes `$SMOKE_DIR/service-preflight.json` and updates `$SMOKE_DIR/lan-preflight.json` with `service_checks`. The probe performs an HTTP request against the prepared frontend URL and a WebSocket upgrade handshake against the prepared Gateway `/ws` URL; a plain open TCP port is not enough. A failure means the frontend or Gateway service is not reachable with the expected protocol at the LAN host from the backend machine, so fix service binding/firewall/port conflicts before collecting browser artifacts.

## Workflow Template

After executing each workflow, prefer recording the observed result through the CLI instead of hand-editing JSON:

```bash
"$SMOKE_RECORD_WORKFLOW_SCRIPT" cold_query \
  --symbol 00005.HK \
  --notes "client A observed loading then snapshot"
```

Use the matching workflow name for each observed check:

- `cold_query`
- `add_to_watchlist`
- `refresh_recovery`
- `redis_clear_recovery`
- `process_restart_recovery`
- `closed_market_effective_date`

For symbol or date-specific checks, pass the observed values when needed:

```bash
"$SMOKE_RECORD_WORKFLOW_SCRIPT" closed_market_effective_date \
  --requested-trade-date 20260525 \
  --effective-trade-date 20260522
```

Only observed passes should be recorded. If you need to regenerate only the workflow checklist, use `build-multi-trader-smoke-workflows-template`; otherwise prefer the preflight command above.
`$SMOKE_RECORD_WORKFLOW_SCRIPT` automatically supplies `--workflows-path` and current ISO `--observed-at`; pass workflow-specific fields after the workflow name.
The record command refuses to mark a workflow passed if its required supporting fields are incomplete. For example, cold-query, add-watchlist, and Redis-clear checks need canonical symbols, refresh recovery must record that the browser was actually refreshed, Redis-clear recovery must record that the scoped cache clear was executed, process-restart recovery must record that the backend was restarted, and closed-market evidence needs distinct requested/effective dates plus visible source-date evidence.
Short numeric symbols passed to the record command are normalized to canonical HK symbols, and invalid symbols are rejected before `workflows.json` is updated.
Malformed `--observed-at` values are rejected before `workflows.json` is updated. Use an ISO datetime from the current run.
Final smoke verification also requires every passed workflow to include an ISO `observed_at` after this run's `lan-preflight.json.prepared_at` and not after the final `observed_at` used by `finalize-multi-trader-smoke`; stale or future hand-edited workflow evidence is rejected.

Before finalizing, inspect the smoke directory:

```bash
PYTHONPATH=backend python -m beast_market.ops_cli inspect-multi-trader-smoke \
  --root-path "$SMOKE_DIR"
```

For a concise operator prompt during a live run:

```bash
"$SMOKE_INSPECT_NEXT_ACTION_SCRIPT"
```

The inspect command is read-only. It reports missing/invalid artifacts, client/performance artifact counts, incomplete workflow names, workflow evidence-field blockers grouped by workflow name, `preflight_readiness` including service-check status, `runtime_health_readiness` including generated time, observed client counts, declared client counts, max connected clients, and subscribe sample count, and `package_readiness` blockers for the final zip handoff, including missing or stale `smoke-run-manifest.json`. It also emits ordered `next_actions`; use the first action as the next operator step during a live run. The `--next-action` flag prints only that first action plus readiness/blocker summary, `preflight_readiness`, and `runtime_health_readiness` while keeping the same exit-code behavior. A freshly prepared directory will point operators through backend runtime startup, frontend startup, service probe, client activity, client artifact import, workflow recording, finalize/package, and handoff in that order. The `client_activity` action means opening the prepared LAN URL from at least two machines, subscribing the smoke watchlists, and exporting client/performance JSON after runtime health has observed both clients; when available it also includes the prepared `CLIENT_INSTRUCTIONS.md` path. When `lan-preflight.json` was generated by `prepare-multi-trader-smoke`, those actions reuse its exact run-local `start-backend.sh`, `restart-backend.sh`, `start-frontend.sh`, `verify-services.sh`, `import-artifacts.sh`, `record-workflow.sh`, `finalize-package.sh`, and `verify-handoff.sh` scripts instead of generic placeholders. When the required artifacts are parseable, inspect also builds an in-memory smoke observation and runs the final smoke verifier as `smoke_preview`, so client live-mode, LAN URL, watchlist, runtime health, and performance blockers are visible before final evidence files are generated. For a pre-handoff script gate, run `$SMOKE_VERIFY_HANDOFF_SCRIPT` or add `--require-package-ready`; the command then returns non-zero unless both final smoke readiness and package handoff readiness are satisfied.
If service preflight is already passing but `runtime-health-verification.json` is still missing, inspect also reports prepared port bind availability and, on Linux, the listener process ids and command names under `preflight_readiness.ports.*.listeners`. A reachable Gateway on the prepared port without this run's runtime health usually means an older process is serving `/ws`; start the generated backend script when the port is free, or use `$SMOKE_RESTART_BACKEND_SCRIPT` to restart only the prepared smoke backend when it is already the listener for this run. Do not hand-kill unrelated 5173/9020 development services.
If `runtime_health_readiness.blockers` includes `multi_trader_smoke_minute_bars_missing`, stop before client collection. The smoke would only prove degraded snapshots, not the intended native 1m chart path. Generate or repair `silver_minute_bars_v1` for every runtime symbol, then use `$SMOKE_RESTART_BACKEND_SCRIPT` or the generated `restart-backend.sh` to restart the prepared smoke backend with the same `--runtime-health-path`. Rerun service/inspect and continue only after `missing_minute_bar_symbols` is empty. The xtquant CSV exporter path is `backend/tools/xtquant_silver_export.py`; it must run under a Python ABI compatible with the installed xtquant SDK.
Service reachability evidence is time-bound to the smoke run. The `service-preflight.json.checked_at` timestamp must be a valid ISO datetime after `lan-preflight.json.prepared_at` and not after the final `--observed-at`; rerun `verify-multi-trader-smoke-services` after preparing each smoke directory instead of reusing an old probe.
The final `--observed-at` itself is a hard input gate. `build-multi-trader-smoke-observation` rejects missing or malformed values before writing an observation, and `finalize-multi-trader-smoke` writes failed evidence with `multi_trader_smoke_observed_at_missing` or `multi_trader_smoke_observed_at_invalid` instead of producing a malformed artifact chain.

## Client Workflow

Use different watchlists with at least one overlapping symbol.

Example:

- Client A: `00700.HK`, `00939.HK`
- Client B: `00700.HK`, `00005.HK`

Required checks:

1. Cold query a symbol not in the initial watchlist and observe loading followed by a snapshot.
2. Add a queried symbol to watchlist and refresh the browser; the watchlist must restore.
3. Clear Redis for one target symbol, resubscribe or refresh, and confirm the snapshot rebuilds.
4. Restart the prepared smoke backend with `$SMOKE_RESTART_BACKEND_SCRIPT`, then confirm first screen restores from Redis or 1m bars.
5. On closed-market or pre-open dates, confirm the UI shows requested date and effective date distinctly, plus source dates.

For the Redis clear step, use the scoped cache command rather than manual Redis deletion:

For prepared smoke runs, prefer the exact dry-run and confirm commands generated in `$SMOKE_DIR/README.md`; they are pinned to that run's requested trade date and Redis-clear workflow symbol.

```bash
PYTHONPATH=backend python -m beast_market.ops_cli clear-runtime-cache \
  --trade-date 20260525 \
  --symbols 00700.HK \
  --redis-url redis://127.0.0.1:6379/0 \
  --dry-run

PYTHONPATH=backend python -m beast_market.ops_cli clear-runtime-cache \
  --trade-date 20260525 \
  --symbols 00700.HK \
  --redis-url redis://127.0.0.1:6379/0 \
  --confirm
```

Dry-run output may include the pattern `ccass:history:<symbol>:*` to show intended scope. Confirmed deletion only deletes concrete Redis keys expanded from that pattern; it does not delete Mammoth silver, Kafka topics, smoke evidence, or local runtime-state JSONL.

For the process-restart step, use the generated restart helper instead of manually finding and killing a process:

```bash
"$SMOKE_RESTART_BACKEND_SCRIPT"
```

The helper matches the prepared Gateway port and this run's `runtime-health-verification.json` in the `real_data_runner` command line before sending a signal, so it is scoped to the current smoke backend. After both clients observe restored first-screen state, record the workflow:

```bash
"$SMOKE_RECORD_WORKFLOW_SCRIPT" process_restart_recovery
```

After each client completes the workflow, export both frontend artifacts from the dashboard smoke buttons:

- Client smoke JSON
- Performance smoke JSON

Before exporting, compare the `machine_id` shown beside the smoke buttons on every client. The two clients must show different values. If two clients show the same value, use the refresh icon beside the smoke export buttons on one client and export both JSON files again from that client. Keep both browser tabs open after export until the backend operator imports the files and `inspect-multi-trader-smoke` shows that runtime health observed those declared client ids; final verification rejects client artifacts whose machine ids were not observed by the Gateway in the same smoke run.

Copy the downloaded files to the backend host, then import them into the prepared smoke directory:

```bash
"$SMOKE_IMPORT_ARTIFACTS_SCRIPT" ~/Downloads/smoke-json/
```

Pass `client` or `performance` as an optional second argument only when auto-detection is not desired.

To import one file at a time:

```bash
PYTHONPATH=backend python -m beast_market.ops_cli import-multi-trader-smoke-artifact \
  --root-path "$SMOKE_DIR" \
  --input-path ~/Downloads/multi-trader-smoke-client-desk-a.json

PYTHONPATH=backend python -m beast_market.ops_cli import-multi-trader-smoke-artifact \
  --root-path "$SMOKE_DIR" \
  --input-path ~/Downloads/multi-trader-smoke-performance-desk-a.json
```

The import commands detect client vs performance JSON from the file contents and write to `$SMOKE_DIR/clients/` or `$SMOKE_DIR/performance/`. They also avoid overwriting existing files. Both single-file and directory imports write `$SMOKE_DIR/smoke-import-manifest.json` with input/output provenance for the final evidence package. Re-running either import appends a new `runs[]` entry instead of replacing earlier import evidence; the plural command also skips unrelated JSON files and reports them in `skipped`.
Client JSON import performs semantic prechecks before writing the artifact: the client must be in live mode, use non-loopback page and Gateway URLs, include a valid watchlist with matching `symbol_statuses`, report `connected: true`, and report `refresh_recovered: true`. `inspect-multi-trader-smoke` and `finalize-multi-trader-smoke` apply the same checks to any JSON files already under `clients/`, reporting bad files as `multi_trader_smoke_clients_invalid` with `invalid_paths`. If import or inspect fails, fix the frontend URL/mode/session state and export again instead of copying the file manually.
Performance JSON import also requires the frontend-exported schema: `schema_version: 1`, ISO `exported_at`, non-empty `machine_id`, and valid `performance_samples.subscribe_snapshot_ms` values. `inspect-multi-trader-smoke` and `finalize-multi-trader-smoke` apply the same checks to files already under `performance/`, reporting bad files as `multi_trader_smoke_performance_invalid` with `invalid_paths`.
The low-level `build-multi-trader-smoke-observation` command uses the same performance reader. Do not pass raw sample arrays as `--performance-samples-path`; use frontend-exported performance JSON or omit the option and rely on client-embedded/runtime-health performance evidence.

Do not bypass the import commands by manually copying files into `clients/` or `performance/`; `finalize-multi-trader-smoke --package` and `package-multi-trader-smoke` require a passed `$SMOKE_DIR/multi-trader-smoke-evidence.json`, passed `$SMOKE_DIR/lan-preflight.json` and `$SMOKE_DIR/service-preflight.json` evidence, a fresh `$SMOKE_DIR/smoke-run-manifest.json`, plus a valid `$SMOKE_DIR/smoke-import-manifest.json` with at least one imported artifact, matching import counts, every imported `output_path` still present under `$SMOKE_DIR`, and import provenance for every recognizable frontend client/performance artifact under `clients/` or `performance/`.

The client smoke JSON also embeds the frontend performance samples as fallback evidence. If a performance JSON is missing, `finalize-multi-trader-smoke` can still recover those samples from `$SMOKE_DIR/clients/`; exporting the separate performance JSON remains useful for a clearer artifact split.
Performance smoke JSON includes the exporting `machine_id` as provenance, and exported filenames sanitize the same machine id so renamed files are not the only source of client identity.
When performance files are provided, the generated smoke observation includes `performance_artifacts[]` with each file path, `machine_id`, and subscribe-snapshot sample count. Use that evidence to spot a missing or mislabeled client performance export before final verification.
Final smoke verification validates `performance_artifacts[]` when present: every listed performance file must have a `machine_id` that matches one of the client machines, and every client machine must be covered by at least one performance artifact.
Performance artifact `exported_at` timestamps are also checked against the current smoke window. A performance export from before `lan-preflight.json.prepared_at`, after the final `--observed-at`, or without a valid ISO timestamp is rejected.

## Runtime Health

When using `backend.tools.real_data_runner`, the `--runtime-health-path` option writes a Phase 6 smoke health artifact continuously while the service runs. Keep the final `$SMOKE_DIR/runtime-health-verification.json` from the same run and use it directly in `build-multi-trader-smoke-observation`; do not run `verify-runtime-health` against this lightweight runner artifact. The runner artifact stays failed until at least two distinct clients have connected, max concurrent clients has reached at least two, at least two declared frontend `client_id` values have been observed, and at least one subscribe snapshot sample exists, so collect it after the real LAN clients have opened the frontend and subscribed. Final smoke verification also consumes `gateway_activity.client_queue` and the artifact `generated_at`, so stale, pre-client, pre-preflight, or post-observation runtime health files cannot pass by relying only on frontend exports. The frontend sends the persisted smoke machine id as the Gateway `client_id`; reset duplicate machine ids before testing.

When using the full supervised app runtime instead of `real_data_runner`, first write `artifacts/runtime-health.json`, then verify it:

```bash
PYTHONPATH=backend python -m beast_market.ops_cli verify-runtime-health \
  --runtime-health-path artifacts/runtime-health.json \
  --output-path "$SMOKE_DIR/runtime-health-verification.json"
```

The runtime health evidence must include:

- Gateway host is not loopback.
- `symbol_runtime` per-symbol hydrate counters.
- `symbol_runtime_manager.active_hydrations == 0`.
- `symbol_runtime_manager.capacity_rejections == 0`.
- Subscribe snapshot performance samples or derived p95 evidence.
- `subscribe_snapshot_ms` evidence must be non-empty and the service-side p95 must be less than or equal to 200 ms.
- Top-level ISO `generated_at`; final smoke requires it to be after `lan-preflight.json.prepared_at` and not after the final observation `observed_at`.

## Finalize Smoke

After client, workflow, performance, runtime health, LAN preflight, and service-preflight artifacts are in `$SMOKE_DIR`, build the observation and evidence in one step:

```bash
PYTHONPATH=backend python -m beast_market.ops_cli finalize-multi-trader-smoke \
  --root-path "$SMOKE_DIR" \
  --observed-at "$(date --iso-8601=seconds)" \
  --package
```

Or use the prepared finalize script:

```bash
"$SMOKE_FINALIZE_PACKAGE_SCRIPT"
```

This writes:

- `$SMOKE_DIR/multi-trader-smoke-observation.json`
- `$SMOKE_DIR/multi-trader-smoke-evidence.json`
- `$SMOKE_DIR/smoke-run-manifest.json`
- `$SMOKE_DIR/multi-trader-smoke-evidence.zip` after packaging
- `$SMOKE_DIR/smoke-run-package.json` after packaging

The finalized observation embeds `$SMOKE_DIR/lan-preflight.json`, and the smoke evidence fails if that preflight evidence is blocked or not client-routable.
If required artifacts are missing or malformed, including missing or mismatched `service-preflight.json`, `finalize-multi-trader-smoke` still writes a failed `$SMOKE_DIR/multi-trader-smoke-evidence.json` with explicit missing-path or invalid-path blockers.
If `finalize-multi-trader-smoke --package` fails after smoke evidence generation, the command prints `package_readiness` in the JSON output; use that section to identify missing import provenance, missing imported output files, or an unpassed evidence file before retrying the package step.
The smoke run manifest records every JSON artifact's relative path, byte size, and sha256 hash, excluding itself.

If you finalized without `--package`, package the smoke directory JSON evidence for handoff:

```bash
PYTHONPATH=backend python -m beast_market.ops_cli package-multi-trader-smoke \
  --root-path "$SMOKE_DIR"
```

The package command writes a zip containing the collected JSON evidence and `smoke-run-package.json` with the zip byte size, sha256, and included relative paths. It refuses to package if `lan-preflight.json` / `service-preflight.json` is missing or not passed, or if `smoke-run-manifest.json` is missing or stale against the JSON files that would enter the zip. The package metadata itself is written after the zip is created, so it is not embedded in the zip and is also ignored on later reruns.

For debugging, the two lower-level commands remain available:

```bash
PYTHONPATH=backend python -m beast_market.ops_cli build-multi-trader-smoke-observation \
  --clients-path "$SMOKE_DIR/clients" \
  --performance-samples-path "$SMOKE_DIR/performance" \
  --workflows-path "$SMOKE_DIR/workflows.json" \
  --runtime-health-path "$SMOKE_DIR/runtime-health-verification.json" \
  --preflight-path "$SMOKE_DIR/lan-preflight.json" \
  --observed-at "$(date --iso-8601=seconds)" \
  --output-path "$SMOKE_DIR/multi-trader-smoke-observation.json"
```

```bash
PYTHONPATH=backend python -m beast_market.ops_cli verify-multi-trader-smoke \
  --observation-path "$SMOKE_DIR/multi-trader-smoke-observation.json" \
  --output-path "$SMOKE_DIR/multi-trader-smoke-evidence.json"
```

If this smoke is included in the final production evidence bundle, pass both smoke artifacts:

```bash
PYTHONPATH=backend python -m beast_market.ops_cli verify-evidence-bundle \
  ... \
  --multi-trader-smoke-path "$SMOKE_DIR/multi-trader-smoke-evidence.json" \
  --multi-trader-smoke-preflight-path "$SMOKE_DIR/lan-preflight.json" \
  --multi-trader-smoke-manifest-path "$SMOKE_DIR/smoke-run-manifest.json" \
  --multi-trader-smoke-package-path "$SMOKE_DIR/multi-trader-smoke-evidence.zip" \
  --multi-trader-smoke-package-metadata-path "$SMOKE_DIR/smoke-run-package.json"
```

The bundle verifier rejects a smoke evidence file without matching passed preflight evidence and manifest evidence, rejects preflight Gateway URLs that differ from the deployed frontend live URL, checks manifest hashes for the smoke evidence and preflight files, and verifies the optional package zip against `smoke-run-package.json` when package paths are supplied. A package zip cannot be submitted by itself; package verification also requires the external smoke evidence, LAN preflight, and smoke-run manifest paths. When a package zip is supplied, it must include the same smoke evidence, LAN preflight, and smoke-run manifest files submitted to the bundle verifier by hash, plus a passed `service-preflight.json` that matches `lan-preflight.json.service_checks`. Packaged service reachability evidence is also checked against the smoke run time window, so a self-consistent zip with stale `checked_at` still fails. The zip's own `smoke-run-manifest.json` is also used to hash-check every listed file inside the zip, including the smoke observation, and the zip must not contain extra JSON artifacts omitted from that manifest. It must also include a valid `smoke-import-manifest.json`; the verifier parses it from inside the zip and checks schema, non-empty imports, runs, counts, and that every imported `output_path` is present in the zip.

The final evidence passes only when:

- At least two distinct client machines are present.
- `lan-preflight.json` passed, contains passed `service_checks`, each service-check URL matches the prepared frontend/Gateway URL, and its Gateway URL matches the deployed frontend live URL when included in the final evidence bundle.
- Every client artifact reports `schema_version: 1`, ISO `exported_at`, `data_source_mode: live`, a non-loopback `page_url`, and a non-loopback Gateway `gateway_url`; mock-mode, missing/malformed timestamps, stale exports, future exports, or loopback exports are rejected. The client `page_url` origin must match `lan-preflight.json.page_url`, and the client `gateway_url` must exactly match `lan-preflight.json.gateway_url`.
- Every passed workflow reports ISO `observed_at` within the same prepared-to-finalized window.
- Watchlists are different and have overlap.
- All required workflows passed with supporting fields.
- Warm snapshot p95 is less than or equal to 200 ms.
- Overlapping symbols did not duplicate hydration.
- Runtime health passed and includes symbol runtime manager evidence.
- Runtime health includes a valid ISO `generated_at` within this smoke run's prepared-to-observed window.
- Gateway WebSocket evidence uses a non-loopback host, valid port, and `/ws` path.

## Common Failures

- `multi_trader_smoke_client_machine_duplicate`: two artifacts used the same `machine_id`; use the dashboard refresh icon beside the smoke export buttons on one client, or clear `localStorage` key `market-terminal.smoke.machine-id.v1`, then export both client and performance JSON again from that client.
- `multi_trader_smoke_insufficient_client_machines`: fewer than two distinct client machines were imported; import both clients' artifacts after fixing any duplicate `machine_id`.
- `multi_trader_smoke_client_not_live`: at least one client artifact came from mock mode or missed `data_source_mode`; switch the frontend to live mode and export again.
- `multi_trader_smoke_lan_host_not_client_routable`: preflight used loopback or wildcard host; set `BACKEND_LAN_IP` to the backend machine's LAN address.
- `multi_trader_smoke_lan_host_not_local`: preflight used an IP not detected on the backend machine; re-check `BACKEND_LAN_IP` or omit `--require-local-lan-host` when using DNS.
- `multi_trader_smoke_lan_host_auto_detect_failed`: `--lan-host auto` could not find a client-routable local IP; rerun prepare with an explicit backend LAN IP.
- `multi_trader_smoke_preflight_missing`: final smoke observation was built without `lan-preflight.json`; rerun `finalize-multi-trader-smoke` from a prepared smoke directory.
- `multi_trader_smoke_observed_at_missing`: finalize was run without a final observation timestamp; rerun with `--observed-at "$(date --iso-8601=seconds)"`.
- `multi_trader_smoke_observed_at_invalid`: finalize was run with a malformed final observation timestamp; rerun with an ISO datetime.
- `multi_trader_smoke_preflight_prepared_at_missing`: `lan-preflight.json` has no prepared timestamp; rerun `prepare-multi-trader-smoke`.
- `multi_trader_smoke_preflight_prepared_at_invalid`: `lan-preflight.json.prepared_at` is malformed; rerun `prepare-multi-trader-smoke`.
- `multi_trader_smoke_service_preflight_missing`: run `verify-multi-trader-smoke-services --root-path "$SMOKE_DIR"` after starting backend/frontend, then finalize again.
- `multi_trader_smoke_frontend_service_unreachable`: frontend dev/preview server did not return a successful HTTP response through the prepared LAN page URL; restart the generated frontend script, check the prepared frontend port, or check the firewall. The smoke frontend command uses `--strictPort`, so a port conflict should fail startup instead of moving to another URL.
- `multi_trader_smoke_gateway_service_unreachable`: Gateway did not complete a WebSocket upgrade handshake through the prepared LAN WebSocket URL; restart `real_data_runner`, verify `/ws`, check port 9020, or check the firewall.
- `multi_trader_smoke_frontend_service_probe_kind_invalid` / `multi_trader_smoke_frontend_service_status_missing`: service-preflight was produced by an old TCP-only probe; rerun `verify-multi-trader-smoke-services`.
- `multi_trader_smoke_gateway_service_probe_kind_invalid` / `multi_trader_smoke_gateway_service_websocket_handshake_missing`: service-preflight was produced by an old TCP-only probe or a non-WebSocket service; rerun `verify-multi-trader-smoke-services` after starting the current Gateway.
- `multi_trader_smoke_frontend_service_check_url_mismatch`: `service-preflight.json` probed a frontend URL different from `lan-preflight.json.page_url`; rerun `verify-multi-trader-smoke-services` from the same prepared smoke directory.
- `multi_trader_smoke_gateway_service_check_url_mismatch`: `service-preflight.json` probed a Gateway URL different from `lan-preflight.json.gateway_url`; rerun `verify-multi-trader-smoke-services` from the same prepared smoke directory.
- `multi_trader_smoke_client_page_url_loopback`: at least one client exported a loopback page URL; open the frontend through `http://<backend-lan-ip>:5173/` from the client machine and export again.
- `multi_trader_smoke_client_gateway_url_loopback`: at least one client connected to loopback Gateway URL; check `VITE_MARKET_WS_URL` or the derived default URL and export again.
- `multi_trader_smoke_client_page_url_preflight_mismatch`: at least one client exported a page URL whose origin differs from `lan-preflight.json.page_url`; reopen the prepared LAN frontend URL and export again.
- `multi_trader_smoke_client_gateway_url_preflight_mismatch`: at least one client connected to a Gateway URL different from `lan-preflight.json.gateway_url`; fix `VITE_MARKET_WS_URL` or the derived frontend URL and export again.
- `multi_trader_smoke_client_artifact_before_preflight`: at least one client artifact was exported before this smoke run was prepared; export fresh browser artifacts after running `prepare-multi-trader-smoke`.
- `multi_trader_smoke_client_artifact_after_observed`: at least one client artifact was exported after the finalize observation timestamp; rerun `finalize-multi-trader-smoke` with a later `--observed-at` or re-export/re-finalize consistently.
- `multi_trader_smoke_client_artifact_exported_at_missing`: at least one client artifact has no `exported_at`; use frontend-exported smoke JSON rather than manually assembled client files.
- `multi_trader_smoke_client_artifact_exported_at_invalid`: at least one client artifact has a malformed `exported_at`; re-export from the frontend smoke controls.
- `multi_trader_smoke_service_preflight_checked_at_missing`: service reachability evidence has no `checked_at`; rerun `verify-multi-trader-smoke-services`.
- `multi_trader_smoke_service_preflight_checked_at_invalid`: service reachability evidence has malformed `checked_at`; rerun `verify-multi-trader-smoke-services`.
- `multi_trader_smoke_service_preflight_before_preflight`: service reachability was probed before this smoke directory was prepared; rerun `verify-multi-trader-smoke-services`.
- `multi_trader_smoke_service_preflight_after_observed`: service reachability was probed after the final observation timestamp; rerun `finalize-multi-trader-smoke` with a later `--observed-at` or keep evidence from the same run window.
- `multi_trader_smoke_workflow_observed_at_missing`: at least one passed workflow has no observation timestamp; record the workflow through `record-multi-trader-smoke-workflow --observed-at ...`.
- `multi_trader_smoke_workflow_observed_at_invalid`: at least one passed workflow has a malformed `observed_at`; record it again with an ISO timestamp.
- `multi_trader_smoke_workflow_before_preflight`: at least one passed workflow was observed before this smoke run was prepared; rerun that workflow in the current smoke directory.
- `multi_trader_smoke_workflow_after_observed`: at least one passed workflow was observed after the finalize timestamp; rerun `finalize-multi-trader-smoke` with a later `--observed-at` or keep evidence from the same run window.
- `multi_trader_smoke_performance_artifact_before_preflight`: at least one performance artifact was exported before this smoke run was prepared; export fresh performance JSON after running `prepare-multi-trader-smoke`.
- `multi_trader_smoke_performance_artifact_after_observed`: at least one performance artifact was exported after the finalize timestamp; rerun `finalize-multi-trader-smoke` with a later `--observed-at` or re-export/re-finalize consistently.
- `multi_trader_smoke_performance_artifact_exported_at_missing`: at least one performance artifact has no `exported_at`; use frontend-exported performance JSON.
- `multi_trader_smoke_performance_artifact_exported_at_invalid`: at least one performance artifact has a malformed `exported_at`; re-export from the frontend smoke controls.
- `multi_trader_smoke_watchlist_overlap_missing`: client watchlists do not share a symbol.
- `multi_trader_smoke_symbol_status_closed_date_evidence_missing`: closed-market symbols did not export distinct requested/effective dates.
- `multi_trader_smoke_runtime_health_reference_missing`: observation was built without a runtime health file path or embedded symbol runtime evidence.
- `multi_trader_smoke_runtime_health_generated_at_missing`: runtime health artifact path was present but no `generated_at` timestamp was preserved; use a current runner/runtime-health artifact instead of a hand-assembled payload.
- `multi_trader_smoke_runtime_health_generated_at_invalid`: runtime health `generated_at` is malformed; regenerate the artifact.
- `multi_trader_smoke_runtime_health_before_preflight`: runtime health was generated before this smoke directory was prepared; rerun or wait for the runner to write a fresh artifact after `prepare-multi-trader-smoke`.
- `multi_trader_smoke_runtime_health_after_observed`: runtime health was generated after the final observation timestamp; rerun `finalize-multi-trader-smoke` with a later `--observed-at` or keep the artifacts from the same run window.
- `multi_trader_smoke_warm_snapshot_p95_invalid`: no frontend performance artifact and no runtime health p95 evidence were available.
- `runtime_health_performance_samples_empty`: runtime health had no `subscribe_snapshot_ms` samples; collect the artifact after real client subscriptions complete.
- `runtime_health_subscribe_snapshot_p95_exceeded`: service-side subscribe snapshot p95 exceeded 200 ms; inspect hydration/cache behavior before packaging smoke evidence.
