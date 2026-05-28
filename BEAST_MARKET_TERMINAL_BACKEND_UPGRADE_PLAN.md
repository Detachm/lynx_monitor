# Beast 市场终端后续开发推进计划

Status: authoritative roadmap and todo as of 2026-05-27.

This document is the only active development roadmap for the market terminal. Architecture principles live in `docs/terminal-runtime-architecture-guidance.md`. Historical schema and wire contracts live in `docs/mammoth-silver-schema-v1.md` and `docs/market-event-contracts-v1.md`.
Phase 6 LAN smoke execution is documented in `docs/lan-multi-trader-smoke-runbook.md`.

## Summary

后续开发不再沿着“全局 runtime loop + 固定 watchlist + tick 聚合 K 线”的方向补功能，而是切到“按股票拥有状态”的终端架构。

已锁定的关键决策：

- 第一里程碑是 `SymbolRuntimeManager` / `SymbolRuntime`，后端架构优先。
- 分钟 K 使用 `silver_minute_bars_v1`，从 xtquant/Mammoth 1m 历史分钟线 hydrate，tick 只补实时当前分钟和短缺口。
- Watchlist 第一版存在前端 `localStorage`，后端只维护实时订阅和热状态，不做登录/用户系统。
- Kafka 的 `raw_market_events_v1` 是 durable fact stream。
- `processed_market_events_v1` 保留为 shadow/audit/downstream 可选链路，不再是 Gateway 唯一状态来源。
- Redis 是 rebuildable read model，不是事实源。
- 生产 runtime 采用 active-pool 模型：默认维护 200 只活跃正股，用户查询池外股票时进入 pinned/temporary 生命周期。
- 当前外网访问形态是本地容器后端 + Vercel 前端 + Cloudflare quick tunnel；quick tunnel 是临时外网通道，不是稳定生产网络边界。
- 交易日 realtime 首屏不得把上一有效日分钟 K 当作今日 K 线返回；非交易日可以展示上一有效交易日分钟 K，并在 freshness 中保留 requested/effective/source date evidence。

必须坚持的边界：

- “全量查询”不能等同于“全市场实时订阅”。第一版只支持冷查询、加入 watchlist、热状态复用。
- 本地 watchlist 意味着服务端无法在交易员打开终端前知道每个人明天要看什么；如果要盘前按人预热，后续必须引入 backend profile。
- 选择 Silver 1m 代表数据管线是主线工作，不能一边要求高性能，一边继续用全日 tick 聚合画 K 线。
- 第一版不做权限/登录，因此只适合可信 LAN 内部分发。

## Implementation Progress

Phase status as of 2026-05-26:

- [x] Phase 0: Guardrails First.
- [x] Phase 1: Silver 1m Data Path.
- [x] Phase 2: SymbolRuntime Foundation.
- [x] Phase 3: Gateway Integration.
- [x] Phase 4: Frontend Dynamic Watchlist.
- [~] Phase 5: Kafka/Redis Production Hardening. Core runtime hardening and automated health gates are implemented and passing backend tests; real production soak/chaos evidence is still pending.
- [~] Phase 6: Real Multi-Trader Smoke. Tooling, runbook, artifact chain, package verification, and anti-forgery gates are implemented; the remaining blocker is a real two-machine LAN smoke run and attached evidence artifacts.

Completed as of 2026-05-26:

- `SymbolRuntimeManager` owns per-symbol lifecycle, ref-count, singleflight hydrate, eviction, state read-model publishing, and degraded evidence.
- Gateway subscribe/unsubscribe goes through runtime manager, returns runtime-owned snapshots, and keeps processed Kafka as a compatibility/shadow path.
- Real-data runner bootstrap now seeds `SymbolRuntimeManager`; real frontend subscribe traffic goes through symbol-owned runtime state instead of the legacy cache-only Gateway path.
- Real-data runner can expand its historical reader on first query for a non-preloaded symbol, so dynamic watchlist additions are not limited to the startup symbol list.
- Hot runtime subscriptions can restore a cleared Redis read model for the target symbol from in-process `SymbolRuntime` snapshot state, without requiring a process restart.
- App runtime, WebSocket service, real-data runner, and frontend dev/preview scripts default to LAN binding (`0.0.0.0`); frontend live URL defaults derive from the page hostname, and runtime health verification rejects loopback Gateway hosts, so another computer on the LAN can open the UI and reach the backend without loopback misrouting.
- Multi-client concurrent subscribe at the Gateway boundary is covered: overlapping clients subscribing the same cold symbol share one hydrate result and keep independent subscriptions.
- WebSocket request handling offloads synchronous subscribe/hydrate work from the asyncio event loop, so one cold symbol request does not block other client connections.
- Kafka producer publish path no longer flushes every message and surfaces delivery errors/timeouts to retry/spool/DLQ handling.
- Runtime health includes symbol runtime manager state, per-symbol runtime counters, state/snapshot sink failure evidence, bounded `subscribe_snapshot_ms` samples, queue metrics, Redis stats, and producer spool/DLQ evidence.
- Runtime health verification now rejects missing/empty/invalid `subscribe_snapshot_ms` samples and fails when the service-side subscribe snapshot p95 exceeds 200 ms, so Phase 6 warm first-screen latency is gated before final smoke packaging.
- Runtime health verification now cross-checks `symbol_runtime_manager` summaries against per-symbol runtime evidence, including runtime count, ref count, active hydrations, state counts, hydrating symbols, realtime-attached symbols, and symbol identity, so stale or hand-edited manager summaries cannot mask broken per-symbol state.
- Runtime health verification now treats Gateway critical client-queue overflow as a blocker while preserving noncritical drops and alert overflow as evidence, so slow-client pressure near critical-response loss cannot pass Phase 5/Phase 6 health gates silently.
- Runtime health verification now rejects Gateway processed-consumed, shadow-drained, or direct-emitted counters that exceed raw-consumer processed activity, catching stale processed-topic reads or corrupted Gateway activity counters.
- Runtime health verification now rejects Gateway terminal-message emitted counts that exceed their processed/direct source counters, preventing fabricated or stale Gateway activity from passing Phase 5 health gates.
- Runtime health verification now rejects Gateway delivered-symbol evidence whose unique symbol count exceeds `terminal_messages_delivered`, so delivery coverage cannot pass with internally inconsistent Gateway counters.
- Runtime health verification now rejects Redis write latency evidence where `max_latency_ms` is lower than `last_latency_ms`, preventing corrupted latency statistics from passing Phase 5 Redis health gates.
- Frontend dynamic watchlist persists in `localStorage`, supports subscribe/unsubscribe, and exports client smoke evidence with per-symbol `loading` / `warm` / `live` / `closed` / `degraded` status.
- Frontend smoke evidence export is aligned with the backend verifier and never emits `idle` for watched symbols; watched symbols without a loaded snapshot are exported as `loading`.
- Frontend smoke evidence now exports a degraded reason from either freshness or subscription error, so degraded symbols have backend-verifiable failure evidence instead of an empty reason list.
- Frontend smoke artifact generation filters invalid performance samples before writing summary or raw sample evidence, avoiding JSON `null`/negative latency values that the backend smoke verifier would reject later.
- Backend smoke readiness/finalize prechecks now semantically validate performance artifacts and report invalid files through `multi_trader_smoke_performance_invalid` plus `invalid_paths`, including bad samples, missing `schema_version`, missing ISO `exported_at`, or missing `machine_id`, instead of failing later with an opaque observation-build error.
- `build-multi-trader-smoke-observation` uses the same performance artifact reader as inspect/finalize, so low-level smoke assembly no longer accepts raw sample arrays or identity-less performance files that final verification would reject.
- `build-multi-trader-smoke-observation` rejects missing or malformed final `observed_at` before writing the observation file, so a bad run timestamp cannot enter the artifact chain and fail only at verification time.
- Native `silver_minute_bars_v1` is implemented across xtquant export, MammothAPI reads, historical readiness, app runtime hydrate, and real-data runner bootstrap.
- First-screen chart initialization uses native 1m bars; historical trade ticks are only replayed for alert continuity and bounded recovery, not to build the initial K-line chart.
- Guardrail tests now enforce that Gateway does not perform historical reads/business aggregation, runtime modules do not bypass MammothAPI for silver reads, real-data bootstrap does not replay ticks through the chart path, and historical tick replay cannot mutate `minute_bars`.
- Big trade detection is now pure previous-effective-day volume-ratio based; the old fixed turnover threshold is removed from business runtime paths and rejected when supplied in runtime config artifacts.
- `real_data_runner` only accepts a positive `--big-trade-volume-baseline-ratio`; the old `--big-trade-turnover-threshold` CLI knob is absent and fails argument parsing.
- `real_data_runner` validates `--trade-date`, `--symbols`, Gateway `--port` / `--path`, and `--runtime-health-interval-seconds` at argument parsing time, so invalid dates, empty symbol lists, non-HK symbols, unsafe ports, wrong WebSocket paths, or non-positive health intervals fail before any historical read starts.
- Broker queue participant display now uses the same three-bucket taxonomy as alert participants: actual mapped participant, `集合竞价`, or `未披露`; placeholder values such as `--` no longer reach the frontend queue participant column.
- Closed-market startup displays the latest effective trade date under the requested date, with requested/effective/source dates in payload freshness.
- Phase 6 LAN multi-trader smoke has an executable runbook that ties frontend exports, workflow evidence, runtime health verification, observation assembly, and final smoke verification into one artifact flow.
- Real-data runner can write a Phase 6 smoke health artifact during LAN smoke runs; the artifact remains failed until at least two distinct clients have connected and subscribe snapshot samples exist, so the lightweight true-data demo proves real client activity before final smoke verification.
- The smoke artifact chain is covered end to end for the real-data runner path: client exports plus workflows plus runner health can build an observation and pass `verify-multi-trader-smoke`.
- Final multi-trader smoke verification consumes runtime health `gateway_activity.client_queue` evidence when a runtime health artifact path is present, and rejects runs whose backend did not observe at least two clients or whose max concurrent client count never reached two.
- Final multi-trader smoke verification now cross-checks Gateway observed client ids against observed/max client counters and rejects duplicate observed or declared client ids, so a hand-edited runtime health artifact cannot claim two real clients from one observed connection id.
- Final multi-trader smoke verification preserves runtime health `generated_at` from the real-data runner or runtime-health verifier and rejects runtime health artifacts generated before this run's `prepared_at`, after the final observation time, or without a valid ISO timestamp.
- Frontend live WebSocket requests carry the smoke `machine_id` as `client_id`; backend runtime health records `observed_declared_client_ids`, and final smoke verification requires those declared ids to cover every frontend client artifact machine id.

Completed as of 2026-05-27:

- Production backend container image and entrypoint run `beast_market.production_runtime` with verified config artifacts.
- Runtime config now includes active-pool settings: `target_size`, `pinned_max_size`, `rank_window_days`, `rank_metric`, excluded instrument types, and eviction grace.
- `ActiveSymbolPoolManager` maintains explicit active symbols, ranked base-active symbols, query-pinned symbols, temporary symbols, churn counters, and health evidence.
- xtquant production client holds a persistent datacenter connection and subscribes active symbols to `hktransaction` and `hkbrokerqueueex`.
- xtquant callbacks remain enqueue-only; realtime processing is bounded per tick so WebSocket handling is not starved under callback/Kafka backlog.
- Production Kafka polling is short-polled through `KAFKA_POLL_TIMEOUT_MS=1`; health snapshot writes are less frequent to reduce hot-loop pressure.
- Startup active symbols are promoted to today's realtime session on trading days even when same-day silver minute bars are unavailable.
- Cold-query hydration now applies the same trading-day rollover before returning a first snapshot, so previous-day minute bars are not mixed with same-day realtime bars.
- Non-trading requested dates still use latest effective-day silver minute bars and expose requested/effective/source date freshness.
- Snapshots resolve display names from `silver_instruments_v1.name`; `00700.HK` displays as `腾讯控股` when instrument data is present.
- Full-tick seeding after realtime attach can populate a same-day live snapshot quickly while normal callbacks continue the minute/queue read model.
- Active-runtime subscribe fast path returns in-process `SymbolRuntime` snapshots without a full rehydrate.
- Vercel deployment is frozen to `TerminalMessage v1` and build-time live settings; the current WebSocket URL must be redeployed when the Cloudflare quick tunnel URL changes.
- Runtime health verification validates declared Gateway client id evidence shape, and final smoke verification rejects stale runtime health artifacts that omit declared client id fields entirely.
- Final multi-trader smoke verification now requires Gateway WebSocket evidence with a non-loopback host, valid port, and `/ws` path, so LAN accessibility is enforced by the evidence gate.
- Final multi-trader smoke verification rejects mock-mode frontend client artifacts; every client must export `data_source_mode: live`.
- Frontend client smoke artifacts include page and Gateway URLs, and final smoke verification rejects loopback client access evidence.
- `prepare-multi-trader-smoke` creates the Phase 6 artifact directory, workflow template, and LAN preflight evidence, rejecting loopback/wildcard hosts before a manual smoke run starts.
- `prepare-multi-trader-smoke --root-path auto` creates a timestamped smoke directory under `artifacts/multi-trader-smoke/` and records the actual root path in output/preflight, reducing manual directory setup during real runs.
- `prepare-multi-trader-smoke --frontend-port auto --gateway-port auto` can allocate free prepared ports starting at 5173/9020 and records both requested and resolved ports, so a real smoke can be prepared without first killing existing dev services when fixed ports are not required.
- `prepare-multi-trader-smoke` writes executable smoke helper scripts for backend startup, frontend startup, service probing, browser artifact import, workflow recording, next-action inspection, package-ready handoff gating, and finalize/package handoff under the smoke directory, each pinned to the repository root so they can be launched from any shell directory; the backend/frontend startup scripts fail before launch when their prepared Gateway/frontend ports are already occupied, preventing Vite auto-port fallback or Gateway port conflicts from producing mismatched LAN evidence.
- `prepare-multi-trader-smoke --silver-root <csv-root>` pins the generated real-data backend script to a specific CSV silver export, so Phase 6 true-data runs do not silently fall back to stale Mammoth patch data.
- `prepare-multi-trader-smoke` now pins the generated frontend script to `VITE_MARKET_DATA_MODE=live`, the prepared `VITE_MARKET_WS_URL`, protocol, and initial frontend watchlist symbols, so browser smoke artifacts cannot accidentally connect to an older Gateway on the default port or pre-populate cold-query/add-to-watchlist workflow symbols.
- A local Chrome headless browser smoke has been used against the prepared true-data frontend to verify page rendering, source-date visibility, smoke export downloads, `data_source_mode: live`, and Gateway URL matching before collecting two-machine LAN evidence.
- `prepare-multi-trader-smoke` writes a run-local `README.md` summarizing the prepared URLs, helper scripts, workflow names, and expected artifacts, plus `CLIENT_INSTRUCTIONS.md` for client machines to open the LAN URL, confirm distinct smoke machine ids, subscribe watchlists, refresh, and export client/performance smoke JSON files.
- `prepare-multi-trader-smoke --print-env` prints shell exports for `SMOKE_DIR`, prepared page/Gateway URLs, every run-local artifact path, backend/frontend/service commands, and helper script paths including inspect/finalize helpers; failed preflight snippets end with `false`, so smoke setup can be used directly with `eval` without masking failure.
- `prepare-multi-trader-smoke --lan-host auto` can select a detected local LAN IP for the smoke URLs, and fails explicitly with `multi_trader_smoke_lan_host_auto_detect_failed` when no client-routable local IP is found.
- LAN preflight evidence must carry a valid ISO `prepared_at`; final smoke, inspect, package, and bundle checks reject missing or malformed prepared timestamps because all client/performance/runtime/service/workflow freshness gates depend on this run boundary.
- `prepare-multi-trader-smoke` normalizes short numeric workflow symbols, rejects invalid symbols up front, and emits a runner command with the requested trade date and workflow symbols, preventing real smoke runs from accidentally using today's date or the default preload pair.
- `prepare-multi-trader-smoke --require-local-lan-host` records detected local addresses and rejects IP literals that are not assigned to the backend machine.
- `verify-multi-trader-smoke-services` probes the prepared frontend with HTTP and the Gateway `/ws` URL with a WebSocket upgrade handshake after services start, writes `service-preflight.json`, and updates `lan-preflight.json` with protocol-level service reachability evidence before client artifacts are collected.
- Final multi-trader smoke verification now requires LAN preflight plus passed service reachability checks, so a run cannot pass if the frontend/Gateway ports were never probed.
- Final multi-trader smoke verification requires service-preflight frontend/Gateway probe URLs to match the prepared `lan-preflight.json` URLs, so reachability evidence cannot be reused from a different host or port.
- Final multi-trader smoke verification validates service-preflight `checked_at` timestamps, rejecting missing, malformed, pre-preflight, or post-observation service reachability probes so a stale port probe cannot be reused for a later smoke run.
- `import-multi-trader-smoke-artifact` and `import-multi-trader-smoke-artifacts` import browser-exported smoke JSON into the correct prepared `clients/` or `performance/` directory based on content, avoiding manual directory mistakes.
- Client smoke artifact import now performs the same semantic prechecks operators care about before finalize: live mode, non-loopback page/Gateway URLs, valid watchlist/status coverage, connected state, and refresh recovery. Bad browser exports fail at import time instead of being discovered only during final smoke verification.
- Performance smoke artifact import requires the frontend-exported schema, ISO `exported_at`, valid samples, and non-empty `machine_id` provenance, so standalone performance files cannot enter the prepared smoke directory without the identity evidence final verification needs.
- Smoke artifact import writes append-only `smoke-import-manifest.json` run entries with input/output provenance and skipped-file evidence for final packaging.
- Smoke artifact import manifests now record imported output paths relative to the smoke root and validate every imported record's kind, input provenance, output scope, kind-to-directory mapping, duplicate outputs, and per-run count/payload consistency before packaging.
- `inspect-multi-trader-smoke` and `finalize-multi-trader-smoke` apply the same client artifact semantic validation to files already present under `clients/`, so manually copied or stale bad exports are reported as `multi_trader_smoke_clients_invalid` with concrete `invalid_paths` before observation assembly.
- `record-multi-trader-smoke-workflow` safely records observed workflow passes into `workflows.json`, avoiding manual JSON key/editing mistakes during real LAN runs.
- `record-multi-trader-smoke-workflow` normalizes short numeric workflow symbols, rejects invalid symbols, and refuses to write `passed: true` when required workflow evidence is incomplete, including missing cold-query/add-watchlist/Redis-clear symbols or missing closed-market requested/effective dates.
- `record-multi-trader-smoke-workflow` rejects malformed `--observed-at` values before writing `workflows.json`, so workflow timestamps produced through the CLI are at least syntactically valid before final smoke timing checks apply the run window.
- Workflow evidence now distinguishes action evidence from outcome evidence for browser refresh, Redis clear, and backend restart recovery; final smoke rejects passed workflows that only claim the screen recovered without `browser_refreshed`, `cache_cleared`, or `backend_restarted`.
- Final multi-trader smoke verification requires every passed workflow to carry a valid ISO `observed_at` within this run's `prepared_at` to final `observed_at` window, so stale or future hand-edited workflow evidence cannot pass.
- `inspect-multi-trader-smoke` gives a read-only pre-finalize readiness summary with missing/invalid artifact paths, artifact counts, incomplete workflow names, workflow evidence-field blockers, and an in-memory final smoke verifier preview when artifacts are parseable.
- `inspect-multi-trader-smoke` also reports `runtime_health_readiness` with generated time, observed client counts, declared client counts, max connected clients, and subscribe snapshot sample count, so operators can distinguish "keep clients open and subscribe" from missing or malformed runtime health.
- `inspect-multi-trader-smoke` blocks client collection when runtime health proves smoke symbols are degraded by missing `silver_minute_bars_v1`, so Phase 6 cannot proceed with stale/no-chart data and must repair native 1m silver first.
- `inspect-multi-trader-smoke` now includes ordered `next_actions` with the next operator stage and command hint, reusing `prepare-multi-trader-smoke`'s generated backend startup script, frontend startup script, service probe script, client URL, client instruction artifact path, import-artifacts script, workflow recording script, finalize/package script, and handoff verification script when available, so real LAN smoke runs can move through backend startup/frontend startup/service probe/client activity/client import/workflow/finalize/package/handoff without interpreting raw blockers manually.
- `inspect-multi-trader-smoke --next-action` prints only the first operator action plus readiness/blocker summary, preflight readiness, and runtime health readiness while preserving normal readiness exit codes, making repeated live-run checks less noisy without hiding client-count or service-preflight evidence.
- `inspect-multi-trader-smoke` reports workflow evidence blockers both as a flat list and grouped by workflow name, so operators can repair the exact incomplete check before finalize.
- `inspect-multi-trader-smoke` also reports package handoff readiness, including missing/invalid passed smoke evidence and import manifest blockers, so `finalize-multi-trader-smoke --package` failures can be found before the final handoff step.
- `inspect-multi-trader-smoke --require-package-ready` returns non-zero unless both smoke readiness and package handoff readiness are satisfied, and `prepare-multi-trader-smoke` exposes it as `$SMOKE_VERIFY_HANDOFF_SCRIPT`, giving scripts a pre-handoff gate before publishing the final zip.
- Package readiness inspection now applies the same service-preflight timing window used by final bundle verification when smoke evidence has `observed_at`, so stale or future service probes are reported before `finalize-multi-trader-smoke --package` handoff.
- `finalize-multi-trader-smoke` turns a prepared smoke directory into observation and final smoke evidence in one command, reducing manual artifact stitching during real LAN testing.
- `finalize-multi-trader-smoke --package` also writes the smoke zip and package metadata in the same command, so real runs can produce a complete handoff bundle without a second step.
- `finalize-multi-trader-smoke --package` includes `package_readiness` in failure output when the smoke evidence was generated but package handoff failed, so operators can repair import provenance or missing output files without guessing.
- Smoke packaging requires a passed `multi-trader-smoke-evidence.json` plus a valid `smoke-import-manifest.json` with at least one imported artifact, matching counts, imported `output_path` files present under the smoke root, and import coverage for every recognizable frontend client/performance artifact under `clients/` or `performance/`, so final packages cannot be produced from failed, manually copied, or incomplete client/performance files without import provenance.
- Smoke packaging requires a fresh `smoke-run-manifest.json` covering every JSON file that will enter the zip, so stale package manifests fail before the final bundle verifier.
- Smoke packaging requires passed `lan-preflight.json` and matching `service-preflight.json`, so final handoff packages include independently auditable LAN host and service reachability evidence.
- Smoke package metadata `smoke-run-package.json` is excluded from smoke manifests and zips, including reruns, so repeated `finalize-multi-trader-smoke --package` executions do not poison the next handoff.
- `finalize-multi-trader-smoke` embeds `lan-preflight.json` into the smoke observation, so standalone smoke evidence also proves LAN preflight passed.
- `finalize-multi-trader-smoke` now requires an independent `service-preflight.json` matching `lan-preflight.json.service_checks`, so standalone smoke evidence cannot pass without the service reachability artifact.
- `finalize-multi-trader-smoke` writes failed evidence with explicit missing-path/invalid-path blockers instead of throwing when required smoke artifacts are absent or malformed.
- `finalize-multi-trader-smoke` validates the final `--observed-at` before artifact assembly and writes `multi_trader_smoke_observed_at_missing` or `multi_trader_smoke_observed_at_invalid` failed evidence instead of producing a malformed observation.
- `finalize-multi-trader-smoke` writes `smoke-run-manifest.json` with relative paths, byte sizes, and sha256 hashes for submitted JSON artifacts.
- `package-multi-trader-smoke` packages the generated smoke JSON evidence into `multi-trader-smoke-evidence.zip` and writes `smoke-run-package.json` with zip sha256/size/file-list metadata.
- Final evidence bundle verification requires the smoke run manifest whenever multi-trader smoke evidence is included, and validates hashes for smoke evidence and LAN preflight files.
- Packaged smoke-run manifest verification now rejects missing/invalid schema versions and duplicate manifest file paths, so handoff zips cannot pass with structurally stale or ambiguous `smoke-run-manifest.json` entries even when listed file hashes match.
- Final evidence bundle verification can validate the packaged smoke zip against `smoke-run-package.json`, including zip sha256, size, and included relative paths.
- Final evidence bundle verification requires packaged smoke zip evidence to include a valid `smoke-import-manifest.json`, and verifies every imported `output_path` is present in the zip, preserving client artifact import provenance even when the package is hand-assembled.
- Final evidence bundle verification also applies the import-manifest record-level provenance checks inside packaged smoke zips, so hand-assembled bundles cannot hide absolute/out-of-scope paths, bad kinds, mismatched client/performance directories, duplicate output records, or inconsistent run entries.
- Final evidence bundle verification now also checks the reverse coverage direction inside packaged smoke zips: every `clients/*.json` or `performance/*.json` artifact in the zip must have an imported `output_path` in `smoke-import-manifest.json`, preventing hand-assembled packages from adding unproven client/performance evidence after import.
- Final evidence bundle verification requires packaged smoke zip evidence to include passed `service-preflight.json` matching `lan-preflight.json.service_checks`, preventing a hand-assembled zip from dropping service reachability evidence.
- Final evidence bundle verification also applies the service-preflight timing gate to packaged smoke zips, so even a self-consistent zip cannot reuse service reachability probes from before the smoke directory was prepared or after the final observation timestamp.
- Final evidence bundle verification cross-checks the package zip's smoke evidence, LAN preflight, and smoke-run manifest entries against the separately submitted artifacts by hash, preventing a self-consistent but stale or altered zip from passing.
- Final evidence bundle verification rejects package-only submissions; package verification requires the external smoke evidence, LAN preflight, and smoke-run manifest artifacts so the final bundle remains independently auditable.
- Final evidence bundle verification uses the packaged `smoke-run-manifest.json` to hash-check every listed file inside the zip, including `multi-trader-smoke-observation.json`, so non-core JSON artifacts cannot be silently swapped after packaging.
- Final evidence bundle verification rejects package zip JSON files that are not listed in the packaged `smoke-run-manifest.json`, so handoff bundles cannot include unaudited extra artifacts.
- Frontend client smoke JSON embeds performance samples, and `finalize-multi-trader-smoke` falls back to client artifacts when the performance directory is empty.
- Frontend performance smoke JSON always includes `machine_id` provenance and exported smoke filenames sanitize the machine id, preserving client identity even when files are moved or renamed.
- Frontend toolbar displays the current smoke `machine_id` and provides an operator reset action, so duplicate client identity can be caught before browser artifacts are exported.
- Final multi-trader smoke verification explicitly rejects duplicate frontend `machine_id` values with `multi_trader_smoke_client_machine_duplicate`, making identity mistakes distinguishable from too-few-client failures.
- Final multi-trader smoke verification cross-checks every client artifact against the embedded LAN preflight: client `page_url` must use the same page origin and client `gateway_url` must exactly match the preflight Gateway URL.
- `prepare-multi-trader-smoke` records `prepared_at`, observation assembly preserves client artifact `exported_at` / path / machine id metadata, and final smoke verification rejects client exports with missing/malformed timestamps, from before the prepared run, or after finalize observation time.
- Smoke observation assembly records `performance_artifacts[]` with performance file path, `machine_id`, and subscribe-snapshot sample count, making missing or mislabeled client performance exports visible before final verification.
- Final multi-trader smoke verification validates `performance_artifacts[]` when present, rejecting missing machine ids, unknown machine ids, and incomplete client-machine coverage.
- Final multi-trader smoke verification validates performance artifact `exported_at` timestamps when performance files are present, rejecting missing, malformed, pre-preflight, or post-observation performance exports just like client smoke exports.
- Final evidence bundle verification now requires passed LAN preflight evidence whenever multi-trader smoke evidence is included, and checks the preflight Gateway URL against the deployed frontend live URL.
- `clear-runtime-cache` dry-run shows the scoped CCASS history wildcard for operator visibility, while confirmed deletion only deletes concrete expanded Redis keys and never treats a wildcard as a key to delete.
- `verify-runtime-config` is a hard automation gate: failed config verification writes evidence and exits non-zero, so deprecated runtime fields or unsafe LAN/Kafka/Redis settings cannot be treated as passing by smoke or deployment scripts.
- `runtime_config_from_artifact` also refuses to construct a runtime config from an artifact that fails the same verifier, preventing direct code paths from bypassing the CLI gate.
- `real_data_runner` CSV silver reads are table-cached before per-symbol filtering, preventing repeated CCASS/tick CSV scans during BOD preload and allowing the Gateway to start from freshly exported CSV silver data in seconds instead of stalling before it listens.
- Kafka publish-failure spool loading now persists malformed JSONL records into a sidecar quarantine file instead of silently dropping them on restart, and runtime health exposes plus gates `quarantined_spool_records`, `spool_path`, and `spool_quarantine_path` so corrupted durable publish evidence cannot pass Phase 5/Phase 6 checks unnoticed or without an auditable artifact location.
- `replay-kafka-spool` dry-run and confirmed replay output now reports the quarantine path and quarantined record count, so operators can see corrupt persisted spool lines before replaying surviving records into Kafka.
- Runtime health verification now requires Kafka producer `publish_attempts` to cover at least ingest plus raw-consumer processed activity, so worker progress cannot pass Phase 5 health gates without matching durable publish-attempt evidence.
- Frontend smoke machine id loading now trims and rewrites persisted localStorage values before Gateway `client_id` use or client artifact export, reducing Phase 6 failures from stale/manual ids whose whitespace no longer matches runtime health declared-client evidence.
- Runtime health and final multi-trader smoke verification now reject observed or declared Gateway client ids that require trimming, so hand-edited or stale artifacts cannot pass with ids that differ from frontend `machine_id` evidence only by whitespace.
- Client and performance smoke artifact import/final verification now reject `machine_id` values that require trimming, keeping frontend exports, Gateway declared-client evidence, and imported artifact provenance under one exact identity contract.
- Final smoke verification also validates `client_artifacts[].machine_ids` provenance with the same exact machine-id contract, so package metadata cannot carry hand-edited or stale ids that only match after trimming.

Open high-priority items:

- Run real LAN multi-trader smoke with at least two machines and attach generated evidence artifacts.

## Key Changes

### Backend Runtime

新增 symbol-owned runtime 层：

- `SymbolRuntimeState`: `COLD`, `HYDRATING`, `WARM`, `LIVE`, `DEGRADED`, `EVICTING`.
- `SymbolRuntime`: 拥有单只股票的 snapshot、minute bars、alerts、queue、CCASS、freshness、source dates。
- `SymbolRuntimeManager`: 管理 symbol -> runtime、ref-count、singleflight hydration、eviction。

默认配置：

- `max_concurrent_hydrations = 8`
- `symbol_eviction_grace_seconds = 300`
- Redis terminal TTL 继续使用现有 8 小时级别配置。
- 同一 `symbol + effective_date` 只允许一个 hydration job。

Gateway subscribe 流程：

1. 校验 symbol。
2. 调用 `runtime_manager.attach(symbol, session_id)`。
3. 如果 symbol 是 cold，进入 singleflight hydrate。
4. 返回完整 snapshot。
5. 市场开市且数据可用时 attach realtime。
6. 后续从 `SymbolRuntime` deltas fanout。

unsubscribe/disconnect 流程：

1. 调用 `runtime_manager.detach(symbol, session_id)`。
2. ref-count 为 0 后进入 `EVICTING`。
3. grace period 后释放 realtime 订阅。
4. Redis snapshot 保留到 TTL。

### Historical Hydration

新增 `silver_minute_bars_v1` 数据路径：

- xtquant exporter 下载 1m bar。
- Mammoth silver schema/manifest 支持 `minute_bars`。
- `MammothAPI.get_minute_bars(symbol, trade_date)` 作为业务读取入口。
- chart 初始化必须使用 1m bars。
- 全日 tick 聚合只允许作为 fallback，并需要测试守卫禁止进入生产主路径。

Hydration 数据顺序：

1. Redis snapshot fresh 则直接返回。
2. Redis stale/missing 时读取 latest two daily bars、effective-day 1m bars、latest/previous CCASS、global broker mapping cache、latest broker queue snapshot。
3. 只在 alert continuity 需要时回放 bounded tick gap。
4. 写回 Redis read model。

Closed-market / pre-open 默认规则：

- Redis key 中的 `{date}` 使用 `requested_trade_date`。
- payload/freshness 必须带 `requested_trade_date`、`effective_trade_date`、`source_dates`、`runtime_state`、`degraded_reasons`。
- 非交易日显示 latest effective trading day，包括上一有效交易日分钟 K，并在 freshness 中明确 requested/effective/source dates。
- 交易日如果今日 1m silver 尚不可用但需要 attach realtime，首屏不得返回上一有效日分钟 K；runtime 先 rollover 到 requested date、清空历史分钟线，并在无法补齐盘中缺口时标记 `intraday_gap_before_attach`。
- 收到今日 realtime/1m 数据后更新今日 minute read model。

### Redis / Kafka / Gateway

Redis read model key：

- `terminal:{date}:snapshot:{symbol}`
- `terminal:{date}:minute:{symbol}`
- `terminal:{date}:alerts:{symbol}`
- `terminal:{date}:queue:{symbol}`
- `terminal:{date}:state:{symbol}`
- `ccass:holding:{symbol}`
- `ref:broker_mapping:{version}`
- `ref:trading_calendar:{market}:{version}`

所有 Redis record 必须包含：

- `schema_version`
- `requested_trade_date`
- `effective_trade_date`
- `source_dates`
- `updated_at`
- `version` 或 `last_event_id`
- `freshness`
- `degraded_reasons`

Kafka 调整：

- 保持 `raw_market_events_v1` as the durable raw fact stream keyed by symbol。
- 生产 Kafka adapter 去掉 publish 后每条 flush 的行为。
- 增加 ACK、retry、delivery callback、spool、persistent DLQ/quarantine。
- `processed_market_events_v1` 保留，但 Gateway 不再强依赖它作为唯一 state path。

TerminalMessage v1 保持兼容，snapshot payload 做 additive 扩展：

- `freshness.runtime_state`
- `freshness.requested_trade_date`
- `freshness.effective_trade_date`
- `freshness.source_dates`
- `freshness.degraded_reasons`

### Frontend

第一版 watchlist 归属前端：

- `localStorage` key: `market-terminal.watchlist.v1`
- 启动时读取本地 watchlist 并逐个 subscribe。
- 搜索/输入 symbol 后 subscribe，成功后加入 watchlist。
- 移除 watchlist 时 unsubscribe。
- 前端状态增加 `loading`, `warm`, `live`, `closed`, `degraded`。
- UI 必须显示 requested date / effective date / freshness，避免把休市数据误认为 realtime。
- 不做登录、权限、多用户 profile、服务端 watchlist 持久化。

## Delivery Phases

### Phase 0: Guardrails First `[x] Completed`

目标：防止继续往旧架构加债。

- [x] 加测试守卫：Gateway 不允许做历史读取或业务聚合。
- [x] 加测试守卫：生产 chart 初始化不允许依赖全日 tick 聚合。
- [x] 加测试守卫：业务模块不得绕过 MammothAPI 读 silver。
- [x] 保留当前真实数据 demo 作为 smoke baseline。

验收：

- [x] 当前 backend/frontend tests 仍通过。
- [x] 新 guardrail tests 能阻止旧路径扩散。

### Phase 1: Silver 1m Data Path `[x] Completed`

目标：先把 K 线主路径切正确。

- [x] 实现 `silver_minute_bars_v1` reader/export/manifest。
- [x] exporter 支持 xtquant 1m 下载。
- [x] `MammothAPI` 增加 minute bar 业务读取。
- [x] 现有 real-data runner 可用 1m bars 初始化图表。
- [x] tick 只更新当前分钟或补 bounded gap。

验收：

- [x] 休市日显示上一有效交易日 1m bars。
- [x] Redis 清空后能从 1m bars 恢复图表。
- [x] 测试证明不需要全日 tick replay 也能首屏可用。

### Phase 2: SymbolRuntime Foundation `[x] Completed`

目标：后端状态所有权切到 symbol。

- [x] 实现 `SymbolRuntime` 和 `SymbolRuntimeManager`。
- [x] 实现 attach/detach/ref-count。
- [x] 实现 singleflight hydrate。
- [x] 实现 `terminal:{date}:state:{symbol}`。
- [x] 先复用现有 Octopus 计算能力，但由 SymbolRuntime 拥有状态和发布边界。

验收：

- [x] 两个客户端订阅同一 symbol，只触发一次 hydrate。
- [x] 一个客户端退订不影响另一个客户端。
- [x] ref-count 为 0 后 grace period 释放 realtime。
- [x] per-symbol health 能显示 lifecycle/freshness/degraded reason。

### Phase 3: Gateway Integration `[x] Completed`

目标：subscribe 真正驱动后端 lifecycle。

- [x] Gateway subscribe 改为调用 runtime manager。
- [x] Gateway snapshot 从 runtime manager 获取。
- [x] Gateway delta fanout 可直接来自 SymbolRuntime。
- [x] 慢客户端继续使用 bounded queue/coalescing。
- [x] processed Kafka 消费路径保留为兼容/shadow，不作为唯一 Gateway path。

验收：

- [x] 冷 symbol 查询返回 loading -> snapshot。
- [x] 热 symbol 查询直接返回 cached snapshot。
- [x] Redis stale 时自动 hydrate。
- [x] Redis clear 单只股票后重新 subscribe 能重建该股票。

### Phase 4: Frontend Dynamic Watchlist `[x] Completed`

目标：交易员可以查非预设股票，并持久化到本机。

- [x] localStorage watchlist。
- [x] 搜索/添加/删除 symbol。
- [x] 启动自动恢复本机 watchlist。
- [x] per-symbol store/UI 状态统一使用 `loading` / `warm` / `live` / `closed` / `degraded`，不再用旧 `subscribed` / `error` 作为展示状态。
- [x] 显示 requested/effective/source dates。

验收：

- [x] 不改后端配置也能查询新 symbol。
- [x] 刷新浏览器后 watchlist 保留。
- [x] 删除 symbol 后前端 unsubscribe，后端 ref-count 下降。
- [x] 休市日 UI 明确显示上一有效交易日。

### Phase 5: Kafka/Redis Production Hardening `[~] Mostly Complete, Production Evidence Pending`

目标：消除高并发和故障恢复风险。

- [x] Kafka producer 去掉 per-message flush。
- [x] 增加 persistent DLQ/quarantine。
- [x] 增加 local spool。
- [x] Redis writes per symbol atomic/pipelined。
- [x] runtime health 覆盖 hydration latency、Redis latency、Kafka lag、queue depth、dropped/coalesced counts。
- [x] runtime health 保留 bounded `subscribe_snapshot_ms` 样本，作为服务端首屏延迟证据。
- [~] 压测多客户端、多 symbol 冷查询。自动化并发/队列门槛已覆盖；真实生产级 soak/chaos 证据待补。

验收：

- [x] Kafka 暂停时 raw facts 不静默丢失。
- [x] Redis 暂停时 symbol 进入 degraded，不阻塞 xtquant callback。
- [x] 多客户端高频订阅不会重复 hydrate 同一 symbol。
- [x] 慢客户端不影响其他客户端和 ingestion。
- [ ] 真实生产级故障演练/soak 证据归档。

### Phase 6: Real Multi-Trader Smoke `[~] Tooling Complete, Real Two-Machine Evidence Pending`

目标：真实 LAN 使用闭环。

- [ ] 至少两台客户端机器完成真实 LAN smoke，并归档证据。
- [x] 用 `prepare-multi-trader-smoke` 创建 artifact 目录、workflow 模板和 `lan-preflight.json`，并先验证 LAN host 不是 loopback/wildcard。
- [x] 启动 backend/frontend 后用 `verify-multi-trader-smoke-services --root-path "$SMOKE_DIR"` 探测 LAN page URL 和 Gateway WebSocket 端口，失败时先修复服务绑定、防火墙或端口冲突。
- [x] `real_data_runner --runtime-health-path "$SMOKE_DIR/runtime-health-verification.json"` 需要在至少两台客户端完成打开和订阅后再取最终 artifact；不足两个 observed clients、不足两个 declared client ids、最大并发连接数从未达到 2、或没有 subscribe sample 时 artifact 和最终 smoke verifier 都必须失败。
- [ ] 不同 watchlist，部分重叠 symbol 的真实双机证据。
- [ ] 冷 symbol 查询、加入 watchlist、刷新恢复的真实双机证据。
- [ ] Redis 单 symbol clear 后恢复的真实双机证据。
- [ ] 进程重启后恢复的真实双机证据。
- [ ] 休市/开盘前 effective date 展示的真实双机证据。
- [x] 用 `import-multi-trader-smoke-artifact` 导入每台客户端导出的 JSON，避免手动放错 `clients/` / `performance/` 目录。
- [x] 用 `build-multi-trader-smoke-workflows-template` 生成标准 workflows 证据模板，再由操作员按真实结果填充。
- [x] 优先用 `record-multi-trader-smoke-workflow` 记录每项真实观测结果，避免手改 `workflows.json`。
- [x] 用 `inspect-multi-trader-smoke --root-path "$SMOKE_DIR"` 在 finalize 前检查缺失 artifact 和未完成 workflow。
- [x] 用 `build-multi-trader-smoke-observation` 合并多台前端 client/performance 导出、workflows、runtime health。
- [x] 推荐用 `finalize-multi-trader-smoke --root-path "$SMOKE_DIR" --package` 一键生成 observation、final smoke evidence、manifest、zip 和 package metadata；低层 `build/verify/package` 命令保留用于排查。
- [x] 用 `package-multi-trader-smoke --root-path "$SMOKE_DIR"` 生成可交付 zip，并用 `smoke-run-package.json` 记录 zip sha256、大小和文件清单。
- [x] 最终 `verify-evidence-bundle` 如果传入 multi-trader smoke evidence，必须同时传入 `--multi-trader-smoke-preflight-path "$SMOKE_DIR/lan-preflight.json"` 和 `--multi-trader-smoke-manifest-path "$SMOKE_DIR/smoke-run-manifest.json"`。
- [x] 如果提交 smoke zip，最终 `verify-evidence-bundle` 同时传入 `--multi-trader-smoke-package-path` 和 `--multi-trader-smoke-package-metadata-path`，由 bundle gate 复核 zip 完整性。
- [x] Phase 6 性能判断同时使用前端导出和 runtime health 中的服务端 subscribe snapshot 证据；未提供前端性能文件时，`build-multi-trader-smoke-observation` 会从 raw runtime health 样本或 `verify-runtime-health` 产出的 p95 证据派生服务端 warm snapshot p95。
- [x] 前端 client smoke artifact 必须包含 `schema_version: 1`、ISO `exported_at`、`data_source_mode: live`、non-loopback `page_url` / `gateway_url` 以及每只 watchlist 股票的 `status`、`snapshot_loaded`、requested/effective date、source dates、degraded reasons；后端 smoke verifier 需要校验这些证据，并要求 client `page_url` origin / `gateway_url` 与本次 `lan-preflight.json` 一致，client `exported_at` 和每个 passed workflow 的 `observed_at` 必须落在本次 preflight `prepared_at` 与 finalize `observed_at` 之间。

验收：

- [x] 首屏 snapshot p95 < 200ms for warm symbols 的自动化 gate。
- [x] 冷 symbol hydration 有明确 loading/degraded 状态。
- [x] 重叠 symbol 不重复 upstream hydrate 的自动化 gate。
- [x] 所有异常都有 runtime health evidence。
- [ ] 两台真实客户端跑通并生成 final smoke evidence、manifest、zip、package metadata。

## Test Plan

Backend:

- Symbol lifecycle transition tests。
- Singleflight hydration concurrency tests。
- Gateway subscribe/unsubscribe ref-count tests。
- Redis state key shape and TTL tests。
- Native 1m hydrate tests。
- Closed-market effective date tests。
- Kafka key/symbol ordering and DLQ tests。
- Slow client queue isolation tests。

Frontend:

- localStorage watchlist persistence tests。
- dynamic subscribe/unsubscribe tests。
- loading/warm/live/closed/degraded state rendering tests。
- freshness/source date rendering tests。
- strict TerminalMessage v1 normalization tests。

End-to-end:

- two-client overlapping subscription。
- cold query -> hydrate -> snapshot -> add watchlist。
- Redis clear -> rebuild。
- process restart -> first-screen recovery。
- xtquant/Kafka/Redis partial outage -> degraded without global failure。
- closed-market day -> latest effective trading day shown with evidence。

## Assumptions

- 第一版只服务可信 LAN，不做登录/权限。
- Watchlist 第一版只存在本地浏览器，不做服务端 profile。
- 不做全市场实时订阅；查询过或 watchlist 中的股票才进入热路径。
- 不做长期历史研究回放。
- 不改变 `TerminalMessage v1` 顶层协议，只做 snapshot/freshness additive 扩展。
- Big trade 继续使用“单笔成交量 >= 前一有效交易日总成交量 * ratio”。
- alert 和 broker queue 的 participant 展示只保留：实际参与者、`集合竞价`、`未披露`。

## Conflict Policy

When documents disagree:

1. `docs/terminal-runtime-architecture-guidance.md` defines architecture principles.
2. This file defines execution order and todos.
3. `docs/market-event-contracts-v1.md` defines wire contracts until a v2 contract is explicitly created.
4. `docs/mammoth-silver-schema-v1.md` defines historical schema.
5. README files are entrypoints only and must not override this roadmap.

Active Markdown set after cleanup:

- `BEAST_MARKET_TERMINAL_BACKEND_UPGRADE_PLAN.md`
- `docs/terminal-runtime-architecture-guidance.md`
- `docs/mammoth-silver-schema-v1.md`
- `docs/market-event-contracts-v1.md`
- `docs/lan-multi-trader-smoke-runbook.md`
- `backend/README.md`

Generated smoke-run Markdown under `artifacts/multi-trader-smoke/**`, dependency package Markdown under `node_modules/**`, and tool cache Markdown such as `.pytest_cache/README.md` are not development guidance and must not be used as roadmap input.

Cleanup rule:

- Delete obsolete roadmap/todo/plan Markdown files instead of leaving them beside this roadmap.
- Keep schema, contract, and runbook documents only when their status line points back to this roadmap or the runtime architecture document.
- Do not introduce a second active roadmap. Amend this file instead.
- As of 2026-05-26, no obsolete roadmap/todo/plan Markdown remains in the active repository documentation set.
