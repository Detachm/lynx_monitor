# Backend Production Runtime Execution Plan

Status: active execution plan as of 2026-05-27.

This file is the working checklist for productionizing the backend runtime. It follows the frozen frontend rule: do not edit `market-terminal/src/**`, do not change UI behavior, and do not change the `TerminalMessage v1` wire contract.

## 1. Target

Move the backend from the `real_data_runner + CSV hydrate` shape to a single-node production runtime:

- Redis provides warm first-screen read models and restart recovery.
- Kafka/Redpanda stores durable raw market facts in `raw_market_events_v1`.
- `SymbolRuntimeManager` and per-symbol runtime state own hydration, realtime attachment, Redis writes, freshness, and deltas.
- Gateway validates subscriptions and fans out `TerminalMessage v1` only.
- xtquant callbacks are enqueue-only and non-blocking.
- Active-pool management limits continuous realtime subscriptions and separates base-active, query-pinned, and temporary symbols.
- Current external access uses Vercel for the frozen frontend and a Cloudflare quick tunnel to the local backend Gateway.

First production scope:

- single machine;
- 10 traders;
- 200 active symbols;
- 100 pinned query symbols;
- recovery and runtime skeleton before deeper realtime market-depth expansion;
- current frontend frozen.

## 2. Acceptance Gates

- Redis warm subscribe p95 <= 200 ms.
- Hot symbol subscribe p95 <= 100 ms.
- xtquant callback to local queue p95 <= 2 ms.
- Callback to WebSocket delta p95 <= 300 ms.
- 10 clients and 200 active symbols with no duplicate hydrate and no global blocking.
- Redis, Kafka, or xtquant failure marks affected symbols `DEGRADED` without stopping unrelated symbols.
- `00700.HK` receives continuous `tick_realtime` and `queue_realtime` during market hours.
- `git diff -- market-terminal/src` remains empty.
- Trading-day first screens must not mix previous-effective-day minute bars with same-day realtime bars.
- Non-trading-day first screens may display latest effective-day minute bars with explicit requested/effective/source date evidence.

## 3. Current Completed Baseline

- `SymbolRuntimeManager` exists and owns ref-count, singleflight hydrate, runtime state, Redis state sink, Redis snapshot restore, realtime attach/detach, eviction, raw-event application, and delta generation.
- `ActiveSymbolPoolManager` exists and tracks base-active, query-pinned, temporary, evicted symbols, pool churn, pinned-capacity rejections, and active-pool health evidence.
- Redis read model keys are implemented:
  - `terminal:{date}:snapshot:{symbol}`
  - `terminal:{date}:minute:{symbol}`
  - `terminal:{date}:alerts:{symbol}`
  - `terminal:{date}:queue:{symbol}`
  - `terminal:{date}:state:{symbol}`
  - `ccass:holding:{symbol}`
  - `ccass:history:{symbol}:{participant_id}`
- Scoped Redis clear exists and does not touch Mammoth silver, Kafka topics, or local audit files.
- Kafka adapter validates canonical symbol keys, waits for delivery callback ACK, retries through `ReliableEventBus`, and spools failures to JSONL.
- Raw consumer validates key/symbol, commits only after processing or explicit quarantine, and writes persistent DLQ evidence.
- Gateway client queues are bounded, coalesce normal realtime deltas, and preserve critical snapshot/alert messages.
- Runtime health exposes process state, Kafka lag, producer spool/DLQ, Redis latency/failures, raw callback queue depth/rejections, raw consumer offsets/DLQ, symbol runtime state, freshness, hydrate latency, gateway clients, and dropped/coalesced queue counts.
- Production xtquant adapter exists:
  - starts/stops xtdata connection;
  - subscribes `hktransaction` and `hkbrokerqueueex` by default for the current SDK;
  - accepts `brokerqueue2` payloads at the normalizer boundary for compatibility, but the current installed SDK rejects `brokerqueue2` for `00700.HK`;
  - callback only wraps payload and forwards to the ingest queue sink;
  - exposes callback enqueue/rejection stats.
- Active startup symbols are promoted to a same-day realtime session on HK trading days even when same-day silver 1m bars are not available yet.
- Cold-query hydration applies the same trading-day rollover before the first snapshot is returned, preventing previous-day close-time K lines from appearing before today's realtime bars.
- Display names are loaded from `silver_instruments_v1.name` when present, with canonical symbol fallback.
- Full-tick seeding after realtime attach can populate the live snapshot before normal callbacks arrive.
- Ingest and Kafka processing are bounded per runtime tick to preserve WebSocket responsiveness under realtime backlog.
- Local production configuration currently uses `KAFKA_POLL_TIMEOUT_MS=1`, `HEALTH_SNAPSHOT_EVERY_TICKS=20`, and `TICK_INTERVAL_SECONDS=0.1`.
- Production runtime launcher exists:
  - `python -m beast_market.production_runtime`;
  - builds Redis, Kafka, DuckDB, and xtquant clients;
  - wires deferred xtquant callback sink to `RealtimeIngestWorker.receive_callback`;
  - uses `build_beast_market_runtime(...)`.
- Callback enqueue health evidence exists:
  - bounded latency samples;
  - last/max latency;
  - p95 latency;
  - sample count.
- Production config/runbook hardening exists:
  - no-secret template at `docs/runtime-config.production.template.json`;
  - README verifies production runtime uses `beast_market.production_runtime`;
  - `real_data_runner` is documented as smoke/dev/debug only.
- Realtime attach/release failures degrade only affected symbols:
  - attach failure still returns Redis/Mammoth snapshot;
  - affected symbol records `realtime_attach_failed`;
  - release failure records `realtime_release_failed` and does not block unrelated eviction.
- Backend-only performance smoke harness exists:
  - `python -m beast_market.performance_smoke`;
  - default scope is 10 clients, 200 symbols, 20 overlap symbols;
  - reports warm/hot subscribe p95, duplicate hydrate count, overlap ref-count, and client queue evidence.
- Real xtquant market-hours probe evidence exists:
  - `artifacts/production-validation/xtquant-live-probe.json`;
  - `artifacts/production-validation/xtquant-live-probe-3symbols.json`;
  - `00700.HK`, `00939.HK`, and `00005.HK` received live `hktransaction` and `hkbrokerqueueex` callbacks during HK market hours;
  - Redis was started locally and `PING` passed.
- Single-node local infra exists:
  - `infra/docker-compose.production.yml`;
  - Redis on `127.0.0.1:6379`;
  - Redpanda Kafka protocol broker on `127.0.0.1:9092`.
- Production dependency list exists:
  - `backend/requirements-production.txt`.
- Local Redpanda validation fallback exists:
  - Docker Hub image pulls were unreliable on this workstation;
  - Redpanda `v24.3.7` server tarball and `rpk` were downloaded under `artifacts/production-validation/`;
  - `artifacts/production-validation/redpanda-local.yaml` starts a single broker on `127.0.0.1:9092`.
- Production runtime validation controls exist:
  - `--gateway-port` avoids collision with the existing `real_data_runner` on `9020`;
  - `--max-ticks` gives bounded validation runs.
- Real Redis + Redpanda + xtquant validation evidence exists:
  - `artifacts/production-validation/runtime-health-redpanda-redis-xtquant-raw-consume7.json`;
  - `00700.HK` received 40 real xtquant callbacks in the bounded run;
  - callback enqueue p95 was about `0.007 ms`, max about `0.014 ms`;
  - raw Kafka group lag reached `0` for `raw_market_events_v1` and `processed_market_events_v1`;
  - Redis wrote read models with `0` failures and max write latency about `5.4 ms`.
- Kafka consumer production bugs found and fixed during real validation:
  - Confluent subscription now keeps raw and processed topics subscribed together;
  - cross-topic records are buffered instead of being validated as the wrong schema;
  - commits use explicit `TopicPartition(topic, partition, offset)`;
  - raw consumer commits `record.offset + 1`;
  - raw consumer commits once per successful batch instead of synchronously per record.
- Mammoth CSV recovery performance bug found and fixed:
  - CSV table reads are cached per `CsvSilverTableReader`;
  - participant history lookups are cached per `(symbol, participant_id, trade_date)`;
  - 3-symbol startup moved from effectively stuck to seconds-scale.
- xtquant SDK teardown caveat is documented by validation:
  - normal Python interpreter shutdown can still trigger native allocator abort after importing xtquant;
  - bounded validation uses `main(...)` followed by `os._exit(rc)` to avoid SDK destructor instability;
  - adapter `stop()` no longer calls SDK `disconnect`, `stop`, or bulk unsubscribe.
- WebSocket validation evidence exists:
  - `artifacts/production-validation/websocket-live-validation.json`;
  - Gateway returned `health` and `snapshot` for `00700.HK`;
  - no realtime delta arrived in that short WebSocket window because it ran near/after the HK close and health showed no new collector events during the window.
- Local-backend + Vercel + Cloudflare quick tunnel validation exists:
  - Vercel production alias is `https://market-terminal-psi.vercel.app`;
  - Vercel bundles `VITE_MARKET_WS_URL` at build time and must be redeployed after a quick tunnel URL change;
  - local WebSocket subscribe for `00700.HK` returned a live snapshot in sub-second validation after the latest fixes;
  - external quick-tunnel subscribe returned a live snapshot in seconds-scale validation, but quick tunnel stability is not guaranteed.

## 4. Active Work Items

1. Real market validation
   - Run a full 10-minute Gateway-attached market-hours observation window.
   - Confirm at least one connected WebSocket client receives increasing `tick_realtime` and `queue_realtime`.
   - Measure callback-to-WebSocket-delta p95 against the `<= 300 ms` gate.
   - Decide the production shutdown policy for the xtquant native SDK destructor issue.
2. Network stabilization
   - Replace account-less Cloudflare quick tunnels with a named tunnel or stable backend host before treating Vercel as a durable external production frontend.
   - Avoid relying on build-time quick tunnel URLs for unattended production.

## 5. Local Commands

Install production dependencies:

```bash
python -m pip install -r backend/requirements-production.txt
```

Start local Redis and Redpanda:

```bash
docker compose -f infra/docker-compose.production.yml up -d
```

Start the current local backend container shape:

```bash
docker build -t thousand-backend:production -f backend/Dockerfile.production .
docker run -d \
  --name thousand-backend-${RUNTIME_TRADE_DATE} \
  --restart unless-stopped \
  --network host \
  --env-file infra/production.env \
  -e PYTHONUNBUFFERED=1 \
  -e PYTHONPATH=/app/backend \
  -e TZ=Asia/Shanghai \
  -v /home/hliu/thousand/artifacts:/app/artifacts \
  -v ${SILVER_ROOT}:/data/silver:ro \
  -v ${XTQUANT_SDK_PATH}:/xtquant/sdk:ro \
  -v ${XTQUANT_DATA_HOME}:/xtquant/data \
  thousand-backend:production
```

Deploy the Vercel frontend against the current Cloudflare tunnel:

```bash
cd market-terminal
npx --yes vercel --prod --yes \
  --build-env VITE_MARKET_DATA_MODE=live \
  --build-env VITE_MARKET_PROTOCOL=terminal-message-v1 \
  --build-env VITE_MARKET_WS_URL=wss://<current-tunnel-host>:443/ws \
  --build-env VITE_MARKET_SYMBOLS=00700.HK,00939.HK,00005.HK,00108.HK,02643.HK
```

Verify runtime config:

```bash
PYTHONPATH=backend python -m beast_market.ops_cli verify-runtime-config \
  --config-path artifacts/runtime-config.json \
  --output-path artifacts/runtime-config-verification.json
```

Start production runtime:

```bash
PYTHONPATH=backend python -m beast_market.production_runtime \
  --config-path artifacts/runtime-config.json \
  --symbols 00700.HK,00939.HK,00005.HK \
  --xtquant-sdk-path /home/hliu/xtbackend/vendor/xtquant_251211_interim-release_cp36m-37m-38-39-310-311-312_linux-gnu_x86_64 \
  --health-snapshot-path artifacts/runtime-health.json
```

For live xtquant runs on this workstation, use `/home/hliu/miniconda3/envs/mammoth/bin/python` because the vendored xtquant SDK is a CPython 3.12 binary package. The default `/home/hliu/miniconda3/bin/python` is Python 3.13 and cannot import that binary extension.

Replay Kafka spool:

```bash
PYTHONPATH=backend python -m beast_market.ops_cli replay-kafka-spool \
  --spool-path artifacts/runtime-state/kafka-spool/publish-failures.jsonl \
  --kafka-bootstrap-servers 127.0.0.1:9092 \
  --confirm
```

Run backend-only 10-client / 200-symbol performance smoke:

```bash
PYTHONPATH=backend python -m beast_market.performance_smoke \
  --client-count 10 \
  --symbol-count 200 \
  --overlap-symbol-count 20 \
  --output-path artifacts/performance-smoke/backend-10x200.json
```

Scoped Redis clear:

```bash
PYTHONPATH=backend python -m beast_market.ops_cli clear-runtime-cache \
  --trade-date 20260526 \
  --symbols 00700.HK \
  --redis-url redis://127.0.0.1:6379/0 \
  --confirm
```

## 6. Verification Discipline

Before closing a backend production task:

```bash
PYTHONPATH=backend python -m pytest backend/tests -q
git diff --check
git diff -- market-terminal/src
```

The frontend diff must be empty.
