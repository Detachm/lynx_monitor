# Market Terminal Runtime State

Last updated: 2026-05-27 Asia/Shanghai.

This document records the current implemented behavior, the intended behavior, and the recent production/debug changes for the market terminal.

## Current Deployment

- Frontend: Vercel production alias `https://market-terminal-psi.vercel.app`.
- Frontend source and wire contract: `market-terminal/src/**` and `TerminalMessage v1` are unchanged.
- Backend runtime: Docker container `thousand-backend-20260527`, image `thousand-backend:production`, host network, restart policy `unless-stopped`.
- Backend Gateway: `0.0.0.0:9020`, WebSocket path `/ws`.
- External WebSocket access: Cloudflare quick tunnel to local `http://127.0.0.1:9020`.
- Redis: container `thousand-redis`, `127.0.0.1:6379`.
- Kafka-compatible raw fact stream: local Redpanda on `127.0.0.1:9092`.
- xtquant: production backend starts xtdata, connects to the local datacenter, and subscribes active symbols.
- Current production trade date: `RUNTIME_TRADE_DATE=20260527`.
- Current initial symbols: `00700.HK,00939.HK,00005.HK,00108.HK,02643.HK`.

## Intended Runtime Model

The runtime is moving to an active-pool model:

- Maintain a bounded active pool of HK equities.
- Default target active pool size is `200`.
- User queries outside the pool become pinned symbols when pinned capacity is available.
- Default pinned capacity is `100`.
- Pinned symbols are not automatically removed by ranking.
- If pinned capacity is full, a queried symbol may attach temporarily but is not promoted.
- Redis stores only rebuildable UI read models, not durable facts.
- Kafka/Redpanda and local spool are the durable raw realtime fact path.
- Mammoth silver is historical baseline, CCASS, broker mapping, reference, and effective-day fallback data.

## Redis Read Model

Redis keys are scoped by requested trade date and symbol:

- `terminal:{date}:snapshot:{symbol}`: latest full terminal snapshot payload.
- `terminal:{date}:minute:{symbol}`: same-day minute K read model.
- `terminal:{date}:alerts:{symbol}`: filtered alert read model.
- `terminal:{date}:queue:{symbol}`: broker queue/display state.
- `terminal:{date}:state:{symbol}`: runtime state.

Ordinary tick callbacks are not stored as standalone Redis facts. They update in-process runtime state, current minute bars, snapshots, and alert lists. Redis can be rebuilt from in-process state, Kafka/spool, or silver baseline depending on availability.

## Same-Day Trading Behavior

For a trading day where same-day silver minute bars are not available yet:

- The runtime may use the previous effective silver day for baseline fields such as previous close, CCASS, broker mapping, and reference data.
- The first screen must not show previous-day minute bars as if they were today.
- Before returning a realtime trading-day snapshot, the runtime rolls the snapshot to the requested trade date and clears historical `minute_bars` and historical alerts.
- The snapshot is marked `LIVE` once realtime is attached.
- If the runtime attached after the market opened and cannot replay the missing interval, freshness records `intraday_gap_before_attach`.

This avoids the invalid chart sequence where yesterday's close-time bars, such as `15:54`, appear before today's realtime bars, such as `14:33`.

For a non-trading requested date:

- The runtime may display the latest effective trading day's minute bars.
- Freshness must preserve `requested_trade_date`, `effective_trade_date`, and `source_dates` so the UI and operators can see that the chart is historical/effective-day data.

## Symbol Display Names

Snapshots now resolve display names from `silver_instruments_v1` when available:

- `00700.HK` should display as `腾讯控股`.
- If no instrument name exists, the runtime falls back to the canonical symbol.

## Recent Backend Changes

The recent changes made to support the current behavior include:

- Added `ActiveSymbolPoolManager` and active/pinned/temporary symbol tracking.
- Added production Docker image and production entrypoint.
- Added xtquant production client with persistent datacenter connection and multi-symbol subscription.
- Added Redis/Redpanda production configuration and runtime verification.
- Changed xtquant callbacks to normalize and enqueue only.
- Added bounded ingest draining so WebSocket handling is not starved by callback or Kafka backlog.
- Set production Kafka polling to short polling through `KAFKA_POLL_TIMEOUT_MS=1`.
- Reduced per-tick raw record processing to keep subscriptions responsive.
- Added same-day rollover before cold-query snapshots are returned.
- Added `silver_instruments_v1` name lookup for snapshot display names.
- Added full-tick seeding after realtime attach so the first same-day snapshot can become live quickly.
- Added fast-path subscribe for active runtime snapshots.
- Prevented active runtime sessions from being detached by unrelated client disconnects.
- Added freshness handling so newly subscribed symbols do not immediately resubscribe before any event can arrive.
- Added health metrics for active pool, callbacks, Redis writes, runtime state, and subscriptions.

## Current Validation

Recent validation after the fixes:

- Backend tests: `103 passed, 7 subtests passed`.
- `git diff -- market-terminal/src` is empty.
- `00700.HK` Redis snapshot validates as:
  - `name=腾讯控股`
  - `tradeDate=20260527`
  - `isHistoricalSession=false`
  - `runtime_state=LIVE`
  - minute bars are same-day bars only.
- Local WebSocket subscription for `00700.HK` returned a snapshot in about `0.7s`.
- External WebSocket through the Cloudflare quick tunnel returned a snapshot in about `5.6s`.

## Current Limitations

- Cloudflare quick tunnel is account-less and not guaranteed stable. It has caused external WebSocket timeouts and stale Vercel bundle URLs. A named/stable tunnel or another stable backend host is required for production-grade external access.
- Vercel bundles `VITE_MARKET_WS_URL` at build time. When a quick tunnel URL changes, Vercel must be redeployed with the new WebSocket URL.
- The backend currently runs on the local workstation with host networking because xtquant and local market data paths are workstation-specific.
- If the runtime starts after market open and cannot replay raw data from the open to attach time, charts begin at attach time and mark `intraday_gap_before_attach`.
- External subscribe latency is dominated by the Vercel-to-quick-tunnel path; local backend subscribe is substantially faster.

## Operational Notes

For the current local production shape:

```bash
docker ps --format '{{.Names}} {{.Status}}' | grep -E 'thousand-backend-20260527|thousand-redis'
docker logs --tail 80 thousand-backend-20260527
docker exec thousand-redis redis-cli GET terminal:20260527:snapshot:00700.HK
```

Frontend deployment uses Vercel build-time environment variables:

```bash
VITE_MARKET_DATA_MODE=live
VITE_MARKET_PROTOCOL=terminal-message-v1
VITE_MARKET_WS_URL=wss://<current-tunnel-host>:443/ws
VITE_MARKET_SYMBOLS=00700.HK,00939.HK,00005.HK,00108.HK,02643.HK
```

## Related Documents

- `docs/container-production-runbook.md`: current local backend + Vercel + Cloudflare tunnel runbook.
- `docs/terminal-runtime-architecture-guidance.md`: architecture rules, including trading-day vs non-trading-day effective-date display.
- `BEAST_MARKET_TERMINAL_BACKEND_UPGRADE_PLAN.md`: active roadmap and completed implementation notes.
- `docs/backend-production-runtime-execution-plan.md`: production runtime checklist and validation discipline.
