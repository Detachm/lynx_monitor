# Market Terminal Runtime Architecture Guidance

Status: authoritative target architecture as of 2026-05-26.

This document is the development source of truth for the market terminal runtime. No older roadmap or target-architecture Markdown should be used as development input. If a future document conflicts with this file, either update it to reference this architecture or remove it from the active documentation set.

## 1. Product Target

The terminal must serve multiple traders on the LAN. Each trader can open a different watchlist and query symbols that were not preconfigured before process start.

The backend must therefore support:

- real market data only in live mode;
- closed-market behavior that shows the latest effective trading day instead of an empty screen;
- dynamic symbol hydration on first query;
- reuse of hot symbol state across clients;
- high fanout without duplicate upstream reads;
- predictable recovery after process restart or Redis cache clear;
- explicit degraded states when a source is unavailable.

The first production scope remains an intraday monitoring terminal. It is not a long-range research replay system.

## 2. First Principles

1. A symbol is the unit of state ownership.
2. User subscription drives symbol lifecycle.
3. Redis is a read model, not the source of truth.
4. Kafka stores durable market facts, not every UI frame by default.
5. Mammoth silver is the historical fact source.
6. xtquant realtime callbacks must be enqueue-only and non-blocking.
7. Minute bars for charts come from native 1m historical bars plus realtime deltas, not from full tick replay as the normal path.
8. Tick replay is a recovery fallback for alert reconstruction, not the main charting path.
9. One cold symbol request must trigger at most one upstream hydration job.
10. Every boundary must expose freshness, lag, and degraded reasons.

## 3. Data Classes

| Class | Examples | Load strategy | Cache strategy |
| --- | --- | --- | --- |
| Global reference | trading calendar, broker mapping, market session config | load once at process start, refresh by version/date | shared Redis/process cache |
| Per-symbol historical baseline | last 2 daily bars, latest/previous CCASS, 1m bars for effective day | hydrate on watchlist warmup or first query | Redis read model with source dates |
| Realtime facts | raw ticks, broker queue, L2 order book | xtquant subscription when symbol is active | Kafka raw topics keyed by symbol |
| Derived terminal state | snapshot, minute bars, alerts, queue display, holding display | owned by `SymbolRuntime` | Redis read model + Gateway deltas |
| Audit and diagnostics | raw JSONL, processed JSONL, runtime health | append-only, scoped by trade date/symbol | local files or object storage, not hot path |

## 4. Target Component Model

```text
Mammoth silver / xtquant history
          |
          v
Historical Hydration Service
          |
          v
SymbolRuntimeManager <---- Gateway subscriptions
          |
          v
SymbolRuntime(symbol)
  - lifecycle
  - hot state
  - freshness
  - Redis read model writer
  - realtime attach/detach
          ^
          |
Kafka raw facts <---- xtquant callback -> bounded queue -> ingest worker
          |
          v
WebSocket Gateway -> TerminalMessage v1 -> frontend
```

The runtime manager owns the mapping from symbol to active `SymbolRuntime`. The gateway does not compute business state and does not directly hydrate historical data. It asks the runtime manager for a symbol snapshot and attaches clients to the symbol stream.

## 5. Symbol Lifecycle

Each symbol must move through explicit states:

| State | Meaning |
| --- | --- |
| `COLD` | No active runtime and no fresh in-process state. |
| `HYDRATING` | One singleflight job is loading historical baseline and Redis snapshot. |
| `WARM` | Snapshot is available, but realtime is not attached or market is closed. |
| `LIVE` | Realtime subscription is active and deltas are flowing. |
| `DEGRADED` | Snapshot may be usable, but one or more required streams/sources are stale or unavailable. |
| `EVICTING` | No clients remain; runtime is flushing state and releasing realtime subscription after a grace period. |

Subscription flow:

1. Client sends `subscribe(symbol)`.
2. Gateway validates and normalizes symbol.
3. Runtime manager increments symbol ref-count.
4. If symbol is `COLD`, runtime manager starts one hydration job.
5. Hydration reads Redis first; if missing or stale, it loads Mammoth silver / xtquant history.
6. Gateway sends the first complete snapshot.
7. If market is open and symbol is eligible, runtime attaches realtime subscription.
8. Gateway fans out deltas to every subscribed client.

Unsubscribe flow:

1. Client sends `unsubscribe(symbol)` or disconnects.
2. Runtime manager decrements ref-count.
3. If ref-count reaches zero, symbol enters `EVICTING`.
4. After a grace period, realtime is detached.
5. Redis read model remains until TTL; in-process state may be evicted.

## 6. Historical Hydration

Hydration must be per symbol and per data type, with singleflight protection:

- `daily_bars`: latest two effective trading days for previous close and volume baseline.
- `minute_bars_1m`: effective day 1m bars for chart initialization.
- `ccass_holdings`: current latest available date `<= trade_date` and previous effective CCASS date.
- `broker_mapping`: global shared mapping, loaded once and versioned.
- `broker_queue`: latest available queue snapshot when realtime is unavailable.
- `trade_ticks`: optional fallback for alert reconstruction and audit only.

For closed-market days, effective trade date is the latest market trading day with available data. Payloads must include requested date, effective date, and source dates so the frontend can show evidence instead of pretending data is realtime.

## 7. Minute Bars and Big Trades

Minute K chart source of truth:

1. Use xtquant/Mammoth native 1m bars to initialize the day.
2. During live market, merge realtime tick deltas into the current minute only.
3. On restart, reload 1m bars and only replay the missing short tick window if needed.

Big trade definition:

- A big trade is based on volume relative to the previous effective trading day's total volume.
- Default rule: `single_trade_volume >= previous_day_total_volume * ratio`.
- The ratio is runtime config, not a hardcoded turnover threshold.
- Participant display for both big-trade alerts and broker queue rows must be one of: actual participant, `集合竞价`, `未披露`.
- Broker queue rows must never surface placeholder values such as blank strings, `--`, or synthetic `Broker ...` names in the participant column; unmapped or undisclosed rows display `未披露`.

## 8. Kafka Boundaries

Kafka is for durable facts and replayable pipelines.

Required raw topics:

- `raw_market_events_v1`: tick, broker queue, L2 order book.

Rules:

- Kafka key is canonical symbol.
- All symbol streams are partitioned by key.
- Consumers reject key/symbol mismatch.
- Offsets are committed only after successful processing or explicit quarantine.
- Bad records go to a persistent DLQ/quarantine path.
- Producer must use ACK, retries, delivery callback, and local spool when Kafka is unavailable.

Derived processed topics are optional production infrastructure:

- `processed_market_events_v1` can be kept for shadow run, audit, or downstream consumers.
- It must not be treated as the only way Gateway can see state.
- UI read state is Redis + in-process `SymbolRuntime`; Gateway may fan out directly from runtime deltas.

## 9. Redis Read Model

Redis stores the latest terminal read model and fast first-screen data. It is rebuildable.

Recommended key families:

| Key | Content |
| --- | --- |
| `terminal:{date}:snapshot:{symbol}` | full first-screen snapshot |
| `terminal:{date}:minute:{symbol}` | effective day 1m bars, bounded |
| `terminal:{date}:alerts:{symbol}` | latest alert display window |
| `terminal:{date}:queue:{symbol}` | latest broker queue / L2 display state |
| `terminal:{date}:state:{symbol}` | lifecycle, freshness, source dates, version |
| `ccass:holding:{symbol}` | current/previous CCASS holdings and change evidence |
| `ref:broker_mapping:{version}` | broker to participant mapping |
| `ref:trading_calendar:{market}:{version}` | global calendar |

Every record must carry:

- `schema_version`;
- `symbol` where applicable;
- `requested_trade_date`;
- `effective_trade_date`;
- `source_dates`;
- `updated_at`;
- `version` or `last_event_id`;
- `freshness`;
- `degraded_reasons`.

Redis writes should be pipelined/atomic per symbol update. Cache clear commands must stay scoped by date and symbol, and clearing Redis must not delete Mammoth silver, Kafka topics, shadow evidence, or local audit files.

## 10. Gateway and Frontend Semantics

Gateway responsibilities:

- validate requests;
- attach/detach client subscriptions;
- request snapshots from `SymbolRuntimeManager`;
- fan out snapshot and deltas;
- isolate slow clients with bounded queues;
- never block ingestion or symbol processing.

Frontend responsibilities:

- maintain dynamic symbol state, not only a fixed startup list;
- subscribe when a symbol enters watchlist or visible workspace;
- unsubscribe when no longer needed;
- display `loading`, `warm`, `live`, `closed`, and `degraded` states;
- treat `TerminalMessage v1` as the only live/mock input boundary.

## 11. Concurrency and Backpressure

Concurrency controls required before multi-trader rollout:

- singleflight hydration keyed by `symbol + data_type + effective_date`;
- per-symbol runtime mailbox or queue;
- bounded xtquant callback queue;
- bounded Gateway client queues;
- rate-limited cold symbol hydration;
- warm pool for watchlist/hot symbols;
- grace-period eviction to avoid subscription flapping;
- metrics for queue depth, dropped noncritical deltas, alert preservation, hydration latency, and Redis write latency.

Concurrency limiting is not because broker mapping or calendar are per symbol. They are global shared data. Limiting is needed because cold symbol hydration, xtquant history calls, realtime subscriptions, Redis writes, and WebSocket fanout can stampede when many traders query many different symbols at once.

## 12. Recovery Model

Recovery priority:

1. Read Redis snapshot if fresh.
2. Load native 1m bars and current/previous CCASS from historical store.
3. Load latest broker queue snapshot.
4. Replay only the missing short tick window needed for alert continuity.
5. Attach realtime subscription if market is open.
6. Emit health evidence with recovered source dates and gaps.

Local JSONL is an audit and debugging artifact. It may help recovery, but it is not the primary hot recovery mechanism.

## 13. Development Guardrails

New code must not:

- add business reads directly from bronze or temporary CSV;
- add chart initialization by full-day tick aggregation as the main path;
- put historical reads in xtquant callback or Kafka consumer hot path;
- make Gateway compute terminal state;
- hydrate the same cold symbol concurrently for multiple clients;
- treat Redis as unrecoverable source of truth;
- create unbounded queues to avoid backpressure decisions;
- hide missing realtime data by showing stale data without source dates and freshness.

## 14. Acceptance Criteria

A production-ready slice must prove:

- two traders can subscribe overlapping and different symbols without duplicate upstream hydration;
- a cold symbol can be queried, hydrated, shown, and then added to watchlist;
- closed-market startup shows the latest effective trading day with evidence;
- Redis clear for one symbol rebuilds that symbol without clearing global state;
- process restart returns first-screen state from Redis or historical hydration before live attach;
- native 1m bars initialize the chart;
- tick replay is bounded and used only for alert/recovery gaps;
- Kafka/Redis/xtquant failures put affected symbols into `DEGRADED` without blocking unrelated symbols;
- backend and frontend tests cover dynamic subscribe/unsubscribe and symbol lifecycle.
