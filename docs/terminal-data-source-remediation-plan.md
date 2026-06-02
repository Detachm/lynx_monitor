# Terminal Data Source Remediation Plan

Status: implementation plan and guardrail, created 2026-06-02.

This plan fixes data-source boundary mixing in three terminal paths: minute bars, big-trade alerts, and broker queue/depth display.

## Summary

The terminal must keep ordinary realtime price ticks, canonical trade ticks, broker queues, and depth/order-book updates as separate fact streams. UI refreshes may be coalesced and rate-limited, but fact streams used for alerts and recovery must not be randomly sampled or inferred from ambiguous payloads.

## Minute Bars And Ordinary Ticks

- First-screen and historical chart bars come from Redis cache first, then native `1m` history when cache is unavailable.
- Ordinary `hktransaction` ticks update only the latest price and latest forming-tick state for the symbol.
- Ordinary ticks must not rewrite confirmed historical minute bars.
- Ordinary ticks must not generate big-trade alerts.
- Native confirmed `1m` bars replace the same minute's forming state; they are not added to ordinary-tick aggregates.
- High-frequency ordinary tick handling uses per-symbol coalescing:
  - tick ingestion updates in-memory state only;
  - frontend tick/latest-bar refreshes flush at about 500-1000 ms or 1-2 Hz per symbol;
  - minute rollover, confirmed `1m` arrival, and snapshot requests force a flush.

## Big Trades

Big-trade alerts are generated only from canonical trade ticks.

A canonical trade tick must represent one trade print and include:

- `source_kind: "canonical_trade_tick"`;
- `source_table: "trade_ticks"` or equivalent source marker;
- `tick_ts`, `price`, single-trade `volume`, `turnover`, and non-neutral `side`;
- at least one provenance key: `trade_id`, `row_hash`, or `source_event_id`.

Pipeline rule:

1. Raw `hktransaction` or silver rows enter a trade-tick normalizer.
2. Only canonical trade ticks enter big-trade filtering.
3. Matching alerts are written to Redis/runtime state with canonical provenance.
4. The frontend displays alerts only as canonical trade-tick derived business facts.

The following records must not create alerts:

- `side=neutral` without a reliable direction source;
- missing participant/broker details when there is no `trade_id`, `row_hash`, or `source_event_id`;
- ambiguous `Volume` semantics that may be cumulative, snapshot, or close-summary volume;
- stale Redis/runtime-state alerts lacking canonical provenance.

Startup recovery must clean same-day non-canonical alerts, replay canonical runtime-state trade ticks first, then backfill from silver `trade_ticks`. If no trade-tick source is available, health reports `trade_tick_source_available=false` and the UI must show no big-trade alerts rather than falling back to ordinary tick guesses.

## Broker Queue And Depth

- Broker-level queue `volume` is nullable.
- Missing values, empty strings, `0`, and SDK placeholder zero values are treated as unknown and displayed as `--`.
- Broker-level volume is separate from price-level depth volume:
  - broker queue sources provide broker identity, price, and queue position;
  - depth/order-book sources provide total size at a price level;
  - price-level total volume must not be copied, averaged, or injected as each broker's volume.
- Default queue display shows 10 levels.
- 100/1000 level controls are enabled only when real depth for that level count has been received.
- Empty broker queues and all-zero depth callbacks must not overwrite the last valid queue/depth state.
- Startup recovery cleans old Redis snapshots by converting broker volume `0` and depth total `0` placeholders to unknown.

## Internal Contract Additions

- Big-trade alerts carry `source_kind`, `source_table`, `source_event_id`, and optional `trade_id`/`row_hash`.
- Broker queue entries use `volume: number | null` and `volume_unknown: boolean`.
- Depth-enriched queue entries use `levelVolume: number | null` and `depthAvailable: boolean`.
- Health distinguishes:
  - `trade_tick_source_available`;
  - `trade_tick_replay_count`;
  - `alert_count_by_symbol`;
  - `broker_queue_last_valid_ts`;
  - `depth_last_valid_ts`.

## Test Coverage

- Ordinary `hktransaction` does not mutate confirmed minute bars and does not create alerts.
- Canonical trade ticks create alerts only when thresholds are met and provenance exists.
- Neutral or provenance-less large prints are rejected.
- Startup recovery removes non-canonical alerts and backfills only canonical trade ticks.
- Missing broker volumes normalize to `null`.
- Empty queues and all-zero depth do not overwrite the last valid state.
- Queue depth controls enable 100/1000 only after real depth is available.
