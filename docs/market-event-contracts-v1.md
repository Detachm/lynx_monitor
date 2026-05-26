# Beast Market Terminal Contracts v1

Status: frozen for the current symbol-runtime baseline defined by `../BEAST_MARKET_TERMINAL_BACKEND_UPGRADE_PLAN.md`. Architecture ownership and runtime lifecycle are defined in `terminal-runtime-architecture-guidance.md`.

All backend-to-frontend and backend-internal market events use an envelope plus a typed payload. Field names are snake_case on the wire. Frontend projections may use camelCase only inside application state after normalization.

## Envelope

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `schema_version` | integer | yes | Must be `1`. |
| `event_id` | string | yes | Globally unique and stable for retries. |
| `symbol` | string | yes | Canonical stock code matching `00000.HK`, for example `00700.HK`. |
| `source` | string | yes | Producer name, for example `xtquant`, `mammoth`, `octopus`, `gateway`. |
| `source_ts` | ISO-8601 string | yes | Event time from the source system. |
| `ingest_ts` | ISO-8601 string | yes | Time the local process accepted the event. |
| `seq` | positive integer | yes | Monotonic per `symbol` stream; booleans, strings, zero, and fractions are invalid. |
| `payload` | object | yes | Typed business payload. |

Time values in v1 are ISO-8601 datetime strings with `T` separators and timezone offsets. Dates inside historical payloads use `YYYYMMDD`.

## RawMarketEvent v1

Topic: `raw_market_events_v1`.

Additional fields:

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `kind` | string | yes | One of `tick`, `broker_queue`, `l2_order_book`. |
| `period` | string | no | Required for period data such as `1m`, `1d`. |

Kafka key: canonical `symbol`; raw consumers reject records whose key is not canonical or does not exactly match the event `symbol`.

Raw payloads keep source-specific detail, but must include normalized identifiers needed downstream. `tick` payloads require finite numeric `price`, `volume`, and `turnover`; `price` must be positive, while `volume` and `turnover` must be non-negative. `broker_queue.entries` and `l2_order_book.ask`/`bid` must be arrays of objects. No source callback may silently drop a raw event; rejected events go to metrics and a dead-letter/spool path.

## ProcessedMarketEvent v1

Topic: `processed_market_events_v1`.

Additional fields:

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `result_type` | string | yes | One of `snapshot`, `big_trade_alert`, `broker_queue`, `l2_order_book`. |
| `period` | string | no | Required when the result is period based. |

Kafka key: canonical `symbol`; processed consumers reject records whose key is not canonical or does not exactly match the event `symbol`.

Processed payloads are computed business results from Octopus and may be written both to Kafka and Redis terminal snapshot keys.
`snapshot` and `l2_order_book` payloads use the snapshot payload shape; `big_trade_alert.alert` and `broker_queue.broker_queue` must be objects.

Architecture note: `processed_market_events_v1` is a valid contract for shadow runs, audit, and downstream integrations. The target terminal runtime does not require Gateway to depend on this topic as the only path to state; Gateway may receive deltas directly from the owning `SymbolRuntime` while Redis remains the first-screen read model.

## TerminalMessage v1

Transport: WebSocket `/ws`.

Additional fields:

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `type` | string | yes | Exactly one of the five frontend message types below. |

Allowed `type` values:

- `snapshot`
- `tick_realtime`
- `alert_realtime`
- `queue_realtime`
- `holding_name_click_response`

The frontend live normalizer treats `TerminalMessage v1` as canonical input. At mock/live
data-source boundaries it rejects non-canonical symbols, blank `event_id` or `source`, non-ISO
`source_ts`/`ingest_ts`, non-positive or non-integer `seq`, and non-object payloads. Legacy top-level
fields are transitional only below that boundary.

### `snapshot` Payload

| Field | Type | Required |
| --- | --- | --- |
| `snapshot` | object | yes |
| `minute_bars` | array | yes |
| `alerts` | array | yes |
| `broker_queue.ask` | array | yes |
| `broker_queue.bid` | array | yes |
| `ccass_holdings` | array | yes |
| `freshness` | object | yes |

### Realtime Payloads

| Type | Payload fields |
| --- | --- |
| `tick_realtime` | object `tick`, optional object `snapshot`, optional `freshness` |
| `alert_realtime` | object `alert`, optional `freshness` |
| `queue_realtime` | optional `side`, object `broker_queue` with `ask` and/or `bid`, optional `freshness` |
| `holding_name_click_response` | non-empty string `participant_name`, positive integer `days`, array `history`, optional `freshness` |

## Health Status v1

Gateway and data sources may emit a non-market health message:

```json
{
  "schema_version": 1,
  "type": "health",
  "source": "gateway",
  "payload": {
    "process": "running",
    "kafka": "connected",
    "redis": "connected",
    "kafka_lag": 0,
    "latest_event_at_by_symbol": {
      "00700.HK": "2026-05-22T09:30:00.020+08:00"
    }
  }
}
```

Minimum health dimensions for Phase 1 are process status, Kafka lag, Redis status, and latest event time per symbol.
