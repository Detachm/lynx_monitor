# Mammoth Silver Schema v1

Status: frozen for the current symbol-runtime baseline defined by `../BEAST_MARKET_TERMINAL_BACKEND_UPGRADE_PLAN.md` and `terminal-runtime-architecture-guidance.md`. `silver_minute_bars_v1` is an active table and the required chart initialization source.

Mammoth silver is the only historical data layer read by business services. Bronze and temporary CSV files are archival or staging inputs and are not business read APIs.

Common columns for every silver table:

| Column | Type | Required | Notes |
| --- | --- | --- | --- |
| `schema_version` | int32 | yes | Must be `1`. |
| `symbol` | string | yes | Normalized code, for example `00700.HK`. |
| `trade_date` | string | yes | `YYYYMMDD`. |
| `source` | string | yes | Historical source identifier. |
| `source_ts` | timestamp | no | Source event or file timestamp. |
| `ingest_ts` | timestamp | yes | Mammoth ingest timestamp. |
| `row_hash` | string | yes | Stable hash for de-duplication. |

## `silver_daily_bars_v1`

| Column | Type | Required |
| --- | --- | --- |
| `schema_version` | int32 | yes |
| `symbol` | string | yes |
| `trade_date` | string | yes |
| `open` | double | yes |
| `high` | double | yes |
| `low` | double | yes |
| `close` | double | yes |
| `volume` | int64 | yes |
| `turnover` | double | yes |
| `source` | string | yes |
| `ingest_ts` | timestamp | yes |
| `row_hash` | string | yes |

Primary key: `symbol`, `trade_date`.

## `silver_trade_ticks_v1`

| Column | Type | Required |
| --- | --- | --- |
| `schema_version` | int32 | yes |
| `symbol` | string | yes |
| `trade_date` | string | yes |
| `tick_ts` | timestamp | yes |
| `price` | double | yes |
| `volume` | int64 | yes |
| `turnover` | double | yes |
| `side` | string | yes |
| `trade_id` | string | no |
| `source` | string | yes |
| `ingest_ts` | timestamp | yes |
| `row_hash` | string | yes |

Primary ordering: `symbol`, `tick_ts`, `trade_id`.

Trade ticks are not the normal source for initializing terminal minute K charts. They are retained for audit, realtime alert reconstruction, and bounded recovery gaps. The runtime initializes charts from `silver_minute_bars_v1` and only applies realtime tick deltas to the active minute.

## `silver_minute_bars_v1`

Required active table for the symbol-runtime architecture.

| Column | Type | Required |
| --- | --- | --- |
| `schema_version` | int32 | yes |
| `symbol` | string | yes |
| `trade_date` | string | yes |
| `bar_ts` | timestamp | yes |
| `open` | double | yes |
| `high` | double | yes |
| `low` | double | yes |
| `close` | double | yes |
| `volume` | int64 | yes |
| `turnover` | double | yes |
| `source` | string | yes |
| `ingest_ts` | timestamp | yes |
| `row_hash` | string | yes |

Primary key: `symbol`, `bar_ts`.

The terminal hydrates effective-day charts from this table or an equivalent xtquant native 1m source. Full-day tick aggregation is a fallback only and must not be the production first-screen chart path.

## `silver_trading_calendar_v1`

Optional global reference table for the symbol-runtime architecture.

| Column | Type | Required |
| --- | --- | --- |
| `schema_version` | int32 | yes |
| `market` | string | yes |
| `trade_date` | string | yes |
| `is_trading_day` | boolean | yes |
| `source` | string | yes |
| `ingest_ts` | timestamp | yes |
| `row_hash` | string | yes |

Primary key: `market`, `trade_date`.

When present, runtime uses this table to distinguish a true closed-market fallback from a trading-day pre-open fallback. If `requested_trade_date` has no same-day 1m bars yet but `silver_trading_calendar_v1` marks it as an HK trading day, the runtime may display the latest effective day while still attaching realtime so the terminal can switch to same-day data after the open. If the calendar is missing, runtime keeps the conservative behavior and only attaches realtime when `effective_trade_date == requested_trade_date`.

## `silver_ccass_holdings_v1`

| Column | Type | Required |
| --- | --- | --- |
| `schema_version` | int32 | yes |
| `symbol` | string | yes |
| `trade_date` | string | yes |
| `participant_id` | string | yes |
| `participant_name` | string | yes |
| `shares` | int64 | yes |
| `percent` | double | yes |
| `change` | int64 | no |
| `is_highlighted` | boolean | no |
| `source` | string | yes |
| `ingest_ts` | timestamp | yes |
| `row_hash` | string | yes |

Primary key: `symbol`, `trade_date`, `participant_id`.

## `silver_broker_queue_v1`

| Column | Type | Required |
| --- | --- | --- |
| `schema_version` | int32 | yes |
| `symbol` | string | yes |
| `trade_date` | string | yes |
| `queue_ts` | timestamp | yes |
| `side` | string | yes |
| `position` | int32 | yes |
| `broker_code` | string | yes |
| `broker_name` | string | no |
| `participant_id` | string | no |
| `participant_name` | string | no |
| `price` | double | no |
| `volume` | int64 | no |
| `source` | string | yes |
| `ingest_ts` | timestamp | yes |
| `row_hash` | string | yes |

Primary ordering: `symbol`, `queue_ts`, `side`, `position`.

## `silver_broker_mapping_v1`

| Column | Type | Required |
| --- | --- | --- |
| `schema_version` | int32 | yes |
| `broker_code` | string | yes |
| `broker_name` | string | yes |
| `participant_id` | string | no |
| `participant_name` | string | no |
| `effective_from` | string | yes |
| `effective_to` | string | no |
| `source` | string | yes |
| `ingest_ts` | timestamp | yes |
| `row_hash` | string | yes |

Primary key: `broker_code`, `effective_from`.

## Manifest

Every historical job writes a manifest containing:

| Field | Type |
| --- | --- |
| `schema_version` | int32 |
| `data_type` | string |
| `source_data_type` | string |
| `date_range` | object with `start` and `end` |
| `symbols` | array of canonical `00000.HK` strings |
| `symbol_count` | int32 matching the duplicate-free symbol list |
| `row_count` | int64 |
| `failed_items` | array |
| `code_version` | string |
| `started_at` | timestamp |
| `finished_at` | timestamp |
| `quality_checks` | object |

Required quality checks: missing required columns, duplicate primary keys, non-canonical `00000.HK` symbol format, invalid date format, negative volume/share values, and empty output handling. The final manifest audit also requires each non-empty failure detail to map to the corresponding `failed_items` entry, and cross-checks `row_count` against `quality_checks.empty_output` so an empty silver result cannot be marked as a passing non-empty manifest.

Final cutover evidence for the current symbol-runtime baseline requires passing manifests for the business read types `daily_bars`, `minute_bars`, `trade_ticks`, `ccass_holdings`, `participant_history`, `broker_queue`, and `broker_mapping`. `trading_calendar` is optional but recommended for production pre-open correctness. Symbol-scoped manifests must use canonical duplicate-free symbol lists. `participant_history` is a logical manifest type backed by `silver_ccass_holdings_v1`, so its `source_data_type` is `ccass_holdings`.
