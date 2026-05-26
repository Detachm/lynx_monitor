from __future__ import annotations

from datetime import datetime, timezone
import math
import re
from typing import Any
from uuid import uuid4

SCHEMA_VERSION = 1
RAW_TOPIC = "raw_market_events_v1"
PROCESSED_TOPIC = "processed_market_events_v1"
TERMINAL_MESSAGE_PROTOCOL = "terminal-message-v1"
CANONICAL_SYMBOL_PATTERN = re.compile(r"^\d{5}\.HK$")
REDIS_RUNTIME_SNAPSHOT_KEY_FAMILIES = (
    "terminal_snapshot",
    "terminal_minute",
    "terminal_alerts",
    "terminal_queue",
    "terminal_state",
    "ccass_holding",
    "ccass_history",
)
REDIS_RUNTIME_SNAPSHOT_KEY_TEMPLATES = {
    "terminal_snapshot": "terminal:{trade_date}:snapshot:{symbol}",
    "terminal_minute": "terminal:{trade_date}:minute:{symbol}",
    "terminal_alerts": "terminal:{trade_date}:alerts:{symbol}",
    "terminal_queue": "terminal:{trade_date}:queue:{symbol}",
    "terminal_state": "terminal:{trade_date}:state:{symbol}",
    "ccass_holding": "ccass:holding:{symbol}",
    "ccass_history": "ccass:history:{symbol}:{participant_id}",
}

TERMINAL_MESSAGE_TYPES = {
    "snapshot",
    "tick_realtime",
    "alert_realtime",
    "queue_realtime",
    "holding_name_click_response",
}

RAW_EVENT_KINDS = {
    "tick",
    "broker_queue",
    "l2_order_book",
}

PROCESSED_EVENT_RESULT_TYPES = {
    "snapshot",
    "big_trade_alert",
    "broker_queue",
    "l2_order_book",
}


class ContractError(ValueError):
    """Raised when a v1 event violates the frozen wire contract."""


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def make_raw_market_event(
    *,
    kind: str,
    symbol: str,
    source: str,
    payload: dict[str, Any],
    seq: int,
    source_ts: str | None = None,
    ingest_ts: str | None = None,
    period: str | None = None,
    event_id: str | None = None,
) -> dict[str, Any]:
    event = {
        "schema_version": SCHEMA_VERSION,
        "kind": kind,
        "event_id": event_id or f"raw-{source}-{symbol}-{seq}-{uuid4().hex[:8]}",
        "symbol": symbol,
        "source": source,
        "source_ts": source_ts or now_iso(),
        "ingest_ts": ingest_ts or now_iso(),
        "seq": seq,
        "payload": payload,
    }
    if period:
        event["period"] = period
    validate_raw_market_event(event)
    return event


def make_processed_market_event(
    *,
    result_type: str,
    symbol: str,
    source: str,
    payload: dict[str, Any],
    seq: int,
    source_ts: str | None = None,
    ingest_ts: str | None = None,
    period: str | None = None,
    event_id: str | None = None,
) -> dict[str, Any]:
    event = {
        "schema_version": SCHEMA_VERSION,
        "result_type": result_type,
        "event_id": event_id or f"processed-{source}-{symbol}-{seq}-{uuid4().hex[:8]}",
        "symbol": symbol,
        "source": source,
        "source_ts": source_ts or now_iso(),
        "ingest_ts": ingest_ts or now_iso(),
        "seq": seq,
        "payload": payload,
    }
    if period:
        event["period"] = period
    validate_processed_market_event(event)
    return event


def make_terminal_message(
    *,
    message_type: str,
    symbol: str,
    source: str,
    payload: dict[str, Any],
    seq: int,
    source_ts: str | None = None,
    ingest_ts: str | None = None,
    event_id: str | None = None,
) -> dict[str, Any]:
    if message_type not in TERMINAL_MESSAGE_TYPES:
        raise ContractError(f"unsupported TerminalMessage type: {message_type}")
    message = {
        "schema_version": SCHEMA_VERSION,
        "type": message_type,
        "event_id": event_id or f"{message_type}-{symbol}-{seq}-{uuid4().hex[:8]}",
        "symbol": symbol,
        "source": source,
        "source_ts": source_ts or now_iso(),
        "ingest_ts": ingest_ts or now_iso(),
        "seq": seq,
        "payload": payload,
    }
    validate_terminal_message(message)
    return message


def validate_raw_market_event(event: dict[str, Any]) -> None:
    validate_event_envelope(event, extra_required=("kind",))
    kind = event["kind"]
    if kind not in RAW_EVENT_KINDS:
        raise ContractError(f"unsupported RawMarketEvent kind: {kind}")

    payload = event["payload"]
    if kind == "tick":
        require_payload_keys(payload, "price", "volume", "turnover")
        if not is_finite_number(payload["price"]) or payload["price"] <= 0:
            raise ContractError("tick payload price must be a positive number")
        if not is_finite_number(payload["volume"]) or payload["volume"] < 0:
            raise ContractError("tick payload volume must be a non-negative number")
        if not is_finite_number(payload["turnover"]) or payload["turnover"] < 0:
            raise ContractError("tick payload turnover must be a non-negative number")
    elif kind == "broker_queue":
        require_payload_keys(payload, "entries")
        if not isinstance(payload["entries"], list):
            raise ContractError("broker_queue payload entries must be an array")
        if any(not isinstance(entry, dict) for entry in payload["entries"]):
            raise ContractError("broker_queue payload entries must contain objects")
    elif kind == "l2_order_book":
        require_payload_keys(payload, "ask", "bid")
        if not isinstance(payload["ask"], list) or not isinstance(payload["bid"], list):
            raise ContractError("l2_order_book payload ask and bid must be arrays")
        if any(not isinstance(entry, dict) for entry in (*payload["ask"], *payload["bid"])):
            raise ContractError("l2_order_book payload ask and bid entries must contain objects")


def validate_processed_market_event(event: dict[str, Any]) -> None:
    validate_event_envelope(event, extra_required=("result_type",))
    result_type = event["result_type"]
    if result_type not in PROCESSED_EVENT_RESULT_TYPES:
        raise ContractError(f"unsupported ProcessedMarketEvent result_type: {result_type}")

    payload = event["payload"]
    if result_type in {"snapshot", "l2_order_book"}:
        require_snapshot_payload(payload)
    elif result_type == "big_trade_alert":
        require_payload_keys(payload, "alert")
        if not isinstance(payload["alert"], dict):
            raise ContractError("big_trade_alert payload alert must be an object")
    elif result_type == "broker_queue":
        require_payload_keys(payload, "broker_queue")
        if not isinstance(payload["broker_queue"], dict):
            raise ContractError("broker_queue payload broker_queue must be an object")


def validate_terminal_message(message: dict[str, Any]) -> None:
    validate_event_envelope(message, extra_required=("type",))
    message_type = message["type"]
    if message_type not in TERMINAL_MESSAGE_TYPES:
        raise ContractError(f"unsupported TerminalMessage type: {message_type}")

    payload = message["payload"]
    if message_type == "snapshot":
        require_snapshot_payload(payload)
    elif message_type == "tick_realtime":
        require_payload_keys(payload, "tick")
        if not isinstance(payload["tick"], dict):
            raise ContractError("tick_realtime payload tick must be an object")
    elif message_type == "alert_realtime":
        require_payload_keys(payload, "alert")
        if not isinstance(payload["alert"], dict):
            raise ContractError("alert_realtime payload alert must be an object")
    elif message_type == "queue_realtime":
        require_payload_keys(payload, "broker_queue")
        if not isinstance(payload["broker_queue"], dict):
            raise ContractError("queue_realtime payload broker_queue must be an object")
    elif message_type == "holding_name_click_response":
        require_payload_keys(payload, "participant_name", "days", "history")
        if not isinstance(payload["participant_name"], str) or not payload["participant_name"].strip():
            raise ContractError("holding_name_click_response participant_name must be a non-empty string")
        if not isinstance(payload["days"], int) or isinstance(payload["days"], bool) or payload["days"] < 1:
            raise ContractError("holding_name_click_response days must be a positive integer")
        if not isinstance(payload["history"], list):
            raise ContractError("holding_name_click_response history must be an array")


def validate_event_envelope(message: dict[str, Any], *, extra_required: tuple[str, ...] = ()) -> None:
    required = (
        "schema_version",
        "event_id",
        "symbol",
        "source",
        "source_ts",
        "ingest_ts",
        "seq",
        "payload",
        *extra_required,
    )
    missing = [key for key in required if key not in message]
    if missing:
        raise ContractError(f"missing required envelope fields: {', '.join(missing)}")
    if message["schema_version"] != SCHEMA_VERSION:
        raise ContractError(f"unsupported schema_version: {message['schema_version']}")
    if not isinstance(message["payload"], dict):
        raise ContractError("payload must be an object")
    if not isinstance(message["seq"], int) or isinstance(message["seq"], bool) or message["seq"] < 1:
        raise ContractError("seq must be a positive integer")
    for key in ("event_id", "source"):
        if not isinstance(message[key], str) or not message[key].strip():
            raise ContractError(f"{key} must be a non-empty string")
    if not isinstance(message["symbol"], str) or not CANONICAL_SYMBOL_PATTERN.fullmatch(message["symbol"]):
        raise ContractError("symbol must use canonical format 00700.HK")
    for key in ("source_ts", "ingest_ts"):
        if not is_iso_datetime(message[key]):
            raise ContractError(f"{key} must be an ISO-8601 datetime string")


def is_iso_datetime(value: Any) -> bool:
    if not isinstance(value, str) or "T" not in value:
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return True


def is_finite_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def require_payload_keys(payload: dict[str, Any], *keys: str) -> None:
    missing = [key for key in keys if key not in payload]
    if missing:
        raise ContractError(f"missing required payload fields: {', '.join(missing)}")


def require_snapshot_payload(payload: dict[str, Any]) -> None:
    require_payload_keys(
        payload,
        "snapshot",
        "minute_bars",
        "alerts",
        "broker_queue",
        "ccass_holdings",
        "freshness",
    )
    if not isinstance(payload["snapshot"], dict):
        raise ContractError("snapshot payload snapshot must be an object")
    if not isinstance(payload["minute_bars"], list):
        raise ContractError("snapshot payload minute_bars must be an array")
    if not isinstance(payload["alerts"], list):
        raise ContractError("snapshot payload alerts must be an array")
    if not isinstance(payload["ccass_holdings"], list):
        raise ContractError("snapshot payload ccass_holdings must be an array")
    if not isinstance(payload["freshness"], dict):
        raise ContractError("snapshot payload freshness must be an object")
    broker_queue = payload["broker_queue"]
    if not isinstance(broker_queue, dict) or "ask" not in broker_queue or "bid" not in broker_queue:
        raise ContractError("snapshot payload broker_queue must contain ask and bid")
    if not isinstance(broker_queue["ask"], list) or not isinstance(broker_queue["bid"], list):
        raise ContractError("snapshot payload broker_queue ask and bid must be arrays")
