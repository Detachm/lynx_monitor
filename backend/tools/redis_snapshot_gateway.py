from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Any

import redis
import websockets


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_symbol(raw: str) -> str:
    value = raw.strip().upper()
    if not value:
        raise ValueError("missing symbol")
    if value.endswith(".HK"):
        code = value[:-3]
    else:
        code = value
    if not code.isdigit():
        raise ValueError(f"invalid symbol: {raw}")
    return f"{int(code):05d}.HK"


class RedisSnapshotGateway:
    def __init__(self, redis_url: str, trade_date: str | None = None) -> None:
        self.redis = redis.Redis.from_url(redis_url, decode_responses=True)
        self.trade_date = trade_date
        self.seq = 0

    async def handler(self, websocket: websockets.WebSocketServerProtocol) -> None:
        await websocket.send(json.dumps(self.health_frame(), separators=(",", ":")))
        async for raw_message in websocket:
            response = self.handle_message(raw_message)
            if response is not None:
                await websocket.send(json.dumps(response, ensure_ascii=False, separators=(",", ":")))

    def handle_message(self, raw_message: str) -> dict[str, Any] | None:
        try:
            request = json.loads(raw_message)
            if not isinstance(request, dict):
                return self.error_frame("invalid request")
            payload = request.get("payload")
            payload = payload if isinstance(payload, dict) else {}
            action = str(request.get("action") or payload.get("action") or "").strip()
            raw_symbol = str(request.get("symbol") or payload.get("symbol") or "").strip()
            if action == "subscribe":
                symbol = normalize_symbol(raw_symbol)
                return self.snapshot_frame(symbol)
            if action == "holding_name_click":
                symbol = normalize_symbol(raw_symbol)
                participant_name = str(
                    request.get("participant_name") or payload.get("participant_name") or ""
                ).strip()
                days = int(request.get("days") or payload.get("days") or 30)
                return self.holding_history_frame(symbol, participant_name, days)
            if action == "unsubscribe":
                return None
            return self.error_frame(f"unsupported action: {action or 'missing'}")
        except Exception as exc:
            return self.error_frame(str(exc))

    def snapshot_frame(self, symbol: str) -> dict[str, Any]:
        key = self.snapshot_key(symbol)
        if not key:
            return self.error_frame(f"snapshot not found: {symbol}")
        raw = self.redis.get(key)
        if not raw:
            return self.error_frame(f"snapshot not found: {symbol}")
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            return self.error_frame(f"invalid snapshot payload: {symbol}")

        trade_date = key.split(":")[1]
        payload.setdefault("symbol", symbol)
        payload.setdefault("schema_version", 1)
        payload.setdefault("snapshot", {})
        payload.setdefault("minute_bars", [])
        payload.setdefault("alerts", [])
        payload.setdefault("broker_queue", {"ask": [], "bid": []})
        payload.setdefault("ccass_holdings", [])
        payload.setdefault("freshness", {})
        freshness = payload["freshness"] if isinstance(payload["freshness"], dict) else {}
        payload["freshness"] = freshness
        freshness.setdefault("requested_trade_date", trade_date)
        freshness.setdefault("effective_trade_date", trade_date)
        freshness.setdefault("runtime_state", "LIVE")
        freshness.setdefault("source_dates", {"minute_bars": trade_date, "realtime": trade_date})
        freshness.setdefault("degraded_reasons", [])
        freshness.setdefault("degraded", False)

        source_ts = (
            str(payload.get("updated_at") or "")
            or str(freshness.get("updated_at") or "")
            or str(payload.get("snapshot", {}).get("updatedAt") or "")
            or utc_now()
        )
        return self.terminal_frame("snapshot", symbol, source_ts, payload)

    def holding_history_frame(self, symbol: str, participant_name: str, days: int) -> dict[str, Any]:
        payload = {
            "participant_name": participant_name or "unknown",
            "days": max(1, min(days, 90)),
            "history": [],
            "freshness": {
                "runtime_state": "LIVE",
                "degraded": False,
                "degraded_reasons": [],
            },
        }
        return self.terminal_frame("holding_name_click_response", symbol, utc_now(), payload)

    def terminal_frame(self, event_type: str, symbol: str, source_ts: str, payload: dict[str, Any]) -> dict[str, Any]:
        self.seq += 1
        return {
            "schema_version": 1,
            "type": event_type,
            "event_id": f"redis-fallback-{event_type}-{symbol}-{self.seq}",
            "symbol": symbol,
            "source": "gateway",
            "source_ts": source_ts,
            "ingest_ts": utc_now(),
            "seq": self.seq,
            "payload": payload,
        }

    def snapshot_key(self, symbol: str) -> str | None:
        if self.trade_date:
            key = f"terminal:{self.trade_date}:snapshot:{symbol}"
            if self.redis.exists(key):
                return key
        keys: list[str] = []
        for key in self.redis.scan_iter(match=f"terminal:*:snapshot:{symbol}", count=1000):
            keys.append(str(key))
        return sorted(keys, reverse=True)[0] if keys else None

    def health_frame(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "type": "health",
            "source": "backend",
            "payload": {
                "process": "running",
                "kafka": "degraded",
                "redis": "connected",
                "kafka_lag": 0,
                "latest_event_at_by_symbol": {},
                "symbol_freshness": {},
            },
        }

    def error_frame(self, message: str) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "type": "error",
            "source": "gateway",
            "payload": {"message": message},
        }


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=os.getenv("HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.getenv("PORT", "9020")))
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0"))
    parser.add_argument("--trade-date", default=os.getenv("TRADE_DATE") or None)
    args = parser.parse_args()

    gateway = RedisSnapshotGateway(args.redis_url, trade_date=args.trade_date)
    async with websockets.serve(gateway.handler, args.host, args.port, max_size=16_000_000):
        print(f"redis snapshot gateway listening on {args.host}:{args.port}/ws", flush=True)
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
