from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any, Protocol
from uuid import uuid4

from .contracts import TERMINAL_MESSAGE_TYPES, now_iso
from .gateway_transport import GatewayV2SessionManager


class WebSocketConnection(Protocol):
    def __aiter__(self) -> AsyncIterator[str]:
        ...

    async def send(self, message: str) -> None:
        ...


ServeFactory = Callable[[Callable[..., Awaitable[None]], str, int], Any]


class GatewayV2WebSocketService:
    """Async WebSocket adapter for GatewayV2SessionManager.

    The service keeps protocol behavior in GatewayV2SessionManager and only handles
    connection lifecycle, JSON send/receive, and broadcast flushing. It can be run
    with the `websockets` package or a compatible framework adapter.
    """

    def __init__(
        self,
        manager: GatewayV2SessionManager,
        *,
        host: str = "0.0.0.0",
        port: int = 9020,
        path: str = "/ws",
        shadow_recorder: Any | None = None,
        send_timeout_seconds: float = 2.0,
    ) -> None:
        if send_timeout_seconds <= 0:
            raise ValueError("send_timeout_seconds must be positive")
        self.manager = manager
        self.host = host
        self.port = port
        self.path = path
        self.shadow_recorder = shadow_recorder
        self.send_timeout_seconds = send_timeout_seconds
        self.clients: dict[str, WebSocketConnection] = {}
        self.failed_client_sends = 0
        self.terminal_messages_delivered = 0
        self.delivered_terminal_symbols: set[str] = set()
        self.last_terminal_message_delivered_at: str | None = None

    async def handle_client(
        self,
        websocket: WebSocketConnection,
        *,
        client_id: str | None = None,
        path: str | None = None,
    ) -> None:
        request_path = path or getattr(websocket, "path", self.path)
        if request_path != self.path:
            await websocket.send(gateway_error(f"unsupported websocket path: {request_path}"))
            return
        resolved_client_id = client_id or f"client-{uuid4().hex}"
        self.clients[resolved_client_id] = websocket
        self.manager.connect(resolved_client_id)
        try:
            if await self.flush_client(resolved_client_id) < 0:
                return
            async for raw_message in websocket:
                try:
                    await asyncio.to_thread(self.manager.handle_message, resolved_client_id, raw_message)
                    self._record_performance_samples()
                except Exception as error:
                    if not await self._send(resolved_client_id, gateway_error(str(error))):
                        return
                if await self.flush_client(resolved_client_id) < 0:
                    return
        finally:
            self.manager.disconnect(resolved_client_id)
            self.clients.pop(resolved_client_id, None)

    async def broadcast_once(self) -> int:
        self.manager.broadcast_processed()
        results = await asyncio.gather(
            *(self.flush_client(client_id) for client_id in list(self.clients)),
            return_exceptions=True,
        )
        return sum(result for result in results if isinstance(result, int) and result > 0)

    async def flush_client(self, client_id: str) -> int:
        websocket = self.clients.get(client_id)
        if websocket is None:
            return 0

        messages = self.manager.flush(client_id)
        for message in messages:
            if not await self._send(client_id, message):
                return -1
            symbol = terminal_message_symbol(message)
            if symbol:
                self.terminal_messages_delivered += 1
                self.delivered_terminal_symbols.add(symbol)
                self.last_terminal_message_delivered_at = now_iso()
        return len(messages)

    async def _send(self, client_id: str, message: str) -> bool:
        websocket = self.clients.get(client_id)
        if websocket is None:
            return False
        try:
            await asyncio.wait_for(websocket.send(message), timeout=self.send_timeout_seconds)
            return True
        except Exception:
            self.failed_client_sends += 1
            self.manager.disconnect(client_id)
            self.clients.pop(client_id, None)
            return False

    async def broadcast_loop(self, *, interval_seconds: float = 0.25, stop: asyncio.Event | None = None) -> None:
        stop_event = stop or asyncio.Event()
        while not stop_event.is_set():
            await self.broadcast_once()
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
            except TimeoutError:
                continue

    @asynccontextmanager
    async def serve(
        self,
        *,
        host: str | None = None,
        port: int | None = None,
        serve_factory: ServeFactory | None = None,
    ):
        factory = serve_factory or default_serve_factory()
        server = factory(self._serve_handler, host or self.host, port or self.port)
        async with server:
            yield server

    async def _serve_handler(self, websocket: WebSocketConnection, path: str | None = None) -> None:
        await self.handle_client(websocket, path=path)

    def _record_performance_samples(self) -> None:
        samples = self.manager.pop_performance_samples()
        if self.shadow_recorder is None:
            return
        for key, values in samples.items():
            for value in values:
                self.shadow_recorder.record_performance_sample(key, value)


def gateway_error(message: str) -> str:
    return json.dumps(
        {
            "schema_version": 1,
            "type": "error",
            "source": "gateway",
            "payload": {"message": message},
        },
        separators=(",", ":"),
        ensure_ascii=False,
    )


def is_terminal_message_frame(message: str) -> bool:
    return terminal_message_symbol(message) is not None


def terminal_message_symbol(message: str) -> str | None:
    try:
        decoded = json.loads(message)
    except json.JSONDecodeError:
        return None
    if not isinstance(decoded, dict):
        return None
    if decoded.get("schema_version") != 1 or decoded.get("type") not in TERMINAL_MESSAGE_TYPES:
        return None
    symbol = decoded.get("symbol")
    return symbol if isinstance(symbol, str) and symbol.strip() else None


def default_serve_factory() -> ServeFactory:
    try:
        import websockets
    except ImportError as error:
        raise RuntimeError("install the 'websockets' package or pass a serve_factory") from error

    return websockets.serve
