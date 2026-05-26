import asyncio
import json
import threading
import unittest
from typing import AsyncIterator

from beast_market import (
    GatewayV2,
    GatewayV2SessionManager,
    GatewayV2WebSocketService,
    InMemoryEventBus,
    InMemoryRedisSnapshotCache,
    PROCESSED_TOPIC,
    ShadowRunRecorder,
    SymbolRuntimeManager,
    make_processed_market_event,
)
from beast_market.websocket_server import is_terminal_message_frame


class WebSocketServerTest(unittest.IsolatedAsyncioTestCase):
    async def test_handle_client_sends_health_snapshot_and_errors(self) -> None:
        bus = InMemoryEventBus()
        cache = InMemoryRedisSnapshotCache()
        cache.set_terminal_snapshot("20260522", "00700.HK", make_snapshot_payload("00700.HK", 388.4))
        service = GatewayV2WebSocketService(
            GatewayV2SessionManager(GatewayV2(bus, cache), trade_date="20260522")
        )
        websocket = FakeWebSocket(
            [
                json.dumps(gateway_request("subscribe", symbol="700")),
                json.dumps(gateway_request("bad")),
            ]
        )

        await service.handle_client(websocket, client_id="client-1")

        sent = [json.loads(message) for message in websocket.sent]
        self.assertEqual(sent[0]["schema_version"], 1)
        self.assertEqual(sent[0]["type"], "health")
        self.assertEqual(sent[0]["source"], "gateway")
        self.assertIsInstance(sent[0]["payload"], dict)
        self.assertIn("latest_event_at_by_symbol", sent[0]["payload"])
        self.assertEqual(sent[1]["type"], "snapshot")
        self.assertEqual(sent[1]["symbol"], "00700.HK")
        self.assertEqual(sent[2]["schema_version"], 1)
        self.assertEqual(sent[2]["type"], "error")
        self.assertEqual(sent[2]["source"], "gateway")
        self.assertIsInstance(sent[2]["payload"], dict)
        self.assertNotIn("client-1", service.clients)
        self.assertNotIn("client-1", service.manager.sessions)

    async def test_handle_client_records_subscribe_snapshot_performance_samples(self) -> None:
        cache = InMemoryRedisSnapshotCache()
        cache.set_terminal_snapshot("20260522", "00700.HK", make_snapshot_payload("00700.HK", 388.4))
        recorder = ShadowRunRecorder(
            session_id="session-1",
            trading_date="20260522",
            started_at="2026-05-22T09:30:00+08:00",
        )
        service = GatewayV2WebSocketService(
            GatewayV2SessionManager(GatewayV2(InMemoryEventBus(), cache), trade_date="20260522"),
            shadow_recorder=recorder,
        )

        await service.handle_client(
            FakeWebSocket([json.dumps(gateway_request("subscribe", symbol="700"))]),
            client_id="client-1",
        )

        self.assertEqual(len(recorder.performance_samples["subscribe_snapshot_ms"]), 1)
        self.assertGreaterEqual(recorder.performance_samples["subscribe_snapshot_ms"][0], 0)
        self.assertEqual(len(service.manager.performance_snapshot()["subscribe_snapshot_ms"]), 1)
        self.assertEqual(service.manager.pop_performance_samples()["subscribe_snapshot_ms"], [])
        self.assertEqual(len(service.manager.performance_snapshot()["subscribe_snapshot_ms"]), 1)

    async def test_concurrent_clients_do_not_block_event_loop_during_cold_hydration(self) -> None:
        gateway = GatewayV2(InMemoryEventBus(), InMemoryRedisSnapshotCache())
        entered_hydration = threading.Event()
        release_hydration = threading.Event()
        calls: list[str] = []
        calls_lock = threading.Lock()

        def hydrate(symbol: str) -> dict:
            with calls_lock:
                calls.append(symbol)
            entered_hydration.set()
            self.assertTrue(release_hydration.wait(timeout=2))
            return make_snapshot_payload(symbol, 388.4)

        runtime_manager = SymbolRuntimeManager(
            gateway,
            trade_date="20260522",
            hydrate_symbol=hydrate,
        )
        service = GatewayV2WebSocketService(
            GatewayV2SessionManager(
                gateway,
                trade_date="20260522",
                symbol_runtime_manager=runtime_manager,
            )
        )
        first = FakeWebSocket([json.dumps(gateway_request("subscribe", symbol="00700.HK"))])
        second = FakeWebSocket([json.dumps(gateway_request("subscribe", symbol="00700.HK"))])

        first_task = asyncio.create_task(service.handle_client(first, client_id="client-1"))
        self.assertTrue(await asyncio.to_thread(entered_hydration.wait, 2))
        second_task = asyncio.create_task(service.handle_client(second, client_id="client-2"))
        await asyncio.sleep(0)
        release_hydration.set()
        await asyncio.wait_for(asyncio.gather(first_task, second_task), timeout=2)

        self.assertEqual(calls, ["00700.HK"])
        self.assertEqual(runtime_manager.runtimes["00700.HK"].hydrate_count, 1)
        first_terminal = [json.loads(message) for message in first.sent if json.loads(message).get("type") == "snapshot"]
        second_terminal = [json.loads(message) for message in second.sent if json.loads(message).get("type") == "snapshot"]
        self.assertEqual(first_terminal[0]["payload"]["snapshot"]["price"], 388.4)
        self.assertEqual(second_terminal[0]["payload"]["snapshot"]["price"], 388.4)
        self.assertEqual(len(service.manager.performance_snapshot()["subscribe_snapshot_ms"]), 2)

    async def test_handle_client_rejects_non_gateway_path(self) -> None:
        service = GatewayV2WebSocketService(
            GatewayV2SessionManager(GatewayV2(InMemoryEventBus(), InMemoryRedisSnapshotCache()), trade_date="20260522"),
            path="/ws",
        )
        websocket = FakeWebSocket([])

        await service.handle_client(websocket, client_id="client-1", path="/legacy")

        sent = [json.loads(message) for message in websocket.sent]
        self.assertEqual(sent[0]["type"], "error")
        self.assertIn("/legacy", sent[0]["payload"]["message"])
        self.assertNotIn("client-1", service.clients)
        self.assertNotIn("client-1", service.manager.sessions)

    async def test_broadcast_once_flushes_processed_events_to_subscribed_clients(self) -> None:
        bus = InMemoryEventBus()
        cache = InMemoryRedisSnapshotCache()
        snapshot = make_snapshot_payload("00700.HK", 388.4)
        cache.set_terminal_snapshot("20260522", "00700.HK", snapshot)
        service = GatewayV2WebSocketService(
            GatewayV2SessionManager(GatewayV2(bus, cache), trade_date="20260522")
        )
        active = FakeWebSocket([])
        idle = FakeWebSocket([])
        service.clients["active"] = active
        service.clients["idle"] = idle
        service.manager.connect("active")
        service.manager.connect("idle")
        service.manager.handle_message("active", gateway_request("subscribe", symbol="00700.HK"))
        await service.flush_client("active")
        await service.flush_client("idle")

        bus.publish(
            PROCESSED_TOPIC,
            "00700.HK",
            make_processed_market_event(
                result_type="snapshot",
                symbol="00700.HK",
                source="octopus",
                seq=1,
                payload={
                    **snapshot,
                    "minute_bars": [
                        {
                            "timestamp": "2026-05-22T09:30:00+08:00",
                            "price": 389,
                            "volume": 1,
                            "turnover": 389,
                            "direction": "up",
                        }
                    ],
                },
            ),
        )

        delivered = await service.broadcast_once()

        self.assertEqual(delivered, 1)
        self.assertEqual(json.loads(active.sent[-1])["type"], "tick_realtime")
        self.assertEqual(len(idle.sent), 1)
        self.assertEqual(service.terminal_messages_delivered, 2)
        self.assertEqual(service.delivered_terminal_symbols, {"00700.HK"})
        self.assertIsNotNone(service.last_terminal_message_delivered_at)

    async def test_terminal_delivery_counter_ignores_health_and_errors(self) -> None:
        service = GatewayV2WebSocketService(
            GatewayV2SessionManager(GatewayV2(InMemoryEventBus(), InMemoryRedisSnapshotCache()), trade_date="20260522")
        )
        websocket = FakeWebSocket([json.dumps(gateway_request("bad"))])

        await service.handle_client(websocket, client_id="client-1")

        self.assertEqual(service.terminal_messages_delivered, 0)
        self.assertEqual(service.delivered_terminal_symbols, set())
        self.assertIsNone(service.last_terminal_message_delivered_at)
        self.assertFalse(is_terminal_message_frame(websocket.sent[0]))
        self.assertFalse(is_terminal_message_frame(websocket.sent[1]))

    async def test_serve_uses_configured_host_port_and_path(self) -> None:
        service = GatewayV2WebSocketService(
            GatewayV2SessionManager(GatewayV2(InMemoryEventBus(), InMemoryRedisSnapshotCache()), trade_date="20260522"),
            host="0.0.0.0",
            port=9021,
            path="/ws",
        )
        factory = RecordingServeFactory()

        async with service.serve(serve_factory=factory):
            await factory.handler(FakeWebSocket([]), "/ws")

        self.assertEqual(factory.host, "0.0.0.0")
        self.assertEqual(factory.port, 9021)

    async def test_serve_defaults_to_lan_bind_host(self) -> None:
        service = GatewayV2WebSocketService(
            GatewayV2SessionManager(GatewayV2(InMemoryEventBus(), InMemoryRedisSnapshotCache()), trade_date="20260522")
        )
        factory = RecordingServeFactory()

        async with service.serve(serve_factory=factory):
            pass

        self.assertEqual(service.host, "0.0.0.0")
        self.assertEqual(factory.host, "0.0.0.0")


class FakeWebSocket:
    def __init__(self, incoming: list[str]) -> None:
        self.incoming = list(incoming)
        self.sent: list[str] = []

    def __aiter__(self) -> AsyncIterator[str]:
        return self

    async def __anext__(self) -> str:
        if not self.incoming:
            raise StopAsyncIteration
        return self.incoming.pop(0)

    async def send(self, message: str) -> None:
        self.sent.append(message)


class RecordingServeFactory:
    def __init__(self) -> None:
        self.handler = None
        self.host = ""
        self.port = 0

    def __call__(self, handler, host: str, port: int):
        self.handler = handler
        self.host = host
        self.port = port
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False


def make_snapshot_payload(symbol: str, price: float) -> dict:
    return {
        "snapshot": {
            "symbol": symbol,
            "name": symbol,
            "currency": "HKD",
            "price": price,
            "previousClose": price,
            "open": price,
            "high": price,
            "low": price,
            "volume": 0,
            "turnover": 0,
            "change": 0,
            "changePercent": 0,
            "updatedAt": "2026-05-22T09:30:00+08:00",
        },
        "minute_bars": [],
        "alerts": [],
        "broker_queue": {"ask": [], "bid": []},
        "ccass_holdings": [],
        "freshness": {"updated_at": "2026-05-22T09:30:00+08:00"},
    }


def gateway_request(action: str, **values: object) -> dict:
    return {
        "schema_version": 1,
        "protocol": "terminal-message-v1",
        "action": action,
        **values,
    }


if __name__ == "__main__":
    unittest.main()
