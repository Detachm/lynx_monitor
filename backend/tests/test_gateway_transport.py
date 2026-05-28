import json
import threading
import unittest

from beast_market import (
    ContractError,
    GatewayV2,
    GatewayV2SessionManager,
    InMemoryEventBus,
    InMemoryRedisSnapshotCache,
    PROCESSED_TOPIC,
    SymbolRuntimeManager,
    SymbolRuntimeState,
    make_processed_market_event,
    make_terminal_message,
    validate_terminal_message,
)


class GatewayTransportTest(unittest.TestCase):
    def test_session_manager_handles_subscribe_broadcast_holding_and_unsubscribe(self) -> None:
        bus = InMemoryEventBus()
        cache = InMemoryRedisSnapshotCache()
        snapshot = make_snapshot_payload("00700.HK", 388.4)
        cache.set_terminal_snapshot("20260522", "00700.HK", snapshot)
        gateway = GatewayV2(bus, cache)
        manager = GatewayV2SessionManager(
            gateway,
            trade_date="20260522",
            history_provider=lambda symbol, participant, days: [
                {"date": "20260522", "shares": 1000, "percent": 1.1, "change": 10}
            ],
            client_queue_size=3,
        )

        manager.connect("active")
        manager.connect("idle")
        client_queue = manager.client_queue_snapshot()
        self.assertEqual(client_queue["connected_clients"], 2)
        self.assertEqual(client_queue["observed_client_count"], 2)
        self.assertEqual(client_queue["observed_client_ids"], ["active", "idle"])
        self.assertEqual(client_queue["max_connected_clients"], 2)

        health = json.loads(manager.flush("active")[0])
        self.assertEqual(health["schema_version"], 1)
        self.assertEqual(health["type"], "health")
        self.assertEqual(health["source"], "gateway")
        self.assertIsInstance(health["payload"], dict)
        self.assertIn("symbol_freshness", health["payload"])
        idle_health = json.loads(manager.flush("idle")[0])
        self.assertEqual(idle_health["schema_version"], 1)
        self.assertEqual(idle_health["type"], "health")
        self.assertEqual(idle_health["source"], "gateway")
        self.assertIsInstance(idle_health["payload"], dict)

        manager.handle_message("active", gateway_request("subscribe", symbol="700"))
        snapshot_message = json.loads(manager.flush("active")[0])
        validate_terminal_message(snapshot_message)
        self.assertEqual(snapshot_message["type"], "snapshot")
        self.assertEqual(snapshot_message["symbol"], "00700.HK")
        samples = manager.pop_performance_samples()
        self.assertEqual(len(samples["subscribe_snapshot_ms"]), 1)
        self.assertGreaterEqual(samples["subscribe_snapshot_ms"][0], 0)
        self.assertEqual(manager.pop_performance_samples()["subscribe_snapshot_ms"], [])
        manager.handle_message("idle", {**gateway_request("health", symbol="00700.HK"), "client_id": "desk-idle"})
        self.assertEqual(manager.client_queue_snapshot()["observed_declared_client_ids"], ["desk-idle"])
        manager.flush("idle")

        bus.publish(
            PROCESSED_TOPIC,
            "00700.HK",
            make_processed_market_event(
                result_type="snapshot",
                symbol="00700.HK",
                source="octopus",
                seq=1,
                payload={**snapshot, "minute_bars": [{"timestamp": "2026-05-22T09:30:00+08:00", "price": 389, "volume": 1, "turnover": 389, "direction": "up"}]},
            ),
        )
        self.assertEqual(manager.broadcast_processed(), 1)
        active_messages = [json.loads(item) for item in manager.flush("active")]
        idle_messages = [json.loads(item) for item in manager.flush("idle")]

        self.assertEqual(active_messages[0]["type"], "tick_realtime")
        self.assertEqual(idle_messages, [])

        manager.handle_message(
            "active",
            gateway_request(
                "holding_name_click",
                symbol="00700.HK",
                participant_name="JPMorgan",
                days=1,
            ),
        )
        holding = json.loads(manager.flush("active")[0])
        validate_terminal_message(holding)
        self.assertEqual(holding["type"], "holding_name_click_response")
        self.assertEqual(holding["payload"]["participant_name"], "JPMorgan")
        self.assertEqual(len(holding["payload"]["history"]), 1)

        manager.handle_message("active", gateway_request("unsubscribe", symbol="00700.HK"))
        self.assertEqual(manager.broadcast_processed(), 0)
        self.assertEqual(manager.flush("active"), [])
        manager.disconnect("idle")
        client_queue_after_disconnect = manager.client_queue_snapshot()
        self.assertEqual(client_queue_after_disconnect["connected_clients"], 1)
        self.assertEqual(client_queue_after_disconnect["observed_client_count"], 2)
        self.assertEqual(client_queue_after_disconnect["max_connected_clients"], 2)

    def test_session_manager_drives_symbol_runtime_attach_detach_and_disconnect(self) -> None:
        bus = InMemoryEventBus()
        cache = InMemoryRedisSnapshotCache()
        snapshot = make_snapshot_payload("00700.HK", 388.4)
        cache.set_terminal_snapshot("20260522", "00700.HK", snapshot)
        gateway = GatewayV2(bus, cache)
        hydrated: list[str] = []
        runtime_manager = SymbolRuntimeManager(
            gateway,
            trade_date="20260522",
            hydrate_symbol=lambda symbol: hydrated.append(symbol),
        )
        manager = GatewayV2SessionManager(
            gateway,
            trade_date="20260522",
            symbol_runtime_manager=runtime_manager,
        )

        manager.connect("one")
        manager.connect("two")
        manager.flush("one")
        manager.flush("two")

        manager.handle_message("one", gateway_request("subscribe", symbol="00700.HK"))
        manager.handle_message("two", gateway_request("subscribe", symbol="00700.HK"))
        manager.flush("one")
        manager.flush("two")

        runtime = runtime_manager.runtimes["00700.HK"]
        self.assertEqual(hydrated, ["00700.HK"])
        self.assertEqual(runtime.hydrate_count, 1)
        self.assertEqual(runtime.ref_count, 2)
        self.assertEqual(runtime.state, SymbolRuntimeState.WARM)

        manager.handle_message("one", gateway_request("unsubscribe", symbol="00700.HK"))
        self.assertEqual(runtime.ref_count, 1)
        self.assertEqual(runtime.state, SymbolRuntimeState.WARM)

        manager.disconnect("two")
        self.assertEqual(runtime.ref_count, 0)
        self.assertEqual(runtime.state, SymbolRuntimeState.EVICTING)

        manager.connect("three")
        manager.flush("three")
        manager.handle_message("three", gateway_request("subscribe", symbol="00700.HK"))
        manager.flush("three")
        self.assertEqual(hydrated, ["00700.HK"])
        self.assertEqual(runtime.hydrate_count, 1)
        self.assertEqual(runtime.ref_count, 1)
        self.assertEqual(runtime.state, SymbolRuntimeState.WARM)

    def test_session_manager_concurrent_subscribe_singleflights_symbol_hydration(self) -> None:
        bus = InMemoryEventBus()
        gateway = GatewayV2(bus, InMemoryRedisSnapshotCache())
        hydrated_payload = make_snapshot_payload("00700.HK", 388.4)
        entered_hydration = threading.Event()
        release_hydration = threading.Event()
        calls: list[str] = []
        errors: list[BaseException] = []
        calls_lock = threading.Lock()
        runtime_manager = SymbolRuntimeManager(
            gateway,
            trade_date="20260522",
            hydrate_symbol=lambda symbol: hydrate(symbol),
        )
        manager = GatewayV2SessionManager(
            gateway,
            trade_date="20260522",
            symbol_runtime_manager=runtime_manager,
        )

        def hydrate(symbol: str) -> dict:
            with calls_lock:
                calls.append(symbol)
            entered_hydration.set()
            self.assertTrue(release_hydration.wait(timeout=2))
            return hydrated_payload

        def subscribe(client_id: str) -> None:
            try:
                manager.handle_message(client_id, gateway_request("subscribe", symbol="00700.HK"))
            except BaseException as error:
                errors.append(error)

        manager.connect("one")
        manager.connect("two")
        manager.flush("one")
        manager.flush("two")

        first = threading.Thread(target=subscribe, args=("one",))
        second = threading.Thread(target=subscribe, args=("two",))
        first.start()
        self.assertTrue(entered_hydration.wait(timeout=2))
        second.start()
        release_hydration.set()
        first.join(timeout=2)
        second.join(timeout=2)

        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertEqual(errors, [])
        runtime = runtime_manager.runtimes["00700.HK"]
        self.assertEqual(calls, ["00700.HK"])
        self.assertEqual(runtime.hydrate_count, 1)
        self.assertEqual(runtime.ref_count, 2)
        self.assertEqual(manager.sessions["one"].subscribed_symbols, {"00700.HK"})
        self.assertEqual(manager.sessions["two"].subscribed_symbols, {"00700.HK"})
        first_snapshot = json.loads(manager.flush("one")[0])
        second_snapshot = json.loads(manager.flush("two")[0])
        self.assertEqual(first_snapshot["payload"]["snapshot"]["price"], 388.4)
        self.assertEqual(second_snapshot["payload"]["snapshot"]["price"], 388.4)
        self.assertEqual(len(manager.performance_snapshot()["subscribe_snapshot_ms"]), 2)
        self.assertEqual(runtime_manager.manager_snapshot()["active_hydrations"], 0)

    def test_session_manager_does_not_mark_failed_subscribe_as_subscribed(self) -> None:
        bus = InMemoryEventBus()
        gateway = GatewayV2(bus, InMemoryRedisSnapshotCache())
        runtime_manager = SymbolRuntimeManager(
            gateway,
            trade_date="20260522",
            hydrate_symbol=lambda symbol: (_ for _ in ()).throw(RuntimeError("hydrate failed")),
        )
        manager = GatewayV2SessionManager(
            gateway,
            trade_date="20260522",
            symbol_runtime_manager=runtime_manager,
        )

        manager.connect("client")
        manager.flush("client")

        with self.assertRaisesRegex(RuntimeError, "hydrate failed"):
            manager.handle_message("client", gateway_request("subscribe", symbol="00700.HK"))

        self.assertEqual(manager.sessions["client"].subscribed_symbols, set())
        self.assertEqual(runtime_manager.runtimes["00700.HK"].ref_count, 0)
        bus.publish(
            PROCESSED_TOPIC,
            "00700.HK",
            make_processed_market_event(
                result_type="snapshot",
                symbol="00700.HK",
                source="octopus",
                seq=1,
                payload={
                    **make_snapshot_payload("00700.HK", 388.4),
                    "minute_bars": [
                        {
                            "timestamp": "2026-05-22T09:30:00+08:00",
                            "price": 388.4,
                            "volume": 1,
                            "turnover": 388.4,
                            "direction": "neutral",
                        }
                    ],
                },
            ),
        )
        self.assertEqual(manager.broadcast_processed(), 0)
        self.assertEqual(manager.flush("client"), [])

    def test_symbol_runtime_evicts_after_grace_period_and_releases_realtime(self) -> None:
        bus = InMemoryEventBus()
        cache = InMemoryRedisSnapshotCache()
        snapshot = make_snapshot_payload("00700.HK", 388.4)
        cache.set_terminal_snapshot("20260522", "00700.HK", snapshot)
        gateway = GatewayV2(bus, cache)
        current = [1000.0]
        hydrated: list[str] = []
        attached: list[str] = []
        released: list[str] = []
        runtime_manager = SymbolRuntimeManager(
            gateway,
            trade_date="20260522",
            hydrate_symbol=lambda symbol: hydrated.append(symbol),
            attach_realtime=lambda symbol: attached.append(symbol),
            release_symbol=lambda symbol: released.append(symbol),
            eviction_grace_seconds=10,
            now=lambda: current[0],
        )

        runtime_manager.attach("00700.HK", "client")
        runtime = runtime_manager.detach("00700.HK", "client")

        self.assertEqual(hydrated, ["00700.HK"])
        self.assertEqual(attached, ["00700.HK"])
        self.assertEqual(runtime.state, SymbolRuntimeState.EVICTING)
        self.assertEqual(runtime.eviction_started_at, 1000.0)
        self.assertEqual(runtime_manager.evict_expired(), [])
        self.assertIn("00700.HK", runtime_manager.runtimes)

        current[0] = 1011.0

        self.assertEqual(runtime_manager.evict_expired(), ["00700.HK"])
        self.assertEqual(released, ["00700.HK"])
        self.assertNotIn("00700.HK", runtime_manager.runtimes)

    def test_symbol_runtime_keeps_warm_when_realtime_attach_is_skipped(self) -> None:
        bus = InMemoryEventBus()
        cache = InMemoryRedisSnapshotCache()
        snapshot = make_snapshot_payload("00700.HK", 388.4)
        cache.set_terminal_snapshot("20260525", "00700.HK", snapshot)
        gateway = GatewayV2(bus, cache)
        runtime_manager = SymbolRuntimeManager(
            gateway,
            trade_date="20260525",
            hydrate_symbol=lambda symbol: snapshot,
            attach_realtime=lambda symbol: False,
        )

        runtime_manager.attach("00700.HK", "client")
        runtime = runtime_manager.runtimes["00700.HK"]

        self.assertEqual(runtime.state, SymbolRuntimeState.WARM)
        self.assertFalse(runtime.realtime_attached)

    def test_symbol_runtime_returns_snapshot_and_degrades_when_realtime_attach_fails(self) -> None:
        bus = InMemoryEventBus()
        cache = InMemoryRedisSnapshotCache()
        snapshot = make_snapshot_payload("00700.HK", 388.4)
        cache.set_terminal_snapshot("20260525", "00700.HK", snapshot)
        gateway = GatewayV2(bus, cache)
        runtime_manager = SymbolRuntimeManager(
            gateway,
            trade_date="20260525",
            hydrate_symbol=lambda symbol: snapshot,
            attach_realtime=lambda symbol: (_ for _ in ()).throw(RuntimeError("xtquant down")),
        )

        message = runtime_manager.attach("00700.HK", "client")

        runtime = runtime_manager.runtimes["00700.HK"]
        self.assertEqual(message["type"], "snapshot")
        self.assertEqual(message["payload"]["snapshot"]["price"], 388.4)
        self.assertEqual(runtime.state, SymbolRuntimeState.DEGRADED)
        self.assertFalse(runtime.realtime_attached)
        self.assertIn("realtime_attach_failed: xtquant down", runtime.degraded_reasons)
        self.assertEqual(runtime.ref_count, 1)

    def test_symbol_runtime_release_failure_degrades_symbol_without_blocking_eviction_loop(self) -> None:
        bus = InMemoryEventBus()
        cache = InMemoryRedisSnapshotCache()
        tencent_snapshot = make_snapshot_payload("00700.HK", 388.4)
        bank_snapshot = make_snapshot_payload("00939.HK", 6.1)
        cache.set_terminal_snapshot("20260522", "00700.HK", tencent_snapshot)
        cache.set_terminal_snapshot("20260522", "00939.HK", bank_snapshot)
        gateway = GatewayV2(bus, cache)
        current = [1000.0]
        runtime_manager = SymbolRuntimeManager(
            gateway,
            trade_date="20260522",
            hydrate_symbol=lambda symbol: tencent_snapshot if symbol == "00700.HK" else bank_snapshot,
            attach_realtime=lambda symbol: True,
            release_symbol=lambda symbol: (_ for _ in ()).throw(RuntimeError("unsubscribe failed")) if symbol == "00700.HK" else None,
            eviction_grace_seconds=10,
            now=lambda: current[0],
        )

        runtime_manager.attach("00700.HK", "client")
        runtime_manager.attach("00939.HK", "client")
        runtime_manager.detach("00700.HK", "client")
        runtime_manager.detach("00939.HK", "client")
        current[0] = 1011.0

        self.assertEqual(runtime_manager.evict_expired(), ["00939.HK"])
        self.assertIn("00700.HK", runtime_manager.runtimes)
        self.assertNotIn("00939.HK", runtime_manager.runtimes)
        self.assertEqual(runtime_manager.runtimes["00700.HK"].state, SymbolRuntimeState.DEGRADED)
        self.assertIn("realtime_release_failed: unsubscribe failed", runtime_manager.runtimes["00700.HK"].degraded_reasons)

    def test_symbol_runtime_snapshot_comes_from_hydrated_runtime_payload_not_gateway_cache(self) -> None:
        bus = InMemoryEventBus()
        cache = InMemoryRedisSnapshotCache()
        cache.set_terminal_snapshot("20260522", "00700.HK", make_snapshot_payload("00700.HK", 999.0))
        gateway = GatewayV2(bus, cache)
        hydrated_payload = make_snapshot_payload("00700.HK", 388.4)
        current = [1000.0]

        def hydrate(symbol: str) -> dict:
            current[0] += 0.025
            return hydrated_payload

        runtime_manager = SymbolRuntimeManager(
            gateway,
            trade_date="20260522",
            hydrate_symbol=hydrate,
            now=lambda: current[0],
        )

        first = runtime_manager.attach("00700.HK", "one")
        cache.set_terminal_snapshot("20260522", "00700.HK", make_snapshot_payload("00700.HK", 777.0))
        second = runtime_manager.attach("00700.HK", "two")

        runtime = runtime_manager.runtimes["00700.HK"]
        self.assertEqual(runtime.hydrate_count, 1)
        self.assertEqual(runtime.hydration_failures, 0)
        self.assertAlmostEqual(runtime.last_hydration_latency_ms, 25.0, places=6)
        self.assertAlmostEqual(runtime.max_hydration_latency_ms, 25.0, places=6)
        self.assertTrue(runtime.snapshot_payload is hydrated_payload)
        self.assertEqual(first["payload"]["snapshot"]["price"], 388.4)
        self.assertEqual(second["payload"]["snapshot"]["price"], 388.4)

    def test_symbol_runtime_concurrent_attach_waits_for_same_hydration_result(self) -> None:
        bus = InMemoryEventBus()
        cache = InMemoryRedisSnapshotCache()
        cache.set_terminal_snapshot("20260522", "00700.HK", make_snapshot_payload("00700.HK", 999.0))
        gateway = GatewayV2(bus, cache)
        hydrated_payload = make_snapshot_payload("00700.HK", 388.4)
        entered_hydration = threading.Event()
        release_hydration = threading.Event()
        calls: list[str] = []
        results: dict[str, dict] = {}
        errors: list[BaseException] = []

        def hydrate(symbol: str) -> dict:
            calls.append(symbol)
            entered_hydration.set()
            self.assertTrue(release_hydration.wait(timeout=2))
            return hydrated_payload

        runtime_manager = SymbolRuntimeManager(
            gateway,
            trade_date="20260522",
            hydrate_symbol=hydrate,
        )

        def attach_client(client_id: str) -> None:
            try:
                results[client_id] = runtime_manager.attach("00700.HK", client_id)
            except BaseException as error:
                errors.append(error)

        first = threading.Thread(target=attach_client, args=("one",))
        second = threading.Thread(target=attach_client, args=("two",))
        first.start()
        self.assertTrue(entered_hydration.wait(timeout=2))
        second.start()
        release_hydration.set()
        first.join(timeout=2)
        second.join(timeout=2)

        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertEqual(errors, [])
        runtime = runtime_manager.runtimes["00700.HK"]
        self.assertEqual(calls, ["00700.HK"])
        self.assertEqual(runtime.hydrate_count, 1)
        self.assertEqual(runtime.ref_count, 2)
        self.assertEqual(results["one"]["payload"]["snapshot"]["price"], 388.4)
        self.assertEqual(results["two"]["payload"]["snapshot"]["price"], 388.4)

    def test_symbol_runtime_records_hydration_failure_evidence(self) -> None:
        gateway = GatewayV2(InMemoryEventBus(), InMemoryRedisSnapshotCache())
        current = [1000.0]

        def fail_hydration(symbol: str) -> None:
            current[0] += 0.01
            raise RuntimeError("hydrate unavailable")

        runtime_manager = SymbolRuntimeManager(
            gateway,
            trade_date="20260522",
            hydrate_symbol=fail_hydration,
            now=lambda: current[0],
        )

        with self.assertRaisesRegex(RuntimeError, "hydrate unavailable"):
            runtime_manager.attach("00700.HK", "client")

        runtime = runtime_manager.runtimes["00700.HK"]
        snapshot = runtime_manager.snapshot()["00700.HK"]
        self.assertEqual(runtime.state, SymbolRuntimeState.DEGRADED)
        self.assertEqual(snapshot["hydration_failures"], 1)
        self.assertEqual(snapshot["last_hydration_error"], "hydrate unavailable")
        self.assertAlmostEqual(snapshot["last_hydration_latency_ms"], 10.0, places=6)

    def test_symbol_runtime_caps_concurrent_hydrations_with_degraded_snapshot(self) -> None:
        gateway = GatewayV2(InMemoryEventBus(), InMemoryRedisSnapshotCache())
        runtime_manager: SymbolRuntimeManager

        def hydrate(symbol: str) -> dict:
            if symbol == "00700.HK":
                nested = runtime_manager.attach("00939.HK", "nested-client")
                validate_terminal_message(nested)
                self.assertEqual(nested["type"], "snapshot")
                self.assertEqual(nested["payload"]["freshness"]["runtime_state"], "DEGRADED")
            return make_snapshot_payload(symbol, 388.4)

        runtime_manager = SymbolRuntimeManager(
            gateway,
            trade_date="20260522",
            hydrate_symbol=hydrate,
            max_concurrent_hydrations=1,
        )

        first = runtime_manager.attach("00700.HK", "client")

        first_runtime = runtime_manager.runtimes["00700.HK"]
        rejected_runtime = runtime_manager.runtimes["00939.HK"]
        self.assertEqual(first["payload"]["snapshot"]["price"], 388.4)
        self.assertEqual(first_runtime.hydrate_count, 1)
        self.assertEqual(rejected_runtime.state, SymbolRuntimeState.DEGRADED)
        self.assertEqual(rejected_runtime.hydration_failures, 1)
        self.assertIn("hydration_capacity_exceeded", rejected_runtime.degraded_reasons[0])
        self.assertEqual(runtime_manager.snapshot()["00939.HK"]["capacity_rejections"], 1)

    def test_symbol_runtime_marks_hydrated_snapshot_with_degraded_freshness(self) -> None:
        gateway = GatewayV2(InMemoryEventBus(), InMemoryRedisSnapshotCache())
        degraded_payload = make_snapshot_payload("00700.HK", 388.4)
        degraded_payload["freshness"] = {
            **degraded_payload["freshness"],
            "degraded": True,
            "degraded_reasons": ["redis_terminal_snapshot_write_failed: redis unavailable"],
        }
        runtime_manager = SymbolRuntimeManager(
            gateway,
            trade_date="20260522",
            hydrate_symbol=lambda symbol: degraded_payload,
            attach_realtime=lambda symbol: None,
        )

        runtime_manager.attach("00700.HK", "client")

        runtime = runtime_manager.runtimes["00700.HK"]
        self.assertEqual(runtime.state, SymbolRuntimeState.DEGRADED)
        self.assertEqual(runtime.degraded_reasons, ["redis_terminal_snapshot_write_failed: redis unavailable"])
        self.assertTrue(runtime.realtime_attached)

    def test_symbol_runtime_seed_snapshot_warms_without_ref_count_or_realtime_attach(self) -> None:
        gateway = GatewayV2(InMemoryEventBus(), InMemoryRedisSnapshotCache())
        payload = make_snapshot_payload("00700.HK", 388.4)
        attached: list[str] = []
        states: list[dict] = []
        runtime_manager = SymbolRuntimeManager(
            gateway,
            trade_date="20260522",
            attach_realtime=lambda symbol: attached.append(symbol),
            state_sink=lambda symbol, state: states.append({"symbol": symbol, **state}),
        )

        runtime = runtime_manager.seed_snapshot("00700.HK", payload)

        self.assertEqual(runtime.state, SymbolRuntimeState.WARM)
        self.assertEqual(runtime.ref_count, 0)
        self.assertEqual(runtime.snapshot_payload["snapshot"]["price"], 388.4)
        self.assertEqual(attached, [])
        self.assertEqual(states[-1]["symbol"], "00700.HK")
        self.assertEqual(states[-1]["runtime_state"], "WARM")
        self.assertEqual(states[-1]["ref_count"], 0)
        self.assertFalse(states[-1]["realtime_attached"])

    def test_symbol_runtime_publishes_attach_detach_state_read_model(self) -> None:
        gateway = GatewayV2(InMemoryEventBus(), InMemoryRedisSnapshotCache())
        payload = make_snapshot_payload("00700.HK", 388.4)
        states: list[dict] = []
        runtime_manager = SymbolRuntimeManager(
            gateway,
            trade_date="20260522",
            hydrate_symbol=lambda symbol: payload,
            attach_realtime=lambda symbol: True,
            state_sink=lambda symbol, state: states.append({"symbol": symbol, **state}),
        )

        runtime_manager.attach("00700.HK", "one")
        runtime_manager.attach("00700.HK", "two")
        runtime_manager.detach("00700.HK", "one")
        runtime_manager.detach("00700.HK", "two")

        self.assertEqual([state["runtime_state"] for state in states], ["WARM", "LIVE", "LIVE", "LIVE", "EVICTING"])
        self.assertEqual([state["ref_count"] for state in states], [1, 1, 2, 1, 0])
        self.assertEqual(states[-1]["subscribers"], [])
        self.assertTrue(states[1]["realtime_attached"])

    def test_symbol_runtime_marks_state_sink_failure_degraded_without_breaking_attach(self) -> None:
        gateway = GatewayV2(InMemoryEventBus(), InMemoryRedisSnapshotCache())
        payload = make_snapshot_payload("00700.HK", 388.4)
        runtime_manager = SymbolRuntimeManager(
            gateway,
            trade_date="20260522",
            hydrate_symbol=lambda symbol: payload,
            attach_realtime=lambda symbol: True,
            state_sink=lambda symbol, state: (_ for _ in ()).throw(RuntimeError("redis down")),
        )

        snapshot = runtime_manager.attach("00700.HK", "client")

        validate_terminal_message(snapshot)
        runtime = runtime_manager.runtimes["00700.HK"]
        manager_snapshot = runtime_manager.manager_snapshot()
        symbol_snapshot = runtime_manager.snapshot()["00700.HK"]
        self.assertEqual(snapshot["type"], "snapshot")
        self.assertEqual(runtime.state, SymbolRuntimeState.DEGRADED)
        self.assertIn("runtime_state_sink_write_failed: redis down", runtime.degraded_reasons)
        self.assertEqual(manager_snapshot["state_sink_failures"], 2)
        self.assertEqual(manager_snapshot["last_state_sink_error"], "runtime_state_sink_write_failed: redis down")
        self.assertEqual(manager_snapshot["state_sink_failure_symbols"], ["00700.HK"])
        self.assertEqual(symbol_snapshot["state"], "DEGRADED")
        self.assertIn("runtime_state_sink_write_failed: redis down", symbol_snapshot["degraded_reasons"])

    def test_symbol_runtime_snapshot_absorbs_gateway_deltas_for_later_hot_subscribers(self) -> None:
        bus = InMemoryEventBus()
        cache = InMemoryRedisSnapshotCache()
        gateway = GatewayV2(bus, cache)
        hydrated_payload = make_snapshot_payload("00700.HK", 388.4)
        states: list[dict] = []
        runtime_manager = SymbolRuntimeManager(
            gateway,
            trade_date="20260522",
            hydrate_symbol=lambda symbol: hydrated_payload,
            state_sink=lambda symbol, state: states.append({"symbol": symbol, **state}),
        )
        manager = GatewayV2SessionManager(
            gateway,
            trade_date="20260522",
            symbol_runtime_manager=runtime_manager,
        )
        manager.connect("one")
        manager.flush("one")
        manager.handle_message("one", gateway_request("subscribe", symbol="00700.HK"))
        manager.flush("one")

        tick = {
            "timestamp": "2026-05-22T09:31:00+08:00",
            "price": 389.2,
            "volume": 1000,
            "turnover": 389200,
            "direction": "up",
        }
        updated_payload = {
            **hydrated_payload,
            "snapshot": {**hydrated_payload["snapshot"], "price": 389.2},
            "minute_bars": [*hydrated_payload["minute_bars"], tick],
            "last_tick": tick,
            "freshness": {
                "updated_at": "2026-05-22T09:31:01+08:00",
                "runtime_state": "LIVE",
            },
        }
        bus.publish(
            PROCESSED_TOPIC,
            "00700.HK",
            make_processed_market_event(
                result_type="snapshot",
                symbol="00700.HK",
                source="octopus",
                seq=2,
                source_ts="2026-05-22T09:31:00+08:00",
                payload=updated_payload,
            ),
        )
        manager.broadcast_processed()
        processed_state = states[-1]

        manager.connect("two")
        manager.flush("two")
        manager.handle_message("two", gateway_request("subscribe", symbol="00700.HK"))
        hot_snapshot = json.loads(manager.flush("two")[0])

        self.assertEqual(runtime_manager.runtimes["00700.HK"].hydrate_count, 1)
        self.assertEqual(hot_snapshot["payload"]["snapshot"]["price"], 389.2)
        self.assertEqual(hot_snapshot["payload"]["minute_bars"][-1]["timestamp"], tick["timestamp"])
        self.assertEqual(hot_snapshot["payload"]["freshness"]["runtime_state"], "LIVE")
        self.assertEqual(processed_state["symbol"], "00700.HK")
        self.assertEqual(processed_state["runtime_state"], "LIVE")
        self.assertEqual(processed_state["freshness"]["runtime_state"], "LIVE")
        self.assertEqual(processed_state["ref_count"], 1)

    def test_session_manager_can_broadcast_without_mutating_symbol_runtime(self) -> None:
        bus = InMemoryEventBus()
        gateway = GatewayV2(bus, InMemoryRedisSnapshotCache())
        hydrated_payload = make_snapshot_payload("00700.HK", 388.4)
        runtime_manager = SymbolRuntimeManager(
            gateway,
            trade_date="20260522",
            hydrate_symbol=lambda symbol: hydrated_payload,
        )
        manager = GatewayV2SessionManager(
            gateway,
            trade_date="20260522",
            symbol_runtime_manager=runtime_manager,
        )
        manager.connect("one")
        manager.flush("one")
        manager.handle_message("one", gateway_request("subscribe", symbol="00700.HK"))
        manager.flush("one")
        message = make_terminal_message(
            message_type="tick_realtime",
            symbol="00700.HK",
            source="test",
            source_ts="2026-05-22T09:31:00+08:00",
            seq=1,
            payload={
                "tick": {
                    "timestamp": "2026-05-22T09:31:00+08:00",
                    "price": 389.2,
                    "volume": 1000,
                    "turnover": 389200,
                    "direction": "up",
                },
                "freshness": {"updated_at": "2026-05-22T09:31:00+08:00", "runtime_state": "LIVE"},
            },
        )

        delivered = manager.broadcast_runtime_messages([message], update_symbol_runtime=False)
        queued = [json.loads(item) for item in manager.flush("one")]

        self.assertEqual(delivered, 1)
        self.assertEqual(queued[0]["type"], "tick_realtime")
        self.assertEqual(runtime_manager.runtimes["00700.HK"].snapshot_payload["snapshot"]["price"], 388.4)

    def test_session_manager_rejects_unknown_actions(self) -> None:
        manager = GatewayV2SessionManager(GatewayV2(InMemoryEventBus(), InMemoryRedisSnapshotCache()), trade_date="20260522")
        manager.connect("client")

        with self.assertRaises(ValueError):
            manager.handle_message("client", gateway_request("bad"))

    def test_session_manager_rejects_non_canonical_request_symbols(self) -> None:
        manager = GatewayV2SessionManager(GatewayV2(InMemoryEventBus(), InMemoryRedisSnapshotCache()), trade_date="20260522")
        manager.connect("client")

        invalid_requests = [
            gateway_request("subscribe", symbol="700.HK"),
            gateway_request("unsubscribe", symbol="abc"),
            gateway_request("holding_name_click", symbol="00700.hkx", participant_name="JPMorgan", days=7),
        ]

        for request in invalid_requests:
            with self.subTest(request=request):
                with self.assertRaisesRegex(ValueError, "canonical symbol"):
                    manager.handle_message("client", request)

    def test_session_manager_rejects_invalid_holding_history_requests(self) -> None:
        manager = GatewayV2SessionManager(GatewayV2(InMemoryEventBus(), InMemoryRedisSnapshotCache()), trade_date="20260522")
        manager.connect("client")

        invalid_requests = [
            (gateway_request("holding_name_click", symbol="00700.HK", participant_name="", days=7), "participant_name"),
            (gateway_request("holding_name_click", symbol="00700.HK", participant_name="JPMorgan", days=0), "positive integer days"),
            (gateway_request("holding_name_click", symbol="00700.HK", participant_name="JPMorgan", days=True), "positive integer days"),
            (gateway_request("holding_name_click", symbol="00700.HK", participant_name="JPMorgan", days="7"), "positive integer days"),
        ]

        for request, error in invalid_requests:
            with self.subTest(request=request):
                with self.assertRaisesRegex(ValueError, error):
                    manager.handle_message("client", request)

    def test_session_manager_rejects_non_v1_client_protocol(self) -> None:
        manager = GatewayV2SessionManager(GatewayV2(InMemoryEventBus(), InMemoryRedisSnapshotCache()), trade_date="20260522")
        manager.connect("client")

        with self.assertRaisesRegex(ValueError, "schema_version"):
            manager.handle_message("client", {"protocol": "terminal-message-v1", "action": "subscribe", "symbol": "00700.HK"})
        with self.assertRaisesRegex(ValueError, "protocol"):
            manager.handle_message("client", {"schema_version": 1, "protocol": "legacy-message", "action": "subscribe", "symbol": "00700.HK"})

    def test_gateway_rejects_non_v1_processed_events_before_terminal_conversion(self) -> None:
        bus = InMemoryEventBus()
        gateway = GatewayV2(bus, InMemoryRedisSnapshotCache())
        event = make_processed_market_event(
            result_type="snapshot",
            symbol="00700.HK",
            source="octopus",
            seq=1,
            payload={
                **make_snapshot_payload("00700.HK", 388.4),
                "minute_bars": [
                    {
                        "timestamp": "2026-05-22T09:30:00+08:00",
                        "price": 388.4,
                        "volume": 1000,
                        "turnover": 388400,
                        "direction": "flat",
                    }
                ],
            },
        )
        event["result_type"] = "legacy_snapshot"
        bus.publish(PROCESSED_TOPIC, "00700.HK", event)

        with self.assertRaisesRegex(ContractError, "unsupported ProcessedMarketEvent result_type"):
            gateway.to_terminal_messages()

    def test_gateway_rejects_processed_kafka_key_symbol_mismatch_before_terminal_conversion(self) -> None:
        bus = InMemoryEventBus()
        gateway = GatewayV2(bus, InMemoryRedisSnapshotCache())
        event = make_processed_market_event(
            result_type="snapshot",
            symbol="00700.HK",
            source="octopus",
            seq=1,
            payload={
                **make_snapshot_payload("00700.HK", 388.4),
                "minute_bars": [
                    {
                        "timestamp": "2026-05-22T09:30:00+08:00",
                        "price": 388.4,
                        "volume": 1000,
                        "turnover": 388400,
                        "direction": "flat",
                    }
                ],
            },
        )
        bus.records[PROCESSED_TOPIC].append({"key": "00939.HK", "value": event})

        with self.assertRaisesRegex(ValueError, "Kafka event key must match event symbol"):
            gateway.to_terminal_messages()


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
