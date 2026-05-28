import json
import unittest

from beast_market import (
    InMemoryEventBus,
    InMemoryRedisSnapshotCache,
    KafkaAdapterConfig,
    KafkaEventBusAdapter,
    PROCESSED_TOPIC,
    RAW_TOPIC,
    RedisAdapterConfig,
    RedisSnapshotCacheAdapter,
    make_raw_market_event,
)


class ProductionAdapterTest(unittest.TestCase):
    def test_kafka_adapter_publishes_symbol_keyed_json_without_per_message_flush(self) -> None:
        producer = FakeKafkaProducer()
        adapter = KafkaEventBusAdapter(producer, config=KafkaAdapterConfig())
        event = make_raw_market_event(
            kind="tick",
            symbol="00700.HK",
            source="xtquant",
            seq=1,
            payload={"price": 388.4, "volume": 1000, "turnover": 388400},
        )

        adapter.publish("raw_market_events_v1", "00700.HK", event)

        self.assertEqual(producer.flush_count, 0)
        produced = producer.produced[0]
        self.assertEqual(produced["topic"], "raw_market_events_v1")
        self.assertEqual(produced["key"], b"00700.HK")
        self.assertEqual(json.loads(produced["value"].decode("utf-8"))["event_id"], event["event_id"])

    def test_kafka_adapter_waits_for_delivery_callback_ack_without_flush(self) -> None:
        producer = CallbackKafkaProducer()
        adapter = KafkaEventBusAdapter(
            producer,
            config=KafkaAdapterConfig(delivery_timeout_seconds=0.1),
        )
        event = make_raw_market_event(
            kind="tick",
            symbol="00700.HK",
            source="xtquant",
            seq=1,
            payload={"price": 388.4, "volume": 1000, "turnover": 388400},
        )

        adapter.publish("raw_market_events_v1", "00700.HK", event)

        self.assertEqual(producer.flush_count, 0)
        self.assertEqual(producer.poll_count, 1)
        self.assertEqual(len(producer.produced), 1)

    def test_kafka_adapter_raises_delivery_callback_error_for_reliable_spool(self) -> None:
        producer = CallbackKafkaProducer(delivery_error=RuntimeError("broker unavailable"))
        adapter = KafkaEventBusAdapter(
            producer,
            config=KafkaAdapterConfig(delivery_timeout_seconds=0.1),
        )
        event = make_raw_market_event(
            kind="tick",
            symbol="00700.HK",
            source="xtquant",
            seq=1,
            payload={"price": 388.4, "volume": 1000, "turnover": 388400},
        )

        with self.assertRaisesRegex(RuntimeError, "broker unavailable"):
            adapter.publish("raw_market_events_v1", "00700.HK", event)

        self.assertEqual(producer.flush_count, 0)
        self.assertEqual(producer.poll_count, 1)

    def test_event_bus_adapters_reject_invalid_v1_topic_keys(self) -> None:
        event = make_raw_market_event(
            kind="tick",
            symbol="00700.HK",
            source="xtquant",
            seq=1,
            payload={"price": 388.4, "volume": 1000, "turnover": 388400},
        )
        adapters = [
            InMemoryEventBus(),
            KafkaEventBusAdapter(FakeKafkaProducer(), config=KafkaAdapterConfig()),
        ]

        for adapter in adapters:
            with self.subTest(adapter=type(adapter).__name__):
                with self.assertRaisesRegex(ValueError, "Kafka event key must use canonical symbol format"):
                    adapter.publish(RAW_TOPIC, "700.HK", event)
                with self.assertRaisesRegex(ValueError, "Kafka event key must match event symbol"):
                    adapter.publish(RAW_TOPIC, "00939.HK", event)
                with self.assertRaisesRegex(ValueError, "Kafka event value symbol must use canonical format"):
                    adapter.publish(PROCESSED_TOPIC, "00700.HK", {**event, "symbol": "00700.hk"})

    def test_kafka_adapter_polls_commits_and_reports_lag(self) -> None:
        consumer = FakeKafkaConsumer(
            records=[
                {
                    "key": b"00700.HK",
                    "value": b'{"event_id":"event-1","symbol":"00700.HK","payload":{}}',
                    "offset": 0,
                }
            ],
            high_watermark=3,
        )
        adapter = KafkaEventBusAdapter(FakeKafkaProducer(), consumer)

        records = adapter.poll("raw_market_events_v1", 0)
        adapter.commit("raw_market_events_v1", 1)

        self.assertEqual(records[0]["key"], "00700.HK")
        self.assertEqual(records[0]["value"]["event_id"], "event-1")
        self.assertEqual(adapter.committed_offset("raw_market_events_v1"), 1)
        self.assertEqual(adapter.lag("raw_market_events_v1", 1), 2)

    def test_kafka_adapter_supports_confluent_style_consumer_messages(self) -> None:
        consumer = FakeConfluentKafkaConsumer(
            messages=[
                FakeConfluentKafkaMessage(
                    topic="raw_market_events_v1",
                    key=b"00700.HK",
                    value=b'{"event_id":"event-1","symbol":"00700.HK","payload":{}}',
                    offset=2,
                    partition=0,
                )
            ]
        )
        adapter = KafkaEventBusAdapter(
            FakeKafkaProducer(),
            consumer,
            KafkaAdapterConfig(poll_timeout_ms=1, max_poll_records=10),
        )

        records = adapter.poll("raw_market_events_v1", 0)
        adapter.commit("raw_market_events_v1", 3)

        self.assertEqual(consumer.subscriptions, [["raw_market_events_v1"]])
        self.assertEqual(records, [
            {
                "key": "00700.HK",
                "value": {"event_id": "event-1", "symbol": "00700.HK", "payload": {}},
                "offset": 2,
                "partition": 0,
                "topic": "raw_market_events_v1",
            }
        ])
        self.assertEqual(adapter.committed_offset("raw_market_events_v1"), 3)
        self.assertEqual(consumer.commit_count, 1)

    def test_redis_adapter_writes_terminal_keys_with_ttl_and_reads_snapshot(self) -> None:
        redis = FakeRedisWithPipeline()
        adapter = RedisSnapshotCacheAdapter(
            redis,
            RedisAdapterConfig(terminal_ttl_seconds=3600, history_ttl_seconds=7200),
        )
        snapshot = {
            "snapshot": {"symbol": "00700.HK", "price": 388.4, "tradeDate": "20260522"},
            "minute_bars": [{"price": 388.4}],
            "alerts": [{"id": "alert-1"}],
            "broker_queue": {"ask": [], "bid": []},
            "ccass_holdings": [{"participantName": "JPMorgan"}],
            "freshness": {
                "updated_at": "2026-05-22T09:30:00+08:00",
                "requested_trade_date": "20260525",
                "effective_trade_date": "20260522",
                "source_dates": {"minute_bars": "20260522", "ccass_current": "20260522"},
                "runtime_state": "WARM",
                "degraded_reasons": ["missing_realtime"],
                "last_event_id": "snapshot-1",
            },
        }

        adapter.set_terminal_snapshot("20260522", "00700.HK", snapshot)
        adapter.set_holding_history("00700.HK", "C00010", [{"date": "20260522"}])

        self.assertEqual(redis.pipeline_transactions, [True])
        self.assertEqual(redis.pipeline_execute_count, 1)
        self.assertEqual(redis.direct_set_count, 1)
        self.assertEqual(redis.ttls["terminal:20260522:snapshot:00700.HK"], 3600)
        self.assertEqual(redis.ttls["ccass:history:00700.HK:C00010"], 7200)
        self.assertEqual(adapter.get_terminal_snapshot("20260522", "00700.HK")["snapshot"]["price"], 388.4)
        self.assertIn("terminal:20260522:queue:00700.HK", redis.values)
        self.assertIn("terminal:20260522:state:00700.HK", redis.values)
        self.assertIn("ccass:holding:00700.HK", redis.values)
        queue_record = json.loads(redis.values["terminal:20260522:queue:00700.HK"])
        state_record = json.loads(redis.values["terminal:20260522:state:00700.HK"])
        history_record = json.loads(redis.values["ccass:history:00700.HK:C00010"])
        self.assertEqual(queue_record["data"], {"ask": [], "bid": []})
        self.assertEqual(queue_record["schema_version"], 1)
        self.assertEqual(queue_record["symbol"], "00700.HK")
        self.assertEqual(queue_record["requested_trade_date"], "20260525")
        self.assertEqual(queue_record["effective_trade_date"], "20260522")
        self.assertEqual(queue_record["source_dates"]["minute_bars"], "20260522")
        self.assertEqual(queue_record["version"], "snapshot-1")
        self.assertEqual(queue_record["last_event_id"], "snapshot-1")
        self.assertEqual(queue_record["freshness"]["runtime_state"], "WARM")
        self.assertEqual(queue_record["degraded_reasons"], ["missing_realtime"])
        self.assertEqual(state_record["data"]["effective_trade_date"], "20260522")
        self.assertIsInstance(queue_record["updated_at"], str)
        self.assertEqual(history_record["data"], [{"date": "20260522"}])
        self.assertIsInstance(history_record["updated_at"], str)
        self.assertEqual(history_record["schema_version"], 1)
        self.assertEqual(history_record["symbol"], "00700.HK")
        self.assertEqual(history_record["participant_id"], "C00010")
        self.assertEqual(history_record["effective_trade_date"], "20260522")
        self.assertEqual(history_record["source_dates"]["ccass_history"], "20260522")
        stats = adapter.stats_snapshot()
        self.assertEqual(stats["writes"], 2)
        self.assertEqual(stats["failures"], 0)
        self.assertGreaterEqual(stats["last_latency_ms"], 0)
        self.assertGreaterEqual(stats["max_latency_ms"], stats["last_latency_ms"])

    def test_redis_adapter_merges_existing_minute_bars_when_runtime_snapshot_is_shorter(self) -> None:
        redis = FakeRedisWithPipeline()
        adapter = RedisSnapshotCacheAdapter(redis, RedisAdapterConfig(terminal_ttl_seconds=3600))
        existing = {
            "snapshot": {"symbol": "00700.HK", "price": 388.4, "tradeDate": "20260522"},
            "minute_bars": [
                {"timestamp": "2026-05-22T09:30:00+08:00", "price": 388.4, "volume": 1000, "turnover": 388400},
                {"timestamp": "2026-05-22T09:31:00+08:00", "price": 388.6, "volume": 1100, "turnover": 427460},
            ],
            "alerts": [],
            "broker_queue": {"ask": [], "bid": []},
            "ccass_holdings": [],
            "freshness": {
                "updated_at": "2026-05-22T09:31:00+08:00",
                "requested_trade_date": "20260522",
                "effective_trade_date": "20260522",
                "source_dates": {"minute_bars": "20260522"},
                "runtime_state": "LIVE",
                "degraded_reasons": [],
            },
        }
        shorter_runtime_snapshot = {
            **existing,
            "snapshot": {"symbol": "00700.HK", "price": 389.0, "tradeDate": "20260522"},
            "minute_bars": [
                {"timestamp": "2026-05-22T09:32:12+08:00", "price": 389.0, "volume": 1200, "turnover": 466800}
            ],
        }

        adapter.set_terminal_snapshot("20260522", "00700.HK", existing)
        adapter.set_terminal_snapshot("20260522", "00700.HK", shorter_runtime_snapshot)

        cached = adapter.get_terminal_snapshot("20260522", "00700.HK")
        minute_record = json.loads(redis.values["terminal:20260522:minute:00700.HK"])
        self.assertEqual([bar["timestamp"] for bar in cached["minute_bars"]], [
            "2026-05-22T09:30:00+08:00",
            "2026-05-22T09:31:00+08:00",
            "2026-05-22T09:32:00+08:00",
        ])
        self.assertEqual(cached["snapshot"]["price"], 389.0)
        self.assertEqual([bar["timestamp"] for bar in minute_record["data"]], [
            "2026-05-22T09:30:00+08:00",
            "2026-05-22T09:31:00+08:00",
            "2026-05-22T09:32:00+08:00",
        ])

    def test_redis_adapter_writes_explicit_symbol_runtime_state(self) -> None:
        redis = FakeRedisWithPipeline()
        adapter = RedisSnapshotCacheAdapter(redis, RedisAdapterConfig(terminal_ttl_seconds=3600))

        adapter.set_terminal_state(
            "20260522",
            "00700.HK",
            {
                "runtime_state": "LIVE",
                "ref_count": 2,
                "subscribers": ["one", "two"],
                "hydrate_count": 1,
                "realtime_attached": True,
                "updated_at": "2026-05-22T09:31:00+08:00",
                "freshness": {
                    "requested_trade_date": "20260525",
                    "effective_trade_date": "20260522",
                    "source_dates": {"minute_bars": "20260522"},
                    "last_event_id": "event-1",
                },
            },
        )

        record = json.loads(redis.values["terminal:20260522:state:00700.HK"])
        self.assertEqual(record["runtime_state"], "LIVE")
        self.assertEqual(record["ref_count"], 2)
        self.assertEqual(record["subscribers"], ["one", "two"])
        self.assertEqual(record["requested_trade_date"], "20260525")
        self.assertEqual(record["effective_trade_date"], "20260522")
        self.assertEqual(record["source_dates"], {"minute_bars": "20260522"})
        self.assertEqual(record["last_event_id"], "event-1")
        self.assertEqual(redis.ttls["terminal:20260522:state:00700.HK"], 3600)

    def test_redis_adapter_pipeline_failure_does_not_partially_update_symbol_read_model(self) -> None:
        redis = FakeRedisWithPipeline(fail_pipeline=True)
        adapter = RedisSnapshotCacheAdapter(redis)
        snapshot = {
            "snapshot": {"symbol": "00700.HK", "price": 388.4},
            "minute_bars": [{"price": 388.4}],
            "alerts": [{"id": "alert-1"}],
            "broker_queue": {"ask": [], "bid": []},
            "ccass_holdings": [{"participantName": "JPMorgan"}],
        }

        with self.assertRaisesRegex(RuntimeError, "simulated redis pipeline failure"):
            adapter.set_terminal_snapshot("20260522", "00700.HK", snapshot)

        self.assertEqual(redis.values, {})
        self.assertEqual(redis.ttls, {})
        stats = adapter.stats_snapshot()
        self.assertEqual(stats["writes"], 1)
        self.assertEqual(stats["failures"], 1)
        self.assertEqual(stats["last_error"], "simulated redis pipeline failure")

    def test_redis_snapshot_cache_rejects_invalid_key_inputs(self) -> None:
        snapshot = {
            "snapshot": {"symbol": "00700.HK", "price": 388.4},
            "minute_bars": [],
            "alerts": [],
            "broker_queue": {"ask": [], "bid": []},
            "ccass_holdings": [],
        }
        caches = [
            InMemoryRedisSnapshotCache(),
            RedisSnapshotCacheAdapter(FakeRedis()),
        ]

        for cache in caches:
            with self.subTest(cache=type(cache).__name__):
                with self.assertRaisesRegex(ValueError, "trade_date must use YYYYMMDD format"):
                    cache.set_terminal_snapshot("2026-05-22", "00700.HK", snapshot)
                with self.assertRaisesRegex(ValueError, "symbol must use canonical format"):
                    cache.set_terminal_snapshot("20260522", "700.HK", snapshot)
                with self.assertRaisesRegex(ValueError, "symbol must use canonical format"):
                    cache.get_terminal_snapshot("20260522", "00700.hk")
                with self.assertRaisesRegex(ValueError, "participant_id must be a non-empty string"):
                    cache.set_holding_history("00700.HK", "", [])


class FakeKafkaProducer:
    def __init__(self) -> None:
        self.produced = []
        self.flush_count = 0

    def produce(self, topic: str, key: bytes, value: bytes) -> None:
        self.produced.append({"topic": topic, "key": key, "value": value})

    def flush(self) -> None:
        self.flush_count += 1


class CallbackKafkaProducer:
    def __init__(self, delivery_error: Exception | None = None) -> None:
        self.delivery_error = delivery_error
        self.produced = []
        self.flush_count = 0
        self.poll_count = 0
        self.callback = None

    def produce(self, topic: str, key: bytes, value: bytes, on_delivery=None) -> None:
        self.produced.append({"topic": topic, "key": key, "value": value})
        self.callback = on_delivery

    def poll(self, timeout: float) -> None:
        self.poll_count += 1
        if self.callback is not None:
            callback = self.callback
            self.callback = None
            callback(self.delivery_error, {"offset": 1})

    def flush(self) -> None:
        self.flush_count += 1


class FakeKafkaConsumer:
    def __init__(self, records: list[dict], high_watermark: int) -> None:
        self.records = records
        self.high_watermark_value = high_watermark
        self.committed_offsets = {}

    def poll(self, topic: str, offset: int, timeout_ms: int) -> list[dict]:
        return self.records[offset:]

    def commit(self, topic: str, offset: int) -> None:
        self.committed_offsets[topic] = offset

    def committed(self, topic: str) -> int:
        return self.committed_offsets.get(topic, 0)

    def high_watermark(self, topic: str) -> int:
        return self.high_watermark_value


class FakeConfluentKafkaMessage:
    def __init__(self, *, topic: str, key: bytes, value: bytes, offset: int, partition: int) -> None:
        self._topic = topic
        self._key = key
        self._value = value
        self._offset = offset
        self._partition = partition

    def error(self):
        return None

    def topic(self):
        return self._topic

    def key(self):
        return self._key

    def value(self):
        return self._value

    def offset(self):
        return self._offset

    def partition(self):
        return self._partition


class FakeConfluentKafkaConsumer:
    def __init__(self, messages: list[FakeConfluentKafkaMessage]) -> None:
        self.messages = list(messages)
        self.subscriptions = []
        self.commit_count = 0

    def subscribe(self, topics: list[str]) -> None:
        self.subscriptions.append(topics)

    def poll(self, timeout: float):
        return self.messages.pop(0) if self.messages else None

    def commit(self, asynchronous: bool = False) -> None:
        self.commit_count += 1


class FakeRedis:
    def __init__(self) -> None:
        self.values = {}
        self.ttls = {}

    def set(self, key: str, value: str, ex: int) -> None:
        self.values[key] = value
        self.ttls[key] = ex

    def get(self, key: str):
        return self.values.get(key)


class FakeRedisWithPipeline(FakeRedis):
    def __init__(self, *, fail_pipeline: bool = False) -> None:
        super().__init__()
        self.fail_pipeline = fail_pipeline
        self.pipeline_transactions = []
        self.pipeline_execute_count = 0
        self.direct_set_count = 0

    def set(self, key: str, value: str, ex: int) -> None:
        self.direct_set_count += 1
        super().set(key, value, ex)

    def pipeline(self, transaction: bool = True):
        self.pipeline_transactions.append(transaction)
        return FakeRedisPipeline(self, fail=self.fail_pipeline)


class FakeRedisPipeline:
    def __init__(self, redis: FakeRedisWithPipeline, *, fail: bool = False) -> None:
        self.redis = redis
        self.fail = fail
        self.pending: list[tuple[str, str, int]] = []

    def set(self, key: str, value: str, ex: int):
        self.pending.append((key, value, ex))
        return self

    def execute(self):
        self.redis.pipeline_execute_count += 1
        if self.fail:
            raise RuntimeError("simulated redis pipeline failure")
        for key, value, ex in self.pending:
            self.redis.values[key] = value
            self.redis.ttls[key] = ex
        return [True] * len(self.pending)


if __name__ == "__main__":
    unittest.main()
