import unittest

from beast_market import (
    ContractError,
    make_processed_market_event,
    make_raw_market_event,
    make_terminal_message,
    validate_processed_market_event,
    validate_raw_market_event,
    validate_terminal_message,
)


class ContractValidationTest(unittest.TestCase):
    def test_raw_market_event_rejects_unknown_kind_and_missing_payload_fields(self) -> None:
        with self.assertRaisesRegex(ContractError, "unsupported RawMarketEvent kind"):
            make_raw_market_event(
                kind="legacy_tick",
                symbol="00700.HK",
                source="xtquant",
                seq=1,
                payload={"price": 388.4, "volume": 1000, "turnover": 388400},
            )

        event = make_raw_market_event(
            kind="tick",
            symbol="00700.HK",
            source="xtquant",
            seq=1,
            payload={"price": 388.4, "volume": 1000, "turnover": 388400},
        )
        del event["payload"]["turnover"]

        with self.assertRaisesRegex(ContractError, "missing required payload fields: turnover"):
            validate_raw_market_event(event)

    def test_raw_market_event_rejects_weak_payload_shapes(self) -> None:
        tick_cases = [
            ({"price": "388.4", "volume": 1000, "turnover": 388400}, "price must be a positive number"),
            ({"price": 0, "volume": 1000, "turnover": 388400}, "price must be a positive number"),
            ({"price": 388.4, "volume": -1, "turnover": 388400}, "volume must be a non-negative number"),
            ({"price": 388.4, "volume": 1000, "turnover": True}, "turnover must be a non-negative number"),
        ]
        for payload, error in tick_cases:
            with self.subTest(payload=payload):
                with self.assertRaisesRegex(ContractError, error):
                    make_raw_market_event(
                        kind="tick",
                        symbol="00700.HK",
                        source="xtquant",
                        seq=1,
                        payload=payload,
                    )

        with self.assertRaisesRegex(ContractError, "broker_queue payload entries must contain objects"):
            make_raw_market_event(
                kind="broker_queue",
                symbol="00700.HK",
                source="xtquant",
                seq=1,
                payload={"entries": [1]},
            )

        with self.assertRaisesRegex(
            ContractError,
            "l2_order_book payload ask and bid entries must contain objects",
        ):
            make_raw_market_event(
                kind="l2_order_book",
                symbol="00700.HK",
                source="xtquant",
                seq=1,
                payload={"ask": [{"price": 388.6}], "bid": [1]},
            )

    def test_processed_market_event_rejects_unknown_result_type_and_invalid_snapshot_shape(self) -> None:
        with self.assertRaisesRegex(ContractError, "unsupported ProcessedMarketEvent result_type"):
            make_processed_market_event(
                result_type="legacy_snapshot",
                symbol="00700.HK",
                source="octopus",
                seq=1,
                payload=valid_snapshot_payload(),
            )

        event = make_processed_market_event(
            result_type="snapshot",
            symbol="00700.HK",
            source="octopus",
            seq=1,
            payload=valid_snapshot_payload(),
        )
        event["payload"]["broker_queue"]["ask"] = {}

        with self.assertRaisesRegex(ContractError, "broker_queue ask and bid must be arrays"):
            validate_processed_market_event(event)

    def test_processed_market_event_rejects_weak_realtime_payload_shapes(self) -> None:
        cases = [
            (
                "big_trade_alert",
                {"alert": "bad"},
                "big_trade_alert payload alert must be an object",
            ),
            (
                "broker_queue",
                {"broker_queue": []},
                "broker_queue payload broker_queue must be an object",
            ),
        ]

        for result_type, payload, error in cases:
            with self.subTest(result_type=result_type):
                with self.assertRaisesRegex(ContractError, error):
                    make_processed_market_event(
                        result_type=result_type,
                        symbol="00700.HK",
                        source="octopus",
                        seq=1,
                        payload=payload,
                    )

    def test_terminal_message_envelope_requires_non_empty_strings_and_v1_payload(self) -> None:
        message = make_terminal_message(
            message_type="snapshot",
            symbol="00700.HK",
            source="gateway",
            seq=1,
            payload=valid_snapshot_payload(),
        )
        message["event_id"] = ""

        with self.assertRaisesRegex(ContractError, "event_id must be a non-empty string"):
            validate_terminal_message(message)

    def test_terminal_message_rejects_weak_holding_history_payload(self) -> None:
        message = make_terminal_message(
            message_type="holding_name_click_response",
            symbol="00700.HK",
            source="gateway",
            seq=1,
            payload={
                "participant_name": "JPMorgan",
                "days": 7,
                "history": [],
            },
        )
        weak_cases = [
            ("participant_name", "", "participant_name must be a non-empty string"),
            ("days", 0, "days must be a positive integer"),
            ("days", True, "days must be a positive integer"),
            ("history", {"date": "2026-05-22"}, "history must be an array"),
        ]

        for field, value, error in weak_cases:
            invalid = dict(message)
            invalid["payload"] = dict(message["payload"])
            invalid["payload"][field] = value
            with self.subTest(field=field, value=value):
                with self.assertRaisesRegex(ContractError, error):
                    validate_terminal_message(invalid)

    def test_terminal_message_rejects_weak_realtime_payload_shapes(self) -> None:
        cases = [
            (
                "tick_realtime",
                {"tick": "bad"},
                "tick_realtime payload tick must be an object",
            ),
            (
                "alert_realtime",
                {"alert": "bad"},
                "alert_realtime payload alert must be an object",
            ),
            (
                "queue_realtime",
                {"broker_queue": []},
                "queue_realtime payload broker_queue must be an object",
            ),
        ]

        for message_type, payload, error in cases:
            with self.subTest(message_type=message_type):
                with self.assertRaisesRegex(ContractError, error):
                    make_terminal_message(
                        message_type=message_type,
                        symbol="00700.HK",
                        source="gateway",
                        seq=1,
                        payload=payload,
                    )

    def test_event_envelope_rejects_weak_symbol_timestamp_and_sequence(self) -> None:
        event = make_raw_market_event(
            kind="tick",
            symbol="00700.HK",
            source="xtquant",
            seq=1,
            source_ts="2026-05-22T09:30:00.000+08:00",
            ingest_ts="2026-05-22T09:30:00.020+08:00",
            payload={"price": 388.4, "volume": 1000, "turnover": 388400},
        )

        weak_cases = [
            ("symbol", "700", "symbol must use canonical format"),
            ("symbol", "00700.hk", "symbol must use canonical format"),
            ("source_ts", "2026-05-22 09:30:00", "source_ts must be an ISO-8601 datetime string"),
            ("source_ts", "not-a-date", "source_ts must be an ISO-8601 datetime string"),
            ("ingest_ts", "2026-05-22 09:30:00", "ingest_ts must be an ISO-8601 datetime string"),
            ("seq", True, "seq must be a positive integer"),
            ("seq", 1.5, "seq must be a positive integer"),
            ("seq", "1", "seq must be a positive integer"),
        ]

        for field, value, error in weak_cases:
            invalid = dict(event)
            invalid[field] = value
            with self.subTest(field=field, value=value):
                with self.assertRaisesRegex(ContractError, error):
                    validate_raw_market_event(invalid)


def valid_snapshot_payload() -> dict:
    return {
        "snapshot": {
            "symbol": "00700.HK",
            "name": "00700.HK",
            "currency": "HKD",
            "price": 388.4,
            "previousClose": 386.2,
            "open": 386.2,
            "high": 388.4,
            "low": 386.2,
            "volume": 1000,
            "turnover": 388400,
            "change": 2.2,
            "changePercent": 0.57,
            "updatedAt": "2026-05-22T09:30:00+08:00",
        },
        "minute_bars": [],
        "alerts": [],
        "broker_queue": {"ask": [], "bid": []},
        "ccass_holdings": [],
        "freshness": {"updated_at": "2026-05-22T09:30:00+08:00"},
    }


if __name__ == "__main__":
    unittest.main()
