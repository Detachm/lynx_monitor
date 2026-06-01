from __future__ import annotations

from datetime import datetime, timedelta, timezone

HK_TZ = timezone(timedelta(hours=8))


def parse_hk_datetime(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=HK_TZ)
    return parsed.astimezone(HK_TZ)


def is_regular_hk_trading_minute(value: str, trade_date: str | None = None) -> bool:
    parsed = parse_hk_datetime(value)
    if parsed is None:
        return False
    if trade_date and parsed.strftime("%Y%m%d") != trade_date:
        return False
    minute_of_day = parsed.hour * 60 + parsed.minute
    return (9 * 60 + 30 <= minute_of_day < 12 * 60) or (13 * 60 <= minute_of_day <= 16 * 60)
