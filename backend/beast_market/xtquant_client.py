from __future__ import annotations

import importlib
import inspect
import os
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
import time
from typing import Any, Callable, Iterable

from .runtime import normalize_subscription_symbol


CallbackSink = Callable[[dict[str, Any]], bool | None]
DEFAULT_XTQUANT_PERIODS = ("1m", "hktransaction", "hkbrokerqueueex")
DEFAULT_XTQUANT_DATA_HOME = Path("/home/hliu/xtbackend/.runtime/xtquant")
DEFAULT_XTQUANT_CONFIG = Path("/home/hliu/beast/services/mammoth/historical-ingestion-service/config/bronze_ingest_routine.yaml")
DEFAULT_XTQUANT_ALLOW_OPTIMIZE_ADDRESSES = (
    "42.228.16.210:55300",
    "42.228.16.211:55300",
    "115.231.218.12:55300",
    "115.231.218.13:55300",
)


@dataclass
class XtQuantMarketDataStats:
    starts: int = 0
    stops: int = 0
    subscribe_calls: int = 0
    unsubscribe_calls: int = 0
    callbacks_received: int = 0
    callbacks_enqueued: int = 0
    callback_rejections: int = 0
    failed: int = 0
    errors: list[str] = field(default_factory=list)


class DeferredCallbackSink:
    """Mutable callback bridge used when the ingest worker is created later."""

    def __init__(self) -> None:
        self.sink: CallbackSink | None = None

    def set(self, sink: CallbackSink) -> None:
        self.sink = sink

    def __call__(self, payload: dict[str, Any]) -> bool:
        if self.sink is None:
            return False
        accepted = self.sink(payload)
        return accepted is not False


class XtQuantMarketDataClient:
    """Thin xtdata subscription adapter for the production runtime.

    The callback path only wraps xtdata payloads with symbol/period metadata and
    forwards them to the configured sink. Normalization, Redis writes, business
    computation, and Kafka publishing stay outside the SDK callback thread.
    """

    def __init__(
        self,
        *,
        xtdata_module: Any | None = None,
        xtdatacenter_module: Any | None = None,
        callback_sink: CallbackSink | None = None,
        periods: Iterable[str] | None = None,
        sdk_path: str | Path | None = None,
        data_home: str | Path = DEFAULT_XTQUANT_DATA_HOME,
        token_env: str = "XTQUANT_TOKEN",
        token_config: str | Path = DEFAULT_XTQUANT_CONFIG,
        port: int = 58628,
        connect_retries: int = 30,
        port_retry_count: int = 5,
    ) -> None:
        self.xtdata = xtdata_module
        self.xtdatacenter = xtdatacenter_module
        self.callback_sink = callback_sink
        self.periods = tuple(periods or DEFAULT_XTQUANT_PERIODS)
        self.sdk_path = Path(sdk_path) if sdk_path is not None else None
        self.data_home = Path(data_home)
        self.token_env = token_env
        self.token_config = Path(token_config)
        self.port = port
        self.connect_retries = connect_retries
        self.port_retry_count = port_retry_count
        if not self.periods:
            raise ValueError("periods must not be empty")
        self.stats = XtQuantMarketDataStats()
        self.running = False
        self.subscribed_symbols: set[str] = set()
        self.subscription_handles: dict[tuple[str, str], Any] = {}

    def start(self) -> None:
        module = self._xtdata()
        original_port = self.port
        last_error: Exception | None = None
        try:
            for candidate_port in range(original_port, original_port + max(1, self.port_retry_count)):
                self.port = candidate_port
                try:
                    self._start_datacenter()
                    connect_xtdata(module, self.port, retries=self.connect_retries)
                    break
                except Exception as error:
                    last_error = error
                    if not is_retryable_xtquant_start_error(error):
                        raise
            else:
                raise RuntimeError(f"failed to start xtquant datacenter: {last_error}")
            if hasattr(module, "enable_hello"):
                module.enable_hello = False
            self.running = True
            self.stats.starts += 1
        except Exception as error:
            self._record_failure(error)
            raise

    def stop(self) -> None:
        try:
            self.subscription_handles.clear()
            self.subscribed_symbols.clear()
            self.running = False
            self.stats.stops += 1
        except Exception as error:
            self._record_failure(error)
            raise

    def subscribe(self, raw_symbol: str) -> None:
        symbol = normalize_subscription_symbol(raw_symbol)
        try:
            if symbol in self.subscribed_symbols:
                return
            for period in self.periods:
                handle = self._subscribe_period(symbol, period)
                self.subscription_handles[(symbol, period)] = handle
            self.subscribed_symbols.add(symbol)
            self.stats.subscribe_calls += 1
        except Exception as error:
            self._record_failure(error)
            raise

    def get_full_ticks(self, raw_symbols: Iterable[str]) -> dict[str, dict[str, Any]]:
        module = self._xtdata()
        get_full_tick = getattr(module, "get_full_tick", None)
        if not callable(get_full_tick):
            return {}
        symbols = [normalize_subscription_symbol(symbol) for symbol in raw_symbols]
        if not symbols:
            return {}
        data = get_full_tick(symbols)
        if not isinstance(data, dict):
            return {}
        ticks: dict[str, dict[str, Any]] = {}
        for raw_symbol, value in data.items():
            if not isinstance(raw_symbol, str) or not isinstance(value, dict):
                continue
            try:
                symbol = normalize_subscription_symbol(raw_symbol)
            except ValueError:
                continue
            ticks[symbol] = value
        return ticks

    def get_minute_bars(self, raw_symbol: str, trade_date: str) -> list[dict[str, Any]]:
        """Fetch today's native xtquant 1m bars for startup/cold hydration backfill."""

        module = self._xtdata()
        symbol = normalize_subscription_symbol(raw_symbol)
        download = getattr(module, "download_history_data", None)
        if callable(download):
            try:
                download(symbol, "1m", start_time=trade_date, end_time=trade_date)
            except Exception:
                pass
        get_market_data_ex = getattr(module, "get_market_data_ex", None)
        if not callable(get_market_data_ex):
            return []
        try:
            data = get_market_data_ex(
                [],
                [symbol],
                "1m",
                start_time=trade_date,
                end_time=trade_date,
                count=-1,
                fill_data=False,
            )
        except Exception:
            return []
        frame = data.get(symbol) if isinstance(data, dict) else None
        records = frame_records(frame)
        bars = [normalize_xtquant_1m_record(record, trade_date) for record in records]
        return [bar for bar in bars if bar is not None]

    def unsubscribe(self, raw_symbol: str) -> None:
        symbol = normalize_subscription_symbol(raw_symbol)
        try:
            for period in self.periods:
                handle = self.subscription_handles.pop((symbol, period), None)
                self._unsubscribe_period(symbol, period, handle)
            self.subscribed_symbols.discard(symbol)
            self.stats.unsubscribe_calls += 1
        except Exception as error:
            self._record_failure(error)
            raise

    def stats_snapshot(self) -> dict[str, Any]:
        return {
            **asdict(self.stats),
            "running": self.running,
            "subscribed_symbols": sorted(self.subscribed_symbols),
            "periods": list(self.periods),
            "subscription_count": len(self.subscription_handles),
            "sdk_path": str(self.sdk_path) if self.sdk_path is not None else "",
            "data_home": str(self.data_home),
            "port": self.port,
        }

    def _xtdata(self) -> Any:
        if self.xtdata is None:
            if self.sdk_path is not None:
                sys.path.insert(0, str(self.sdk_path))
            try:
                self.xtdata = importlib.import_module("xtquant.xtdata")
            except ImportError as error:
                raise RuntimeError("xtquant.xtdata is required for live market subscriptions") from error
        return self.xtdata

    def _xtdatacenter(self) -> Any:
        if self.xtdatacenter is None:
            if self.sdk_path is not None:
                sys.path.insert(0, str(self.sdk_path))
            try:
                self.xtdatacenter = importlib.import_module("xtquant.xtdatacenter")
            except ImportError as error:
                raise RuntimeError("xtquant.xtdatacenter is required for live market subscriptions") from error
        return self.xtdatacenter

    def _start_datacenter(self) -> None:
        xtdc = self._xtdatacenter()
        token = os.environ.get(self.token_env) or read_token_from_config(self.token_config)
        if token:
            call_optional_with_args(xtdc, "set_token", token)
        call_optional_with_args(xtdc, "set_allow_optmize_address", list(DEFAULT_XTQUANT_ALLOW_OPTIMIZE_ADDRESSES))
        call_optional_with_args(xtdc, "set_data_home_dir", str(self.data_home))
        call_optional_with_args(xtdc, "init", False)
        listen = getattr(xtdc, "listen", None)
        if callable(listen):
            try:
                listen(port=self.port)
            except OSError as error:
                if not is_xtquant_port_in_use_error(error):
                    raise

    def _subscribe_period(self, symbol: str, period: str) -> Any:
        module = self._xtdata()
        subscribe = getattr(module, "subscribe_quote", None)
        if not callable(subscribe):
            raise RuntimeError("xtdata.subscribe_quote is required for live market subscriptions")
        callback = self._callback(symbol, period)
        return call_subscribe_quote(subscribe, symbol=symbol, period=period, callback=callback)

    def _unsubscribe_period(self, symbol: str, period: str, handle: Any) -> None:
        module = self._xtdata()
        unsubscribe = getattr(module, "unsubscribe_quote", None)
        if not callable(unsubscribe):
            return
        call_unsubscribe_quote(unsubscribe, symbol=symbol, period=period, handle=handle)

    def _callback(self, symbol: str, period: str) -> Callable[..., None]:
        def on_data(*args: Any, **kwargs: Any) -> None:
            raw_payload = callback_raw_payload(args, kwargs)
            for data in callback_data_items(raw_payload, symbol):
                self.stats.callbacks_received += 1
                payload = {"symbol": symbol, "period": period, "data": data}
                accepted = self.callback_sink(payload) if self.callback_sink is not None else False
                if accepted is False:
                    self.stats.callback_rejections += 1
                else:
                    self.stats.callbacks_enqueued += 1

        return on_data

    def _record_failure(self, error: Exception) -> None:
        self.stats.failed += 1
        self.stats.errors.append(str(error))


def call_optional(target: Any, name: str) -> None:
    candidate = getattr(target, name, None)
    if callable(candidate):
        candidate()


def call_optional_with_args(target: Any, name: str, *args: Any) -> None:
    candidate = getattr(target, name, None)
    if callable(candidate):
        candidate(*args)


def connect_xtdata(xtdata: Any, port: int, *, retries: int) -> None:
    connect = getattr(xtdata, "connect", None)
    if not callable(connect):
        raise RuntimeError("xtdata.connect is required for live market subscriptions")
    last_error: Exception | None = None
    for _ in range(max(1, retries)):
        try:
            connect("127.0.0.1", port)
            return
        except Exception as error:
            last_error = error
            time.sleep(1)
    raise RuntimeError(f"failed to connect xtquant datacenter on {port}: {last_error}")


def is_xtquant_port_in_use_error(error: OSError) -> bool:
    message = str(error)
    return "监听端口失败" in message or "address already in use" in message.lower()


def is_retryable_xtquant_start_error(error: Exception) -> bool:
    message = str(error)
    return (
        is_xtquant_port_in_use_error(error) if isinstance(error, OSError) else False
    ) or "failed to connect xtquant datacenter" in message or "无法连接xtquant服务" in message


def read_token_from_config(path: Path) -> str:
    if not path.exists():
        return ""
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("token:"):
            return stripped.split(":", 1)[1].strip().strip("'\"")
    return ""


def call_subscribe_quote(subscribe: Callable[..., Any], *, symbol: str, period: str, callback: Callable[..., None]) -> Any:
    attempts = [
        lambda: call_with_supported_kwargs(
            subscribe,
            stock_code=symbol,
            code=symbol,
            symbol=symbol,
            period=period,
            start_time="",
            end_time="",
            count=0,
            callback=callback,
        ),
        lambda: subscribe(symbol, period=period, start_time="", end_time="", count=0, callback=callback),
        lambda: subscribe(symbol, period=period, callback=callback),
        lambda: subscribe(symbol, period, "", "", 0, callback),
        lambda: subscribe(symbol, period, callback),
    ]
    return call_first_success(attempts)


def call_unsubscribe_quote(unsubscribe: Callable[..., Any], *, symbol: str, period: str, handle: Any) -> None:
    attempts: list[Callable[[], Any]] = []
    if handle is not None:
        attempts.append(lambda: unsubscribe(handle))
    attempts.extend(
        [
            lambda: call_with_supported_kwargs(unsubscribe, seq=handle, subscribe_id=handle, stock_code=symbol, code=symbol, symbol=symbol, period=period),
            lambda: unsubscribe(symbol, period=period),
            lambda: unsubscribe(symbol, period),
        ]
    )
    call_first_success(attempts)


def call_with_supported_kwargs(func: Callable[..., Any], **candidates: Any) -> Any:
    try:
        signature = inspect.signature(func)
    except (TypeError, ValueError):
        return func(**candidates)
    params = signature.parameters
    if any(param.kind == inspect.Parameter.VAR_KEYWORD for param in params.values()):
        return func(**candidates)
    kwargs: dict[str, Any] = {}
    symbol_names = ("stock_code", "code", "symbol")
    for symbol_name in symbol_names:
        if symbol_name in params and symbol_name in candidates:
            kwargs[symbol_name] = candidates[symbol_name]
            break
    for name in ("period", "start_time", "end_time", "count", "callback", "seq", "subscribe_id"):
        if name in params and name in candidates:
            kwargs[name] = candidates[name]
    return func(**kwargs)


def call_first_success(attempts: list[Callable[[], Any]]) -> Any:
    last_error: Exception | None = None
    for attempt in attempts:
        try:
            return attempt()
        except TypeError as error:
            last_error = error
    if last_error is not None:
        raise last_error
    return None


def callback_raw_payload(args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
    if "data" in kwargs:
        return kwargs["data"]
    if len(args) == 1:
        return args[0]
    if args:
        return args[-1]
    return kwargs


def callback_data_items(raw_payload: Any, symbol: str) -> list[dict[str, Any]]:
    if isinstance(raw_payload, list):
        return [item for item in raw_payload if isinstance(item, dict)]
    if isinstance(raw_payload, tuple):
        return [item for item in raw_payload if isinstance(item, dict)]
    if not isinstance(raw_payload, dict):
        return []

    normalized_symbol = normalize_subscription_symbol(symbol)
    for key, value in raw_payload.items():
        if not isinstance(key, str):
            continue
        try:
            key_symbol = normalize_subscription_symbol(key)
        except ValueError:
            continue
        if key_symbol != normalized_symbol:
            continue
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
        if isinstance(value, dict):
            return [value]
    return [raw_payload]


HK_TZ = timezone(timedelta(hours=8))


def frame_records(frame: Any) -> list[dict[str, Any]]:
    if frame is None:
        return []
    candidate = frame
    reset_index = getattr(candidate, "reset_index", None)
    if callable(reset_index):
        try:
            candidate = reset_index()
        except Exception:
            candidate = frame
    to_dict = getattr(candidate, "to_dict", None)
    if callable(to_dict):
        try:
            records = to_dict("records")
        except TypeError:
            records = to_dict()
        if isinstance(records, list):
            return [record for record in records if isinstance(record, dict)]
    if isinstance(candidate, list):
        return [record for record in candidate if isinstance(record, dict)]
    return []


def normalize_xtquant_1m_record(record: dict[str, Any], trade_date: str) -> dict[str, Any] | None:
    close = first_numeric(record, "close", "Close", "price", "Price", "lastPrice", "last_price")
    if close is None or close <= 0:
        return None
    timestamp = minute_record_timestamp(record, trade_date)
    volume = first_numeric(record, "volume", "Volume", "qty", "quantity")
    turnover = first_numeric(record, "amount", "Amount", "turnover", "Turnover")
    return {
        "timestamp": timestamp,
        "open": first_numeric(record, "open", "Open") or close,
        "high": first_numeric(record, "high", "High") or close,
        "low": first_numeric(record, "low", "Low") or close,
        "close": close,
        "price": close,
        "volume": int(volume or 0),
        "turnover": float(turnover or 0.0),
    }


def minute_record_timestamp(record: dict[str, Any], trade_date: str) -> str:
    for key in ("bar_ts", "timestamp", "Timestamp", "datetime", "Datetime"):
        value = record.get(key)
        if isinstance(value, str) and "T" in value:
            return value
    time_value = record.get("time")
    numeric_time = first_numeric(record, "time", "Time")
    if numeric_time and numeric_time > 10_000_000_000:
        return datetime.fromtimestamp(numeric_time / 1000, tz=timezone.utc).astimezone(HK_TZ).isoformat(timespec="milliseconds")
    index_value = str(record.get("index") or record.get("Index") or "")
    if len(index_value) >= 12 and index_value[:12].isdigit():
        return datetime.strptime(index_value[:12], "%Y%m%d%H%M").replace(tzinfo=HK_TZ).isoformat(timespec="milliseconds")
    time_text = str(time_value or record.get("Time") or "").replace(":", "")
    if len(time_text) >= 4 and time_text[:4].isdigit():
        return datetime.strptime(f"{trade_date}{time_text[:4]}", "%Y%m%d%H%M").replace(tzinfo=HK_TZ).isoformat(timespec="milliseconds")
    return datetime.now(timezone.utc).astimezone(HK_TZ).isoformat(timespec="milliseconds")


def first_numeric(data: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = data.get(key)
        if value in (None, ""):
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None
