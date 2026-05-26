from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .adapters import validate_snapshot_key_inputs


@dataclass(frozen=True)
class RuntimeStatePaths:
    directory: Path
    raw_events_path: Path
    processed_events_path: Path
    alerts_path: Path


@dataclass(frozen=True)
class RuntimeStateClearResult:
    dry_run: bool
    confirmed: bool
    paths: list[str]
    deleted_paths: list[str]


class RuntimeStateStore:
    """File-backed same-day runtime state for recovery and audit."""

    def __init__(self, root: str | Path = "artifacts/runtime-state") -> None:
        self.root = Path(root)

    def paths_for(self, trade_date: str, symbol: str) -> RuntimeStatePaths:
        validate_snapshot_key_inputs(trade_date, symbol)
        directory = self.root / trade_date / symbol
        return RuntimeStatePaths(
            directory=directory,
            raw_events_path=directory / "raw-events.jsonl",
            processed_events_path=directory / "processed-events.jsonl",
            alerts_path=directory / "alerts.jsonl",
        )

    def append_raw_event(self, trade_date: str, symbol: str, event: dict[str, Any]) -> None:
        self._append_jsonl(self.paths_for(trade_date, symbol).raw_events_path, event)

    def callback_rejections_path(self, trade_date: str) -> Path:
        if not isinstance(trade_date, str) or not trade_date.isdigit() or len(trade_date) != 8:
            raise ValueError("trade_date must use YYYYMMDD format")
        return self.root / trade_date / "callback-rejections.jsonl"

    def raw_consumer_dead_letters_path(self, trade_date: str) -> Path:
        if not isinstance(trade_date, str) or not trade_date.isdigit() or len(trade_date) != 8:
            raise ValueError("trade_date must use YYYYMMDD format")
        return self.root / trade_date / "raw-consumer-dead-letters.jsonl"

    def append_callback_rejection(self, trade_date: str, payload: dict[str, Any], reason: str) -> None:
        if not isinstance(payload, dict):
            raise ValueError("callback rejection payload must be an object")
        self._append_jsonl(
            self.callback_rejections_path(trade_date),
            {
                "schema_version": 1,
                "reason": reason,
                "payload": payload,
            },
        )

    def append_raw_consumer_dead_letter(self, trade_date: str, *, topic: str, key: str, value: dict[str, Any], reason: str) -> None:
        if not isinstance(value, dict):
            raise ValueError("raw consumer dead letter value must be an object")
        self._append_jsonl(
            self.raw_consumer_dead_letters_path(trade_date),
            {
                "schema_version": 1,
                "topic": topic,
                "key": key,
                "reason": reason,
                "value": value,
            },
        )

    def append_processed_event(self, trade_date: str, symbol: str, event: dict[str, Any]) -> None:
        paths = self.paths_for(trade_date, symbol)
        self._append_jsonl(paths.processed_events_path, event)
        if event.get("result_type") == "big_trade_alert":
            alert = event.get("payload", {}).get("alert") if isinstance(event.get("payload"), dict) else None
            if isinstance(alert, dict):
                self._append_jsonl(paths.alerts_path, alert)

    def load_raw_events(self, trade_date: str, symbol: str) -> list[dict[str, Any]]:
        return self._load_jsonl(self.paths_for(trade_date, symbol).raw_events_path)

    def load_processed_events(self, trade_date: str, symbol: str) -> list[dict[str, Any]]:
        return self._load_jsonl(self.paths_for(trade_date, symbol).processed_events_path)

    def load_alerts(self, trade_date: str, symbol: str) -> list[dict[str, Any]]:
        return self._load_jsonl(self.paths_for(trade_date, symbol).alerts_path)

    def clear(
        self,
        trade_date: str,
        symbols: list[str],
        *,
        dry_run: bool = True,
        confirm: bool = False,
        include_callback_rejections: bool = False,
        include_dead_letters: bool = False,
    ) -> RuntimeStateClearResult:
        return clear_runtime_state_files(
            self.root,
            trade_date=trade_date,
            symbols=symbols,
            dry_run=dry_run,
            confirm=confirm,
            include_callback_rejections=include_callback_rejections,
            include_dead_letters=include_dead_letters,
        )

    def _append_jsonl(self, path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, separators=(",", ":"), sort_keys=True, ensure_ascii=False) + "\n")

    def _load_jsonl(self, path: Path) -> list[dict[str, Any]]:
        if not path.exists():
            return []
        rows: list[dict[str, Any]] = []
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if not line.strip():
                continue
            decoded = json.loads(line)
            if not isinstance(decoded, dict):
                raise ValueError(f"{path} line {line_number} must contain a JSON object")
            rows.append(decoded)
        return rows


def clear_runtime_state_files(
    root: str | Path,
    *,
    trade_date: str,
    symbols: list[str],
    dry_run: bool = True,
    confirm: bool = False,
    include_callback_rejections: bool = False,
    include_dead_letters: bool = False,
) -> RuntimeStateClearResult:
    paths: list[Path] = []
    for symbol in symbols:
        validate_snapshot_key_inputs(trade_date, symbol)
        directory = Path(root) / trade_date / symbol
        paths.extend(
            [
                directory / "raw-events.jsonl",
                directory / "processed-events.jsonl",
                directory / "alerts.jsonl",
            ]
        )
    if include_callback_rejections:
        if not isinstance(trade_date, str) or not trade_date.isdigit() or len(trade_date) != 8:
            raise ValueError("trade_date must use YYYYMMDD format")
        paths.append(Path(root) / trade_date / "callback-rejections.jsonl")
    if include_dead_letters:
        if not isinstance(trade_date, str) or not trade_date.isdigit() or len(trade_date) != 8:
            raise ValueError("trade_date must use YYYYMMDD format")
        paths.append(Path(root) / trade_date / "raw-consumer-dead-letters.jsonl")
    existing = [path for path in paths if path.exists()]
    if not dry_run and not confirm:
        raise ValueError("clear-runtime-state requires --confirm when not running as dry-run")
    deleted: list[Path] = []
    if not dry_run:
        for path in existing:
            path.unlink()
            deleted.append(path)
    return RuntimeStateClearResult(
        dry_run=dry_run,
        confirmed=confirm,
        paths=[str(path) for path in paths],
        deleted_paths=[str(path) for path in deleted],
    )
