from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Any


REALTIME_COALESCIBLE_TYPES = {"tick_realtime", "queue_realtime"}
CRITICAL_RESPONSE_TYPES = {"snapshot", "holding_name_click_response"}


@dataclass
class ClientQueueStats:
    enqueued: int = 0
    coalesced: int = 0
    dropped: int = 0
    alerts_enqueued: int = 0
    alert_dropped: int = 0
    alert_overflow: int = 0
    critical_overflow: int = 0


@dataclass
class GatewayClientQueue:
    """Per-client queue with plan-aligned slow-client isolation semantics."""

    client_id: str
    max_size: int = 100
    queue: deque[dict[str, Any]] = field(default_factory=deque)
    stats: ClientQueueStats = field(default_factory=ClientQueueStats)

    def enqueue(self, message: dict[str, Any]) -> None:
        message_type = str(message.get("type", ""))
        symbol = str(message.get("symbol", ""))
        if message_type in REALTIME_COALESCIBLE_TYPES and self._replace_latest(message_type, symbol, message):
            self.stats.coalesced += 1
            return

        if len(self.queue) >= self.max_size:
            if message_type == "alert_realtime":
                if self._drop_one_non_alert():
                    self.stats.alert_overflow += 1
                else:
                    self.stats.alert_dropped += 1
                    return
            elif message_type in CRITICAL_RESPONSE_TYPES:
                if self._drop_one_non_critical():
                    self.stats.critical_overflow += 1
                else:
                    self.stats.dropped += 1
                    return
            else:
                self.stats.dropped += 1
                return

        if len(self.queue) >= self.max_size:
            if message_type == "alert_realtime":
                self.stats.alert_dropped += 1
                return
            self.stats.dropped += 1
            return

        self.queue.append(message)
        self.stats.enqueued += 1
        if message_type == "alert_realtime":
            self.stats.alerts_enqueued += 1

    def drain(self, limit: int | None = None) -> list[dict[str, Any]]:
        drained = []
        while self.queue and (limit is None or len(drained) < limit):
            drained.append(self.queue.popleft())
        return drained

    def _replace_latest(self, message_type: str, symbol: str, message: dict[str, Any]) -> bool:
        for index in range(len(self.queue) - 1, -1, -1):
            queued = self.queue[index]
            if queued.get("type") == message_type and queued.get("symbol") == symbol:
                self.queue[index] = message
                return True
        return False

    def _drop_one_non_alert(self) -> bool:
        for index, queued in enumerate(self.queue):
            if queued.get("type") != "alert_realtime":
                del self.queue[index]
                self.stats.dropped += 1
                return True
        return False

    def _drop_one_non_critical(self) -> bool:
        for index, queued in enumerate(self.queue):
            if queued.get("type") not in CRITICAL_RESPONSE_TYPES and queued.get("type") != "alert_realtime":
                del self.queue[index]
                self.stats.dropped += 1
                return True
        return False
