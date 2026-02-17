from __future__ import annotations

from dataclasses import dataclass, field
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, Protocol, Tuple

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from .config import settings
from .domain.windows import PriceWindow


class ProcessorState(Protocol):
    producer: AIOKafkaProducer | None
    consumer: AIOKafkaConsumer | None
    price_windows: Dict[str, PriceWindow]
    last_alert: Dict[Tuple[str, str], datetime]
    latest_headline: Tuple[str | None, float | None, datetime | None]
    log: Any

    bad_price_messages: int
    bad_price_log_every: int
    last_price_ts: Dict[str, datetime]
    late_price_messages: int
    late_price_log_every: int
    alerts_emitted: int

    async def commit_msg(self, msg) -> None: ...

    async def send_price_dlq(self, payload: bytes) -> None: ...

    def record_latest_headline(self, title: str | None, sentiment: float | None, ts: datetime | None) -> None: ...

    def record_alert(self, symbol: str, window: str, ts: datetime) -> None: ...

    def should_drop_late(self, symbol: str, ts: datetime) -> bool: ...

    def record_bad_price(self) -> bool: ...


@dataclass
class ProcessorStateImpl:
    producer: AIOKafkaProducer | None = None
    consumer: AIOKafkaConsumer | None = None
    price_windows: Dict[str, PriceWindow] = field(default_factory=lambda: defaultdict(PriceWindow))
    last_alert: Dict[Tuple[str, str], datetime] = field(default_factory=dict)
    latest_headline: Tuple[str | None, float | None, datetime | None] = (None, None, None)
    log: Any = None

    bad_price_messages: int = 0
    bad_price_log_every: int = settings.bad_price_log_every
    last_price_ts: Dict[str, datetime] = field(default_factory=dict)
    late_price_messages: int = 0
    late_price_log_every: int = settings.late_price_log_every
    alerts_emitted: int = 0

    def record_latest_headline(self, title: str | None, sentiment: float | None, ts: datetime | None) -> None:
        self.latest_headline = (title, sentiment, ts)

    def record_alert(self, symbol: str, window: str, ts: datetime) -> None:
        self.last_alert[(symbol, window)] = ts
        self.alerts_emitted += 1

    def should_drop_late(self, symbol: str, ts: datetime) -> bool:
        last_ts = self.last_price_ts.get(symbol)
        tolerance = timedelta(seconds=settings.late_price_tolerance_sec)
        if last_ts and ts < last_ts - tolerance:
            self.late_price_messages += 1
            return True
        if last_ts is None or ts > last_ts:
            self.last_price_ts[symbol] = ts
        return False

    def record_bad_price(self) -> bool:
        self.bad_price_messages += 1
        return self.bad_price_messages == 1 or self.bad_price_messages % self.bad_price_log_every == 0
