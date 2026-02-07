from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Protocol, Tuple

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

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
