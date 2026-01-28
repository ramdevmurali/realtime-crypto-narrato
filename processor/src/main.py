import asyncio
import json
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Deque, Dict, Tuple

import feedparser
import websockets
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from dateutil import parser as dateparser

from .config import settings
from .db import init_tables, insert_price, insert_metric, insert_headline, insert_anomaly
from .utils import now_utc, simple_sentiment, llm_summarize


class PriceWindow:
    def __init__(self):
        self.buffer: Deque[Tuple[datetime, float]] = deque()

    def add(self, ts: datetime, price: float):
        self.buffer.append((ts, price))


class StreamProcessor:
    def __init__(self):
        self.producer: AIOKafkaProducer | None = None
        self.consumer: AIOKafkaConsumer | None = None
        self.price_windows: Dict[str, PriceWindow] = defaultdict(PriceWindow)
        self.last_alert: Dict[Tuple[str, str], datetime] = {}
        self.latest_headline: Tuple[str | None, float | None] = (None, None)

    async def start(self):
        # will be filled with startup logic
        ...

    async def stop(self):
        # will be filled with teardown logic
        ...


def main():
    processor = StreamProcessor()
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(processor.start())
    except KeyboardInterrupt:
        loop.run_until_complete(processor.stop())


if __name__ == "__main__":
    main()
