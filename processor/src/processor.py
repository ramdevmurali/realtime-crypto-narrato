import asyncio
import json
from collections import defaultdict
from datetime import datetime
from typing import Dict, Tuple

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from dateutil import parser as dateparser

from .config import settings
from .db import init_tables, insert_price, insert_metric
from .utils import now_utc
from .windows import PriceWindow
from .ingest import price_ingest_task, news_ingest_task
from .metrics import compute_metrics
from .anomaly import check_anomalies


class StreamProcessor:
    def __init__(self):
        self.producer: AIOKafkaProducer | None = None
        self.consumer: AIOKafkaConsumer | None = None
        self.price_windows: Dict[str, PriceWindow] = defaultdict(PriceWindow)
        self.last_alert: Dict[Tuple[str, str], datetime] = {}
        self.latest_headline: Tuple[str | None, float | None] = (None, None)

    async def start(self):
        await init_tables()
        self.producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_brokers)
        await self.producer.start()
        self.consumer = AIOKafkaConsumer(
            settings.price_topic,
            bootstrap_servers=settings.kafka_brokers,
            group_id="processor",
            enable_auto_commit=True,
            auto_offset_reset="latest",
        )
        await self.consumer.start()

        await asyncio.gather(
            price_ingest_task(self),
            news_ingest_task(self),
            self.process_prices_task(),
        )

    async def stop(self):
        if self.consumer:
            await self.consumer.stop()
        if self.producer:
            await self.producer.stop()

    async def process_prices_task(self):
        """Consume prices from Kafka, compute metrics, check anomalies."""
        assert self.consumer
        async for msg in self.consumer:
            data = json.loads(msg.value.decode())
            symbol = data.get('symbol')
            price = float(data.get('price'))
            ts = dateparser.parse(data.get('time')) if data.get('time') else now_utc()

            await insert_price(ts, symbol, price)
            win = self.price_windows[symbol]
            win.add(ts, price)
            metrics = compute_metrics(self.price_windows, symbol, ts)
            if metrics:
                await insert_metric(ts, symbol, metrics)
            await check_anomalies(self, symbol, ts, metrics or {})
