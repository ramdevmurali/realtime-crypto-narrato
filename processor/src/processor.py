import asyncio
import json
from collections import defaultdict
from datetime import datetime
from typing import Dict, Tuple

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from dateutil import parser as dateparser

import contextlib

from .config import settings
from .db import init_tables, insert_price, insert_metric, get_pool
from .utils import now_utc, with_retries
from .windows import PriceWindow
from .ingest import price_ingest_task, news_ingest_task
from .metrics import compute_metrics
from .anomaly import check_anomalies
from .logging_config import get_logger


class StreamProcessor:
    def __init__(self):
        self.producer: AIOKafkaProducer | None = None
        self.consumer: AIOKafkaConsumer | None = None
        self.price_windows: Dict[str, PriceWindow] = defaultdict(PriceWindow)
        self.last_alert: Dict[Tuple[str, str], datetime] = {}
        self.latest_headline: Tuple[str | None, float | None] = (None, None)
        self.bad_price_messages = 0
        self.log = get_logger(__name__)

    async def start(self):
        self.log.info("processor_starting", extra={"component": "processor"})
        await self.healthcheck()
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

        tasks = [
            asyncio.create_task(price_ingest_task(self)),
            asyncio.create_task(news_ingest_task(self)),
            asyncio.create_task(self.process_prices_task()),
        ]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            self.log.info("processor_cancelled")
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def stop(self):
        if self.consumer:
            await self.consumer.stop()
        if self.producer:
            await self.producer.stop()
        self.log.info("processor_stopped", extra={"component": "processor"})

    async def healthcheck(self):
        # DB check
        try:
            pool = await get_pool()
            async with pool.acquire() as conn:
                await conn.fetchval("SELECT 1;")
            self.log.info("db_health_ok")
        except Exception as exc:
            self.log.error("db_health_fail", extra={"error": str(exc)})
            raise

        # Kafka check
        temp_prod = AIOKafkaProducer(bootstrap_servers=settings.kafka_brokers)
        try:
            await temp_prod.start()
            self.log.info("kafka_health_ok")
        except Exception as exc:
            self.log.error("kafka_health_fail", extra={"error": str(exc)})
            raise
        finally:
            with contextlib.suppress(Exception):
                await temp_prod.stop()

    async def process_prices_task(self):
        """Consume prices from Kafka, compute metrics, check anomalies."""
        assert self.consumer
        async for msg in self.consumer:
            try:
                data = json.loads(msg.value.decode())
            except json.JSONDecodeError:
                self.bad_price_messages += 1
                self.log.warning(
                    "price_message_decode_failed",
                    extra={"raw": msg.value, "bad_price_messages": self.bad_price_messages},
                )
                if self.producer:
                    with contextlib.suppress(Exception):
                        await self.producer.send_and_wait(settings.price_dlq_topic, msg.value)
                continue
            symbol = data.get('symbol')
            price = float(data.get('price'))
            ts = dateparser.parse(data.get('time')) if data.get('time') else now_utc()

            await with_retries(insert_price, ts, symbol, price, log=self.log, op="insert_price")
            win = self.price_windows[symbol]
            win.add(ts, price)
            metrics = compute_metrics(self.price_windows, symbol, ts)
            if metrics:
                await with_retries(insert_metric, ts, symbol, metrics, log=self.log, op="insert_metric")
            await check_anomalies(self, symbol, ts, metrics or {})


async def main():
    processor = StreamProcessor()
    try:
        await processor.start()
    except KeyboardInterrupt:
        processor.log.info("processor_shutdown_requested")
    finally:
        await processor.stop()


if __name__ == "__main__":
    asyncio.run(main())
