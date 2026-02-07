import asyncio
from collections import defaultdict
from datetime import datetime
from typing import Dict, Tuple
import contextlib

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

from .config import settings
from .io.db import init_tables
from .domain.windows import PriceWindow
from .services.ingest import price_ingest_task, news_ingest_task
from .utils import with_retries
from .logging_config import get_logger
from .services.runtime import log_startup_config
from .services.health import healthcheck
from .services.price_consumer import consume_prices


class StreamProcessor:
    def __init__(self):
        self.producer: AIOKafkaProducer | None = None
        self.consumer: AIOKafkaConsumer | None = None
        self.price_windows: Dict[str, PriceWindow] = defaultdict(PriceWindow)
        self.last_alert: Dict[Tuple[str, str], datetime] = {}
        self.latest_headline: Tuple[str | None, float | None, datetime | None] = (None, None, None)
        self.bad_price_messages = 0
        self.bad_price_log_every = settings.bad_price_log_every
        self.last_price_ts: Dict[str, datetime] = {}
        self.late_price_messages = 0
        self.late_price_log_every = settings.late_price_log_every
        self.log = get_logger(__name__)

    async def start(self):
        self.log.info("processor_starting", extra={"component": "processor"})
        log_startup_config(self.log)
        await healthcheck(self.log)
        await init_tables()
        self.producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_brokers)
        await self.producer.start()
        self.consumer = AIOKafkaConsumer(
            settings.price_topic,
            bootstrap_servers=settings.kafka_brokers,
            group_id="processor",
            enable_auto_commit=False,
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

    async def process_prices_task(self):
        await consume_prices(self)

    async def commit_msg(self, msg):
        try:
            await with_retries(
                self.consumer.commit,
                log=self.log,
                op="commit_price",
            )
        except Exception as exc:
            self.log.warning("price_commit_failed", extra={"error": str(exc), "offset": msg.offset})

    async def send_price_dlq(self, payload: bytes):
        if not self.producer:
            return
        with contextlib.suppress(Exception):
            await self.producer.send_and_wait(settings.price_dlq_topic, payload)
