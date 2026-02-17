import asyncio
from datetime import datetime
import contextlib

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

from .config import settings
from .io.db import init_tables
from .services.ingest import price_ingest_task, news_ingest_task
from .utils import with_retries
from .logging_config import get_logger
from .services.runtime import log_startup_config
from .services.health import healthcheck
from .services.price_consumer import consume_prices
from .runtime_interface import RuntimeService
from .processor_state import ProcessorStateImpl


class StreamProcessor(ProcessorStateImpl, RuntimeService):
    def __init__(self):
        super().__init__(log=get_logger(__name__))

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
            group_id=settings.processor_consumer_group,
            enable_auto_commit=False,
            auto_offset_reset=settings.kafka_auto_offset_reset,
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
