import asyncio
import json
import time
import uuid
from datetime import datetime, timezone

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from processor.src.config import settings
from processor.src.io.models.messages import NewsMsg
from processor.src.io.db import get_pool, init_pool
from processor.src.services.sentiment_sidecar import SentimentSidecar
from processor.src.utils import simple_sentiment


async def _wait_for_headline(title: str, timeout_sec: int = 10):
    deadline = time.monotonic() + timeout_sec
    await init_pool()
    pool = await get_pool()
    while time.monotonic() < deadline:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT sentiment FROM headlines WHERE title=$1 ORDER BY time DESC LIMIT 1",
                title,
            )
        if row:
            return float(row["sentiment"])
        await asyncio.sleep(0.5)
    raise AssertionError("timeout waiting for headline")


async def _start_sidecar():
    sidecar = SentimentSidecar()
    task = asyncio.create_task(sidecar.start())
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if sidecar._consumer and sidecar._consumer.assignment():
            break
        await asyncio.sleep(0.1)
    return sidecar, task


async def _stop_sidecar(sidecar, task):
    await sidecar.stop()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_sentiment_sidecar_integration(integration_services, integration_settings):
    sidecar, task = await _start_sidecar()
    title = f"test headline {uuid.uuid4().hex}"
    msg = NewsMsg(
        time=datetime.now(timezone.utc),
        title=title,
        url=None,
        source="rss",
        sentiment=0.0,
    )

    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_brokers)
    await producer.start()
    try:
        enriched_consumer = AIOKafkaConsumer(
            settings.news_enriched_topic,
            bootstrap_servers=settings.kafka_brokers,
            group_id=f"news-enriched-{uuid.uuid4().hex}",
            auto_offset_reset="earliest",
        )
        await enriched_consumer.start()
        try:
            await producer.send_and_wait(settings.news_topic, msg.to_bytes())
            enriched_msg = await asyncio.wait_for(enriched_consumer.getone(), timeout=10)
            payload = json.loads(enriched_msg.value.decode())
            assert payload["title"] == title
        finally:
            await enriched_consumer.stop()
    finally:
        await producer.stop()
        await _stop_sidecar(sidecar, task)

    sentiment = await _wait_for_headline(title)
    assert sentiment == pytest.approx(float(simple_sentiment(title)), rel=1e-6)
