import asyncio
import time
import uuid
from datetime import timedelta

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition

from processor.src import app as app_module
from processor.src.services import price_pipeline as pipeline_module
from processor.src.config import settings
from processor.src.io.db import get_pool, init_pool, close_pool
from processor.src.io.models.messages import PriceMsg
from processor.src.utils import now_utc


async def _start_processor(group_id: str):
    proc = app_module.StreamProcessor()
    await init_pool()
    proc.producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_brokers)
    await proc.producer.start()
    proc.consumer = AIOKafkaConsumer(
        settings.price_topic,
        bootstrap_servers=settings.kafka_brokers,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await proc.consumer.start()
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if proc.consumer.assignment():
            break
        await asyncio.sleep(0.1)
    task = asyncio.create_task(proc.process_prices_task())
    await asyncio.sleep(0.5)
    return proc, task


async def _stop_processor(proc, task):
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    if proc.consumer:
        await proc.consumer.stop()
    if proc.producer:
        await proc.producer.stop()
    await close_pool()


async def _publish_prices(prices):
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_brokers)
    await producer.start()
    try:
        for msg in prices:
            await producer.send_and_wait(settings.price_topic, msg.to_bytes())
    finally:
        await producer.stop()


async def _wait_for_count(query: str, args, min_count: int, timeout_sec: int = 10):
    deadline = time.monotonic() + timeout_sec
    await init_pool()
    pool = await get_pool()
    while time.monotonic() < deadline:
        async with pool.acquire() as conn:
            count = await conn.fetchval(query, *args)
        if count >= min_count:
            return
        await asyncio.sleep(0.5)
    raise AssertionError("timeout waiting for DB rows")


async def _wait_for_calls(counter: dict, min_count: int, timeout_sec: int = 5):
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        if counter["count"] >= min_count:
            return
        await asyncio.sleep(0.05)
    raise AssertionError("timeout waiting for retry attempts")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_e2e_happy_path(integration_services, integration_settings):
    group_id = f"processor-it-{uuid.uuid4().hex}"
    proc, task = await _start_processor(group_id)
    try:
        now = now_utc()
        prices = [
            PriceMsg(symbol="btcusdt", price=100.0, time=now),
            PriceMsg(symbol="btcusdt", price=110.0, time=now + timedelta(seconds=30)),
        ]
        await _publish_prices(prices)
        await _wait_for_count("SELECT count(*) FROM metrics WHERE symbol=$1", ("btcusdt",), 1)
        await _wait_for_count("SELECT count(*) FROM anomalies WHERE symbol=$1", ("btcusdt",), 1)
    finally:
        await _stop_processor(proc, task)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_e2e_retry_path(integration_services, integration_settings, monkeypatch):
    group_id = f"processor-it-{uuid.uuid4().hex}"
    calls = {"count": 0}
    real_insert = pipeline_module.insert_price
    original_retry_max = settings.retry_max_attempts

    async def flaky_insert(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError("transient db error")
        return await real_insert(*args, **kwargs)

    monkeypatch.setattr(pipeline_module, "insert_price", flaky_insert)
    settings.retry_max_attempts = 2
    proc, task = await _start_processor(group_id)
    try:
        now = now_utc()
        price = PriceMsg(symbol="btcusdt", price=100.0, time=now)
        await _publish_prices([price])
        await _wait_for_count("SELECT count(*) FROM prices WHERE symbol=$1", ("btcusdt",), 1)
        await _wait_for_calls(calls, 2)
    finally:
        settings.retry_max_attempts = original_retry_max
        await _stop_processor(proc, task)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_e2e_dlq_path(integration_services, integration_settings):
    group_id = f"processor-it-{uuid.uuid4().hex}"
    proc, task = await _start_processor(group_id)
    try:
        producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_brokers)
        await producer.start()
        try:
            record = await producer.send_and_wait(settings.price_topic, b"not-json")
        finally:
            await producer.stop()

        dlq_consumer = AIOKafkaConsumer(
            settings.price_dlq_topic,
            bootstrap_servers=settings.kafka_brokers,
            group_id=f"dlq-it-{uuid.uuid4().hex}",
            auto_offset_reset="earliest",
        )
        await dlq_consumer.start()
        try:
            msg = await asyncio.wait_for(dlq_consumer.getone(), timeout=10)
            assert msg.value == b"not-json"
        finally:
            await dlq_consumer.stop()

        tp = TopicPartition(record.topic, record.partition)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            committed = await proc.consumer.committed(tp)
            if committed and committed >= record.offset + 1:
                return
            await asyncio.sleep(0.5)
        raise AssertionError("offset not committed after DLQ")
    finally:
        await _stop_processor(proc, task)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_e2e_kafka_transient_failure(integration_services, integration_settings, monkeypatch):
    group_id = f"processor-it-{uuid.uuid4().hex}"
    proc, task = await _start_processor(group_id)
    try:
        calls = {"count": 0}
        assert proc.producer is not None
        original_send = proc.producer.send_and_wait

        async def flaky_send(topic, payload, *args, **kwargs):
            calls["count"] += 1
            if calls["count"] == 1:
                raise RuntimeError("transient kafka error")
            return await original_send(topic, payload, *args, **kwargs)

        monkeypatch.setattr(proc.producer, "send_and_wait", flaky_send)

        now = now_utc()
        prices = [
            PriceMsg(symbol="btcusdt", price=100.0, time=now),
            PriceMsg(symbol="btcusdt", price=110.0, time=now + timedelta(seconds=30)),
        ]
        await _publish_prices(prices)

        await _wait_for_count("SELECT count(*) FROM anomalies WHERE symbol=$1", ("btcusdt",), 1)
        assert calls["count"] >= 2
    finally:
        await _stop_processor(proc, task)
