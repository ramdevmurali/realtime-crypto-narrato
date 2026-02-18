import asyncio
import json
import time
import uuid
from datetime import datetime, timezone

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from processor.src.config import settings
from processor.src.io.db import get_pool, init_pool
from processor.src.io.models.messages import SummaryRequestMsg
from processor.src.services.summary_sidecar import SummarySidecar
from processor.src.utils import parse_iso_datetime


async def _wait_for_summary(symbol: str, window: str, ts: str, timeout_sec: int = 10):
    deadline = time.monotonic() + timeout_sec
    ts_dt = parse_iso_datetime(ts)
    await init_pool()
    pool = await get_pool()
    while time.monotonic() < deadline:
        async with pool.acquire() as conn:
            summary = await conn.fetchval(
                "SELECT summary FROM anomalies WHERE symbol=$1 AND window_name=$2 AND time=$3",
                symbol,
                window,
                ts_dt,
            )
        if summary:
            return summary
        await asyncio.sleep(0.5)
    raise AssertionError("timeout waiting for summary")


async def _start_sidecar():
    sidecar = SummarySidecar()
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
async def test_summary_sidecar_integration(integration_services, integration_settings, monkeypatch):
    monkeypatch.setattr(settings, "summary_metrics_port", None)

    sidecar, task = await _start_sidecar()
    try:
        symbol = f"it-{uuid.uuid4().hex}"
        ts = datetime.now(timezone.utc).isoformat()
        ts_dt = parse_iso_datetime(ts)
        payload = SummaryRequestMsg(
            time=ts,
            symbol=symbol,
            window="1m",
            direction="up",
            ret=0.06,
            threshold=0.05,
            headline="headline",
            sentiment=0.1,
        )

        producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_brokers)
        await producer.start()
        try:
            await init_pool()
            pool = await get_pool()
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO anomalies(
                        time, symbol, window_name, direction, return_value,
                        threshold, headline, sentiment, summary, alert_published
                    )
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                    ON CONFLICT DO NOTHING
                    """,
                    ts_dt,
                    symbol,
                    "1m",
                    "up",
                    0.06,
                    0.05,
                    "headline",
                    0.1,
                    None,
                    False,
                )
            consumer = AIOKafkaConsumer(
                settings.alerts_topic,
                bootstrap_servers=settings.kafka_brokers,
                group_id=f"alerts-it-{uuid.uuid4().hex}",
                auto_offset_reset="earliest",
            )
            await consumer.start()
            try:
                await producer.send_and_wait(settings.summaries_topic, payload.to_bytes())
                enriched_msg = await asyncio.wait_for(consumer.getone(), timeout=10)
                out = json.loads(enriched_msg.value.decode())
                assert out["symbol"] == symbol
                assert out["window"] == "1m"
                assert out.get("summary")
            finally:
                await consumer.stop()
        finally:
            await producer.stop()

        summary = await _wait_for_summary(symbol, "1m", ts)
        assert summary
    finally:
        await _stop_sidecar(sidecar, task)
