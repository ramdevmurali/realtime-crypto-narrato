import asyncio
import logging
import contextlib
import os
import subprocess
import time
import uuid
from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio
from aiokafka import AIOKafkaProducer

from processor.src import config as config_module
from processor.src.io.db import init_tables, close_pool, init_pool


def _compose_file() -> Path:
    return Path(__file__).resolve().parents[3] / "infra" / "docker-compose.yml"


async def _wait_for_db(dsn: str, timeout_sec: int = 60):
    deadline = time.monotonic() + timeout_sec
    last_exc: Exception | None = None
    while time.monotonic() < deadline:
        try:
            conn = await asyncpg.connect(dsn=dsn)
            await conn.execute("SELECT 1;")
            await conn.close()
            return
        except Exception as exc:
            last_exc = exc
            await asyncio.sleep(1)
    if last_exc is not None:
        logging.getLogger(__name__).warning("timescaledb not ready", exc_info=last_exc)
    raise RuntimeError("timescaledb not ready")


async def _wait_for_kafka(brokers: str, timeout_sec: int = 60):
    deadline = time.monotonic() + timeout_sec
    last_exc: Exception | None = None
    while time.monotonic() < deadline:
        producer = AIOKafkaProducer(bootstrap_servers=brokers)
        try:
            await producer.start()
            await producer.stop()
            return
        except Exception as exc:
            last_exc = exc
            await asyncio.sleep(1)
        finally:
            with contextlib.suppress(Exception):
                await producer.stop()
    if last_exc is not None:
        logging.getLogger(__name__).warning("redpanda not ready", exc_info=last_exc)
    raise RuntimeError("redpanda not ready")


@pytest_asyncio.fixture(scope="session")
async def integration_services():
    compose = _compose_file()
    subprocess.run(
        ["docker", "compose", "-f", str(compose), "up", "-d", "redpanda", "timescaledb"],
        check=True,
    )
    db_url = os.getenv("INTEGRATION_DB_URL", "postgres://postgres:postgres@localhost:5432/anomalies")
    brokers = os.getenv("INTEGRATION_KAFKA_BROKERS", "localhost:9092")
    config_module.settings.database_url = db_url
    config_module.settings.kafka_brokers_raw = brokers
    await _wait_for_db(db_url)
    await _wait_for_kafka(brokers)
    await init_pool()
    await init_tables()
    await close_pool()
    yield


@pytest.fixture()
def integration_settings():
    settings = config_module.settings
    suffix = uuid.uuid4().hex[:8]
    old = {
        "database_url": settings.database_url,
        "kafka_brokers_raw": settings.kafka_brokers_raw,
        "price_topic": settings.price_topic,
        "price_dlq_topic": settings.price_dlq_topic,
        "alerts_topic": settings.alerts_topic,
        "summaries_topic": settings.summaries_topic,
        "news_topic": settings.news_topic,
        "news_enriched_topic": settings.news_enriched_topic,
        "news_dlq_topic": settings.news_dlq_topic,
        "summary_consumer_group": settings.summary_consumer_group,
        "sentiment_sidecar_group": settings.sentiment_sidecar_group,
        "retry_max_attempts": settings.retry_max_attempts,
        "retry_backoff_base_sec": settings.retry_backoff_base_sec,
        "retry_backoff_cap_sec": settings.retry_backoff_cap_sec,
        "late_price_tolerance_sec": settings.late_price_tolerance_sec,
        "llm_provider": settings.llm_provider,
        "sentiment_provider": settings.sentiment_provider,
    }
    settings.database_url = os.getenv("INTEGRATION_DB_URL", "postgres://postgres:postgres@localhost:5432/anomalies")
    settings.kafka_brokers_raw = os.getenv("INTEGRATION_KAFKA_BROKERS", "localhost:9092")
    settings.price_topic = f"prices-it-{suffix}"
    settings.price_dlq_topic = f"prices-dlq-it-{suffix}"
    settings.alerts_topic = f"alerts-it-{suffix}"
    settings.summaries_topic = f"summaries-it-{suffix}"
    settings.news_topic = f"news-it-{suffix}"
    settings.news_enriched_topic = f"news-enriched-it-{suffix}"
    settings.news_dlq_topic = f"news-dlq-it-{suffix}"
    settings.summary_consumer_group = f"summary-it-{suffix}"
    settings.sentiment_sidecar_group = f"sentiment-it-{suffix}"
    settings.retry_max_attempts = 2
    settings.retry_backoff_base_sec = 0.1
    settings.retry_backoff_cap_sec = 1.0
    settings.late_price_tolerance_sec = 0
    settings.llm_provider = "stub"
    settings.sentiment_provider = "stub"
    try:
        yield
    finally:
        for key, value in old.items():
            setattr(settings, key, value)


async def _truncate_tables():
    conn = await asyncpg.connect(dsn=config_module.settings.database_url)
    try:
        await conn.execute("TRUNCATE TABLE prices, metrics, anomalies, headlines;")
    finally:
        await conn.close()


@pytest_asyncio.fixture(autouse=True)
async def db_cleanup(integration_settings):
    await _truncate_tables()
    yield
    await _truncate_tables()
    await close_pool()
