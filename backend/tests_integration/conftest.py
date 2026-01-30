import os
from datetime import datetime, timezone, timedelta

import asyncpg
import pytest
import pytest_asyncio
import httpx

TEST_SYMBOL = "testcoin"


def _default_db_url():
    return os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/anomalies",
    )


@pytest_asyncio.fixture
async def db_pool():
    pool = await asyncpg.create_pool(dsn=_default_db_url(), min_size=1, max_size=3)
    try:
        yield pool
    finally:
        await pool.close()


@pytest_asyncio.fixture
async def client():
    async with httpx.AsyncClient(base_url="http://localhost:8000") as c:
        yield c


@pytest_asyncio.fixture
async def seed_data(db_pool):
    ts = datetime.now(timezone.utc)
    prices = [
        (ts - timedelta(minutes=5), TEST_SYMBOL, 100.0),
        (ts - timedelta(minutes=1), TEST_SYMBOL, 105.0),
        (ts, TEST_SYMBOL, 110.0),
    ]
    metrics = [
        (ts, TEST_SYMBOL, 0.05, 0.10, None, 0.02, 0.03, None, 1.2),
    ]
    headlines = [
        (ts, "Test headline", "http://example.com", "rss", -0.2),
    ]
    alerts = [
        (ts, TEST_SYMBOL, "1m", "up", 0.05, 0.05, "test summary", "Test headline", -0.2),
    ]

    async with db_pool.acquire() as conn:
        await conn.executemany(
            "INSERT INTO prices(time, symbol, price) VALUES($1, $2, $3)", prices
        )
        await conn.executemany(
            """
            INSERT INTO metrics(time, symbol, return_1m, return_5m, return_15m, vol_1m, vol_5m, vol_15m, attention)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
            """,
            metrics,
        )
        await conn.executemany(
            "INSERT INTO headlines(time, title, url, source, sentiment) VALUES($1,$2,$3,$4,$5)",
            headlines,
        )
        await conn.executemany(
            """
            INSERT INTO anomalies(time, symbol, window_name, direction, return_value, threshold, summary, headline, sentiment)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
            """,
            alerts,
        )

    yield

    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM prices WHERE symbol=$1", TEST_SYMBOL)
        await conn.execute("DELETE FROM metrics WHERE symbol=$1", TEST_SYMBOL)
        await conn.execute("DELETE FROM headlines WHERE source='rss' AND title='Test headline'")
        await conn.execute("DELETE FROM anomalies WHERE symbol=$1", TEST_SYMBOL)
