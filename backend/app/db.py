import asyncpg
from typing import Optional

from .config import settings

_pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(dsn=settings.database_url, min_size=1, max_size=5)
    return _pool


async def close_pool():
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


async def fetch_prices(symbol: str, limit: int = 200):
    pool = await get_pool()
    query = """
        SELECT time, symbol, price
        FROM prices
        WHERE symbol = $1
        ORDER BY time DESC
        LIMIT $2
    """
    return await pool.fetch(query, symbol, limit)


async def fetch_latest_metrics(symbol: str):
    pool = await get_pool()
    query = """
        SELECT *
        FROM metrics
        WHERE symbol = $1
        ORDER BY time DESC
        LIMIT 1
    """
    return await pool.fetchrow(query, symbol)


async def fetch_headlines(limit: int = 20):
    pool = await get_pool()
    query = """
        SELECT time, title, url, source, sentiment
        FROM headlines
        ORDER BY time DESC
        LIMIT $1
    """
    return await pool.fetch(query, limit)


async def fetch_alerts(limit: int = 20):
    pool = await get_pool()
    query = """
        SELECT
            time,
            symbol,
            window_name AS window,
            return_value AS return,
            direction,
            threshold,
            summary,
            headline,
            sentiment
        FROM anomalies
        ORDER BY time DESC
        LIMIT $1
    """
    return await pool.fetch(query, limit)
