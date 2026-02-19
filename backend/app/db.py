import asyncpg
from datetime import datetime
from decimal import Decimal
from typing import Optional, TypedDict

from .config import settings

_pool: Optional[asyncpg.Pool] = None


class PriceRow(TypedDict):
    time: datetime
    symbol: str
    price: float | Decimal


class HeadlineRow(TypedDict):
    time: datetime
    title: str
    url: str | None
    source: str | None
    sentiment: float | None


AlertRow = TypedDict(
    "AlertRow",
    {
        "time": datetime,
        "symbol": str,
        "window": str,
        "direction": str,
        "return": float | Decimal,
        "threshold": float | Decimal,
        "summary": str | None,
        "headline": str | None,
        "sentiment": float | None,
    },
)


async def init_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(dsn=settings.database_url, min_size=1, max_size=5)
    return _pool


async def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("db pool not initialized; call init_pool() first")
    return _pool


async def close_pool():
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


async def fetch_prices(symbol: str, limit: int = 200) -> list[PriceRow]:
    pool = await get_pool()
    query = """
        SELECT time, symbol, price
        FROM prices
        WHERE symbol = $1
        ORDER BY time DESC
        LIMIT $2
    """
    return await pool.fetch(query, symbol, limit)


async def fetch_latest_metrics(symbol: str) -> asyncpg.Record | None:
    pool = await get_pool()
    query = """
        SELECT *
        FROM metrics
        WHERE symbol = $1
        ORDER BY time DESC
        LIMIT 1
    """
    return await pool.fetchrow(query, symbol)


async def fetch_headlines(limit: int = 20, since: datetime | None = None) -> list[HeadlineRow]:
    pool = await get_pool()
    if since is None:
        query = """
            SELECT time, title, url, source, sentiment
            FROM headlines
            ORDER BY time DESC
            LIMIT $1
        """
        return await pool.fetch(query, limit)
    query = """
        SELECT time, title, url, source, sentiment
        FROM headlines
        WHERE time >= $1
        ORDER BY time DESC
        LIMIT $2
    """
    return await pool.fetch(query, since, limit)


async def fetch_alerts(limit: int = 20, since: datetime | None = None) -> list[AlertRow]:
    pool = await get_pool()
    if since is None:
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
        WHERE time >= $1
        ORDER BY time DESC
        LIMIT $2
    """
    return await pool.fetch(query, since, limit)
