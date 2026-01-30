import asyncpg
from typing import Any, Dict
from .config import settings
from .logging_config import get_logger

_pool: asyncpg.Pool | None = None
log = get_logger(__name__)


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(dsn=settings.database_url)
    return _pool


async def init_tables():
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS prices (
                    time TIMESTAMPTZ NOT NULL,
                    symbol TEXT NOT NULL,
                    price DOUBLE PRECISION NOT NULL,
                    PRIMARY KEY (time, symbol)
                );
                """
            )
            await conn.execute("SELECT create_hypertable('prices','time', if_not_exists => TRUE);")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_prices_symbol_time_desc ON prices(symbol, time DESC);")

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS metrics (
                    time TIMESTAMPTZ NOT NULL,
                    symbol TEXT NOT NULL,
                    return_1m DOUBLE PRECISION,
                    return_5m DOUBLE PRECISION,
                    return_15m DOUBLE PRECISION,
                    vol_1m DOUBLE PRECISION,
                    vol_5m DOUBLE PRECISION,
                    vol_15m DOUBLE PRECISION,
                    return_z_1m DOUBLE PRECISION,
                    return_z_5m DOUBLE PRECISION,
                    return_z_15m DOUBLE PRECISION,
                    return_z_ewma_1m DOUBLE PRECISION,
                    return_z_ewma_5m DOUBLE PRECISION,
                    return_z_ewma_15m DOUBLE PRECISION,
                    vol_z_1m DOUBLE PRECISION,
                    vol_z_5m DOUBLE PRECISION,
                    vol_z_15m DOUBLE PRECISION,
                    vol_spike_1m BOOLEAN,
                    vol_spike_5m BOOLEAN,
                    vol_spike_15m BOOLEAN,
                    p05_return_1m DOUBLE PRECISION,
                    p05_return_5m DOUBLE PRECISION,
                    p05_return_15m DOUBLE PRECISION,
                    p95_return_1m DOUBLE PRECISION,
                    p95_return_5m DOUBLE PRECISION,
                    p95_return_15m DOUBLE PRECISION,
                    attention DOUBLE PRECISION,
                    PRIMARY KEY (time, symbol)
                );
                """
            )
            await conn.execute("SELECT create_hypertable('metrics','time', if_not_exists => TRUE);")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_metrics_symbol_time_desc ON metrics(symbol, time DESC);")

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS headlines (
                    time TIMESTAMPTZ NOT NULL,
                    title TEXT NOT NULL,
                    source TEXT,
                    url TEXT,
                    sentiment DOUBLE PRECISION,
                    PRIMARY KEY (time, title)
                );
                """
            )
            await conn.execute("SELECT create_hypertable('headlines','time', if_not_exists => TRUE);")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_headlines_time_desc ON headlines(time DESC);")

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS anomalies (
                    time TIMESTAMPTZ NOT NULL,
                    symbol TEXT NOT NULL,
                    window_name TEXT NOT NULL,
                    direction TEXT,
                    return_value DOUBLE PRECISION,
                    threshold DOUBLE PRECISION,
                    headline TEXT,
                    sentiment DOUBLE PRECISION,
                    summary TEXT,
                    PRIMARY KEY (time, symbol, window_name)
                );
                """
            )
            await conn.execute("SELECT create_hypertable('anomalies','time', if_not_exists => TRUE);")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_anomalies_symbol_time_desc ON anomalies(symbol, time DESC);")
    except Exception as exc:
        log.error("db_init_failed", extra={"error": str(exc)})
        raise


async def insert_price(time, symbol, price):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO prices(time, symbol, price)
            VALUES ($1, $2, $3)
            ON CONFLICT (time, symbol) DO NOTHING
            """,
            time,
            symbol,
            price,
        )


async def insert_metric(time, symbol, metrics: Dict[str, Any]):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO metrics(
                time, symbol,
                return_1m, return_5m, return_15m,
                vol_1m, vol_5m, vol_15m,
                return_z_1m, return_z_5m, return_z_15m,
                return_z_ewma_1m, return_z_ewma_5m, return_z_ewma_15m,
                vol_z_1m, vol_z_5m, vol_z_15m,
                vol_spike_1m, vol_spike_5m, vol_spike_15m,
                p05_return_1m, p05_return_5m, p05_return_15m,
                p95_return_1m, p95_return_5m, p95_return_15m,
                attention
            )
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25)
            """,
            time,
            symbol,
            metrics.get('return_1m'),
            metrics.get('return_5m'),
            metrics.get('return_15m'),
            metrics.get('vol_1m'),
            metrics.get('vol_5m'),
            metrics.get('vol_15m'),
            metrics.get('return_z_1m'),
            metrics.get('return_z_5m'),
            metrics.get('return_z_15m'),
            metrics.get('return_z_ewma_1m'),
            metrics.get('return_z_ewma_5m'),
            metrics.get('return_z_ewma_15m'),
            metrics.get('vol_z_1m'),
            metrics.get('vol_z_5m'),
            metrics.get('vol_z_15m'),
            metrics.get('vol_spike_1m'),
            metrics.get('vol_spike_5m'),
            metrics.get('vol_spike_15m'),
            metrics.get('p05_return_1m'),
            metrics.get('p05_return_5m'),
            metrics.get('p05_return_15m'),
            metrics.get('p95_return_1m'),
            metrics.get('p95_return_5m'),
            metrics.get('p95_return_15m'),
            metrics.get('attention'),
        )


async def insert_headline(time, title, source, url, sentiment):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO headlines(time, title, source, url, sentiment) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING",
            time,
            title,
            source,
            url,
            sentiment,
        )


async def insert_anomaly(time, symbol, window, direction, ret, threshold, headline, sentiment, summary):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO anomalies(time, symbol, window_name, direction, return_value, threshold, headline, sentiment, summary)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
            ON CONFLICT DO NOTHING
            """,
            time,
            symbol,
            window,
            direction,
            ret,
            threshold,
            headline,
            sentiment,
            summary,
        )


async def close_pool():
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
