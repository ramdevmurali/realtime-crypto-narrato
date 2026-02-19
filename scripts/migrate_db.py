#!/usr/bin/env python3
import asyncio
import os
import sys

import asyncpg

ROOT = os.path.dirname(os.path.abspath(__file__))
PROCESSOR_SRC = os.path.join(ROOT, "..", "processor", "src")
if PROCESSOR_SRC not in sys.path:
    sys.path.append(PROCESSOR_SRC)

from processor.src.config import settings


DDL = [
    "CREATE EXTENSION IF NOT EXISTS timescaledb;",
    """
    CREATE TABLE IF NOT EXISTS prices (
        time TIMESTAMPTZ NOT NULL,
        symbol TEXT NOT NULL,
        price DOUBLE PRECISION NOT NULL,
        PRIMARY KEY (time, symbol)
    );
    """,
    "SELECT create_hypertable('prices','time', if_not_exists => TRUE);",
    "CREATE INDEX IF NOT EXISTS idx_prices_symbol_time_desc ON prices(symbol, time DESC);",
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
    """,
    "SELECT create_hypertable('metrics','time', if_not_exists => TRUE);",
    "CREATE INDEX IF NOT EXISTS idx_metrics_symbol_time_desc ON metrics(symbol, time DESC);",
    """
    CREATE TABLE IF NOT EXISTS headlines (
        time TIMESTAMPTZ NOT NULL,
        title TEXT NOT NULL,
        source TEXT,
        url TEXT,
        sentiment DOUBLE PRECISION,
        PRIMARY KEY (time, title)
    );
    """,
    "SELECT create_hypertable('headlines','time', if_not_exists => TRUE);",
    "CREATE INDEX IF NOT EXISTS idx_headlines_time_desc ON headlines(time DESC);",
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
        alert_published BOOLEAN DEFAULT TRUE,
        PRIMARY KEY (time, symbol, window_name)
    );
    """,
    "ALTER TABLE anomalies ADD COLUMN IF NOT EXISTS alert_published BOOLEAN DEFAULT TRUE;",
    "SELECT create_hypertable('anomalies','time', if_not_exists => TRUE);",
    "CREATE INDEX IF NOT EXISTS idx_anomalies_symbol_time_desc ON anomalies(symbol, time DESC);",
]


async def _apply_retention(conn, table: str, days: int | None) -> None:
    if days is None:
        return
    await conn.execute(
        "SELECT add_retention_policy($1, INTERVAL '1 day' * $2, if_not_exists => TRUE);",
        table,
        days,
    )


async def migrate() -> None:
    conn = await asyncpg.connect(dsn=settings.database_url)
    try:
        for stmt in DDL:
            await conn.execute(stmt)
        await _apply_retention(conn, "prices", settings.retention_prices_days)
        await _apply_retention(conn, "metrics", settings.retention_metrics_days)
        await _apply_retention(conn, "headlines", settings.retention_headlines_days)
        await _apply_retention(conn, "anomalies", settings.retention_anomalies_days)
    finally:
        await conn.close()


def main() -> None:
    asyncio.run(migrate())
    print("db migration complete")


if __name__ == "__main__":
    main()
