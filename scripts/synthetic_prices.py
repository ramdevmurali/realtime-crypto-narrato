#!/usr/bin/env python3
import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(ROOT, "..", "processor", "src"))

from windows import PriceWindow  # type: ignore
from metrics import compute_metrics  # type: ignore
from db import insert_price, insert_metric, get_pool, close_pool  # type: ignore


async def run():
    await get_pool()
    win = PriceWindow()
    symbol = "testcoin"

    base = datetime.now(timezone.utc)
    prices = [100, 105, 95, 120, 130, 125]
    ts_seq = [base + timedelta(seconds=30 * i) for i in range(len(prices))]

    for ts, price in zip(ts_seq, prices):
        win.add(ts, price)
        await insert_price(ts, symbol, price)
        metrics = compute_metrics({symbol: win}, symbol, ts)
        if metrics:
            await insert_metric(ts, symbol, metrics)

    print("Finished synthetic feed. Latest metrics row:")
    import asyncpg
    pool = await get_pool()
    row = await pool.fetchrow(
        """
        SELECT * FROM metrics
        WHERE symbol=$1
        ORDER BY time DESC
        LIMIT 1
        """,
        symbol,
    )
    print(dict(row))
    await close_pool()


if __name__ == "__main__":
    asyncio.run(run())
