#!/usr/bin/env python3
import argparse
import asyncio
from datetime import timedelta

import asyncpg
from aiokafka import AIOKafkaProducer

from processor.src.config import settings
from processor.src.io.models.messages import NewsMsg
from processor.src.utils import now_utc


async def fetch_headlines(pool, since_hours: int, limit: int):
    since_ts = now_utc() - timedelta(hours=since_hours)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT time, title, source, url, sentiment
            FROM headlines
            WHERE time >= $1
            ORDER BY time ASC
            LIMIT $2
            """,
            since_ts,
            limit,
        )
    return rows


async def publish_headlines(rows, sleep_ms: int):
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_brokers)
    await producer.start()
    sent = 0
    try:
        for row in rows:
            msg = NewsMsg(
                time=row["time"],
                title=row["title"],
                url=row["url"],
                source=row["source"],
                sentiment=float(row["sentiment"] or 0.0),
            )
            await producer.send_and_wait(settings.news_topic, msg.to_bytes())
            sent += 1
            if sleep_ms > 0:
                await asyncio.sleep(sleep_ms / 1000)
    finally:
        await producer.stop()
    return sent


async def main():
    parser = argparse.ArgumentParser(description="Replay recent headlines to Kafka news topic")
    parser.add_argument("--since-hours", type=int, default=24, help="How many hours back to replay")
    parser.add_argument("--limit", type=int, default=200, help="Max number of headlines to replay")
    parser.add_argument("--sleep-ms", type=int, default=0, help="Sleep between messages (ms)")
    args = parser.parse_args()

    pool = await asyncpg.create_pool(dsn=settings.database_url)
    try:
        rows = await fetch_headlines(pool, args.since_hours, args.limit)
    finally:
        await pool.close()

    if not rows:
        print("no headlines found for replay")
        return

    sent = await publish_headlines(rows, args.sleep_ms)
    print(f"replayed {sent} headlines to {settings.news_topic}")


if __name__ == "__main__":
    asyncio.run(main())
