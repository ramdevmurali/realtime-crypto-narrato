import asyncio
import json
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Deque, Dict, Tuple

import feedparser
import websockets
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from dateutil import parser as dateparser

from .config import settings
from .db import init_tables, insert_price, insert_metric, insert_headline, insert_anomaly
from .utils import now_utc, simple_sentiment, llm_summarize


class PriceWindow:
    def __init__(self):
        self.buffer: Deque[Tuple[datetime, float]] = deque()

    def add(self, ts: datetime, price: float):
        self.buffer.append((ts, price))


class StreamProcessor:
    def __init__(self):
        self.producer: AIOKafkaProducer | None = None
        self.consumer: AIOKafkaConsumer | None = None
        self.price_windows: Dict[str, PriceWindow] = defaultdict(PriceWindow)
        self.last_alert: Dict[Tuple[str, str], datetime] = {}
        self.latest_headline: Tuple[str | None, float | None] = (None, None)

    async def start(self):
        # will be filled with startup logic
        ...

    async def stop(self):
        # will be filled with teardown logic
        ...

    async def price_ingest_task(self):
        """Stream prices from Binance and publish raw ticks to Kafka."""
        assert self.producer
        stream_names = "/".join(f"{sym}@miniTicker" for sym in settings.symbols)
        url = f"{settings.binance_stream}?streams={stream_names}"
        while True:
            try:
                async with websockets.connect(url) as ws:
                    async for msg in ws:
                        data = json.loads(msg)
                        payload = data.get("data", {})
                        symbol = payload.get("s", "").lower()
                        price = float(payload.get("c", 0))
                        ts = now_utc()
                        body = {"symbol": symbol, "price": price, "time": ts.isoformat()}
                        await self.producer.send_and_wait(settings.price_topic, json.dumps(body).encode())
            except Exception:
                await asyncio.sleep(2)

    async def news_ingest_task(self):
        """Poll RSS feed, sentiment tag, and publish headlines."""
        assert self.producer
        seen_ids: set[str] = set()
        while True:
            try:
                feed = feedparser.parse(settings.news_rss)
                for entry in feed.entries[:20]:
                    uid = entry.get('id') or entry.get('link') or entry.get('title')
                    if uid in seen_ids:
                        continue
                    seen_ids.add(uid)
                    published = entry.get('published') or entry.get('updated')
                    ts = dateparser.parse(published) if published else now_utc()
                    title = entry.get('title', 'untitled')
                    url = entry.get('link')
                    source = entry.get('source', {}).get('title') if entry.get('source') else "rss"
                    sentiment = simple_sentiment(title)
                    payload = {
                        "time": ts.isoformat(),
                        "title": title,
                        "url": url,
                        "source": source,
                        "sentiment": sentiment,
                    }
                    await insert_headline(ts, title, source, url, sentiment)
                    self.latest_headline = (title, sentiment)
                    await self.producer.send_and_wait(settings.news_topic, json.dumps(payload).encode())
            except Exception:
                pass
            await asyncio.sleep(60)

    async def process_prices_task(self):
        """Consume prices from Kafka, compute metrics, check anomalies."""
        assert self.consumer
        async for msg in self.consumer:
            data = json.loads(msg.value.decode())
            symbol = data.get('symbol')
            price = float(data.get('price'))
            ts = dateparser.parse(data.get('time')) if data.get('time') else now_utc()

            await insert_price(ts, symbol, price)
            win = self.price_windows[symbol]
            win.add(ts, price)
            metrics = await self.compute_metrics(symbol, ts)
            if metrics:
                await insert_metric(ts, symbol, metrics)
            await self.check_anomalies(symbol, ts, metrics or {})


def main():
    processor = StreamProcessor()
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(processor.start())
    except KeyboardInterrupt:
        loop.run_until_complete(processor.stop())


if __name__ == "__main__":
    main()
