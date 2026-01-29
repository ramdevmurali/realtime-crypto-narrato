import asyncio
import json
import feedparser
import websockets
from dateutil import parser as dateparser

from .config import settings
from .db import insert_headline
from .utils import now_utc, simple_sentiment


async def process_feed_entry(processor, entry, seen_ids: set[str]):
    uid = entry.get('id') or entry.get('link') or entry.get('title')
    if uid in seen_ids:
        return False
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
    processor.latest_headline = (title, sentiment)
    await processor.producer.send_and_wait(settings.news_topic, json.dumps(payload).encode())
    return True


async def price_ingest_task(processor):
    """Stream prices from Binance and publish raw ticks to Kafka."""
    assert processor.producer
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
                    await processor.producer.send_and_wait(settings.price_topic, json.dumps(body).encode())
        except Exception:
            await asyncio.sleep(2)


async def news_ingest_task(processor):
    """Poll RSS feed, sentiment tag, and publish headlines."""
    assert processor.producer
    seen_ids: set[str] = set()
    while True:
        try:
            feed = feedparser.parse(settings.news_rss)
            for entry in feed.entries[:20]:
                await process_feed_entry(processor, entry, seen_ids)
        except Exception:
            pass
        await asyncio.sleep(60)
