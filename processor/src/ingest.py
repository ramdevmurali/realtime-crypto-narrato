import asyncio
import json
import feedparser
import websockets
from dateutil import parser as dateparser

from .config import settings
from .db import insert_headline
from .utils import now_utc, simple_sentiment, sleep_backoff, with_retries
from .logging_config import get_logger


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
    await with_retries(insert_headline, ts, title, source, url, sentiment, log=getattr(processor, "log", None), op="insert_headline")
    processor.latest_headline = (title, sentiment)
    await with_retries(processor.producer.send_and_wait, settings.news_topic, json.dumps(payload).encode(), log=getattr(processor, "log", None), op="send_news")
    return True


async def price_ingest_task(processor):
    """Stream prices from Binance and publish raw ticks to Kafka."""
    assert processor.producer
    log = getattr(processor, "log", get_logger(__name__))
    stream_names = "/".join(f"{sym}@miniTicker" for sym in settings.symbols)
    url = f"{settings.binance_stream}?streams={stream_names}"
    attempt = 0
    while True:
        try:
            async with websockets.connect(url) as ws:
                attempt = 0
                async for msg in ws:
                    data = json.loads(msg)
                    payload = data.get("data", {})
                    symbol = payload.get("s", "").lower()
                    price = float(payload.get("c", 0))
                    ts = now_utc()
                    body = {"symbol": symbol, "price": price, "time": ts.isoformat()}
                    await with_retries(processor.producer.send_and_wait, settings.price_topic, json.dumps(body).encode(), log=log, op="send_price")
                    log.info("price_published", extra={"symbol": symbol, "price": price})
        except Exception as exc:
            log.warning("price_ws_error", extra={"error": str(exc), "attempt": attempt})
            await sleep_backoff(attempt, base=1, cap=30)
            attempt += 1


async def news_ingest_task(processor):
    """Poll RSS feed, sentiment tag, and publish headlines."""
    assert processor.producer
    log = getattr(processor, "log", get_logger(__name__))
    seen_ids: set[str] = set()
    attempt = 0
    while True:
        try:
            feed = feedparser.parse(settings.news_rss)
            attempt = 0
            for entry in feed.entries[:20]:
                await process_feed_entry(processor, entry, seen_ids)
        except Exception as exc:
            log.warning("news_poll_error", extra={"error": str(exc), "attempt": attempt})
            await sleep_backoff(attempt, base=5, cap=60)
            attempt += 1
            continue
        await asyncio.sleep(60)
