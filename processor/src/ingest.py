import asyncio
import json
from collections import deque
from datetime import datetime, timedelta
import feedparser
import websockets
from dateutil import parser as dateparser

from .config import settings
from .db import insert_headline
from .utils import now_utc, simple_sentiment, sleep_backoff, with_retries
from .logging_config import get_logger
from .models.messages import PriceMsg, NewsMsg


def _prune_seen(seen_cache: dict[str, datetime], seen_order: deque[str], now_ts):
    cutoff = now_ts - timedelta(seconds=settings.rss_seen_ttl_sec)
    while seen_order:
        oldest = seen_order[0]
        seen_ts = seen_cache.get(oldest)
        if seen_ts is None or seen_ts < cutoff:
            seen_order.popleft()
            seen_cache.pop(oldest, None)
            continue
        break
    while len(seen_order) > settings.rss_seen_max:
        oldest = seen_order.popleft()
        seen_cache.pop(oldest, None)


async def process_feed_entry(processor, entry, seen_cache: dict[str, datetime], seen_order: deque[str], seen_now):
    uid = entry.get('id') or entry.get('link') or entry.get('title')
    if uid in seen_cache:
        return False
    seen_cache[uid] = seen_now
    seen_order.append(uid)
    if len(seen_order) > settings.rss_seen_max:
        oldest = seen_order.popleft()
        seen_cache.pop(oldest, None)

    published = entry.get('published') or entry.get('updated')
    ts = dateparser.parse(published) if published else now_utc()
    title = entry.get('title', 'untitled')
    url = entry.get('link')
    source = entry.get('source', {}).get('title') if entry.get('source') else "rss"
    sentiment = simple_sentiment(title)
    msg = NewsMsg(time=ts, title=title, url=url, source=source, sentiment=sentiment)
    await with_retries(insert_headline, ts, title, source, url, sentiment, log=getattr(processor, "log", None), op="insert_headline")
    processor.latest_headline = (title, sentiment, ts)
    await with_retries(processor.producer.send_and_wait, settings.news_topic, msg.to_bytes(), log=getattr(processor, "log", None), op="send_news")
    return True


async def price_ingest_task(processor):
    """Stream prices from Binance and publish raw ticks to Kafka."""
    assert processor.producer
    log = getattr(processor, "log", get_logger(__name__))
    stream_names = "/".join(f"{sym}@miniTicker" for sym in settings.symbols)
    url = f"{settings.binance_stream}?streams={stream_names}"
    attempt = 0
    failures = 0
    price_messages_sent = 0
    while True:
        try:
            async with websockets.connect(url) as ws:
                attempt = 0
                failures = 0
                async for msg in ws:
                    data = json.loads(msg)
                    payload = data.get("data", {})
                    symbol = payload.get("s", "").lower()
                    price = float(payload.get("c", 0))
                    ts = now_utc()
                    msg = PriceMsg(symbol=symbol, price=price, time=ts)
                    await with_retries(processor.producer.send_and_wait, settings.price_topic, msg.to_bytes(), log=log, op="send_price")
                    log.info("price_published", extra={"symbol": symbol, "price": price})
                    price_messages_sent += 1
                    if price_messages_sent % settings.price_publish_log_every == 0:
                        log.info("price_published_count", extra={"count": price_messages_sent})
        except asyncio.CancelledError:
            log.info("price_ingest_cancelled")
            break
        except Exception as exc:
            log.warning("price_ws_error", extra={"error": str(exc), "attempt": attempt, "failures": failures})
            backoff_base = settings.price_backoff_base_after_failures_sec if failures >= settings.price_backoff_failures_threshold else settings.price_backoff_base_sec
            await sleep_backoff(attempt, base=backoff_base, cap=settings.price_backoff_cap_sec)
            attempt += 1
            failures += 1
            if failures % settings.price_failure_log_every == 0:
                log.warning("price_failure_count", extra={"failures": failures})


async def news_ingest_task(processor):
    """Poll RSS feed, sentiment tag, and publish headlines."""
    assert processor.producer
    log = getattr(processor, "log", get_logger(__name__))
    seen_cache: dict[str, datetime] = {}
    seen_order: deque[str] = deque()
    attempt = 0
    failures = 0
    news_messages_sent = 0
    while True:
        try:
            feed = await asyncio.to_thread(feedparser.parse, settings.news_rss)
            attempt = 0
            failures = 0
            seen_now = now_utc()
            _prune_seen(seen_cache, seen_order, seen_now)
            for entry in feed.entries[:settings.news_batch_limit]:
                sent = await process_feed_entry(processor, entry, seen_cache, seen_order, seen_now)
                if sent:
                    news_messages_sent += 1
                    if news_messages_sent % settings.news_publish_log_every == 0:
                        log.info("news_published_count", extra={"count": news_messages_sent})
        except asyncio.CancelledError:
            log.info("news_ingest_cancelled")
            break
        except Exception as exc:
            log.warning("news_poll_error", extra={"error": str(exc), "attempt": attempt, "failures": failures})
            backoff_base = settings.news_backoff_base_after_failures_sec if failures >= settings.news_backoff_failures_threshold else settings.news_backoff_base_sec
            await sleep_backoff(attempt, base=backoff_base, cap=settings.news_backoff_cap_sec)
            attempt += 1
            failures += 1
            if failures % settings.news_failure_log_every == 0:
                log.warning("news_failure_count", extra={"failures": failures})
            continue
        await asyncio.sleep(settings.news_poll_interval_sec)
