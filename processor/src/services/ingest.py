import asyncio
import json
from collections import deque
from datetime import datetime, timedelta, timezone
import feedparser
import websockets
from dateutil import parser as dateparser

from ..config import settings
from ..io.db import insert_headline
from ..utils import now_utc, parse_iso_datetime, simple_sentiment
from ..retry import sleep_backoff, with_retries
from ..logging_config import get_logger
from ..metrics import get_metrics
from ..io.models.messages import PriceMsg, NewsMsg
from ..processor_state import ProcessorState


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


def build_news_msg(
    entry,
    seen_cache: dict[str, datetime],
    seen_order: deque[str],
    pending: set[str | None],
    seen_now,
) -> tuple[bool, NewsMsg | None, str | None]:
    uid = entry.get('id') or entry.get('link') or entry.get('title')
    if uid is None:
        log = get_logger(__name__)
        log.warning("news_entry_missing_uid", extra={"published": entry.get("published"), "source": entry.get("source")})
        return False, None, None
    if uid in seen_cache or uid in pending:
        return False, None, None
    pending.add(uid)

    published = entry.get('published') or entry.get('updated')
    ts = dateparser.parse(published) if published else now_utc()
    title = entry.get('title', settings.rss_default_title)
    url = entry.get('link')
    source = entry.get('source', {}).get('title') if entry.get('source') else settings.rss_default_source
    sentiment = simple_sentiment(title)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    msg = NewsMsg(time=ts.isoformat(), title=title, url=url, source=source, sentiment=sentiment)
    return True, msg, uid


def mark_seen(uid: str | None, seen_cache: dict[str, datetime], seen_order: deque[str], seen_now) -> None:
    seen_cache[uid] = seen_now
    seen_order.append(uid)
    if len(seen_order) > settings.rss_seen_max:
        oldest = seen_order.popleft()
        seen_cache.pop(oldest, None)


async def publish_news_msg(processor: ProcessorState, msg: NewsMsg) -> None:
    await with_retries(
        insert_headline,
        msg.time,
        msg.title,
        msg.source,
        msg.url,
        msg.sentiment,
        log=getattr(processor, "log", None),
        op="insert_headline",
    )
    processor.record_latest_headline(msg.title, msg.sentiment, parse_iso_datetime(msg.time))
    await with_retries(
        processor.producer.send_and_wait,
        settings.news_topic,
        msg.to_bytes(),
        log=getattr(processor, "log", None),
        op="send_news",
    )


def _entry_with_source(entry, source_title: str):
    source = entry.get("source") if hasattr(entry, "get") else None
    if source and source.get("title"):
        return entry
    enriched = dict(entry)
    enriched["source"] = {"title": source_title}
    return enriched


async def price_ingest_task(processor: ProcessorState):
    """Stream prices from Binance and publish raw ticks to Kafka."""
    assert processor.producer
    log = getattr(processor, "log", get_logger(__name__))
    stream_names = "/".join(f"{sym}@miniTicker" for sym in settings.symbols)
    url = f"{settings.binance_stream}?streams={stream_names}"
    attempt = 0
    failures = 0
    price_messages_sent = 0
    metrics = get_metrics()
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
                    event_ms = payload.get("E")
                    if event_ms:
                        try:
                            ts = datetime.fromtimestamp(event_ms / 1000, tz=timezone.utc)
                        except Exception:
                            ts = now_utc()
                    else:
                        ts = now_utc()
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                    msg = PriceMsg(symbol=symbol, price=price, time=ts.isoformat())
                    await with_retries(processor.producer.send_and_wait, settings.price_topic, msg.to_bytes(), log=log, op="send_price")
                    log.info("price_published", extra={"symbol": symbol, "price": price})
                    price_messages_sent += 1
                    if price_messages_sent % settings.price_publish_log_every == 0:
                        log.info("price_published_count", extra={"count": price_messages_sent})
        except asyncio.CancelledError:
            log.info("price_ingest_cancelled")
            break
        except Exception as exc:
            metrics.inc("price_ingest_failures")
            metrics.inc("price_ingest_retries")
            log.warning("price_ws_error", extra={"error": str(exc), "attempt": attempt, "failures": failures})
            backoff_base = settings.price_backoff_base_after_failures_sec if failures >= settings.price_backoff_failures_threshold else settings.price_backoff_base_sec
            await sleep_backoff(attempt, base=backoff_base, cap=settings.price_backoff_cap_sec)
            attempt += 1
            failures += 1
            if failures % settings.price_failure_log_every == 0:
                log.warning("price_failure_count", extra={"failures": failures})
            if failures % settings.ingest_stuck_log_every == 0:
                log.warning(
                    "ingest_stuck",
                    extra={
                        "component": "price",
                        "failures": failures,
                        "attempt": attempt,
                        "backoff_base": backoff_base,
                        "backoff_cap": settings.price_backoff_cap_sec,
                    },
                )


async def news_ingest_task(processor: ProcessorState):
    """Poll RSS feed, sentiment tag, and publish headlines."""
    assert processor.producer
    log = getattr(processor, "log", get_logger(__name__))
    seen_cache: dict[str, datetime] = {}
    seen_order: deque[str] = deque()
    pending: set[str | None] = set()
    attempt = 0
    failures = 0
    news_messages_sent = 0
    metrics = get_metrics()
    telemetry = get_metrics("processor")
    stale_polls = 0
    while True:
        try:
            all_entries = []
            feed_status = []
            successful_feeds = 0
            for feed_url in settings.news_rss_urls:
                try:
                    feed = await asyncio.to_thread(feedparser.parse, feed_url)
                except Exception as exc:
                    telemetry.inc("news_feed_errors")
                    feed_status.append({"feed": feed_url, "status": "error", "error": str(exc)})
                    log.warning("news_feed_error", extra={"feed": feed_url, "error": str(exc)})
                    continue
                source_title = (
                    feed.get("feed", {}).get("title")
                    if hasattr(feed, "get")
                    else None
                ) or settings.rss_default_source
                entries = feed.entries[:settings.news_batch_limit]
                all_entries.extend(_entry_with_source(entry, source_title) for entry in entries)
                feed_status.append({"feed": feed_url, "status": "ok", "entries": len(entries)})
                successful_feeds += 1
            if successful_feeds == 0:
                raise RuntimeError("all_news_feeds_failed")
            attempt = 0
            failures = 0
            seen_now = now_utc()
            _prune_seen(seen_cache, seen_order, seen_now)
            for entry in all_entries:
                sent, msg, uid = build_news_msg(entry, seen_cache, seen_order, pending, seen_now)
                if sent and msg:
                    try:
                        await publish_news_msg(processor, msg)
                    except Exception:
                        pending.discard(uid)
                        raise
                    mark_seen(uid, seen_cache, seen_order, seen_now)
                    pending.discard(uid)
                    news_messages_sent += 1
                    telemetry.inc("news_entries_ingested")
                    if news_messages_sent % settings.news_publish_log_every == 0:
                        log.info("news_published_count", extra={"count": news_messages_sent})
            latest_ts = None
            if hasattr(processor, "latest_headline"):
                latest = processor.latest_headline
                if isinstance(latest, tuple) and len(latest) >= 3:
                    latest_ts = latest[2]
            if latest_ts is not None:
                latest_dt = parse_iso_datetime(latest_ts)
                age_sec = max(0.0, (seen_now - latest_dt).total_seconds())
                telemetry.observe("latest_headline_age_sec", age_sec)
                stale = age_sec > settings.headline_stale_warn_sec
            else:
                age_sec = None
                stale = True
            if stale:
                stale_polls += 1
                telemetry.inc("news_stale_polls")
                if stale_polls == 1 or stale_polls % settings.news_stale_log_every == 0:
                    log.warning(
                        "headline_stale",
                        extra={
                            "stale_polls": stale_polls,
                            "latest_headline_age_sec": age_sec,
                            "feed_status": feed_status,
                        },
                    )
            else:
                stale_polls = 0
        except asyncio.CancelledError:
            log.info("news_ingest_cancelled")
            break
        except Exception as exc:
            metrics.inc("news_ingest_failures")
            metrics.inc("news_ingest_retries")
            log.warning("news_poll_error", extra={"error": str(exc), "attempt": attempt, "failures": failures})
            backoff_base = settings.news_backoff_base_after_failures_sec if failures >= settings.news_backoff_failures_threshold else settings.news_backoff_base_sec
            await sleep_backoff(attempt, base=backoff_base, cap=settings.news_backoff_cap_sec)
            attempt += 1
            failures += 1
            if failures % settings.news_failure_log_every == 0:
                log.warning("news_failure_count", extra={"failures": failures})
            if failures % settings.ingest_stuck_log_every == 0:
                log.warning(
                    "ingest_stuck",
                    extra={
                        "component": "news",
                        "failures": failures,
                        "attempt": attempt,
                        "backoff_base": backoff_base,
                        "backoff_cap": settings.news_backoff_cap_sec,
                    },
                )
            continue
        await asyncio.sleep(settings.news_poll_interval_sec)
