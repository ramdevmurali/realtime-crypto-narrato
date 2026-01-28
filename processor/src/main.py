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
        # drop anything older than 15m + small buffer
        cutoff = ts - timedelta(minutes=16)
        while self.buffer and self.buffer[0][0] < cutoff:
            self.buffer.popleft()

    def _oldest_for_window(self, ts: datetime, window: timedelta):
        cutoff = ts - window
        candidate = None
        for t, p in self.buffer:
            if t <= cutoff:
                candidate = (t, p)
            else:
                break
        return candidate

    def get_return(self, ts: datetime, window: timedelta):
        ref = self._oldest_for_window(ts, window)
        if not ref:
            return None
        _, past_price = ref
        latest_price = self.buffer[-1][1]
        if past_price == 0:
            return None
        return (latest_price - past_price) / past_price

    def get_vol(self, ts: datetime, window: timedelta):
        cutoff = ts - window
        window_prices = [p for t, p in self.buffer if t >= cutoff]
        if len(window_prices) < 3:
            return None
        returns = []
        for i in range(1, len(window_prices)):
            prev = window_prices[i - 1]
            cur = window_prices[i]
            if prev == 0:
                continue
            returns.append((cur - prev) / prev)
        if len(returns) < 2:
            return None
        mean = sum(returns) / len(returns)
        var = sum((r - mean) ** 2 for r in returns) / len(returns)
        return var ** 0.5


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

    async def compute_metrics(self, symbol: str, ts: datetime):
        win = self.price_windows[symbol]
        windows = {
            "1m": timedelta(minutes=1),
            "5m": timedelta(minutes=5),
            "15m": timedelta(minutes=15),
        }
        metrics = {}
        for label, delta in windows.items():
            ret = win.get_return(ts, delta)
            metrics[f"return_{label}"] = ret
            metrics[f"vol_{label}"] = win.get_vol(ts, delta)

        # attention = max(|return| / threshold)
        ratios = []
        thr = {
            "1m": settings.alert_threshold_1m,
            "5m": settings.alert_threshold_5m,
            "15m": settings.alert_threshold_15m,
        }
        for label in ["1m", "5m", "15m"]:
            r = metrics.get(f"return_{label}")
            if r is not None and thr[label] > 0:
                ratios.append(abs(r) / thr[label])
        metrics["attention"] = max(ratios) if ratios else None

        # If we have no returns at all, treat as insufficient data
        if all(metrics[f"return_{lbl}"] is None for lbl in ["1m", "5m", "15m"]):
            return None
        return metrics

    async def check_anomalies(self, symbol: str, ts: datetime, metrics: Dict):
        assert self.producer
        headline, sentiment = self.latest_headline
        thresholds = {
            "1m": settings.alert_threshold_1m,
            "5m": settings.alert_threshold_5m,
            "15m": settings.alert_threshold_15m,
        }
        for label, threshold in thresholds.items():
            ret = metrics.get(f"return_{label}")
            if ret is None:
                continue
            if abs(ret) >= threshold:
                key = (symbol, label)
                last_ts = self.last_alert.get(key)
                if last_ts and ts - last_ts < timedelta(seconds=60):
                    continue
                direction = "up" if ret >= 0 else "down"
                api_key = None
                if settings.llm_provider == "openai":
                    api_key = settings.openai_api_key
                elif settings.llm_provider == "google":
                    api_key = settings.google_api_key
                summary = llm_summarize(settings.llm_provider, api_key, symbol, label, ret, headline, sentiment)
                alert_payload = {
                    "time": ts.isoformat(),
                    "symbol": symbol,
                    "window": label,
                    "direction": direction,
                    "ret": ret,
                    "threshold": threshold,
                    "headline": headline,
                    "sentiment": sentiment,
                    "summary": summary,
                }
                await insert_anomaly(ts, symbol, label, direction, ret, threshold, headline, sentiment, summary)
                await self.producer.send_and_wait(settings.alerts_topic, json.dumps(alert_payload).encode())
                self.last_alert[key] = ts


def main():
    processor = StreamProcessor()
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(processor.start())
    except KeyboardInterrupt:
        loop.run_until_complete(processor.stop())


if __name__ == "__main__":
    main()
