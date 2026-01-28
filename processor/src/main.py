import asyncio
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from dateutil import parser as dateparser

from .config import settings
from .db import init_tables, insert_price, insert_metric, insert_anomaly
from .utils import now_utc, llm_summarize
from .windows import PriceWindow
from .ingest import price_ingest_task, news_ingest_task


class StreamProcessor:
    def __init__(self):
        self.producer: AIOKafkaProducer | None = None
        self.consumer: AIOKafkaConsumer | None = None
        self.price_windows: Dict[str, PriceWindow] = defaultdict(PriceWindow)
        self.last_alert: Dict[Tuple[str, str], datetime] = {}
        self.latest_headline: Tuple[str | None, float | None] = (None, None)

    async def start(self):
        await init_tables()
        self.producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_brokers)
        await self.producer.start()
        self.consumer = AIOKafkaConsumer(
            settings.price_topic,
            bootstrap_servers=settings.kafka_brokers,
            group_id="processor",
            enable_auto_commit=True,
            auto_offset_reset="latest",
        )
        await self.consumer.start()

        await asyncio.gather(
            price_ingest_task(self),
            news_ingest_task(self),
            self.process_prices_task(),
        )

    async def stop(self):
        if self.consumer:
            await self.consumer.stop()
        if self.producer:
            await self.producer.stop()

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
