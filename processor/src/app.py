import asyncio
import json
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, Tuple

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from dateutil import parser as dateparser

import contextlib

from .config import settings, get_thresholds, get_windows
from .db import init_tables, insert_price, insert_metric, get_pool
from .utils import now_utc, with_retries
from .models.messages import PriceMsg
from .windows import PriceWindow
from .ingest import price_ingest_task, news_ingest_task
from .metrics import compute_metrics
from .anomaly import check_anomalies
from .logging_config import get_logger


class StreamProcessor:
    def __init__(self):
        self.producer: AIOKafkaProducer | None = None
        self.consumer: AIOKafkaConsumer | None = None
        self.price_windows: Dict[str, PriceWindow] = defaultdict(PriceWindow)
        self.last_alert: Dict[Tuple[str, str], datetime] = {}
        self.latest_headline: Tuple[str | None, float | None, datetime | None] = (None, None, None)
        self.bad_price_messages = 0
        self.bad_price_log_every = settings.bad_price_log_every
        self.last_price_ts: Dict[str, datetime] = {}
        self.late_price_messages = 0
        self.late_price_log_every = settings.late_price_log_every
        self.log = get_logger(__name__)

    async def start(self):
        self.log.info("processor_starting", extra={"component": "processor"})
        windows = get_windows()
        self.log.info(
            "processor_config",
            extra={
                "config": settings.safe_dict(),
                "windows": {k: int(v.total_seconds()) for k, v in windows.items()},
                "window_labels": list(windows.keys()),
                "thresholds": get_thresholds(),
                "topics": {
                    "price_topic": settings.price_topic,
                    "news_topic": settings.news_topic,
                    "alerts_topic": settings.alerts_topic,
                    "summaries_topic": settings.summaries_topic,
                    "summaries_dlq_topic": settings.summaries_dlq_topic,
                    "price_dlq_topic": settings.price_dlq_topic,
                },
                "policies": {
                    "late_price_tolerance_sec": settings.late_price_tolerance_sec,
                    "anomaly_cooldown_sec": settings.anomaly_cooldown_sec,
                    "headline_max_age_sec": settings.headline_max_age_sec,
                    "rss_seen_ttl_sec": settings.rss_seen_ttl_sec,
                    "rss_seen_max": settings.rss_seen_max,
                    "window_max_gap_factor": settings.window_max_gap_factor,
                    "vol_resample_sec": settings.vol_resample_sec,
                    "retry_max_attempts": settings.retry_max_attempts,
                    "retry_backoff_base_sec": settings.retry_backoff_base_sec,
                    "retry_backoff_cap_sec": settings.retry_backoff_cap_sec,
                },
            },
        )
        await self.healthcheck()
        await init_tables()
        self.producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_brokers)
        await self.producer.start()
        self.consumer = AIOKafkaConsumer(
            settings.price_topic,
            bootstrap_servers=settings.kafka_brokers,
            group_id="processor",
            enable_auto_commit=False,
            auto_offset_reset="latest",
        )
        await self.consumer.start()

        tasks = [
            asyncio.create_task(price_ingest_task(self)),
            asyncio.create_task(news_ingest_task(self)),
            asyncio.create_task(self.process_prices_task()),
        ]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            self.log.info("processor_cancelled")
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def stop(self):
        if self.consumer:
            await self.consumer.stop()
        if self.producer:
            await self.producer.stop()
        self.log.info("processor_stopped", extra={"component": "processor"})

    async def healthcheck(self):
        # DB check
        try:
            pool = await get_pool()
            async with pool.acquire() as conn:
                await conn.fetchval("SELECT 1;")
            self.log.info("db_health_ok")
        except Exception as exc:
            self.log.error("db_health_fail", extra={"error": str(exc)})
            raise

        # Kafka check
        temp_prod = AIOKafkaProducer(bootstrap_servers=settings.kafka_brokers)
        try:
            await temp_prod.start()
            self.log.info("kafka_health_ok")
        except Exception as exc:
            self.log.error("kafka_health_fail", extra={"error": str(exc)})
            raise
        finally:
            with contextlib.suppress(Exception):
                await temp_prod.stop()

    async def process_prices_task(self):
        """Consume prices from Kafka, compute metrics, check anomalies."""
        assert self.consumer
        async for msg in self.consumer:
            try:
                data = json.loads(msg.value.decode())
                price_msg = PriceMsg.model_validate(data)
            except (json.JSONDecodeError, Exception):
                self.bad_price_messages += 1
                if self.bad_price_messages == 1 or self.bad_price_messages % self.bad_price_log_every == 0:
                    self.log.warning(
                        "price_message_decode_failed",
                        extra={
                            "raw": msg.value,
                            "bad_price_messages": self.bad_price_messages,
                        },
                    )
                await self._send_price_dlq(msg.value)
                await self._commit_msg(msg)
                continue
            symbol = price_msg.symbol
            price = float(price_msg.price)
            ts = price_msg.time
            last_ts = self.last_price_ts.get(symbol)
            tolerance = timedelta(seconds=settings.late_price_tolerance_sec)
            if last_ts and ts < last_ts - tolerance:
                self.late_price_messages += 1
                if self.late_price_messages == 1 or self.late_price_messages % self.late_price_log_every == 0:
                    self.log.warning(
                        "price_message_late",
                        extra={
                            "symbol": symbol,
                            "time": ts.isoformat(),
                            "last_seen": last_ts.isoformat(),
                            "late_price_messages": self.late_price_messages,
                        },
                    )
                await self._commit_msg(msg)
                continue
            if last_ts is None or ts > last_ts:
                self.last_price_ts[symbol] = ts

            try:
                inserted = await with_retries(insert_price, ts, symbol, price, log=self.log, op="insert_price")
            except Exception as exc:
                self.log.error("price_insert_failed", extra={"error": str(exc), "symbol": symbol})
                await self._send_price_dlq(msg.value)
                await self._commit_msg(msg)
                continue

            if inserted:
                win = self.price_windows[symbol]
                win.add(ts, price)
                metrics = compute_metrics(self.price_windows, symbol, ts)
                if metrics:
                    try:
                        await with_retries(insert_metric, ts, symbol, metrics, log=self.log, op="insert_metric")
                    except Exception as exc:
                        self.log.error("metric_insert_failed", extra={"error": str(exc), "symbol": symbol})
                        await self._send_price_dlq(msg.value)
                        await self._commit_msg(msg)
                        continue
                try:
                    await check_anomalies(self, symbol, ts, metrics or {})
                except Exception as exc:
                    self.log.error("anomaly_check_failed", extra={"error": str(exc), "symbol": symbol})
                    await self._send_price_dlq(msg.value)
                    await self._commit_msg(msg)
                    continue

            await self._commit_msg(msg)

    async def _commit_msg(self, msg):
        try:
            await with_retries(
                self.consumer.commit,
                log=self.log,
                op="commit_price",
            )
        except Exception as exc:
            self.log.warning("price_commit_failed", extra={"error": str(exc), "offset": msg.offset})

    async def _send_price_dlq(self, payload: bytes):
        if not self.producer:
            return
        with contextlib.suppress(Exception):
            await self.producer.send_and_wait(settings.price_dlq_topic, payload)


async def main():
    processor = StreamProcessor()
    try:
        await processor.start()
    except KeyboardInterrupt:
        processor.log.info("processor_shutdown_requested")
    finally:
        await processor.stop()


if __name__ == "__main__":
    asyncio.run(main())
