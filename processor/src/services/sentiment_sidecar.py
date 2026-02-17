import asyncio
import time
from typing import Iterable, List, Tuple

import asyncpg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition, OffsetAndMetadata

from ..config import settings
from ..logging_config import get_logger
from ..runtime_interface import RuntimeService
from ..utils import simple_sentiment, with_retries, now_utc
from ..io.models.messages import NewsMsg, EnrichedNewsMsg
from . import sentiment_model
from .sidecar_runtime import SidecarRuntime


log = get_logger(__name__)


async def _commit_message(consumer, msg, log):
    tp = TopicPartition(msg.topic, msg.partition)
    try:
        await consumer.commit({tp: OffsetAndMetadata(msg.offset + 1, None)})
    except Exception as exc:
        log.warning("news_commit_failed", extra={"error": str(exc), "offset": msg.offset})


async def _send_dlq(producer, payload: bytes, log):
    try:
        await with_retries(
            producer.send_and_wait,
            settings.news_dlq_topic,
            payload,
            log=log,
            op="send_news_dlq",
        )
    except Exception as exc:
        log.error("news_dlq_failed", extra={"error": str(exc)})


async def _upsert_headline(pool, payload: NewsMsg, sentiment: float):
    await pool.execute(
        """
        INSERT INTO headlines (time, title, source, url, sentiment)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (time, title) DO UPDATE
        SET sentiment = EXCLUDED.sentiment
        """,
        payload.time,
        payload.title,
        payload.source,
        payload.url,
        sentiment,
    )


def _fallback_results(titles: List[str]) -> List[Tuple[float, None, None]]:
    return [(float(simple_sentiment(title)), None, None) for title in titles]


async def process_sentiment_batch(messages: Iterable, consumer, producer, pool, log):
    parsed: List[Tuple[object, NewsMsg]] = []
    for msg in messages:
        try:
            payload = NewsMsg.model_validate_json(msg.value)
        except Exception as exc:
            log.warning("news_message_decode_failed", extra={"error": str(exc)})
            await _send_dlq(producer, msg.value, log)
            await _commit_message(consumer, msg, log)
            continue
        parsed.append((msg, payload))

    if not parsed:
        return

    titles = [payload.title for _, payload in parsed]
    now = now_utc()
    lag_values = [max(0.0, (now - payload.time).total_seconds() * 1000) for _, payload in parsed]
    queue_lag_ms = max(lag_values) if lag_values else 0.0
    if settings.sentiment_batch_size and len(parsed) >= settings.sentiment_batch_size:
        log.warning("sentiment_batch_maxed", extra={"batch_size": len(parsed), "queue_lag_ms": queue_lag_ms})

    fallback_used = False
    infer_start = time.perf_counter()
    try:
        results = sentiment_model.predict(titles)
    except Exception:
        results = _fallback_results(titles)
        fallback_used = True
    infer_ms = (time.perf_counter() - infer_start) * 1000

    if settings.sentiment_max_latency_ms and infer_ms > settings.sentiment_max_latency_ms:
        log.warning(
            "sentiment_batch_slow",
            extra={"sentiment_infer_ms": infer_ms, "batch_size": len(parsed), "queue_lag_ms": queue_lag_ms},
        )
        if settings.sentiment_fallback_on_slow:
            results = _fallback_results(titles)
            fallback_used = True

    if len(results) != len(parsed):
        log.warning(
            "sentiment_result_mismatch",
            extra={"expected": len(parsed), "actual": len(results)},
        )
        results = _fallback_results(titles)
        fallback_used = True

    log.info(
        "sentiment_batch_processed",
        extra={
            "batch_size": len(parsed),
            "sentiment_infer_ms": infer_ms,
            "queue_lag_ms": queue_lag_ms,
            "fallback_used": fallback_used,
        },
    )

    for (msg, payload), (score, label, confidence) in zip(parsed, results):
        try:
            await with_retries(
                _upsert_headline,
                pool,
                payload,
                score,
                log=log,
                op="upsert_headline",
            )
            enriched = EnrichedNewsMsg(
                time=payload.time,
                title=payload.title,
                url=payload.url,
                source=payload.source,
                sentiment=score,
                label=label,
                confidence=confidence,
            )
            await with_retries(
                producer.send_and_wait,
                settings.news_enriched_topic,
                enriched.to_bytes(),
                log=log,
                op="send_enriched_news",
            )
            await _commit_message(consumer, msg, log)
        except Exception as exc:
            log.warning("sentiment_handle_failed", extra={"error": str(exc)})
            await _send_dlq(producer, msg.value, log)
            await _commit_message(consumer, msg, log)


class SentimentSidecar(SidecarRuntime, RuntimeService):
    def __init__(self):
        super().__init__(log, "sentiment_sidecar_stop")

    async def start(self) -> None:
        self.log.info(
            "sentiment_sidecar_start",
            extra={"brokers": settings.kafka_brokers, "topic": settings.news_topic},
        )

        if settings.sentiment_provider == "onnx":
            try:
                sentiment_model.load_model()
                self.log.info("sentiment_model_loaded", extra={"model_path": settings.sentiment_model_path})
            except Exception as exc:
                self.log.warning(
                    "sentiment_model_load_failed",
                    extra={"error": str(exc), "model_path": settings.sentiment_model_path},
                )
                if settings.sentiment_fail_fast:
                    raise

        self._producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_brokers)
        self._consumer = AIOKafkaConsumer(
            settings.news_topic,
            bootstrap_servers=settings.kafka_brokers,
            enable_auto_commit=False,
            auto_offset_reset="latest",
            group_id=settings.sentiment_sidecar_group,
        )

        self._pool = await asyncpg.create_pool(dsn=settings.database_url)
        await self._producer.start()
        await self._consumer.start()

        try:
            while not self.should_stop():
                msg_batch = await self._consumer.getmany(
                    timeout_ms=settings.summary_poll_timeout_ms,
                    max_records=settings.sentiment_batch_size,
                )
                messages = [msg for batch in msg_batch.values() for msg in batch]
                if messages:
                    await process_sentiment_batch(messages, self._consumer, self._producer, self._pool, self.log)
                await asyncio.sleep(0)  # yield
        finally:
            await self._shutdown()


async def main():
    sidecar = SentimentSidecar()
    sidecar.install_signal_handlers()
    await sidecar.start()


if __name__ == "__main__":
    asyncio.run(main())
