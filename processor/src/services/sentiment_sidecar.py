import asyncio
import json
import time
from typing import Iterable, List, Tuple

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition, OffsetAndMetadata

from ..config import settings
from ..logging_config import get_logger
from ..runtime_interface import RuntimeService
from ..io.db import init_pool
from ..utils import simple_sentiment, with_retries, now_utc, parse_iso_datetime
from ..io.models.messages import NewsMsg, EnrichedNewsMsg
from ..metrics import MetricsRegistry, get_metrics
from . import sentiment_model
from .sidecar_runtime import SidecarRuntime


log = get_logger(__name__)
async def _commit_message(consumer, msg, log):
    tp = TopicPartition(msg.topic, msg.partition)
    try:
        await consumer.commit({tp: OffsetAndMetadata(msg.offset + 1, None)})
    except Exception as exc:
        log.warning("news_commit_failed", extra={"error": str(exc), "offset": msg.offset})


async def _send_dlq(producer, payload: bytes, log, metrics: MetricsRegistry, offset: int | None = None) -> bool:
    try:
        await with_retries(
            producer.send_and_wait,
            settings.news_dlq_topic,
            payload,
            log=log,
            op="send_news_dlq",
        )
        return True
    except Exception as exc:
        metrics.inc("sentiment_dlq_failed")
        log.error("news_dlq_failed", extra={"error": str(exc), "offset": offset})
        return False


async def _upsert_headline(pool, payload: NewsMsg, sentiment: float):
    ts = parse_iso_datetime(payload.time)
    await pool.execute(
        """
        INSERT INTO headlines (time, title, source, url, sentiment)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (time, title) DO UPDATE
        SET sentiment = EXCLUDED.sentiment
        """,
        ts,
        payload.title,
        payload.source,
        payload.url,
        sentiment,
    )


def _fallback_results(titles: List[str]) -> List[Tuple[float, None, None]]:
    return [(float(simple_sentiment(title)), None, None) for title in titles]


async def infer_sentiment_batch(messages: Iterable, consumer, producer, log, metrics: MetricsRegistry):
    parsed: List[Tuple[object, NewsMsg]] = []
    for msg in messages:
        try:
            payload = NewsMsg.model_validate_json(msg.value)
        except Exception as exc:
            log.warning("news_message_decode_failed", extra={"error": str(exc)})
            metrics.inc("sentiment_errors")
            metrics.inc("sentiment_dlq")
            dlq_ok = await _send_dlq(producer, msg.value, log, metrics, offset=msg.offset)
            if dlq_ok:
                await _commit_message(consumer, msg, log)
            continue
        parsed.append((msg, payload))

    if not parsed:
        return [], [], False, 0.0, 0.0

    metrics.inc("sentiment_batches")
    titles = [payload.title for _, payload in parsed]
    now = now_utc()
    lag_values = [max(0.0, (now - parse_iso_datetime(payload.time)).total_seconds() * 1000) for _, payload in parsed]
    queue_lag_ms = max(lag_values) if lag_values else 0.0
    if settings.sentiment_batch_size and len(parsed) >= settings.sentiment_batch_size:
        log.warning("sentiment_batch_maxed", extra={"batch_size": len(parsed), "queue_lag_ms": queue_lag_ms})

    fallback_used = False
    infer_start = time.perf_counter()
    try:
        results, model_fallback = sentiment_model.predict(titles)
        if model_fallback:
            fallback_used = True
            if settings.sentiment_provider != "stub":
                metrics.inc("sentiment_errors")
    except Exception:
        results = _fallback_results(titles)
        fallback_used = True
        metrics.inc("sentiment_errors")
    infer_ms = (time.perf_counter() - infer_start) * 1000
    metrics.observe("sentiment_infer_ms", infer_ms)
    metrics.observe("queue_lag_ms", queue_lag_ms)

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
        metrics.inc("sentiment_errors")

    if fallback_used:
        metrics.inc("sentiment_fallbacks")
        fallback_count = metrics.snapshot()["counters"].get("sentiment_fallbacks", 0)
        if fallback_count == 1 or fallback_count % settings.sentiment_fallback_log_every == 0:
            log.warning(
                "sentiment_fallback_used",
                extra={"provider": settings.sentiment_provider, "batch_size": len(parsed)},
            )

    log.info(
        "sentiment_batch_processed",
        extra={
            "batch_size": len(parsed),
            "sentiment_infer_ms": infer_ms,
            "queue_lag_ms": queue_lag_ms,
            "fallback_used": fallback_used,
        },
    )
    return parsed, results, fallback_used, infer_ms, queue_lag_ms


async def persist_and_publish_sentiment_batch(parsed, results, producer, pool, consumer, log, metrics: MetricsRegistry):
    if not parsed:
        return
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
            event_id = f"{payload.time}:{payload.title}:{payload.url}"
            enriched = EnrichedNewsMsg(
                event_id=event_id,
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
            metrics.inc("sentiment_errors")
            metrics.inc("sentiment_dlq")
            dlq_ok = await _send_dlq(producer, msg.value, log, metrics, offset=msg.offset)
            if dlq_ok:
                await _commit_message(consumer, msg, log)


class SentimentSidecar(SidecarRuntime, RuntimeService):
    def __init__(self):
        super().__init__(log, "sentiment_sidecar_stop")
        self._metrics_server: asyncio.AbstractServer | None = None
        self.metrics = MetricsRegistry()

    def reset(self) -> None:
        self.metrics = MetricsRegistry()

    async def _handle_metrics(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        closed = False
        try:
            request_line = await reader.readline()
            if not request_line:
                writer.close()
                closed = True
                return
            parts = request_line.decode(errors="ignore").split()
            path = parts[1] if len(parts) > 1 else "/"
            # Consume remaining headers.
            while True:
                line = await reader.readline()
                if not line or line == b"\r\n":
                    break
            if path != "/metrics":
                body = json.dumps({"error": "not found"}).encode()
                status = "404 Not Found"
            else:
                body = json.dumps(self.metrics.snapshot()).encode()
                status = "200 OK"
            headers = [
                f"HTTP/1.1 {status}",
                "Content-Type: application/json",
                f"Content-Length: {len(body)}",
                "Connection: close",
                "",
                "",
            ]
            writer.write("\r\n".join(headers).encode() + body)
            await writer.drain()
        finally:
            if not closed:
                writer.close()

    async def start(self) -> None:
        self.log.info(
            "sentiment_sidecar_start",
            extra={
                "brokers": settings.kafka_brokers,
                "topic": settings.news_topic,
                "sentiment_provider": settings.sentiment_provider,
                "sentiment_model_path": settings.sentiment_model_path,
                "tokenizer_type": "tokenizers" if settings.sentiment_light_runtime else "transformers",
            },
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

        if settings.sentiment_metrics_port:
            try:
                self._metrics_server = await asyncio.start_server(
                    self._handle_metrics,
                    settings.sentiment_metrics_host,
                    settings.sentiment_metrics_port,
                )
                self.log.info(
                    "sentiment_metrics_listen",
                    extra={
                        "host": settings.sentiment_metrics_host,
                        "port": settings.sentiment_metrics_port,
                    },
                )
            except Exception as exc:
                self.log.warning("sentiment_metrics_start_failed", extra={"error": str(exc)})

        self._producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_brokers)
        self._consumer = AIOKafkaConsumer(
            settings.news_topic,
            bootstrap_servers=settings.kafka_brokers,
            enable_auto_commit=False,
            auto_offset_reset=settings.kafka_auto_offset_reset,
            group_id=settings.sentiment_sidecar_group,
        )

        self._pool = await init_pool()
        await self._producer.start()
        await self._consumer.start()

        try:
            await self._run_task_supervisor(
                {"sentiment_loop": lambda: asyncio.create_task(self._run_loop())}
            )
        finally:
            await self._shutdown()

    async def _shutdown(self) -> None:
        if self._metrics_server:
            self._metrics_server.close()
            await self._metrics_server.wait_closed()
            self._metrics_server = None
        await super()._shutdown()

    async def _run_loop(self) -> None:
        while not self.should_stop():
            msg_batch = await self._consumer.getmany(
                timeout_ms=settings.sentiment_poll_timeout_ms,
                max_records=settings.sentiment_batch_size,
            )
            messages = [msg for batch in msg_batch.values() for msg in batch]
            if messages:
                parsed, results, _fallback, _infer_ms, _queue_lag_ms = await infer_sentiment_batch(
                    messages,
                    self._consumer,
                    self._producer,
                    self.log,
                    self.metrics,
                )
                await persist_and_publish_sentiment_batch(
                    parsed,
                    results,
                    self._producer,
                    self._pool,
                    self._consumer,
                    self.log,
                    self.metrics,
                )
            await asyncio.sleep(0)  # yield


async def main():
    sidecar = SentimentSidecar()
    sidecar.install_signal_handlers()
    await sidecar.start()


if __name__ == "__main__":
    asyncio.run(main())
