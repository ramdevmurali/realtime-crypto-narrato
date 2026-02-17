import asyncio
import json
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition, OffsetAndMetadata

from ..config import settings
from ..logging_config import get_logger
from ..runtime_interface import RuntimeService
from ..metrics import get_metrics
from ..io.db import init_pool
from ..utils import llm_summarize, with_retries
from ..io.models.messages import SummaryRequestMsg, AlertMsg
from .sidecar_runtime import SidecarRuntime


log = get_logger(__name__)


class SummarySidecar(SidecarRuntime, RuntimeService):
    def __init__(self):
        super().__init__(log, "summary_sidecar_stop")
        self._llm_semaphore = asyncio.Semaphore(settings.summary_llm_concurrency)

    def reset(self) -> None:
        self._llm_semaphore = asyncio.Semaphore(settings.summary_llm_concurrency)

    async def start(self) -> None:
        self.log.info(
            "summary_sidecar_start",
            extra={"brokers": settings.kafka_brokers, "topic": settings.summaries_topic},
        )

        self._producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_brokers)
        self._consumer = AIOKafkaConsumer(
            settings.summaries_topic,
            bootstrap_servers=settings.kafka_brokers,
            enable_auto_commit=False,
            auto_offset_reset=settings.kafka_auto_offset_reset,
            group_id=settings.summary_consumer_group,
        )

        self._pool = await init_pool()
        await self._producer.start()
        await self._consumer.start()

        try:
            while not self.should_stop():
                msg_batch = await self._consumer.getmany(
                    timeout_ms=settings.summary_poll_timeout_ms,
                    max_records=settings.summary_batch_max,
                )
                for tp, messages in msg_batch.items():
                    for msg in messages:
                        await process_summary_record(
                            msg,
                            self._consumer,
                            self._producer,
                            self._pool,
                            self.log,
                            self._llm_semaphore,
                        )
                await asyncio.sleep(0)  # yield control
        finally:
            await self._shutdown()


async def main():
    sidecar = SummarySidecar()
    sidecar.install_signal_handlers()
    await sidecar.start()


async def compute_summary(payload, llm_provider: str, api_key: str | None, semaphore: asyncio.Semaphore) -> str:
    # Call LLM (with lightweight concurrency cap)
    async with semaphore:
        return await asyncio.to_thread(
            llm_summarize,
            llm_provider,
            api_key,
            payload["symbol"],
            payload["window"],
            payload["ret"],
            payload.get("headline"),
            payload.get("sentiment"),
        )


async def persist_and_publish_summary(payload, summary: str, producer, pool, log):
    log.info("summary_request_received", extra=payload)

    # Upsert anomalies table with enriched summary
    await pool.execute(
        """
        INSERT INTO anomalies (time, symbol, window_name, direction, return_value, threshold, headline, sentiment, summary)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        ON CONFLICT (time, symbol, window_name) DO UPDATE
        SET summary = EXCLUDED.summary
        """,
        payload["time"],
        payload["symbol"],
        payload["window"],
        payload.get("direction"),
        payload.get("ret"),
        payload.get("threshold"),
        payload.get("headline"),
        payload.get("sentiment"),
        summary,
    )

    # Republish enriched alert to alerts topic
    enriched_alert = AlertMsg(
        time=payload["time"],
        symbol=payload["symbol"],
        window=payload["window"],
        direction=payload["direction"],
        ret=payload["ret"],
        threshold=payload["threshold"],
        headline=payload.get("headline"),
        sentiment=payload.get("sentiment"),
        summary=summary,
    )
    await producer.send_and_wait(settings.alerts_topic, enriched_alert.to_bytes())
    log.info("summary_enriched", extra={"symbol": payload["symbol"], "window": payload["window"]})


async def _commit_message(consumer, msg, log):
    tp = TopicPartition(msg.topic, msg.partition)
    try:
        await consumer.commit({tp: OffsetAndMetadata(msg.offset + 1, None)})
    except Exception as exc:
        log.warning("summary_commit_failed", extra={"error": str(exc), "offset": msg.offset})


async def process_summary_record(msg, consumer, producer, pool, log, semaphore: asyncio.Semaphore):
    try:
        payload = SummaryRequestMsg.model_validate_json(msg.value).model_dump()
        api_key = None
        if settings.llm_provider == "openai":
            api_key = settings.openai_api_key
        elif settings.llm_provider == "google":
            api_key = settings.google_api_key

        try:
            summary = await compute_summary(payload, settings.llm_provider, api_key, semaphore)
        except Exception as exc:
            log.exception(
                "summary_llm_error",
                extra={"error": str(exc), "symbol": payload.get("symbol"), "window": payload.get("window")},
            )
            raise

        await with_retries(
            persist_and_publish_summary,
            payload,
            summary,
            producer,
            pool,
            log,
            log=log,
            op="persist_and_publish_summary",
        )
        await _commit_message(consumer, msg, log)
        return True
    except Exception as exc:
        log.warning("summary_handle_failed", extra={"error": str(exc)})
        try:
            await with_retries(
                producer.send_and_wait,
                settings.summaries_dlq_topic,
                msg.value,
                log=log,
                op="send_summary_dlq",
            )
        except Exception as dlq_exc:
            metrics = get_metrics()
            metrics.inc("summary_dlq_failed")
            log.error("summary_dlq_failed", extra={"error": str(dlq_exc), "offset": msg.offset})
        await _commit_message(consumer, msg, log)
        return False


if __name__ == "__main__":
    asyncio.run(main())
