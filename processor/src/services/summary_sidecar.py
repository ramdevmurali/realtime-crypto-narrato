import asyncio
import json
import signal
import asyncpg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition, OffsetAndMetadata

from ..config import settings
from ..logging_config import get_logger
from ..utils import llm_summarize, with_retries
from ..io.models.messages import SummaryRequestMsg, AlertMsg


log = get_logger(__name__)
_llm_semaphore = asyncio.Semaphore(2)


async def main():
    loop = asyncio.get_running_loop()

    stop_event = asyncio.Event()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    log.info("summary_sidecar_start", extra={"brokers": settings.kafka_brokers, "topic": settings.summaries_topic})

    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_brokers, loop=loop)
    consumer = AIOKafkaConsumer(
        settings.summaries_topic,
        bootstrap_servers=settings.kafka_brokers,
        loop=loop,
        enable_auto_commit=False,
        auto_offset_reset="latest",
        group_id=settings.summary_consumer_group,
    )

    pool = await asyncpg.create_pool(dsn=settings.database_url)
    await producer.start()
    await consumer.start()

    try:
        while not stop_event.is_set():
            msg_batch = await consumer.getmany(
                timeout_ms=settings.summary_poll_timeout_ms,
                max_records=settings.summary_batch_max,
            )
            for tp, messages in msg_batch.items():
                for msg in messages:
                    await process_summary_record(msg, consumer, producer, pool, log)
            await asyncio.sleep(0)  # yield control
    finally:
        await consumer.stop()
        await producer.stop()
        await pool.close()
        log.info("summary_sidecar_stop")


async def handle_summary_message(raw_value: bytes, producer, pool, log):
    payload = SummaryRequestMsg.model_validate_json(raw_value).model_dump()
    log.info("summary_request_received", extra=payload)

    # Call LLM (with lightweight concurrency cap)
    async with _llm_semaphore:
        api_key = None
        if settings.llm_provider == "openai":
            api_key = settings.openai_api_key
        elif settings.llm_provider == "google":
            api_key = settings.google_api_key

        try:
            summary = await asyncio.to_thread(
                llm_summarize,
                settings.llm_provider,
                api_key,
                payload["symbol"],
                payload["window"],
                payload["ret"],
                payload.get("headline"),
                payload.get("sentiment"),
            )
        except Exception as exc:
            log.exception(
                "summary_llm_error",
                extra={"error": str(exc), "symbol": payload.get("symbol"), "window": payload.get("window")},
            )
            raise

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


async def process_summary_record(msg, consumer, producer, pool, log):
    try:
        await with_retries(
            handle_summary_message,
            msg.value,
            producer,
            pool,
            log,
            log=log,
            op="handle_summary_message",
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
            log.error("summary_dlq_failed", extra={"error": str(dlq_exc)})
        await _commit_message(consumer, msg, log)
        return False


if __name__ == "__main__":
    asyncio.run(main())
