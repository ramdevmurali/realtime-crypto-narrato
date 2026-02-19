import asyncio
import json
import base64
import time
from pathlib import Path
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition, OffsetAndMetadata

from ..config import settings
from ..logging_config import get_logger
from ..runtime_interface import RuntimeService
from ..metrics import get_metrics
from ..io.db import init_pool, fetch_anomaly_alert_published, mark_anomaly_alert_published
from ..llm import llm_summarize
from ..retry import with_retries
from ..utils import now_utc, parse_iso_datetime
from ..io.models.messages import SummaryRequestMsg, AlertMsg
from .sidecar_runtime import SidecarRuntime


log = get_logger(__name__)


def _rotate_dlq_buffer(path: Path, max_bytes: int, log) -> None:
    if max_bytes <= 0:
        return
    try:
        if not path.exists():
            return
        if path.stat().st_size <= max_bytes:
            return
        log.warning(
            "summary_dlq_buffer_rotate",
            extra={"path": str(path), "size": path.stat().st_size, "max_bytes": max_bytes},
        )
        # rotate up to 3 backups
        for idx in range(3, 0, -1):
            src = Path(f"{path}.{idx}")
            dst = Path(f"{path}.{idx + 1}")
            if src.exists():
                src.replace(dst)
        path.replace(Path(f"{path}.1"))
    except Exception as exc:
        log.warning("summary_dlq_buffer_rotate_failed", extra={"error": str(exc), "path": str(path)})


def _append_summary_dlq_buffer(payload: bytes, log) -> None:
    path = Path(settings.summary_dlq_buffer_path)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        _rotate_dlq_buffer(path, settings.summary_dlq_buffer_max_bytes, log)
        record = {
            "time": now_utc().isoformat(),
            "payload_b64": base64.b64encode(payload).decode("ascii"),
        }
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record) + "\n")
    except Exception as exc:
        log.warning("summary_dlq_buffer_write_failed", extra={"error": str(exc), "path": str(path)})


class SummarySidecar(SidecarRuntime, RuntimeService):
    def __init__(self):
        super().__init__(log, "summary_sidecar_stop")
        self._llm_semaphore = asyncio.Semaphore(settings.summary_llm_concurrency)
        self._metrics_server: asyncio.AbstractServer | None = None

    def reset(self) -> None:
        self._llm_semaphore = asyncio.Semaphore(settings.summary_llm_concurrency)

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
            while True:
                line = await reader.readline()
                if not line or line == b"\r\n":
                    break
            if path != "/metrics":
                body = json.dumps({"error": "not found"}).encode()
                status = "404 Not Found"
            else:
                body = json.dumps(get_metrics("summary").snapshot()).encode()
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

    async def _run_loop(self) -> None:
        while not self.should_stop():
            msg_batch = await self._consumer.getmany(
                timeout_ms=settings.summary_poll_timeout_ms,
                max_records=settings.summary_batch_max,
            )
            if msg_batch:
                get_metrics("summary").inc("summary_batches")
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

        if settings.summary_metrics_port:
            try:
                self._metrics_server = await asyncio.start_server(
                    self._handle_metrics,
                    settings.summary_metrics_host,
                    settings.summary_metrics_port,
                )
                self.log.info(
                    "summary_metrics_listen",
                    extra={
                        "host": settings.summary_metrics_host,
                        "port": settings.summary_metrics_port,
                    },
                )
            except Exception as exc:
                self.log.warning("summary_metrics_start_failed", extra={"error": str(exc)})

        self._pool = await init_pool()
        await self._producer.start()
        await self._consumer.start()

        try:
            await self._run_task_supervisor(
                {"summary_loop": lambda: asyncio.create_task(self._run_loop())}
            )
        finally:
            await self._shutdown()

    async def _shutdown(self) -> None:
        if self._metrics_server:
            self._metrics_server.close()
            await self._metrics_server.wait_closed()
            self._metrics_server = None
        await super()._shutdown()


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


async def persist_summary(payload, summary: str, pool, log, ts):
    log.info("summary_request_received", extra=payload)

    # Upsert anomalies table with enriched summary
    await pool.execute(
        """
        INSERT INTO anomalies (time, symbol, window_name, direction, return_value, threshold, headline, sentiment, summary)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        ON CONFLICT (time, symbol, window_name) DO UPDATE
        SET summary = EXCLUDED.summary
        """,
        ts,
        payload["symbol"],
        payload["window"],
        payload.get("direction"),
        payload.get("ret"),
        payload.get("threshold"),
        payload.get("headline"),
        payload.get("sentiment"),
        summary,
    )


async def publish_summary_alert(payload, summary: str, producer, log, event_id: str):
    enriched_alert = AlertMsg(
        event_id=event_id,
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
    metrics = get_metrics("summary")
    started = time.perf_counter()
    try:
        payload = SummaryRequestMsg.model_validate_json(msg.value).model_dump()
        api_key = None
        ts = parse_iso_datetime(payload["time"])
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

        event_id = payload.get("event_id") or f"{payload['time']}:{payload['symbol']}:{payload['window']}"
        await with_retries(
            persist_summary,
            payload,
            summary,
            pool,
            log,
            ts,
            log=log,
            op="persist_summary",
        )
        published = await with_retries(
            fetch_anomaly_alert_published,
            ts,
            payload["symbol"],
            payload["window"],
            log=log,
            op="fetch_anomaly_alert_published",
        )
        if published:
            metrics.inc("summary_publish_skipped")
            log.info("summary_alert_already_published", extra={"event_id": event_id})
            await _commit_message(consumer, msg, log)
            metrics.inc("summary_success")
            return True
        await with_retries(
            publish_summary_alert,
            payload,
            summary,
            producer,
            log,
            event_id,
            log=log,
            op="publish_summary_alert",
        )
        try:
            await with_retries(
                mark_anomaly_alert_published,
                ts,
                payload["symbol"],
                payload["window"],
                log=log,
                op="mark_anomaly_alert_published",
            )
        except Exception as exc:
            log.warning("summary_alert_mark_failed", extra={"event_id": event_id, "error": str(exc)})
        await _commit_message(consumer, msg, log)
        metrics.inc("summary_success")
        return True
    except Exception as exc:
        log.warning("summary_handle_failed", extra={"error": str(exc)})
        metrics.inc("summary_failures")
        metrics.inc("summary_dlq")
        try:
            await with_retries(
                producer.send_and_wait,
                settings.summaries_dlq_topic,
                msg.value,
                log=log,
                op="send_summary_dlq",
            )
        except Exception as dlq_exc:
            metrics.inc("summary_dlq_failed")
            log.error("summary_dlq_failed", extra={"error": str(dlq_exc), "offset": msg.offset})
            _append_summary_dlq_buffer(msg.value, log)
        await _commit_message(consumer, msg, log)
        return False
    finally:
        elapsed_ms = (time.perf_counter() - started) * 1000
        metrics.observe("summary_latency_ms", elapsed_ms)


if __name__ == "__main__":
    asyncio.run(main())
