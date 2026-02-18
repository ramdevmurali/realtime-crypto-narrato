import asyncio
import json
from datetime import datetime
import contextlib
import time
from collections import deque
from typing import Callable

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

from .config import settings
from .io.db import init_tables, init_pool, close_pool
from .services.ingest import price_ingest_task, news_ingest_task
from .utils import with_retries
from .logging_config import get_logger
from .services.runtime import log_startup_config
from .services.health import healthcheck
from .services.price_consumer import consume_prices
from .runtime_interface import RuntimeService
from .processor_state import ProcessorStateImpl
from .metrics import get_metrics


class StreamProcessor(ProcessorStateImpl, RuntimeService):
    def __init__(self):
        super().__init__(log=get_logger(__name__))
        self._metrics_server: asyncio.AbstractServer | None = None

    async def start(self):
        self.log.info("processor_starting", extra={"component": "processor"})
        log_startup_config(self.log)
        await self._start_metrics_server()
        await init_pool()
        await healthcheck(self.log)
        await init_tables()
        self.producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_brokers)
        await self.producer.start()
        self.consumer = AIOKafkaConsumer(
            settings.price_topic,
            bootstrap_servers=settings.kafka_brokers,
            group_id=settings.processor_consumer_group,
            enable_auto_commit=False,
            auto_offset_reset=settings.kafka_auto_offset_reset,
        )
        await self.consumer.start()

        await self._run_task_supervisor(
            {
                "price_ingest": lambda: asyncio.create_task(price_ingest_task(self)),
                "news_ingest": lambda: asyncio.create_task(news_ingest_task(self)),
                "price_consume": lambda: asyncio.create_task(self.process_prices_task()),
            }
        )

    async def stop(self):
        if self.consumer:
            await self.consumer.stop()
        if self.producer:
            await self.producer.stop()
        await self._stop_metrics_server()
        await close_pool()
        self.log.info("processor_stopped", extra={"component": "processor"})

    async def process_prices_task(self):
        await consume_prices(self)

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
                body = json.dumps(get_metrics().snapshot()).encode()
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

    async def _start_metrics_server(self) -> None:
        if not settings.processor_metrics_port:
            return
        try:
            self._metrics_server = await asyncio.start_server(
                self._handle_metrics,
                settings.processor_metrics_host,
                settings.processor_metrics_port,
            )
            self.log.info(
                "processor_metrics_listen",
                extra={
                    "host": settings.processor_metrics_host,
                    "port": settings.processor_metrics_port,
                },
            )
        except Exception as exc:
            self.log.warning("processor_metrics_start_failed", extra={"error": str(exc)})

    async def _stop_metrics_server(self) -> None:
        if self._metrics_server:
            self._metrics_server.close()
            await self._metrics_server.wait_closed()
            self._metrics_server = None

    async def _run_task_supervisor(self, task_factories: dict[str, Callable[[], asyncio.Task]]) -> None:
        tasks = {}
        task_names = {}
        restart_times = deque()

        for name, factory in task_factories.items():
            task = factory()
            tasks[name] = task
            task_names[task] = name

        try:
            while True:
                done, _ = await asyncio.wait(tasks.values(), return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    name = task_names.pop(task, "unknown")
                    tasks.pop(name, None)
                    try:
                        exc = task.exception()
                    except asyncio.CancelledError:
                        exc = None
                    if exc:
                        self.log.error("task_failed", extra={"task": name, "error": str(exc)})
                    else:
                        self.log.warning("task_exited", extra={"task": name})

                    now = time.monotonic()
                    restart_times.append(now)
                    while restart_times and now - restart_times[0] > 60:
                        restart_times.popleft()
                    if len(restart_times) > settings.task_restart_max_per_min:
                        self.log.error(
                            "task_restart_throttled",
                            extra={"max_per_min": settings.task_restart_max_per_min, "task": name},
                        )
                        await asyncio.sleep(60)
                        restart_times.clear()

                    await asyncio.sleep(settings.task_restart_backoff_sec)
                    new_task = task_factories[name]()
                    tasks[name] = new_task
                    task_names[new_task] = name
        except asyncio.CancelledError:
            self.log.info("task_supervisor_cancelled")
            raise
        finally:
            for t in tasks.values():
                t.cancel()
            await asyncio.gather(*tasks.values(), return_exceptions=True)

    async def commit_msg(self, msg):
        try:
            await with_retries(
                self.consumer.commit,
                log=self.log,
                op="commit_price",
            )
        except Exception as exc:
            self.log.warning("price_commit_failed", extra={"error": str(exc), "offset": msg.offset})

    async def send_price_dlq(self, payload: bytes) -> bool:
        if not self.producer:
            self.log.warning("price_dlq_unavailable")
            from .metrics import get_metrics

            get_metrics().inc("price_dlq_failed")
            return False
        try:
            await self.producer.send_and_wait(settings.price_dlq_topic, payload)
            return True
        except Exception as exc:
            from .metrics import get_metrics

            get_metrics().inc("price_dlq_failed")
            self.log.warning(
                "price_dlq_failed",
                extra={"error": str(exc), "payload_bytes": len(payload) if payload else 0},
            )
            return False
