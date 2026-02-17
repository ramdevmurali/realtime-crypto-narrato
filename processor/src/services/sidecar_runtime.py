from __future__ import annotations

import asyncio
import contextlib
import signal

import asyncpg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer


class SidecarRuntime:
    def __init__(self, log, stop_event_name: str):
        self.log = log
        self._stop_event = asyncio.Event()
        self._stopped = False
        self._producer: AIOKafkaProducer | None = None
        self._consumer: AIOKafkaConsumer | None = None
        self._pool: asyncpg.Pool | None = None
        self._stop_event_name = stop_event_name

    def request_stop(self) -> None:
        self._stop_event.set()

    def should_stop(self) -> bool:
        return self._stop_event.is_set()

    def install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.request_stop)

    async def stop(self) -> None:
        self._stop_event.set()
        await self._shutdown()

    async def _shutdown(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        if self._consumer:
            with contextlib.suppress(Exception):
                await self._consumer.stop()
        if self._producer:
            with contextlib.suppress(Exception):
                await self._producer.stop()
        if self._pool:
            with contextlib.suppress(Exception):
                await self._pool.close()
        self.log.info(self._stop_event_name)
