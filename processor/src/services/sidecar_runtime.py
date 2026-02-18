from __future__ import annotations

import asyncio
import contextlib
import signal
import time
from collections import deque
from typing import Callable

import asyncpg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from ..io.db import close_pool
from ..config import settings

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
            await close_pool()
            self._pool = None
        self.log.info(self._stop_event_name)

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
                if self.should_stop():
                    break
                for task in done:
                    name = task_names.pop(task, "unknown")
                    tasks.pop(name, None)
                    try:
                        exc = task.exception()
                    except asyncio.CancelledError:
                        exc = None
                    if exc:
                        self.log.error("sidecar_task_failed", extra={"task": name, "error": str(exc)})
                    else:
                        self.log.warning("sidecar_task_exited", extra={"task": name})

                    now = time.monotonic()
                    restart_times.append(now)
                    while restart_times and now - restart_times[0] > 60:
                        restart_times.popleft()
                    if len(restart_times) > settings.sidecar_restart_max_per_min:
                        self.log.error(
                            "sidecar_task_restart_throttled",
                            extra={"max_per_min": settings.sidecar_restart_max_per_min, "task": name},
                        )
                        await asyncio.sleep(60)
                        restart_times.clear()

                    await asyncio.sleep(settings.sidecar_restart_backoff_sec)
                    if self.should_stop():
                        break
                    new_task = task_factories[name]()
                    tasks[name] = new_task
                    task_names[new_task] = name
                if self.should_stop():
                    break
        except asyncio.CancelledError:
            self.log.info("sidecar_task_supervisor_cancelled")
            raise
        finally:
            for t in tasks.values():
                t.cancel()
            await asyncio.gather(*tasks.values(), return_exceptions=True)
