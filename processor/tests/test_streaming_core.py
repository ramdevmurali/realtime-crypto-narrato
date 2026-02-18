import asyncio

import pytest

from processor.src.streaming_core import StreamProcessor
from processor.src.config import settings


@pytest.mark.asyncio
async def test_task_supervisor_restarts_on_failure(monkeypatch):
    proc = StreamProcessor()
    restarts = {"count": 0}

    async def failing():
        restarts["count"] += 1
        raise RuntimeError("boom")

    monkeypatch.setattr(settings, "task_restart_backoff_sec", 0.01)
    monkeypatch.setattr(settings, "task_restart_max_per_min", 1000)

    supervisor = asyncio.create_task(
        proc._run_task_supervisor({"fail": lambda: asyncio.create_task(failing())})
    )
    await asyncio.sleep(0.05)
    supervisor.cancel()
    with pytest.raises(asyncio.CancelledError):
        await supervisor

    assert restarts["count"] >= 2
