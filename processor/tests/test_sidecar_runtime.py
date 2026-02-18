import asyncio

import pytest

from processor.src.config import settings
from processor.src.services.summary_sidecar import SummarySidecar
from processor.src.services.sentiment_sidecar import SentimentSidecar


@pytest.mark.asyncio
async def test_summary_sidecar_supervisor_restarts(monkeypatch):
    sidecar = SummarySidecar()
    restarts = {"count": 0}

    async def failing():
        restarts["count"] += 1
        raise RuntimeError("boom")

    monkeypatch.setattr(settings, "sidecar_restart_backoff_sec", 0.01)
    monkeypatch.setattr(settings, "sidecar_restart_max_per_min", 1000)

    supervisor = asyncio.create_task(
        sidecar._run_task_supervisor({"fail": lambda: asyncio.create_task(failing())})
    )
    await asyncio.sleep(0.05)
    supervisor.cancel()
    with pytest.raises(asyncio.CancelledError):
        await supervisor

    assert restarts["count"] >= 2


@pytest.mark.asyncio
async def test_sentiment_sidecar_supervisor_restarts(monkeypatch):
    sidecar = SentimentSidecar()
    restarts = {"count": 0}

    async def failing():
        restarts["count"] += 1
        raise RuntimeError("boom")

    monkeypatch.setattr(settings, "sidecar_restart_backoff_sec", 0.01)
    monkeypatch.setattr(settings, "sidecar_restart_max_per_min", 1000)

    supervisor = asyncio.create_task(
        sidecar._run_task_supervisor({"fail": lambda: asyncio.create_task(failing())})
    )
    await asyncio.sleep(0.05)
    supervisor.cancel()
    with pytest.raises(asyncio.CancelledError):
        await supervisor

    assert restarts["count"] >= 2
