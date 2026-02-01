import asyncio
import json
import pytest

from services import summary_sidecar  # type: ignore
from config import settings  # type: ignore


class FakePool:
    def __init__(self):
        self.calls = []

    async def execute(self, sql, *params):
        self.calls.append((sql.strip(), params))


class FakeProducer:
    def __init__(self):
        self.sent = []

    async def send_and_wait(self, topic, payload):
        self.sent.append((topic, payload))


@pytest.mark.asyncio
async def test_handle_summary_message(monkeypatch):
    # Stub llm to return deterministic text
    monkeypatch.setattr(summary_sidecar, "llm_summarize", lambda *args, **kwargs: "LLM SUMMARY")
    # Ensure semaphore allows the call
    summary_sidecar._llm_semaphore = summary_sidecar.asyncio.Semaphore(1)

    pool = FakePool()
    producer = FakeProducer()
    log = summary_sidecar.get_logger(__name__)

    payload = {
        "time": "2026-01-27T12:00:00+00:00",
        "symbol": "btcusdt",
        "window": "1m",
        "direction": "up",
        "ret": 0.07,
        "threshold": 0.05,
        "headline": "headline",
        "sentiment": 0.1,
    }

    await summary_sidecar.handle_summary_message(json.dumps(payload).encode(), producer, pool, log)

    # DB upsert called
    assert len(pool.calls) == 1
    sql, params = pool.calls[0]
    assert "INSERT INTO anomalies" in sql
    assert params[1] == "btcusdt"
    assert params[2] == "1m"
    assert params[3] == "up"
    assert params[8] == "LLM SUMMARY"

    # Alert republished with enriched summary
    assert len(producer.sent) == 1
    topic, out_payload = producer.sent[0]
    assert topic == settings.alerts_topic
    out = json.loads(out_payload.decode())
    assert out["summary"] == "LLM SUMMARY"
    assert out["symbol"] == "btcusdt"


@pytest.mark.asyncio
async def test_handle_summary_message_llm_failure(monkeypatch):
    def failing_llm(*args, **kwargs):
        raise RuntimeError("fail")

    monkeypatch.setattr(summary_sidecar, "llm_summarize", failing_llm)

    pool = FakePool()
    producer = FakeProducer()
    log = summary_sidecar.get_logger(__name__)

    payload = {
        "time": "2026-01-27T12:00:00+00:00",
        "symbol": "btcusdt",
        "window": "1m",
        "direction": "up",
        "ret": 0.07,
        "threshold": 0.05,
        "headline": "headline",
        "sentiment": 0.1,
    }

    # Should swallow error and not crash
    await summary_sidecar.handle_summary_message(json.dumps(payload).encode(), producer, pool, log)

    # No DB writes, no publishes
    assert pool.calls == []
    assert producer.sent == []


@pytest.mark.asyncio
async def test_handle_summary_message_batch(monkeypatch):
    monkeypatch.setattr(summary_sidecar, "llm_summarize", lambda *args, **kwargs: "LLM SUMMARY")
    summary_sidecar._llm_semaphore = summary_sidecar.asyncio.Semaphore(2)

    pool = FakePool()
    producer = FakeProducer()
    log = summary_sidecar.get_logger(__name__)

    base_payload = {
        "time": "2026-01-27T12:00:00+00:00",
        "symbol": "btcusdt",
        "window": "1m",
        "direction": "up",
        "ret": 0.07,
        "threshold": 0.05,
        "headline": "headline",
        "sentiment": 0.1,
    }
    payloads = [
        base_payload,
        {**base_payload, "symbol": "ethusdt", "ret": 0.08},
    ]

    for p in payloads:
        await summary_sidecar.handle_summary_message(json.dumps(p).encode(), producer, pool, log)

    # Two upserts, two publishes
    assert len(pool.calls) == 2
    assert len(producer.sent) == 2
    symbols = {params[1] for _, params in pool.calls}
    assert symbols == {"btcusdt", "ethusdt"}
