import asyncio
from datetime import datetime, timezone

import pytest

from processor.src import ingest
from processor.src.config import settings


class FakeWS:
    def __init__(self, messages):
        self.messages = messages

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self.messages:
            raise StopAsyncIteration
        return self.messages.pop(0)


class FakeProducer:
    def __init__(self):
        self.sent = []

    async def send_and_wait(self, topic, payload):
        self.sent.append((topic, payload))


class FakeProcessor:
    def __init__(self):
        self.producer = FakeProducer()
        self.latest_headline = (None, None)


@pytest.mark.asyncio
async def test_process_feed_entry_dedupes(monkeypatch):
    calls = []

    async def fake_insert_headline(*args, **kwargs):
        calls.append(args)

    monkeypatch.setattr(ingest, "insert_headline", fake_insert_headline)

    proc = FakeProcessor()
    seen = set()
    entry = {
        "id": "abc",
        "title": "Breaking",
        "link": "http://example.com",
        "published": "2026-01-27T12:00:00Z",
        "source": {"title": "rss"},
    }

    # first time should publish
    published = await ingest.process_feed_entry(proc, entry, seen)
    assert published is True
    assert len(proc.producer.sent) == 1
    assert len(calls) == 1

    # second time same id should skip
    published = await ingest.process_feed_entry(proc, entry, seen)
    assert published is False
    assert len(proc.producer.sent) == 1
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_price_ingest_task_publishes_once(monkeypatch):
    fake_msg = json.dumps({
        "data": {"s": "BTCUSDT", "c": "43210.12"}
    })

    async def fake_connect(url):
        return FakeWS([fake_msg])

    monkeypatch.setattr(ingest.websockets, "connect", fake_connect)

    proc = FakeProcessor()
    proc.producer = FakeProducer()

    task = asyncio.create_task(ingest.price_ingest_task(proc))
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert len(proc.producer.sent) == 1
    topic, payload = proc.producer.sent[0]
    assert topic == settings.price_topic
    assert b"43210.12" in payload
