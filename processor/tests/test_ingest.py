import asyncio
from datetime import datetime, timezone

import pytest

from processor.src import ingest
from processor.src.config import settings


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
