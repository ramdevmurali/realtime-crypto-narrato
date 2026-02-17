import asyncio
import json
from collections import deque
from datetime import datetime, timezone, timedelta

import pytest

from processor.src.services import ingest
from processor.src.config import settings


class FakeWS:
    class StopStream(BaseException):
        pass

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
            raise FakeWS.StopStream
        return self.messages.pop(0)


class FakeProducer:
    def __init__(self):
        self.sent = []

    async def send_and_wait(self, topic, payload):
        self.sent.append((topic, payload))


class FakeProcessor:
    def __init__(self):
        self.producer = FakeProducer()
        self.latest_headline = (None, None, None)


@pytest.mark.asyncio
async def test_process_feed_entry_dedupes(monkeypatch):
    calls = []

    async def fake_insert_headline(*args, **kwargs):
        calls.append(args)

    monkeypatch.setattr(ingest, "insert_headline", fake_insert_headline)

    proc = FakeProcessor()
    seen_cache = {}
    seen_order = deque()
    entry = {
        "id": "abc",
        "title": "Breaking",
        "link": "http://example.com",
        "published": "2026-01-27T12:00:00Z",
        "source": {"title": "rss"},
    }
    seen_now = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)

    # first time should publish
    published = await ingest.persist_and_publish_feed_entry(proc, entry, seen_cache, seen_order, seen_now)
    assert published is True
    assert len(proc.producer.sent) == 1
    assert len(calls) == 1

    # second time same id should skip
    published = await ingest.persist_and_publish_feed_entry(proc, entry, seen_cache, seen_order, seen_now)
    assert published is False
    assert len(proc.producer.sent) == 1
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_process_feed_entry_ttl_eviction(monkeypatch):
    calls = []

    async def fake_insert_headline(*args, **kwargs):
        calls.append(args)

    monkeypatch.setattr(ingest, "insert_headline", fake_insert_headline)
    monkeypatch.setattr(settings, "rss_seen_ttl_sec", 60)
    monkeypatch.setattr(settings, "rss_seen_max", 5000)

    proc = FakeProcessor()
    seen_cache = {"abc": datetime(2026, 1, 27, 11, 0, tzinfo=timezone.utc)}
    seen_order = deque(["abc"])
    now_ts = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    ingest._prune_seen(seen_cache, seen_order, now_ts)
    assert "abc" not in seen_cache

    entry = {
        "id": "abc",
        "title": "Breaking",
        "link": "http://example.com",
        "published": "2026-01-27T12:00:00Z",
        "source": {"title": "rss"},
    }
    published = await ingest.persist_and_publish_feed_entry(proc, entry, seen_cache, seen_order, now_ts)
    assert published is True


@pytest.mark.asyncio
async def test_process_feed_entry_max_cap(monkeypatch):
    calls = []

    async def fake_insert_headline(*args, **kwargs):
        calls.append(args)

    monkeypatch.setattr(ingest, "insert_headline", fake_insert_headline)
    monkeypatch.setattr(settings, "rss_seen_ttl_sec", 86400)
    monkeypatch.setattr(settings, "rss_seen_max", 2)

    proc = FakeProcessor()
    seen_cache = {}
    seen_order = deque()
    now_ts = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)

    for i in range(3):
        entry = {
            "id": f"id-{i}",
            "title": f"News {i}",
            "link": f"http://example.com/{i}",
            "published": "2026-01-27T12:00:00Z",
            "source": {"title": "rss"},
        }
        published = await ingest.persist_and_publish_feed_entry(proc, entry, seen_cache, seen_order, now_ts + timedelta(seconds=i))
        assert published is True

    assert len(seen_cache) == 2
    assert len(seen_order) == 2


@pytest.mark.asyncio
async def test_price_ingest_task_publishes_once(monkeypatch):
    fake_msg = json.dumps({
        "data": {"s": "BTCUSDT", "c": "43210.12"}
    })

    def fake_connect(url):
        return FakeWS([fake_msg])

    monkeypatch.setattr(ingest.websockets, "connect", fake_connect)

    proc = FakeProcessor()
    proc.producer = FakeProducer()

    with pytest.raises(FakeWS.StopStream):
        await ingest.price_ingest_task(proc)

    assert len(proc.producer.sent) == 1
    topic, payload = proc.producer.sent[0]
    assert topic == settings.price_topic
    assert b"43210.12" in payload
