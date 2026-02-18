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

    def record_latest_headline(self, title, sentiment, ts):
        self.latest_headline = (title, sentiment, ts)


def test_build_news_msg_dedupes():
    proc = FakeProcessor()
    seen_cache = {}
    seen_order = deque()
    pending = set()
    entry = {
        "id": "abc",
        "title": "Breaking",
        "link": "http://example.com",
        "published": "2026-01-27T12:00:00Z",
        "source": {"title": "rss"},
    }
    seen_now = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)

    # first time should publish
    published, msg, uid = ingest.build_news_msg(entry, seen_cache, seen_order, pending, seen_now)
    assert published is True
    assert msg is not None
    ingest.mark_seen(uid, seen_cache, seen_order, seen_now)
    pending.discard(uid)

    # second time same id should skip
    published, msg, _ = ingest.build_news_msg(entry, seen_cache, seen_order, pending, seen_now)
    assert published is False
    assert msg is None


def test_build_news_msg_ttl_eviction(monkeypatch):
    monkeypatch.setattr(settings, "rss_seen_ttl_sec", 60)
    monkeypatch.setattr(settings, "rss_seen_max", 5000)

    proc = FakeProcessor()
    seen_cache = {"abc": datetime(2026, 1, 27, 11, 0, tzinfo=timezone.utc)}
    seen_order = deque(["abc"])
    pending = set()
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
    published, msg, uid = ingest.build_news_msg(entry, seen_cache, seen_order, pending, now_ts)
    assert published is True
    assert msg is not None
    ingest.mark_seen(uid, seen_cache, seen_order, now_ts)
    pending.discard(uid)


def test_build_news_msg_max_cap(monkeypatch):
    monkeypatch.setattr(settings, "rss_seen_ttl_sec", 86400)
    monkeypatch.setattr(settings, "rss_seen_max", 2)

    proc = FakeProcessor()
    seen_cache = {}
    seen_order = deque()
    pending = set()
    now_ts = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)

    for i in range(3):
        entry = {
            "id": f"id-{i}",
            "title": f"News {i}",
            "link": f"http://example.com/{i}",
            "published": "2026-01-27T12:00:00Z",
            "source": {"title": "rss"},
        }
        published, msg, uid = ingest.build_news_msg(entry, seen_cache, seen_order, pending, now_ts + timedelta(seconds=i))
        assert published is True
        assert msg is not None
        ingest.mark_seen(uid, seen_cache, seen_order, now_ts + timedelta(seconds=i))
        pending.discard(uid)

    assert len(seen_cache) == 2
    assert len(seen_order) == 2


@pytest.mark.asyncio
async def test_publish_news_msg_inserts_and_publishes(monkeypatch):
    calls = []

    async def fake_insert_headline(*args, **kwargs):
        calls.append(args)

    monkeypatch.setattr(ingest, "insert_headline", fake_insert_headline)

    proc = FakeProcessor()
    entry = {
        "id": "abc",
        "title": "Breaking",
        "link": "http://example.com",
        "published": "2026-01-27T12:00:00Z",
        "source": {"title": "rss"},
    }
    seen_now = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    published, msg, _ = ingest.build_news_msg(entry, {}, deque(), set(), seen_now)
    assert published is True
    assert msg is not None

    await ingest.publish_news_msg(proc, msg)
    assert len(calls) == 1
    assert len(proc.producer.sent) == 1
    assert proc.latest_headline[0] == "Breaking"


def test_build_news_msg_retry_after_failure():
    seen_cache = {}
    seen_order = deque()
    pending = set()
    entry = {
        "id": "abc",
        "title": "Breaking",
        "link": "http://example.com",
        "published": "2026-01-27T12:00:00Z",
        "source": {"title": "rss"},
    }
    seen_now = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)

    published, msg, uid = ingest.build_news_msg(entry, seen_cache, seen_order, pending, seen_now)
    assert published is True
    assert msg is not None
    # simulate failure: clear pending without marking seen
    pending.discard(uid)

    published, msg, _ = ingest.build_news_msg(entry, seen_cache, seen_order, pending, seen_now)
    assert published is True
    assert msg is not None


def test_build_news_msg_missing_uid_does_not_dedupe_all():
    seen_cache = {}
    seen_order = deque()
    pending = set()
    entry = {
        "published": "2026-01-27T12:00:00Z",
        "source": {"title": "rss"},
    }
    seen_now = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)

    published, msg, uid = ingest.build_news_msg(entry, seen_cache, seen_order, pending, seen_now)
    assert published is False
    assert msg is None
    assert uid is None
    assert pending == set()
    assert seen_cache == {}
    assert len(seen_order) == 0


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


@pytest.mark.asyncio
async def test_price_ingest_task_uses_event_time(monkeypatch):
    event_ms = 1769515200000
    fake_msg = json.dumps({
        "data": {"s": "BTCUSDT", "c": "43210.12", "E": event_ms}
    })

    def fake_connect(url):
        return FakeWS([fake_msg])

    monkeypatch.setattr(ingest.websockets, "connect", fake_connect)

    proc = FakeProcessor()
    proc.producer = FakeProducer()

    with pytest.raises(FakeWS.StopStream):
        await ingest.price_ingest_task(proc)

    _, payload = proc.producer.sent[0]
    data = json.loads(payload.decode())
    assert data["time"].startswith("2026-01-27T12:00:00")


@pytest.mark.asyncio
async def test_price_ingest_task_falls_back_to_now(monkeypatch):
    fake_msg = json.dumps({
        "data": {"s": "BTCUSDT", "c": "43210.12"}
    })

    def fake_connect(url):
        return FakeWS([fake_msg])

    fixed_now = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(ingest, "now_utc", lambda: fixed_now)
    monkeypatch.setattr(ingest.websockets, "connect", fake_connect)

    proc = FakeProcessor()
    proc.producer = FakeProducer()

    with pytest.raises(FakeWS.StopStream):
        await ingest.price_ingest_task(proc)

    _, payload = proc.producer.sent[0]
    data = json.loads(payload.decode())
    assert data["time"].startswith("2026-01-27T12:00:00")
