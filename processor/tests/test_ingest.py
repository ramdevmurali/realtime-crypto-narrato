import asyncio
import json
from collections import deque
from datetime import datetime, timezone, timedelta

import pytest

from processor.src.services import ingest
from processor.src.services import sentiment_sidecar
from processor.src.config import settings
from processor.src import metrics as metrics_module


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


class FakeLog:
    def __init__(self):
        self.warnings = []
        self.infos = []

    def warning(self, msg, extra=None):
        self.warnings.append((msg, extra or {}))

    def info(self, msg, extra=None):
        self.infos.append((msg, extra or {}))

    def error(self, msg, extra=None):
        self.warnings.append((msg, extra or {}))


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
async def test_ingest_to_sentiment_flow(monkeypatch):
    class _Pool:
        def __init__(self):
            self.calls = []

        async def execute(self, sql, *params):
            self.calls.append((sql.strip(), params))

    class _Producer:
        def __init__(self):
            self.sent = []

        async def send_and_wait(self, topic, payload):
            self.sent.append((topic, payload))

    class _Consumer:
        def __init__(self):
            self.commits = []

        async def commit(self, offsets):
            self.commits.append(offsets)

    class _Msg:
        topic = "news"
        partition = 0
        offset = 1

        def __init__(self, payload):
            self.value = payload

    def _expected_event_id(payload):
        title_norm = (payload.title or "").strip().lower()
        source_norm = (payload.source or "unknown").strip().lower()
        url_norm = (payload.url or "").strip()
        canonical = f"{payload.time}|{source_norm}|{title_norm}|{url_norm}"
        digest = sentiment_sidecar.hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:12]
        return f"news:{source_norm}:{digest}"

    def fake_predict(texts):
        return ([(0.7, "positive", 0.9) for _ in texts], False)

    monkeypatch.setattr(sentiment_sidecar.sentiment_model, "predict", fake_predict)

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
    published, msg, _ = ingest.build_news_msg(entry, seen_cache, seen_order, pending, seen_now)
    assert published is True
    assert msg is not None

    pool = _Pool()
    producer = _Producer()
    consumer = _Consumer()
    metrics = sentiment_sidecar.MetricsRegistry(service_name="sentiment_sidecar")

    parsed, results, _fallback_used, *_ = await sentiment_sidecar.infer_sentiment_batch(
        [_Msg(msg.to_bytes())],
        consumer,
        producer,
        sentiment_sidecar.log,
        metrics,
    )
    await sentiment_sidecar.persist_and_publish_sentiment_batch(
        parsed,
        results,
        producer,
        pool,
        consumer,
        sentiment_sidecar.log,
        metrics,
    )

    assert len(pool.calls) == 1
    sql, params = pool.calls[0]
    assert "INSERT INTO headlines" in sql
    assert params[4] == 0.7

    assert len(producer.sent) == 1
    topic, payload = producer.sent[0]
    assert topic == settings.news_enriched_topic
    out = json.loads(payload.decode())
    assert out["event_id"] == _expected_event_id(msg)


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


@pytest.mark.asyncio
async def test_news_ingest_task_merges_multi_feed_and_dedupes(monkeypatch):
    metrics_module._GLOBAL_METRICS = metrics_module.MetricsRegistry(service_name="processor")

    class Feed:
        def __init__(self, title, entries):
            self.feed = {"title": title}
            self.entries = entries

        def get(self, key, default=None):
            if key == "feed":
                return self.feed
            return default

    def fake_parse(url):
        if "bad" in url:
            raise RuntimeError("feed down")
        if "feed-a" in url:
            return Feed(
                "feed-a",
                [
                    {
                        "id": "shared",
                        "title": "A shared headline",
                        "link": "https://a.example/shared",
                        "published": "2026-01-27T12:00:00Z",
                    },
                ],
            )
        return Feed(
            "feed-b",
            [
                {
                    "id": "shared",
                    "title": "B duplicate headline",
                    "link": "https://b.example/shared",
                    "published": "2026-01-27T12:00:00Z",
                },
                {
                    "id": "unique-b",
                    "title": "B unique headline",
                    "link": "https://b.example/unique",
                    "published": "2026-01-27T12:00:01Z",
                },
            ],
        )

    published = []

    async def fake_publish(_processor, msg):
        published.append(msg)

    async def stop_after_one_poll(_seconds):
        raise asyncio.CancelledError()

    monkeypatch.setattr(settings, "news_rss_urls_raw", "https://feed-a/rss,https://bad/rss,https://feed-b/rss")
    monkeypatch.setattr(settings, "news_batch_limit", 20)
    monkeypatch.setattr(ingest.feedparser, "parse", fake_parse)
    monkeypatch.setattr(ingest, "publish_news_msg", fake_publish)
    monkeypatch.setattr(ingest.asyncio, "sleep", stop_after_one_poll)

    proc = FakeProcessor()
    with pytest.raises(asyncio.CancelledError):
        await ingest.news_ingest_task(proc)

    assert len(published) == 2
    assert {msg.source for msg in published} == {"feed-a", "feed-b"}
    counters = metrics_module.get_metrics().snapshot()["counters"]
    assert counters.get("processor.news_entries_ingested") == 2
    assert counters.get("processor.news_feed_errors") == 1


@pytest.mark.asyncio
async def test_news_ingest_task_tracks_stale_headline(monkeypatch):
    metrics_module._GLOBAL_METRICS = metrics_module.MetricsRegistry(service_name="processor")

    class Feed:
        def __init__(self):
            self.feed = {"title": "feed-a"}
            self.entries = []

        def get(self, key, default=None):
            if key == "feed":
                return self.feed
            return default

    sleep_calls = {"count": 0}

    async def sleep_two_cycles(_seconds):
        sleep_calls["count"] += 1
        if sleep_calls["count"] >= 2:
            raise asyncio.CancelledError()
        return None

    monkeypatch.setattr(settings, "news_rss_urls_raw", "https://feed-a/rss")
    monkeypatch.setattr(settings, "headline_stale_warn_sec", 60)
    monkeypatch.setattr(settings, "news_stale_log_every", 2)
    monkeypatch.setattr(ingest.feedparser, "parse", lambda _url: Feed())
    monkeypatch.setattr(ingest.asyncio, "sleep", sleep_two_cycles)

    proc = FakeProcessor()
    proc.log = FakeLog()
    proc.latest_headline = ("Old headline", 0.1, datetime(2026, 1, 27, 11, 0, tzinfo=timezone.utc))

    with pytest.raises(asyncio.CancelledError):
        await ingest.news_ingest_task(proc)

    counters = metrics_module.get_metrics().snapshot()["counters"]
    rolling = metrics_module.get_metrics().snapshot()["rolling"]
    assert counters.get("processor.news_stale_polls") == 2
    assert rolling.get("processor.latest_headline_age_sec", {}).get("count", 0) >= 2
    assert len([w for w in proc.log.warnings if w[0] == "headline_stale"]) >= 2
