import hashlib
import json
import pytest
from datetime import datetime, timezone

from processor.src.config import settings
from processor.src.io.models.messages import NewsMsg
from processor.src.services import sentiment_sidecar
from processor.src.utils import simple_sentiment


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


class FakeConsumer:
    def __init__(self):
        self.commits = []

    async def commit(self, offsets):
        self.commits.append(offsets)


class FakeMsg:
    topic = "news"
    partition = 0
    offset = 5

    def __init__(self, payload):
        self.value = payload


def _expected_event_id(payload: NewsMsg) -> str:
    title_norm = (payload.title or "").strip().lower()
    source_norm = (payload.source or "unknown").strip().lower()
    url_norm = (payload.url or "").strip()
    canonical = f"{payload.time}|{source_norm}|{title_norm}|{url_norm}"
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:12]
    return f"news:{source_norm}:{digest}"


async def _run_sentiment_flow(messages, consumer, producer, pool):
    metrics = sentiment_sidecar.MetricsRegistry()
    parsed, results, fallback_used, *_ = await sentiment_sidecar.infer_sentiment_batch(
        messages,
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
    return fallback_used, metrics


@pytest.mark.asyncio
async def test_sentiment_sidecar_enriches_and_publishes(monkeypatch):
    pool = FakePool()
    producer = FakeProducer()
    consumer = FakeConsumer()

    def fake_predict(texts):
        return ([(0.5, "positive", 0.9) for _ in texts], False)

    monkeypatch.setattr(sentiment_sidecar.sentiment_model, "predict", fake_predict)

    msg = NewsMsg(
        time=datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc),
        title="headline",
        url="http://x",
        source="rss",
        sentiment=0.0,
    )
    await _run_sentiment_flow([FakeMsg(msg.to_bytes())], consumer, producer, pool)

    assert len(pool.calls) == 1
    sql, params = pool.calls[0]
    assert "INSERT INTO headlines" in sql
    assert params[4] == 0.5

    assert len(producer.sent) == 1
    topic, payload = producer.sent[0]
    assert topic == settings.news_enriched_topic
    out = json.loads(payload.decode())
    assert out["sentiment"] == 0.5
    assert out["label"] == "positive"
    assert out["confidence"] == 0.9
    assert out["event_id"] == _expected_event_id(msg)
    assert out["event_id"].startswith("news:rss:")
    assert len(consumer.commits) == 1


@pytest.mark.asyncio
async def test_sentiment_sidecar_falls_back_on_model_error(monkeypatch):
    pool = FakePool()
    producer = FakeProducer()
    consumer = FakeConsumer()

    def boom(*args, **kwargs):
        raise RuntimeError("fail")

    monkeypatch.setattr(sentiment_sidecar.sentiment_model, "predict", boom)

    title = "bitcoin up"
    msg = NewsMsg(
        time=datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc),
        title=title,
        url=None,
        source="rss",
        sentiment=0.0,
    )
    fallback_used, metrics = await _run_sentiment_flow([FakeMsg(msg.to_bytes())], consumer, producer, pool)
    assert fallback_used is True
    snapshot = metrics.snapshot()
    assert snapshot["counters"].get("sentiment_fallbacks") == 1

    assert len(producer.sent) == 1
    _, payload = producer.sent[0]
    out = json.loads(payload.decode())
    assert out["label"] is None
    assert out["confidence"] is None
    assert out["sentiment"] == pytest.approx(float(simple_sentiment(title)), rel=1e-6)
    assert len(consumer.commits) == 1


@pytest.mark.asyncio
async def test_sentiment_sidecar_dlq_on_failure(monkeypatch):
    pool = FakePool()
    producer = FakeProducer()
    consumer = FakeConsumer()

    def fake_predict(texts):
        return ([(0.1, "neutral", 0.7) for _ in texts], False)

    async def fail_upsert(*args, **kwargs):
        raise RuntimeError("db fail")

    async def no_retry(fn, *args, **kwargs):
        kwargs.pop("log", None)
        kwargs.pop("op", None)
        kwargs.pop("max_attempts", None)
        return await fn(*args, **kwargs)

    monkeypatch.setattr(sentiment_sidecar.sentiment_model, "predict", fake_predict)
    monkeypatch.setattr(sentiment_sidecar, "_upsert_headline", fail_upsert)
    monkeypatch.setattr(sentiment_sidecar, "with_retries", no_retry)

    msg = NewsMsg(
        time=datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc),
        title="headline",
        url=None,
        source="rss",
        sentiment=0.0,
    )
    await _run_sentiment_flow([FakeMsg(msg.to_bytes())], consumer, producer, pool)

    assert len(producer.sent) == 1
    topic, _ = producer.sent[0]
    assert topic == settings.news_dlq_topic
    assert len(consumer.commits) == 1


@pytest.mark.asyncio
async def test_sentiment_fallback_on_slow_batch(monkeypatch):
    pool = FakePool()
    producer = FakeProducer()
    consumer = FakeConsumer()

    monkeypatch.setattr(settings, "sentiment_max_latency_ms", 1)
    monkeypatch.setattr(settings, "sentiment_fallback_on_slow", True)

    def fake_predict(texts):
        return ([(0.5, "positive", 0.9) for _ in texts], False)

    times = [0.0, 0.01]

    def fake_perf_counter():
        return times.pop(0)

    monkeypatch.setattr(sentiment_sidecar.sentiment_model, "predict", fake_predict)
    monkeypatch.setattr(sentiment_sidecar.time, "perf_counter", fake_perf_counter)

    msg = NewsMsg(
        time=datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc),
        title="headline",
        url="http://x",
        source="rss",
        sentiment=0.0,
    )
    fallback_used, metrics = await _run_sentiment_flow([FakeMsg(msg.to_bytes())], consumer, producer, pool)
    assert fallback_used is True
    snapshot = metrics.snapshot()
    assert snapshot["counters"].get("sentiment_fallbacks") == 1


@pytest.mark.asyncio
async def test_sentiment_result_mismatch_falls_back(monkeypatch):
    pool = FakePool()
    producer = FakeProducer()
    consumer = FakeConsumer()

    def fake_predict(texts):
        return ([], False)

    monkeypatch.setattr(sentiment_sidecar.sentiment_model, "predict", fake_predict)

    msg = NewsMsg(
        time=datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc),
        title="headline",
        url="http://x",
        source="rss",
        sentiment=0.0,
    )
    fallback_used, metrics = await _run_sentiment_flow([FakeMsg(msg.to_bytes())], consumer, producer, pool)
    assert fallback_used is True
    snapshot = metrics.snapshot()
    assert snapshot["counters"].get("sentiment_errors") == 1


@pytest.mark.asyncio
async def test_sentiment_dlq_failure_does_not_commit(monkeypatch):
    pool = FakePool()
    producer = FakeProducer()
    consumer = FakeConsumer()

    def fake_predict(texts):
        return ([(0.1, "neutral", 0.7) for _ in texts], False)

    async def fail_upsert(*args, **kwargs):
        raise RuntimeError("db fail")

    async def fail_send(*args, **kwargs):
        raise RuntimeError("dlq fail")

    async def no_retry(fn, *args, **kwargs):
        kwargs.pop("log", None)
        kwargs.pop("op", None)
        kwargs.pop("max_attempts", None)
        return await fn(*args, **kwargs)

    monkeypatch.setattr(sentiment_sidecar.sentiment_model, "predict", fake_predict)
    monkeypatch.setattr(sentiment_sidecar, "_upsert_headline", fail_upsert)
    monkeypatch.setattr(sentiment_sidecar, "with_retries", no_retry)
    monkeypatch.setattr(producer, "send_and_wait", fail_send)

    msg = NewsMsg(
        time=datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc),
        title="headline",
        url=None,
        source="rss",
        sentiment=0.0,
    )
    _fallback_used, metrics = await _run_sentiment_flow([FakeMsg(msg.to_bytes())], consumer, producer, pool)

    after = metrics.snapshot()["counters"].get("sentiment_dlq_failed", 0)
    assert after == 1
    assert len(consumer.commits) == 0
