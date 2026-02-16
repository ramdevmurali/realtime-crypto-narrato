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


@pytest.mark.asyncio
async def test_sentiment_sidecar_enriches_and_publishes(monkeypatch):
    pool = FakePool()
    producer = FakeProducer()
    consumer = FakeConsumer()

    def fake_predict(texts):
        return [(0.5, "positive", 0.9) for _ in texts]

    monkeypatch.setattr(sentiment_sidecar.sentiment_model, "predict", fake_predict)

    msg = NewsMsg(
        time=datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc),
        title="headline",
        url="http://x",
        source="rss",
        sentiment=0.0,
    )
    await sentiment_sidecar.process_sentiment_batch([FakeMsg(msg.to_bytes())], consumer, producer, pool, sentiment_sidecar.log)

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
    await sentiment_sidecar.process_sentiment_batch([FakeMsg(msg.to_bytes())], consumer, producer, pool, sentiment_sidecar.log)

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
        return [(0.1, "neutral", 0.7) for _ in texts]

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
    await sentiment_sidecar.process_sentiment_batch([FakeMsg(msg.to_bytes())], consumer, producer, pool, sentiment_sidecar.log)

    assert len(producer.sent) == 1
    topic, _ = producer.sent[0]
    assert topic == settings.news_dlq_topic
    assert len(consumer.commits) == 1
