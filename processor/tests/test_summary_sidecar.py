import asyncio
import json
import base64
import pytest

from processor.src.services import summary_sidecar  # type: ignore
from processor.src.metrics import MetricsRegistry, NamespacedMetricsRegistry
from processor.src.config import settings  # type: ignore


class _FakeReader:
    def __init__(self, path="/metrics"):
        self._lines = [
            f"GET {path} HTTP/1.1\r\n".encode(),
            b"Host: localhost\r\n",
            b"\r\n",
        ]

    async def readline(self):
        if not self._lines:
            return b""
        return self._lines.pop(0)


class _FakeWriter:
    def __init__(self):
        self.data = b""
        self.closed = False

    def write(self, data: bytes):
        self.data += data

    async def drain(self):
        return None

    def close(self):
        self.closed = True


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


async def _run_summary_record(
    payload,
    monkeypatch,
    llm_result="LLM SUMMARY",
    fail_llm=False,
    fail_publish=False,
    consumer=None,
    producer=None,
    pool=None,
    fetch_published=None,
    mark_published=None,
):
    class FakeConsumer:
        def __init__(self):
            self.commits = []

        async def commit(self, offsets):
            self.commits.append(offsets)

    class FakeMsg:
        topic = "summaries"
        partition = 0
        offset = 10
        value = json.dumps(payload).encode()

    if fail_llm:
        def failing_llm(*args, **kwargs):
            raise RuntimeError("fail")
        monkeypatch.setattr(summary_sidecar, "llm_summarize", failing_llm)
    else:
        monkeypatch.setattr(summary_sidecar, "llm_summarize", lambda *args, **kwargs: llm_result)

    async def no_retry(fn, *args, **kwargs):
        kwargs.pop("log", None)
        kwargs.pop("op", None)
        kwargs.pop("max_attempts", None)
        return await fn(*args, **kwargs)

    if fail_publish:
        async def fail_handle(*args, **kwargs):
            raise RuntimeError("fail")
        monkeypatch.setattr(summary_sidecar, "publish_summary_alert", fail_handle)

    if fetch_published is None:
        async def fetch_published(*args, **kwargs):
            return False
    if mark_published is None:
        async def mark_published(*args, **kwargs):
            return None

    monkeypatch.setattr(summary_sidecar, "fetch_anomaly_alert_published", fetch_published)
    monkeypatch.setattr(summary_sidecar, "mark_anomaly_alert_published", mark_published)
    monkeypatch.setattr(summary_sidecar, "with_retries", no_retry)

    consumer = consumer or FakeConsumer()
    producer = producer or FakeProducer()
    pool = pool or FakePool()
    log = summary_sidecar.get_logger(__name__)
    ok = await summary_sidecar.process_summary_record(
        FakeMsg(),
        consumer,
        producer,
        pool,
        log,
        summary_sidecar.asyncio.Semaphore(1),
    )
    return ok, consumer, producer, pool


@pytest.mark.asyncio
async def test_commit_message_uses_non_null_metadata():
    class FakeConsumer:
        def __init__(self):
            self.calls = []

        async def commit(self, offsets):
            self.calls.append(offsets)

    class FakeMsg:
        topic = "summaries"
        partition = 2
        offset = 41

    class FakeLog:
        def __init__(self):
            self.warnings = []

        def warning(self, key, extra=None):
            self.warnings.append((key, extra))

    consumer = FakeConsumer()
    log = FakeLog()

    await summary_sidecar._commit_message(consumer, FakeMsg(), log)

    assert len(consumer.calls) == 1
    commit_map = consumer.calls[0]
    assert len(commit_map) == 1
    tp, offset_meta = next(iter(commit_map.items()))
    assert tp.topic == "summaries"
    assert tp.partition == 2
    assert offset_meta.offset == 42
    assert offset_meta.metadata == ""
    assert log.warnings == []


@pytest.mark.asyncio
async def test_commit_message_logs_warning_on_failure():
    class FailingConsumer:
        async def commit(self, offsets):
            raise RuntimeError("commit boom")

    class FakeMsg:
        topic = "summaries"
        partition = 1
        offset = 10

    class FakeLog:
        def __init__(self):
            self.warnings = []

        def warning(self, key, extra=None):
            self.warnings.append((key, extra))

    log = FakeLog()

    await summary_sidecar._commit_message(FailingConsumer(), FakeMsg(), log)

    assert len(log.warnings) == 1
    key, extra = log.warnings[0]
    assert key == "summary_commit_failed"
    assert extra["topic"] == "summaries"
    assert extra["partition"] == 1
    assert extra["offset"] == 10
    assert extra["error_type"] == "RuntimeError"
    assert "commit boom" in extra["error"]


@pytest.mark.asyncio
async def test_persist_and_publish_summary(monkeypatch):
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

    ok, consumer, producer, pool = await _run_summary_record(payload, monkeypatch)
    assert ok is True

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
    assert out["event_id"] == "2026-01-27T12:00:00+00:00:btcusdt:1m"


@pytest.mark.asyncio
async def test_persist_and_publish_summary_llm_failure(monkeypatch):
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

    ok, consumer, producer, pool = await _run_summary_record(payload, monkeypatch, fail_llm=True)
    assert ok is False
    assert len(producer.sent) == 1
    topic, _ = producer.sent[0]
    assert topic == settings.summaries_dlq_topic
    assert len(consumer.commits) == 1


@pytest.mark.asyncio
async def test_persist_and_publish_summary_batch(monkeypatch):
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

    pool = FakePool()
    producer = FakeProducer()
    consumer = None
    for p in payloads:
        ok, consumer, producer, pool = await _run_summary_record(
            p,
            monkeypatch,
            llm_result="LLM SUMMARY",
            consumer=consumer,
            producer=producer,
            pool=pool,
        )
        assert ok is True

    # Two upserts, two publishes
    assert len(pool.calls) == 2
    assert len(producer.sent) == 2
    symbols = {params[1] for _, params in pool.calls}
    assert symbols == {"btcusdt", "ethusdt"}


@pytest.mark.asyncio
async def test_summary_preserves_event_id(monkeypatch):
    payload = {
        "event_id": "2026-01-27T12:00:00+00:00:btcusdt:1m",
        "time": "2026-01-27T12:00:00+00:00",
        "symbol": "btcusdt",
        "window": "1m",
        "direction": "up",
        "ret": 0.07,
        "threshold": 0.05,
        "headline": "headline",
        "sentiment": 0.1,
    }

    ok, consumer, producer, pool = await _run_summary_record(payload, monkeypatch)
    assert ok is True
    topic, out_payload = producer.sent[0]
    assert topic == settings.alerts_topic
    out = json.loads(out_payload.decode())
    assert out["event_id"] == payload["event_id"]


@pytest.mark.asyncio
async def test_summary_skips_publish_when_already_published(monkeypatch):
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

    async def published_true(*args, **kwargs):
        return True

    async def fail_publish(*args, **kwargs):
        raise AssertionError("publish should be skipped when already published")

    async def fail_mark(*args, **kwargs):
        raise AssertionError("mark should be skipped when already published")

    monkeypatch.setattr(summary_sidecar, "publish_summary_alert", fail_publish)

    base_metrics = MetricsRegistry(service_name="summary_sidecar")
    metrics = NamespacedMetricsRegistry(base_metrics, "summary")
    monkeypatch.setattr(summary_sidecar, "get_metrics", lambda *args, **kwargs: metrics)

    ok, consumer, producer, pool = await _run_summary_record(
        payload,
        monkeypatch,
        fetch_published=published_true,
        mark_published=fail_mark,
    )
    assert ok is True
    assert len(producer.sent) == 0
    assert len(pool.calls) == 1
    assert metrics.snapshot()["counters"].get("summary.summary_publish_skipped") == 1


@pytest.mark.asyncio
async def test_summary_publishes_and_marks_when_unpublished(monkeypatch):
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

    calls = {"mark": 0}

    async def published_false(*args, **kwargs):
        return False

    async def mark_called(*args, **kwargs):
        calls["mark"] += 1

    ok, consumer, producer, pool = await _run_summary_record(
        payload,
        monkeypatch,
        fetch_published=published_false,
        mark_published=mark_called,
    )
    assert ok is True
    assert len(producer.sent) == 1


@pytest.mark.asyncio
async def test_summary_metrics_handler_returns_json():
    sidecar = summary_sidecar.SummarySidecar()
    reader = _FakeReader()
    writer = _FakeWriter()

    await sidecar._handle_metrics(reader, writer)
    assert b"200 OK" in writer.data
    body = writer.data.split(b"\r\n\r\n", 1)[1]
    payload = json.loads(body.decode())
    assert "counters" in payload
    assert "rolling" in payload
    assert payload["service_name"] in ("summary_sidecar", "processor")
    assert "start_time" in payload


@pytest.mark.asyncio
async def test_process_summary_record_sends_dlq_on_failure(monkeypatch):
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
    ok, consumer, producer, pool = await _run_summary_record(payload, monkeypatch, fail_publish=True)
    assert ok is False
    assert len(producer.sent) == 1
    topic, _ = producer.sent[0]
    assert topic == settings.summaries_dlq_topic
    assert len(consumer.commits) == 1


@pytest.mark.asyncio
async def test_summary_dlq_failure_increments_metric(monkeypatch, tmp_path):
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

    class FailingProducer(FakeProducer):
        async def send_and_wait(self, topic, payload):
            raise RuntimeError("dlq fail")

    buffer_path = tmp_path / "summary_dlq.jsonl"
    monkeypatch.setattr(settings, "summary_dlq_buffer_path", str(buffer_path))
    monkeypatch.setattr(settings, "summary_dlq_buffer_max_bytes", 1024)

    base_metrics = MetricsRegistry(service_name="summary_sidecar")
    metrics = NamespacedMetricsRegistry(base_metrics, "summary")
    monkeypatch.setattr(summary_sidecar, "get_metrics", lambda *args, **kwargs: metrics)
    before = metrics.snapshot()["counters"].get("summary.summary_dlq_failed", 0)
    ok, consumer, producer, pool = await _run_summary_record(
        payload,
        monkeypatch,
        fail_publish=True,
        producer=FailingProducer(),
    )
    assert ok is False
    after = metrics.snapshot()["counters"].get("summary.summary_dlq_failed", 0)
    assert after == before + 1


@pytest.mark.asyncio
async def test_summary_metrics_increment_on_success(monkeypatch):
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

    base_metrics = MetricsRegistry(service_name="summary_sidecar")
    metrics = NamespacedMetricsRegistry(base_metrics, "summary")
    monkeypatch.setattr(summary_sidecar, "get_metrics", lambda *args, **kwargs: metrics)

    ok, _, _, _ = await _run_summary_record(payload, monkeypatch)
    assert ok is True
    snapshot = metrics.snapshot()
    assert snapshot["counters"].get("summary.summary_success") == 1
    assert snapshot["rolling"]["summary.summary_latency_ms"]["count"] == 1


@pytest.mark.asyncio
async def test_summary_metrics_increment_on_failure(monkeypatch):
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

    base_metrics = MetricsRegistry(service_name="summary_sidecar")
    metrics = NamespacedMetricsRegistry(base_metrics, "summary")
    monkeypatch.setattr(summary_sidecar, "get_metrics", lambda *args, **kwargs: metrics)

    ok, _, _, _ = await _run_summary_record(payload, monkeypatch, fail_publish=True)
    assert ok is False
    snapshot = metrics.snapshot()
    assert snapshot["counters"].get("summary.summary_failures") == 1
    assert snapshot["counters"].get("summary.summary_dlq") == 1


@pytest.mark.asyncio
async def test_summary_dlq_failure_buffers_message(monkeypatch, tmp_path):
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

    class FailingProducer(FakeProducer):
        async def send_and_wait(self, topic, payload):
            raise RuntimeError("dlq fail")

    buffer_path = tmp_path / "summary_dlq.jsonl"
    monkeypatch.setattr(settings, "summary_dlq_buffer_path", str(buffer_path))
    monkeypatch.setattr(settings, "summary_dlq_buffer_max_bytes", 1024)

    ok, _, _, _ = await _run_summary_record(
        payload,
        monkeypatch,
        fail_publish=True,
        producer=FailingProducer(),
    )
    assert ok is False
    assert buffer_path.exists()
    lines = buffer_path.read_text().strip().splitlines()
    assert len(lines) == 1
    record = json.loads(lines[0])
    decoded = base64.b64decode(record["payload_b64"])
    assert json.loads(decoded.decode())["symbol"] == "btcusdt"


@pytest.mark.asyncio
async def test_summary_dlq_success_does_not_buffer(monkeypatch, tmp_path):
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

    buffer_path = tmp_path / "summary_dlq.jsonl"
    monkeypatch.setattr(settings, "summary_dlq_buffer_path", str(buffer_path))
    monkeypatch.setattr(settings, "summary_dlq_buffer_max_bytes", 1024)

    ok, _, _, _ = await _run_summary_record(payload, monkeypatch, fail_publish=True)
    assert ok is False
    assert not buffer_path.exists()
