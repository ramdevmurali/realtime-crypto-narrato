import json
import pytest

from processor.src import app as app_module
from processor.src.services import price_pipeline as pipeline_module


class FakeConsumer:
    def __init__(self, msg):
        self.msg = msg
        self._sent = False
        self.commits = []

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._sent:
            raise StopAsyncIteration
        self._sent = True
        return self.msg

    async def commit(self, offsets=None):
        self.commits.append(offsets)


class FakeConsumerSeq:
    def __init__(self, msgs):
        self.msgs = list(msgs)
        self.commits = []

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self.msgs:
            raise StopAsyncIteration
        return self.msgs.pop(0)

    async def commit(self, offsets=None):
        self.commits.append(offsets)


class FakeProducer:
    def __init__(self):
        self.sent = []

    async def send_and_wait(self, topic, payload):
        self.sent.append((topic, payload))


class FakeMsg:
    topic = "prices"
    partition = 0
    offset = 5

    def __init__(self, payload):
        self.value = payload


@pytest.mark.asyncio
async def test_process_prices_task_skips_duplicate(monkeypatch):
    proc = app_module.StreamProcessor()
    payload = json.dumps(
        {"symbol": "btcusdt", "price": 100.0, "time": "2026-01-27T12:00:00Z"}
    ).encode()
    msg = FakeMsg(payload)
    proc.consumer = FakeConsumer(msg)
    proc.producer = FakeProducer()

    async def no_retry(fn, *args, **kwargs):
        return await fn(*args, **kwargs)

    async def fake_insert_price(*args, **kwargs):
        return False

    async def fake_insert_metric(*args, **kwargs):
        raise AssertionError("insert_metric called")

    async def fake_check_anomalies(*args, **kwargs):
        raise AssertionError("check_anomalies called")

    monkeypatch.setattr(pipeline_module, "with_retries", no_retry)
    monkeypatch.setattr(pipeline_module, "insert_price", fake_insert_price)
    monkeypatch.setattr(pipeline_module, "insert_metric", fake_insert_metric)
    monkeypatch.setattr(pipeline_module, "check_anomalies", fake_check_anomalies)

    await proc.process_prices_task()

    assert "btcusdt" not in proc.price_windows
    assert len(proc.consumer.commits) == 1


@pytest.mark.asyncio
async def test_process_prices_task_drops_late_message(monkeypatch):
    proc = app_module.StreamProcessor()
    now = "2026-01-27T12:00:00Z"
    late = "2026-01-27T11:59:59Z"
    payload1 = json.dumps({"symbol": "btcusdt", "price": 100.0, "time": now}).encode()
    payload2 = json.dumps({"symbol": "btcusdt", "price": 101.0, "time": late}).encode()
    msg1 = FakeMsg(payload1)
    msg2 = FakeMsg(payload2)
    proc.consumer = FakeConsumerSeq([msg1, msg2])
    proc.producer = FakeProducer()

    async def no_retry(fn, *args, **kwargs):
        return await fn(*args, **kwargs)

    calls = {"insert_price": 0}

    async def fake_insert_price(*args, **kwargs):
        calls["insert_price"] += 1
        return False

    async def fake_insert_metric(*args, **kwargs):
        raise AssertionError("insert_metric called")

    async def fake_check_anomalies(*args, **kwargs):
        raise AssertionError("check_anomalies called")

    monkeypatch.setattr(pipeline_module, "with_retries", no_retry)
    monkeypatch.setattr(pipeline_module, "insert_price", fake_insert_price)
    monkeypatch.setattr(pipeline_module, "insert_metric", fake_insert_metric)
    monkeypatch.setattr(pipeline_module, "check_anomalies", fake_check_anomalies)

    await proc.process_prices_task()

    assert calls["insert_price"] == 1
    assert len(proc.consumer.commits) == 2
