import json
import pytest

from processor.src import app as app_module


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

    async def commit(self, offsets):
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

    monkeypatch.setattr(app_module, "with_retries", no_retry)
    monkeypatch.setattr(app_module, "insert_price", fake_insert_price)
    monkeypatch.setattr(app_module, "insert_metric", fake_insert_metric)
    monkeypatch.setattr(app_module, "check_anomalies", fake_check_anomalies)

    await proc.process_prices_task()

    assert "btcusdt" not in proc.price_windows
    assert len(proc.consumer.commits) == 1
