import asyncio
from datetime import datetime, timezone

import pytest

from processor.src import anomaly
from processor.src.config import settings


class FakeProducer:
    def __init__(self):
        self.sent = []

    async def send_and_wait(self, topic, payload):
        self.sent.append((topic, payload))


class FakeProcessor:
    def __init__(self):
        self.producer = FakeProducer()
        self.last_alert = {}
        self.latest_headline = ("headline", 0.1)


def test_check_anomalies_triggers_and_updates_state(monkeypatch):
    calls = []

    async def fake_insert_anomaly(*args, **kwargs):
        calls.append(args)

    monkeypatch.setattr(anomaly, "insert_anomaly", fake_insert_anomaly)

    proc = FakeProcessor()
    ts = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    metrics = {"return_1m": settings.alert_threshold_1m + 0.01}

    asyncio.get_event_loop().run_until_complete(anomaly.check_anomalies(proc, "btcusdt", ts, metrics))

    assert proc.last_alert[("btcusdt", "1m")] == ts
    assert len(proc.producer.sent) == 1
    topic, payload = proc.producer.sent[0]
    assert topic == settings.alerts_topic
    assert b"btcusdt" in payload
    assert len(calls) == 1  # insert_anomaly called


def test_check_anomalies_below_threshold_no_alert(monkeypatch):
    calls = []

    async def fake_insert_anomaly(*args, **kwargs):
        calls.append(args)

    monkeypatch.setattr(anomaly, "insert_anomaly", fake_insert_anomaly)

    proc = FakeProcessor()
    ts = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    metrics = {"return_1m": settings.alert_threshold_1m - 0.01}

    asyncio.get_event_loop().run_until_complete(anomaly.check_anomalies(proc, "ethusdt", ts, metrics))

    assert ("ethusdt", "1m") not in proc.last_alert
    assert len(proc.producer.sent) == 0
    assert len(calls) == 0


def test_check_anomalies_respects_rate_limit(monkeypatch):
    calls = []

    async def fake_insert_anomaly(*args, **kwargs):
        calls.append(args)

    monkeypatch.setattr(anomaly, "insert_anomaly", fake_insert_anomaly)

    proc = FakeProcessor()
    ts = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    metrics = {"return_1m": settings.alert_threshold_1m + 0.02}

    loop = asyncio.get_event_loop()
    loop.run_until_complete(anomaly.check_anomalies(proc, "btc", ts, metrics))
    # Second call within 60s should be suppressed
    loop.run_until_complete(anomaly.check_anomalies(proc, "btc", ts + timedelta(seconds=10), metrics))

    assert proc.last_alert[("btc", "1m")] == ts
    assert len(proc.producer.sent) == 1
    assert len(calls) == 1


def test_check_anomalies_direction_up_down(monkeypatch):
    calls = []

    async def fake_insert_anomaly(*args, **kwargs):
        calls.append(args)

    monkeypatch.setattr(anomaly, "insert_anomaly", fake_insert_anomaly)

    proc = FakeProcessor()
    ts = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)

    # positive return => up
    metrics_up = {"return_1m": settings.alert_threshold_1m + 0.02}
    asyncio.get_event_loop().run_until_complete(anomaly.check_anomalies(proc, "up", ts, metrics_up))

    # negative return => down
    metrics_down = {"return_1m": -settings.alert_threshold_1m - 0.02}
    asyncio.get_event_loop().run_until_complete(anomaly.check_anomalies(proc, "down", ts + timedelta(seconds=70), metrics_down))

    assert len(proc.producer.sent) == 2
    up_payload = proc.producer.sent[0][1].decode()
    down_payload = proc.producer.sent[1][1].decode()
    assert '"direction": "up"' in up_payload
    assert '"direction": "down"' in down_payload
