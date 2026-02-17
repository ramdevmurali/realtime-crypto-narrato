import asyncio
from datetime import datetime, timezone, timedelta

import pytest
import json

from processor.src.services import anomaly_service
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
        self.latest_headline = (None, None, None)
        self.alerts_emitted = 0

    def record_alert(self, symbol, window, ts):
        self.last_alert[(symbol, window)] = ts
        self.alerts_emitted += 1


@pytest.mark.asyncio
async def test_check_anomalies_triggers_and_updates_state(monkeypatch):
    calls = []

    async def fake_insert_anomaly(*args, **kwargs):
        calls.append(args)
        return True

    monkeypatch.setattr(anomaly_service, "insert_anomaly", fake_insert_anomaly)

    proc = FakeProcessor()
    ts = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    metrics = {"return_1m": settings.alert_threshold_1m + 0.01}

    await anomaly_service.check_anomalies(proc, "btcusdt", ts, metrics)

    assert proc.last_alert[("btcusdt", "1m")] == ts
    assert len(proc.producer.sent) == 2  # summary request + alert
    topic0, payload0 = proc.producer.sent[0]
    assert topic0 == settings.summaries_topic
    assert b"btcusdt" in payload0
    topic1, payload1 = proc.producer.sent[1]
    assert topic1 == settings.alerts_topic
    assert b"btcusdt" in payload1
    assert len(calls) == 1  # insert_anomaly called


@pytest.mark.asyncio
async def test_check_anomalies_below_threshold_no_alert(monkeypatch):
    calls = []

    async def fake_insert_anomaly(*args, **kwargs):
        calls.append(args)
        return True

    monkeypatch.setattr(anomaly_service, "insert_anomaly", fake_insert_anomaly)

    proc = FakeProcessor()
    ts = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    metrics = {"return_1m": settings.alert_threshold_1m - 0.01}

    await anomaly_service.check_anomalies(proc, "ethusdt", ts, metrics)

    assert ("ethusdt", "1m") not in proc.last_alert
    assert len(proc.producer.sent) == 0
    assert len(calls) == 0


@pytest.mark.asyncio
async def test_check_anomalies_respects_rate_limit(monkeypatch):
    calls = []

    async def fake_insert_anomaly(*args, **kwargs):
        calls.append(args)
        return True

    monkeypatch.setattr(anomaly_service, "insert_anomaly", fake_insert_anomaly)

    proc = FakeProcessor()
    ts = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    metrics = {"return_1m": settings.alert_threshold_1m + 0.02}

    await anomaly_service.check_anomalies(proc, "btc", ts, metrics)
    # Second call within cooldown should be suppressed
    await anomaly_service.check_anomalies(proc, "btc", ts + timedelta(seconds=10), metrics)

    assert proc.last_alert[("btc", "1m")] == ts
    assert len(proc.producer.sent) == 2  # summary+alert once
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_check_anomalies_direction_up_down(monkeypatch):
    calls = []

    async def fake_insert_anomaly(*args, **kwargs):
        calls.append(args)
        return True

    monkeypatch.setattr(anomaly_service, "insert_anomaly", fake_insert_anomaly)

    proc = FakeProcessor()
    ts = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)

    # positive return => up
    metrics_up = {"return_1m": settings.alert_threshold_1m + 0.02}
    await anomaly_service.check_anomalies(proc, "up", ts, metrics_up)

    # negative return => down
    metrics_down = {"return_1m": -settings.alert_threshold_1m - 0.02}
    await anomaly_service.check_anomalies(proc, "down", ts + timedelta(seconds=70), metrics_down)

    assert len(proc.producer.sent) == 4  # summary+alert for each
    up_payload = json.loads(proc.producer.sent[1][1].decode())
    down_payload = json.loads(proc.producer.sent[3][1].decode())
    assert up_payload["direction"] == "up"
    assert down_payload["direction"] == "down"


@pytest.mark.asyncio
async def test_check_anomalies_includes_latest_headline_and_sentiment(monkeypatch):
    calls = []

    async def fake_insert_anomaly(*args, **kwargs):
        calls.append(args)
        return True

    monkeypatch.setattr(anomaly_service, "insert_anomaly", fake_insert_anomaly)

    proc = FakeProcessor()
    ts = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    proc.latest_headline = ("Breaking news", -0.3, ts - timedelta(minutes=5))
    metrics = {"return_1m": settings.alert_threshold_1m + 0.02}

    await anomaly_service.check_anomalies(proc, "btc", ts, metrics)

    assert len(proc.producer.sent) == 2  # summary+alert
    payload_str = proc.producer.sent[1][1].decode()
    assert "Breaking news" in payload_str
    assert "-0.3" in payload_str


@pytest.mark.asyncio
async def test_check_anomalies_omits_stale_headline(monkeypatch):
    calls = []

    async def fake_insert_anomaly(*args, **kwargs):
        calls.append(args)
        return True

    monkeypatch.setattr(anomaly_service, "insert_anomaly", fake_insert_anomaly)

    proc = FakeProcessor()
    ts = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    proc.latest_headline = ("Old news", 0.2, ts - timedelta(minutes=30))
    metrics = {"return_1m": settings.alert_threshold_1m + 0.02}

    await anomaly_service.check_anomalies(proc, "btc", ts, metrics)

    payload = json.loads(proc.producer.sent[1][1].decode())
    assert payload["headline"] is None
    assert payload["sentiment"] is None


@pytest.mark.asyncio
async def test_check_anomalies_no_metrics_no_alert(monkeypatch):
    calls = []

    async def fake_insert_anomaly(*args, **kwargs):
        calls.append(args)
        return True

    monkeypatch.setattr(anomaly_service, "insert_anomaly", fake_insert_anomaly)

    proc = FakeProcessor()
    ts = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)

    await anomaly_service.check_anomalies(proc, "btc", ts, {})

    assert len(proc.producer.sent) == 0
    assert len(calls) == 0
    assert proc.last_alert == {}


@pytest.mark.asyncio
async def test_check_anomalies_skips_duplicate_inserts(monkeypatch):
    async def fake_insert_anomaly(*args, **kwargs):
        return False

    monkeypatch.setattr(anomaly_service, "insert_anomaly", fake_insert_anomaly)

    proc = FakeProcessor()
    ts = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    metrics = {"return_1m": settings.alert_threshold_1m + 0.02}

    await anomaly_service.check_anomalies(proc, "btcusdt", ts, metrics)

    assert len(proc.producer.sent) == 0
    assert proc.last_alert == {}
