from collections import defaultdict
from datetime import datetime, timedelta, timezone

import pytest

from processor.src.domain.windows import PriceWindow
from processor.src.services import price_pipeline


class FakeLog:
    def error(self, *args, **kwargs):
        pass


class FakeProc:
    def __init__(self):
        self.price_windows = defaultdict(PriceWindow)
        self.log = FakeLog()


def test_compute_price_metrics_no_db_side_effects(monkeypatch):
    proc = FakeProc()
    ts = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    proc.price_windows["btcusdt"].add(ts - timedelta(seconds=30), 100.0)

    called = {"insert_metric": False}

    async def fake_insert_metric(*args, **kwargs):
        called["insert_metric"] = True

    monkeypatch.setattr(price_pipeline, "insert_metric", fake_insert_metric)

    inserted, metrics = price_pipeline.compute_price_metrics(proc, "btcusdt", 110.0, ts)
    assert inserted is True
    assert metrics is not None
    assert "return_1m" in metrics
    assert called["insert_metric"] is False


@pytest.mark.asyncio
async def test_persist_and_publish_price_handles_none_metrics(monkeypatch):
    proc = FakeProc()
    ts = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)

    async def fake_insert_metric(*args, **kwargs):
        raise AssertionError("insert_metric should not be called when metrics is None")

    calls = {"anomaly": None}

    async def fake_check_anomalies(_proc, _symbol, _ts, metrics):
        calls["anomaly"] = metrics

    monkeypatch.setattr(price_pipeline, "insert_metric", fake_insert_metric)
    monkeypatch.setattr(price_pipeline, "check_anomalies", fake_check_anomalies)

    await price_pipeline.persist_and_publish_price(proc, "btcusdt", ts, None)
    assert calls["anomaly"] == {}
