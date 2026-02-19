from collections import defaultdict
from datetime import datetime, timedelta, timezone

import pytest

from processor.src.domain.windows import PriceWindow
from processor.src import config as config_module
from processor.src.services import price_pipeline


class FakeLog:
    def error(self, *args, **kwargs):
        pass


class FakeProc:
    def __init__(self):
        windows = config_module.get_windows()
        self.price_windows = defaultdict(
            lambda: PriceWindow(
                history_maxlen=config_module.settings.window_history_maxlen,
                max_window=max(windows.values()),
                vol_resample_sec=config_module.settings.vol_resample_sec,
                window_max_gap_factor=config_module.settings.window_max_gap_factor,
                vol_max_gap_factor=config_module.settings.vol_max_gap_factor,
            )
        )
        self.log = FakeLog()
        self.producer = None


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

    async def fake_check_anomalies(_proc, _symbol, _ts, metrics, **_kwargs):
        calls["anomaly"] = metrics

    monkeypatch.setattr(price_pipeline, "insert_metric", fake_insert_metric)
    monkeypatch.setattr(price_pipeline, "check_anomalies", fake_check_anomalies)

    await price_pipeline.persist_and_publish_price(proc, "btcusdt", ts, None)
    assert calls["anomaly"] == {}


@pytest.mark.asyncio
async def test_process_price_rolls_back_on_metric_failure(monkeypatch):
    proc = FakeProc()
    ts = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    win = proc.price_windows["btcusdt"]
    win.add(ts - timedelta(seconds=30), 100.0)
    snapshot = list(win.buffer)

    async def fake_insert_price(*args, **kwargs):
        return True

    async def fail_insert_metric(*args, **kwargs):
        raise RuntimeError("fail")

    async def fail_check_anomalies(*args, **kwargs):
        raise AssertionError("check_anomalies should not run")

    monkeypatch.setattr(price_pipeline, "insert_price", fake_insert_price)
    monkeypatch.setattr(price_pipeline, "insert_metric", fail_insert_metric)
    monkeypatch.setattr(price_pipeline, "check_anomalies", fail_check_anomalies)

    with pytest.raises(price_pipeline.PipelineError):
        await price_pipeline.process_price(proc, "btcusdt", 110.0, ts)

    assert win.buffer == snapshot
    assert win.z_ewma == {}


@pytest.mark.asyncio
async def test_process_price_rolls_back_on_anomaly_failure(monkeypatch):
    proc = FakeProc()
    ts = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    win = proc.price_windows["ethusdt"]
    win.add(ts - timedelta(seconds=30), 200.0)
    snapshot = list(win.buffer)

    async def fake_insert_price(*args, **kwargs):
        return True

    async def ok_insert_metric(*args, **kwargs):
        return None

    async def fail_check_anomalies(*args, **kwargs):
        raise RuntimeError("anomaly fail")

    monkeypatch.setattr(price_pipeline, "insert_price", fake_insert_price)
    monkeypatch.setattr(price_pipeline, "insert_metric", ok_insert_metric)
    monkeypatch.setattr(price_pipeline, "check_anomalies", fail_check_anomalies)

    with pytest.raises(price_pipeline.PipelineError):
        await price_pipeline.process_price(proc, "ethusdt", 210.0, ts)

    assert win.buffer == snapshot
    assert win.z_ewma == {}


@pytest.mark.asyncio
async def test_process_price_rolls_back_on_compute_failure(monkeypatch):
    proc = FakeProc()
    ts = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    win = proc.price_windows["btcusdt"]
    win.add(ts - timedelta(seconds=30), 100.0)
    snapshot = list(win.buffer)

    async def fake_insert_price(*args, **kwargs):
        return True

    def fail_compute_metrics(*args, **kwargs):
        raise RuntimeError("compute fail")

    async def fail_insert_metric(*args, **kwargs):
        raise AssertionError("insert_metric should not be called when compute fails")

    async def fail_check_anomalies(*args, **kwargs):
        raise AssertionError("check_anomalies should not be called when compute fails")

    monkeypatch.setattr(price_pipeline, "insert_price", fake_insert_price)
    monkeypatch.setattr(price_pipeline, "compute_metrics", fail_compute_metrics)
    monkeypatch.setattr(price_pipeline, "insert_metric", fail_insert_metric)
    monkeypatch.setattr(price_pipeline, "check_anomalies", fail_check_anomalies)

    with pytest.raises(price_pipeline.PipelineError):
        await price_pipeline.process_price(proc, "btcusdt", 110.0, ts)

    assert win.buffer == snapshot
    assert win.z_ewma == {}
