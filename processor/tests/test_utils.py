from datetime import datetime

import pytest

from processor.src import utils
from processor.src import metrics as metrics_module


@pytest.mark.asyncio
async def test_with_retries_retries_and_increments_metrics(monkeypatch):
    metrics_module._GLOBAL_METRICS = metrics_module.MetricsRegistry(service_name="processor")

    calls = {"count": 0}

    async def flaky():
        calls["count"] += 1
        if calls["count"] < 3:
            raise RuntimeError("fail")
        return "ok"

    async def no_sleep(*_args, **_kwargs):
        return None

    monkeypatch.setattr(utils, "sleep_backoff", no_sleep)
    monkeypatch.setattr(utils.settings, "retry_max_attempts", 3)

    result = await utils.with_retries(flaky, op="test_op")
    assert result == "ok"
    assert calls["count"] == 3

    counters = metrics_module.get_metrics().snapshot()["counters"]
    assert counters.get("processor.retry.total") == 2
    assert counters.get("processor.retry.test_op") == 2


@pytest.mark.asyncio
async def test_with_retries_raises_after_max_attempts(monkeypatch):
    metrics_module._GLOBAL_METRICS = metrics_module.MetricsRegistry(service_name="processor")

    async def always_fail():
        raise RuntimeError("boom")

    async def no_sleep(*_args, **_kwargs):
        return None

    monkeypatch.setattr(utils, "sleep_backoff", no_sleep)
    monkeypatch.setattr(utils.settings, "retry_max_attempts", 2)

    with pytest.raises(RuntimeError):
        await utils.with_retries(always_fail, op="test_op")

    counters = metrics_module.get_metrics().snapshot()["counters"]
    assert counters.get("processor.retry.total") == 2
    assert counters.get("processor.retry.test_op") == 2


@pytest.mark.asyncio
async def test_sleep_backoff_uses_jitter_bounds(monkeypatch):
    captured = []

    async def fake_sleep(value):
        captured.append(value)
        return None

    monkeypatch.setattr(utils.settings, "retry_backoff_base_sec", 1)
    monkeypatch.setattr(utils.settings, "retry_backoff_cap_sec", 4)
    monkeypatch.setattr(utils.settings, "retry_jitter_min", 0.5)
    monkeypatch.setattr(utils.settings, "retry_jitter_max", 1.5)
    monkeypatch.setattr(utils.asyncio, "sleep", fake_sleep)

    await utils.sleep_backoff(attempt=1)

    assert len(captured) == 1
    wait = captured[0]
    min_wait = 2 * 0.5
    max_wait = 2 * 1.5
    assert min_wait <= wait <= max_wait
    assert wait <= 4 * 1.5


def test_parse_iso_datetime_accepts_z_suffix():
    parsed = utils.parse_iso_datetime("2026-02-01T00:00:00Z")
    assert parsed.tzinfo is not None
    assert parsed.isoformat().endswith("+00:00")


def test_parse_iso_datetime_accepts_naive_datetime():
    parsed = utils.parse_iso_datetime(datetime(2026, 2, 1, 0, 0))
    assert parsed.tzinfo is not None
    assert parsed.isoformat().endswith("+00:00")


def test_parse_iso_datetime_rejects_invalid_type():
    with pytest.raises(TypeError):
        utils.parse_iso_datetime(123)
