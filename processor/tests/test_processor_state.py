from datetime import datetime, timedelta, timezone

import pytest

from processor.src.config import settings
from processor.src.processor_state import ProcessorStateImpl


def test_should_drop_late_allows_within_tolerance(monkeypatch):
    monkeypatch.setattr(settings, "late_price_tolerance_sec", 5)
    proc = ProcessorStateImpl()
    last_ts = datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc)
    proc.last_price_ts["btcusdt"] = last_ts

    ts = last_ts - timedelta(seconds=1)
    should_drop = proc.should_drop_late("btcusdt", ts)

    assert should_drop is False
    assert proc.last_price_ts["btcusdt"] == last_ts


def test_should_drop_late_allows_equal_timestamp(monkeypatch):
    monkeypatch.setattr(settings, "late_price_tolerance_sec", 5)
    proc = ProcessorStateImpl()
    last_ts = datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc)
    proc.last_price_ts["btcusdt"] = last_ts

    should_drop = proc.should_drop_late("btcusdt", last_ts)

    assert should_drop is False
    assert proc.last_price_ts["btcusdt"] == last_ts
