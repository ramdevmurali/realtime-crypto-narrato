from datetime import datetime, timedelta, timezone

import pytest

from processor.src.windows import PriceWindow
from processor.src import config as config_module


def test_price_window_prunes_old_points():
    win = PriceWindow()
    now = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)

    # add points from -25m to now every 5 minutes (6 points)
    times = [now - timedelta(minutes=m) for m in [25, 20, 15, 10, 5, 0]]
    prices = [100, 101, 102, 103, 104, 105]
    for t, p in zip(times, prices):
        win.add(t, p)

    # buffer should have pruned anything older than ~16 minutes
    kept_times = [t for t, _ in win.buffer]

    assert kept_times[0] >= now - timedelta(minutes=16)
    assert kept_times[-1] == now
    # ensure ordering preserved and expected count (last 4 points remain)
    assert kept_times == times[2:]


def test_get_return_happy_path():
    win = PriceWindow()
    now = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    data = [
        (now - timedelta(minutes=15), 100.0),
        (now - timedelta(minutes=5), 105.0),
        (now - timedelta(minutes=1), 110.0),
        (now, 115.0),
    ]
    for ts, p in data:
        win.add(ts, p)

    assert pytest.approx(win.get_return(now, timedelta(minutes=1)), rel=1e-6) == (115 - 110) / 110
    assert pytest.approx(win.get_return(now, timedelta(minutes=5)), rel=1e-6) == (115 - 105) / 105
    assert pytest.approx(win.get_return(now, timedelta(minutes=15)), rel=1e-6) == (115 - 100) / 100


def test_get_return_out_of_order_and_ignores_future():
    win = PriceWindow()
    now = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    # add out of order and include a future point
    win.add(now, 110.0)
    win.add(now - timedelta(minutes=5), 100.0)
    win.add(now + timedelta(minutes=1), 120.0)

    ret = win.get_return(now, timedelta(minutes=5))
    assert pytest.approx(ret, rel=1e-6) == (110.0 - 100.0) / 100.0


def test_get_return_gap_too_large():
    win = PriceWindow()
    now = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    win.add(now - timedelta(minutes=10), 100.0)
    win.add(now, 110.0)

    assert win.get_return(now, timedelta(minutes=1)) is None


def test_get_return_insufficient_data():
    win = PriceWindow()
    now = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    win.add(now, 120.0)

    assert win.get_return(now, timedelta(minutes=1)) is None
    assert win.get_return(now, timedelta(minutes=5)) is None
    assert win.get_return(now, timedelta(minutes=15)) is None


def test_get_vol_insufficient_data():
    win = PriceWindow()
    now = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    # fewer than 3 prices
    win.add(now - timedelta(minutes=1), 100)
    win.add(now, 101)

    assert win.get_vol(now, timedelta(minutes=5)) is None


def test_get_vol_happy_path(monkeypatch):
    # resample cadence at 1m to make expected values deterministic
    monkeypatch.setattr(config_module.settings, "vol_resample_sec", 60)
    win = PriceWindow()
    now = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    # uneven sampling: prices every 2 minutes
    prices = [100, 110, 105, 115]
    times = [now - timedelta(minutes=5), now - timedelta(minutes=3), now - timedelta(minutes=1), now]
    for ts, p in zip(times, prices):
        win.add(ts, p)

    vol = win.get_vol(now, timedelta(minutes=5))
    assert vol is not None
    # manual stddev of resampled step returns (-5,-4,-3,-2,-1,0)
    resampled = [100, 100, 110, 110, 105, 115]
    returns = [
        (resampled[i] - resampled[i - 1]) / resampled[i - 1]
        for i in range(1, len(resampled))
    ]
    mean = sum(returns) / len(returns)
    expected_var = sum((r - mean) ** 2 for r in returns) / len(returns)
    expected_vol = expected_var ** 0.5
    assert pytest.approx(vol, rel=1e-6) == expected_vol
