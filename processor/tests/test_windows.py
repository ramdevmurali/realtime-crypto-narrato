from datetime import datetime, timedelta, timezone

import pytest

from processor.src.windows import PriceWindow


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


def test_get_return_insufficient_data():
    win = PriceWindow()
    now = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    win.add(now, 120.0)

    assert win.get_return(now, timedelta(minutes=1)) is None
    assert win.get_return(now, timedelta(minutes=5)) is None
    assert win.get_return(now, timedelta(minutes=15)) is None
