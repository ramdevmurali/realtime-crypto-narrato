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
