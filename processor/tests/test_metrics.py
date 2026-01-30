from datetime import datetime, timedelta, timezone

import pytest

from processor.src.metrics import compute_metrics
from processor.src.windows import PriceWindow


def test_compute_metrics_returns_none_with_no_history():
    pw = PriceWindow()
    now = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    pw.add(now, 100.0)

    price_windows = {"btcusdt": pw}
    assert compute_metrics(price_windows, "btcusdt", now) is None


def test_compute_metrics_attention_and_returns():
    pw = PriceWindow()
    now = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    # returns: 100 -> 105 (-5m) -> 110 (-1m) -> 115 now
    data = [
        (now - timedelta(minutes=5), 100.0),
        (now - timedelta(minutes=1), 105.0),
        (now, 115.0),
    ]
    for ts, p in data:
        pw.add(ts, p)

    price_windows = {"btcusdt": pw}
    metrics = compute_metrics(price_windows, "btcusdt", now)
    assert metrics is not None
    assert metrics["return_1m"] is not None
    assert metrics["attention"] is not None
    # attention should be >= |return_1m| / threshold_1m (0.05)
    expected_ratio = abs(metrics["return_1m"]) / 0.05
    assert metrics["attention"] >= expected_ratio


def test_compute_metrics_attention_max_ratio():
    pw = PriceWindow()
    now = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    # Craft returns: ~4% over 1m, ~10% over 5m
    data = [
        (now - timedelta(minutes=5), 100.0),
        (now - timedelta(minutes=1), 104.0),
        (now, 110.0),
    ]
    for ts, p in data:
        pw.add(ts, p)

    price_windows = {"btcusdt": pw}
    metrics = compute_metrics(price_windows, "btcusdt", now)
    assert metrics is not None

    # expected attention from 5m window: |10%| / 8% = 1.25
    expected = abs(metrics["return_5m"]) / 0.08
    assert pytest.approx(metrics["attention"], rel=1e-6) == expected


def test_compute_metrics_propagates_returns_and_vol():
    pw = PriceWindow()
    now = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    # Prices crafted to give clear returns and vols over 5m window
    data = [
        (now - timedelta(minutes=5), 100.0),
        (now - timedelta(minutes=3), 110.0),
        (now - timedelta(minutes=1), 105.0),
        (now, 115.5),
    ]
    for ts, p in data:
        pw.add(ts, p)

    price_windows = {"ethusdt": pw}
    metrics = compute_metrics(price_windows, "ethusdt", now)
    assert metrics is not None

    expected_return_5m = (115.5 - 100.0) / 100.0  # 0.155
    assert pytest.approx(metrics["return_5m"], rel=1e-6) == expected_return_5m

    # vol over the prices within 5m
    window_prices = [100.0, 110.0, 105.0, 115.5]
    returns = []
    for i in range(1, len(window_prices)):
        prev = window_prices[i - 1]
        cur = window_prices[i]
        returns.append((cur - prev) / prev)
    mean = sum(returns) / len(returns)
    expected_var = sum((r - mean) ** 2 for r in returns) / len(returns)
    expected_vol = expected_var ** 0.5
    assert pytest.approx(metrics["vol_5m"], rel=1e-6) == expected_vol


def test_compute_metrics_return_z_scores():
    pw = PriceWindow()
    now = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    # Prices within 5m: 100 -> 110 -> 90 -> 105
    data = [
        (now - timedelta(minutes=5), 100.0),
        (now - timedelta(minutes=4), 110.0),
        (now - timedelta(minutes=3), 90.0),
        (now, 105.0),
    ]
    for ts, p in data:
        pw.add(ts, p)

    price_windows = {"ethusdt": pw}
    metrics = compute_metrics(price_windows, "ethusdt", now)
    assert metrics is not None

    # return_5m uses oldest vs latest: (105-100)/100 = 0.05
    assert pytest.approx(metrics["return_5m"], rel=1e-6) == 0.05

    # returns series inside window: [0.10, -0.181818, 0.1666667]
    returns = [0.10, -0.1818181818, 0.1666666667]
    mean = sum(returns) / len(returns)
    var = sum((r - mean) ** 2 for r in returns) / len(returns)
    std = var ** 0.5
    expected_z = (0.05 - mean) / std
    assert pytest.approx(metrics["return_z_5m"], rel=1e-6) == expected_z


def test_compute_metrics_return_z_score_insufficient_data():
    pw = PriceWindow()
    now = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    pw.add(now, 100.0)  # only one point
    price_windows = {"btcusdt": pw}
    metrics = compute_metrics(price_windows, "btcusdt", now)
    # metrics is None overall, but if it weren't, z-scores would be None due to insufficient data
    assert metrics is None
