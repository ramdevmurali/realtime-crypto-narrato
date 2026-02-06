from datetime import datetime, timedelta, timezone

import pytest
import math

from processor.src.metrics import compute_metrics
from processor.src import metrics as metrics_module
from processor.src.windows import PriceWindow
from processor.src import config as config_module


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


def test_compute_metrics_propagates_returns_and_vol(monkeypatch):
    monkeypatch.setattr(config_module.settings, "vol_resample_sec", 60)
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

    # vol over the resampled prices within 5m (-5,-4,-3,-2,-1,0)
    window_prices = [100.0, 100.0, 110.0, 110.0, 105.0, 115.5]
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
    # Build a sequence so we get multiple 5m window returns in history.
    prices = [100.0, 102.0, 104.0, 106.0, 108.0, 110.0, 112.0, 114.0]
    times = [now - timedelta(minutes=7) + timedelta(minutes=i) for i in range(len(prices))]
    price_windows = {"ethusdt": pw}
    m1 = m2 = m3 = None
    for ts, p in zip(times, prices):
        pw.add(ts, p)
        if ts == times[5]:
            m1 = compute_metrics(price_windows, "ethusdt", ts)
        if ts == times[6]:
            m2 = compute_metrics(price_windows, "ethusdt", ts)
        if ts == times[7]:
            m3 = compute_metrics(price_windows, "ethusdt", ts)
    assert m1 is not None
    assert m2 is not None
    assert m3 is not None

    ret1 = (prices[5] - prices[0]) / prices[0]
    ret2 = (prices[6] - prices[1]) / prices[1]
    ret3 = (prices[7] - prices[2]) / prices[2]
    hist = [ret1, ret2]
    mean = sum(hist) / len(hist)
    var = sum((r - mean) ** 2 for r in hist) / len(hist)
    std = var ** 0.5
    expected_z = (ret3 - mean) / std
    assert pytest.approx(m3["return_z_5m"], rel=1e-6) == expected_z


def test_compute_metrics_return_z_score_insufficient_data():
    pw = PriceWindow()
    now = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    pw.add(now, 100.0)  # only one point
    price_windows = {"btcusdt": pw}
    metrics = compute_metrics(price_windows, "btcusdt", now)
    # metrics is None overall, but if it weren't, z-scores would be None due to insufficient data
    assert metrics is None


def test_vol_z_and_spike(monkeypatch):
    monkeypatch.setattr(config_module.settings, "vol_resample_sec", 60)
    pw = PriceWindow()
    now = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    prices = [100.0, 102.0, 104.0, 106.0, 108.0, 110.0, 112.0, 114.0]
    times = [now - timedelta(minutes=7) + timedelta(minutes=i) for i in range(len(prices))]

    def window_vol(start_index: int, end_index: int):
        window_prices = prices[start_index:end_index + 1]
        returns = []
        for i in range(1, len(window_prices)):
            prev = window_prices[i - 1]
            cur = window_prices[i]
            returns.append((cur - prev) / prev)
        mean = sum(returns) / len(returns)
        var = sum((r - mean) ** 2 for r in returns) / len(returns)
        return var ** 0.5

    vol1 = window_vol(0, 5)
    vol2 = window_vol(1, 6)
    vol3 = window_vol(2, 7)
    hist = [vol1, vol2]
    mean = sum(hist) / len(hist)
    var = sum((v - mean) ** 2 for v in hist) / len(hist)
    std = var ** 0.5
    vol_z_expected = (vol3 - mean) / std

    monkeypatch.setattr(config_module.settings, "vol_z_spike_threshold", vol_z_expected - 0.1)

    price_windows = {"ethusdt": pw}
    m1 = m2 = m3 = None
    for ts, p in zip(times, prices):
        pw.add(ts, p)
        if ts == times[5]:
            m1 = compute_metrics(price_windows, "ethusdt", ts)
        if ts == times[6]:
            m2 = compute_metrics(price_windows, "ethusdt", ts)
        if ts == times[7]:
            m3 = compute_metrics(price_windows, "ethusdt", ts)
    # Build history with two vols, then compute z for third.
    assert m1 is not None
    assert m2 is not None
    assert m3 is not None
    assert pytest.approx(m3["vol_z_5m"], rel=1e-6) == vol_z_expected
    assert m3["vol_spike_5m"] is True


def test_vol_z_none_with_insufficient_data():
    pw = PriceWindow()
    now = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    pw.add(now - timedelta(minutes=1), 100.0)
    pw.add(now, 101.0)  # only one return point

    price_windows = {"btcusdt": pw}
    metrics = compute_metrics(price_windows, "btcusdt", now)
    # metrics not None because returns exist, but vol_z should be None
    if metrics is not None:
        assert metrics["vol_z_1m"] is None


def test_return_percentiles_happy_path(monkeypatch):
    pw = PriceWindow()
    now = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    prices = [100, 105, 110, 120, 130, 125, 135, 140, 150]
    times = [now - timedelta(minutes=8) + timedelta(minutes=i) for i in range(len(prices))]

    # override percentiles to 0.25 and 0.75 for deterministic check
    monkeypatch.setattr(config_module.settings, "return_percentile_low", 0.25)
    monkeypatch.setattr(config_module.settings, "return_percentile_high", 0.75)

    price_windows = {"btcusdt": pw}
    metrics = None
    for ts, p in zip(times, prices):
        pw.add(ts, p)
        if ts in {times[5], times[6], times[7], times[8]}:
            metrics = compute_metrics(price_windows, "btcusdt", ts)

    # last metrics call at times[8] should use three prior 5m returns in history
    assert metrics is not None

    ret1 = (prices[5] - prices[0]) / prices[0]
    ret2 = (prices[6] - prices[1]) / prices[1]
    ret3 = (prices[7] - prices[2]) / prices[2]
    hist = sorted([ret1, ret2, ret3])

    def interp(p):
        k = (len(hist) - 1) * p
        f = int(k)
        c = math.ceil(k)
        if f == c:
            return hist[f]
        return hist[f] * (c - k) + hist[c] * (k - f)

    assert pytest.approx(metrics["p05_return_5m"], rel=1e-6) == interp(0.25)
    assert pytest.approx(metrics["p95_return_5m"], rel=1e-6) == interp(0.75)


def test_return_percentiles_insufficient_data():
    pw = PriceWindow()
    now = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    pw.add(now - timedelta(minutes=1), 100.0)
    pw.add(now, 101.0)  # only one return
    price_windows = {"btcusdt": pw}
    metrics = compute_metrics(price_windows, "btcusdt", now)
    if metrics is not None:
        assert metrics["p05_return_1m"] is None
        assert metrics["p95_return_1m"] is None


def test_return_z_ewma_smoothing(monkeypatch):
    pw = PriceWindow()
    now = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    pw.add(now - timedelta(minutes=1), 100.0)
    pw.add(now, 110.0)

    # Feed controlled z-scores per call: first 1.0 then 3.0 for the 1m window
    sequences = iter([[1.0, None, None], [3.0, None, None]])

    def fake_z(value, series):
        if current:
            return current.pop(0)
        return None

    monkeypatch.setattr(metrics_module, "_zscore", fake_z)
    monkeypatch.setattr(config_module.settings, "ewma_return_alpha", 0.5)

    price_windows = {"btcusdt": pw}

    current = next(sequences)
    metrics1 = compute_metrics(price_windows, "btcusdt", now)
    assert metrics1["return_z_ewma_1m"] == pytest.approx(1.0)

    # add a new price to keep window populated and change z-score
    later = now + timedelta(seconds=30)
    pw.add(later, 120.0)

    current = next(sequences)
    metrics2 = compute_metrics(price_windows, "btcusdt", later)
    # ewma: 1 + 0.5*(3-1) = 2
    assert metrics2["return_z_ewma_1m"] == pytest.approx(2.0)


def test_return_z_ewma_none_when_no_raw(monkeypatch):
    pw = PriceWindow()
    now = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    pw.add(now - timedelta(minutes=5), 100.0)
    pw.add(now, 105.0)

    monkeypatch.setattr(metrics_module, "_zscore", lambda v, s: None)
    price_windows = {"btcusdt": pw}
    metrics = compute_metrics(price_windows, "btcusdt", now)
    assert metrics is not None
    assert metrics["return_z_ewma_1m"] is None
    assert metrics["return_z_ewma_5m"] is None
    assert metrics["return_z_ewma_15m"] is None


def test_return_z_ewma_cap(monkeypatch):
    pw = PriceWindow()
    now = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    pw.add(now - timedelta(minutes=5), 100.0)
    pw.add(now, 200.0)

    monkeypatch.setattr(metrics_module, "_zscore", lambda v, s: 10.0)
    monkeypatch.setattr(config_module.settings, "ewma_return_alpha", 1.0)

    price_windows = {"btcusdt": pw}
    metrics = compute_metrics(price_windows, "btcusdt", now)
    assert metrics["return_z_ewma_5m"] == pytest.approx(6.0)


def _metrics_for_series(monkeypatch, prices, start, step_minutes=1, symbol="btcusdt"):
    monkeypatch.setattr(config_module.settings, "vol_resample_sec", 60)
    pw = PriceWindow()
    times = [start + timedelta(minutes=i * step_minutes) for i in range(len(prices))]
    metrics = None
    for ts, p in zip(times, prices):
        pw.add(ts, p)
        metrics = compute_metrics({symbol: pw}, symbol, ts)
    return metrics


def test_metrics_constant_prices(monkeypatch):
    now = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    prices = [100, 100, 100, 100, 100, 100]
    metrics = _metrics_for_series(monkeypatch, prices, now - timedelta(minutes=5))
    assert metrics is not None
    assert metrics["return_5m"] == pytest.approx(0.0)
    assert metrics["vol_5m"] == pytest.approx(0.0)
    assert metrics["return_z_5m"] is None
    assert metrics["vol_z_5m"] is None


def test_metrics_monotonic_prices(monkeypatch):
    now = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    prices = [100, 102, 104, 106, 108, 110]
    metrics = _metrics_for_series(monkeypatch, prices, now - timedelta(minutes=5))
    assert metrics is not None
    expected_return = (110 - 100) / 100
    assert metrics["return_5m"] == pytest.approx(expected_return)
    assert metrics["vol_5m"] is not None
    assert metrics["vol_5m"] > 0


def test_metrics_mixed_returns_exact(monkeypatch):
    now = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    prices = [100, 110, 105, 115, 120, 118]
    metrics = _metrics_for_series(monkeypatch, prices, now - timedelta(minutes=5))
    assert metrics is not None
    expected_return = (118 - 100) / 100
    assert metrics["return_5m"] == pytest.approx(expected_return)
    returns = []
    for i in range(1, len(prices)):
        prev = prices[i - 1]
        cur = prices[i]
        returns.append((cur - prev) / prev)
    mean = sum(returns) / len(returns)
    expected_var = sum((r - mean) ** 2 for r in returns) / len(returns)
    expected_vol = expected_var ** 0.5
    assert metrics["vol_5m"] == pytest.approx(expected_vol)


def test_metrics_invariants_scale_and_shift(monkeypatch):
    now = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    base = [100, 110, 105, 115, 120, 118]
    scaled = [p * 10 for p in base]
    shifted = [p + 100 for p in base]

    base_metrics = _metrics_for_series(monkeypatch, base, now - timedelta(minutes=5))
    scaled_metrics = _metrics_for_series(monkeypatch, scaled, now - timedelta(minutes=5))
    shifted_metrics = _metrics_for_series(monkeypatch, shifted, now - timedelta(minutes=5))

    assert base_metrics is not None
    assert scaled_metrics is not None
    assert shifted_metrics is not None

    assert base_metrics["vol_5m"] >= 0
    assert base_metrics["return_5m"] == pytest.approx(scaled_metrics["return_5m"])
    assert base_metrics["vol_5m"] == pytest.approx(scaled_metrics["vol_5m"])
    assert base_metrics["return_5m"] != pytest.approx(shifted_metrics["return_5m"])


def test_metrics_reference_numpy(monkeypatch):
    np = pytest.importorskip("numpy")
    monkeypatch.setattr(config_module.settings, "vol_resample_sec", 60)
    prices = [100, 103, 101, 106, 104, 109, 108, 111, 113]
    now = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
    start = now - timedelta(minutes=8)
    pw = PriceWindow()

    metrics = None
    times = [start + timedelta(minutes=i) for i in range(len(prices))]
    for ts, p in zip(times, prices):
        pw.add(ts, p)
        metrics = compute_metrics({"btcusdt": pw}, "btcusdt", ts)

    assert metrics is not None
    window_prices = prices[-6:]
    rets = np.diff(window_prices) / np.array(window_prices[:-1])
    expected_vol = float(np.std(rets, ddof=0))
    expected_return = (window_prices[-1] - window_prices[0]) / window_prices[0]
    assert metrics["vol_5m"] == pytest.approx(expected_vol, rel=1e-6)
    assert metrics["return_5m"] == pytest.approx(expected_return, rel=1e-6)

    # strict-window history: use first price at/after cutoff for each prior ts
    prior_returns = []
    for i in range(1, len(prices) - 1):
        cutoff = times[i] - timedelta(minutes=5)
        j = next(idx for idx, t in enumerate(times) if t >= cutoff)
        if j == i:
            continue
        prior_returns.append((prices[i] - prices[j]) / prices[j])
    p05 = float(np.quantile(prior_returns, config_module.settings.return_percentile_low, method="linear"))
    p95 = float(np.quantile(prior_returns, config_module.settings.return_percentile_high, method="linear"))
    assert metrics["p05_return_5m"] == pytest.approx(p05, rel=1e-6)
    assert metrics["p95_return_5m"] == pytest.approx(p95, rel=1e-6)
