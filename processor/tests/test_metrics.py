from datetime import datetime, timedelta, timezone

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
