from datetime import datetime, timezone, timedelta

from processor.src.domain.anomaly import detect_anomalies, HeadlineContext
from processor.src.config import settings


def test_detect_anomalies_respects_threshold():
    ts = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    thresholds = {"1m": 0.05}
    metrics = {"return_1m": 0.04}
    events = detect_anomalies(
        "btcusdt",
        ts,
        metrics,
        thresholds,
        last_alerts={},
        headline_ctx=HeadlineContext(None, None, None),
    )
    assert events == []


def test_detect_anomalies_respects_cooldown():
    ts = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    thresholds = {"1m": 0.05}
    metrics = {"return_1m": 0.06}
    last_alerts = {("btcusdt", "1m"): ts - timedelta(seconds=settings.anomaly_cooldown_sec - 1)}
    events = detect_anomalies(
        "btcusdt",
        ts,
        metrics,
        thresholds,
        last_alerts=last_alerts,
        headline_ctx=HeadlineContext(None, None, None),
    )
    assert events == []


def test_detect_anomalies_omits_stale_headline():
    ts = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    thresholds = {"1m": 0.05}
    metrics = {"return_1m": 0.06}
    headline_ctx = HeadlineContext(
        headline="Old",
        sentiment=0.1,
        headline_ts=ts - timedelta(seconds=settings.headline_max_age_sec + 1),
    )
    events = detect_anomalies(
        "btcusdt",
        ts,
        metrics,
        thresholds,
        last_alerts={},
        headline_ctx=headline_ctx,
    )
    assert len(events) == 1
    assert events[0].headline is None
    assert events[0].sentiment is None
