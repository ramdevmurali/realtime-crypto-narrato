import pytest
from pydantic import ValidationError

from processor.src.config import Settings, get_thresholds, get_windows, settings


def test_invalid_window_labels_duplicate():
    with pytest.raises(ValidationError):
        Settings(window_labels_raw="1m,1m")


def test_invalid_window_labels_unit():
    with pytest.raises(ValidationError):
        Settings(window_labels_raw="5x")


def test_invalid_window_labels_unsupported():
    with pytest.raises(ValidationError):
        Settings(window_labels_raw="2m,5m")


def test_invalid_thresholds_for_windows():
    with pytest.raises(ValidationError):
        Settings(window_labels_raw="1m,5m", alert_threshold_5m=0.0)


def test_invalid_llm_provider():
    with pytest.raises(ValidationError):
        Settings(llm_provider="other")


def test_invalid_negative_values():
    with pytest.raises(ValidationError):
        Settings(rss_seen_max=-1)


def test_invalid_percentiles():
    with pytest.raises(ValidationError):
        Settings(return_percentile_low=0.9, return_percentile_high=0.1)


def test_invalid_sentiment_poll_timeout():
    with pytest.raises(ValidationError):
        Settings(sentiment_poll_timeout_ms=0)


def test_anomaly_test_mode_overrides_thresholds(monkeypatch):
    monkeypatch.setattr(settings, "anomaly_test_mode", True)
    monkeypatch.setattr(settings, "anomaly_test_threshold", 0.123)
    thresholds = get_thresholds()
    assert set(thresholds.keys()) == set(get_windows().keys())
    assert all(value == 0.123 for value in thresholds.values())


def test_default_thresholds_unchanged_when_test_mode_off(monkeypatch):
    monkeypatch.setattr(settings, "anomaly_test_mode", False)
    monkeypatch.setattr(settings, "alert_threshold_1m", 0.05)
    monkeypatch.setattr(settings, "alert_threshold_5m", 0.08)
    monkeypatch.setattr(settings, "alert_threshold_15m", 0.12)
    thresholds = get_thresholds()
    assert thresholds == {"1m": 0.05, "5m": 0.08, "15m": 0.12}


def test_news_rss_urls_fallback_to_single_feed():
    cfg = Settings(news_rss="https://example.com/rss", news_rss_urls_raw="")
    assert cfg.news_rss_urls == ["https://example.com/rss"]


def test_news_rss_urls_csv_parsed():
    cfg = Settings(NEWS_RSS_URLS="https://a.example/rss, https://b.example/rss")
    assert cfg.news_rss_urls == ["https://a.example/rss", "https://b.example/rss"]


def test_empty_optional_int_env_parsed_as_none():
    cfg = Settings(summary_metrics_port="", sentiment_max_latency_ms="")
    assert cfg.summary_metrics_port is None
    assert cfg.sentiment_max_latency_ms is None
