from typing import List
from datetime import timedelta
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import field_validator, model_validator


def _csv(val: str | None, default: List[str]) -> List[str]:
    if not val:
        return default
    return [v.strip() for v in val.split(',') if v.strip()]


def _parse_window_label(label: str):
    label = label.strip().lower()
    if len(label) < 2:
        raise ValueError(f"invalid window label: {label}")
    unit = label[-1]
    value = int(label[:-1])
    if unit == "s":
        return timedelta(seconds=value)
    if unit == "m":
        return timedelta(minutes=value)
    if unit == "h":
        return timedelta(hours=value)
    raise ValueError(f"invalid window label: {label}")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_prefix='', extra='ignore')

    database_url: str = "postgres://postgres:postgres@timescaledb:5432/anomalies"
    kafka_brokers_raw: str = "redpanda:29092"
    redis_url: str | None = "redis://redis:6379/0"

    binance_stream: str = "wss://stream.binance.com:9443/stream"
    symbols_raw: str = "btcusdt,ethusdt"
    news_rss: str = "https://www.coindesk.com/arc/outboundfeeds/rss/"

    alert_threshold_1m: float = 0.05
    alert_threshold_5m: float = 0.08
    alert_threshold_15m: float = 0.12

    window_labels_raw: str = "1m,5m,15m"
    late_price_tolerance_sec: int = 0
    anomaly_cooldown_sec: int = 60
    bad_price_log_every: int = 50
    late_price_log_every: int = 50
    price_publish_log_every: int = 500
    news_publish_log_every: int = 200
    price_failure_log_every: int = 5
    news_failure_log_every: int = 3
    price_backoff_failures_threshold: int = 5
    price_backoff_base_sec: float = 1.0
    price_backoff_base_after_failures_sec: float = 2.0
    price_backoff_cap_sec: float = 60.0
    news_backoff_failures_threshold: int = 5
    news_backoff_base_sec: float = 5.0
    news_backoff_base_after_failures_sec: float = 10.0
    news_backoff_cap_sec: float = 90.0
    news_poll_interval_sec: int = 60
    news_batch_limit: int = 20
    retry_max_attempts: int = 3
    retry_backoff_base_sec: float = 1.0
    retry_backoff_cap_sec: float = 30.0
    ewma_return_alpha: float = 0.25  # smoothing for return z-scores
    ewma_z_cap: float = 6.0  # clamp EWMA z-scores
    vol_z_spike_threshold: float = 3.0  # flag vol spikes
    percentile_min_samples: int = 3  # minimum samples for percentile calc
    return_percentile_low: float = 0.05
    return_percentile_high: float = 0.95  # percentiles for return bands
    sentiment_pos_threshold: float = 0.2
    sentiment_neg_threshold: float = -0.2
    sentiment_max_seq_len: int = 512
    alert_log_every: int = 50  # cadence for alert count logs

    headline_max_age_sec: int = 900  # max age for attaching latest headline
    rss_seen_ttl_sec: int = 86400  # dedupe TTL for RSS IDs
    rss_seen_max: int = 5000  # max cached RSS IDs
    window_max_gap_factor: float = 1.5  # max allowed gap vs window size
    vol_resample_sec: int = 5  # cadence for resampling prices in vol calc
    window_history_maxlen: int = 300  # max samples to keep for z-score history

    llm_provider: str = "stub"  # stub|openai|google
    openai_api_key: str | None = None
    google_api_key: str | None = None
    google_model: str = "gemini-2.5-flash"
    openai_model: str = "gpt-3.5-turbo"

    price_topic: str = "prices"
    news_topic: str = "news"
    news_enriched_topic: str = "news-enriched"
    alerts_topic: str = "alerts"
    summaries_topic: str = "summaries"
    summaries_dlq_topic: str = "summaries-deadletter"
    price_dlq_topic: str = "prices-deadletter"
    news_dlq_topic: str = "news-deadletter"
    summary_consumer_group: str = "summary-sidecar"
    summary_poll_timeout_ms: int = 500
    summary_batch_max: int | None = None
    summary_llm_concurrency: int = 2

    sentiment_provider: str = "stub"  # stub|onnx
    sentiment_model_path: str | None = None
    sentiment_batch_size: int = 16
    sentiment_max_latency_ms: int | None = None
    sentiment_sidecar_group: str = "sentiment-sidecar"
    sentiment_fallback_on_slow: bool = False
    sentiment_fail_fast: bool = False
    sentiment_light_runtime: bool = False
    sentiment_metrics_host: str = "0.0.0.0"
    sentiment_metrics_port: int | None = 9101

    def __init__(self, **values):
        # allow CSV env overrides for symbols
        if 'SYMBOLS' in values:
            values['symbols_raw'] = values.get('SYMBOLS')
        super().__init__(**values)

    @field_validator(
        "alert_threshold_1m",
        "alert_threshold_5m",
        "alert_threshold_15m",
        "ewma_return_alpha",
        "ewma_z_cap",
        "vol_z_spike_threshold",
        "percentile_min_samples",
        "alert_log_every",
        "return_percentile_low",
        "return_percentile_high",
        "headline_max_age_sec",
        "rss_seen_ttl_sec",
        "rss_seen_max",
        "window_max_gap_factor",
        "vol_resample_sec",
        "window_history_maxlen",
        "anomaly_cooldown_sec",
        "bad_price_log_every",
        "late_price_log_every",
        "price_publish_log_every",
        "news_publish_log_every",
        "price_failure_log_every",
        "news_failure_log_every",
        "price_backoff_failures_threshold",
        "price_backoff_base_sec",
        "price_backoff_base_after_failures_sec",
        "price_backoff_cap_sec",
        "news_backoff_failures_threshold",
        "news_backoff_base_sec",
        "news_backoff_base_after_failures_sec",
        "news_backoff_cap_sec",
        "news_poll_interval_sec",
        "news_batch_limit",
        "summary_llm_concurrency",
        "retry_max_attempts",
        "retry_backoff_base_sec",
        "retry_backoff_cap_sec",
        "sentiment_batch_size",
        "sentiment_max_seq_len",
    )
    @classmethod
    def _positive(cls, v):
        if v <= 0:
            raise ValueError("must be positive")
        return v

    @field_validator("late_price_tolerance_sec")
    @classmethod
    def _non_negative(cls, v):
        if v < 0:
            raise ValueError("must be >= 0")
        return v

    @field_validator("llm_provider")
    @classmethod
    def _provider_allowed(cls, v):
        if v not in {"stub", "openai", "google"}:
            raise ValueError("llm_provider must be one of: stub, openai, google")
        return v

    @field_validator("sentiment_provider")
    @classmethod
    def _sentiment_provider_allowed(cls, v):
        if v not in {"stub", "onnx"}:
            raise ValueError("sentiment_provider must be one of: stub, onnx")
        return v

    @field_validator("sentiment_max_latency_ms")
    @classmethod
    def _positive_optional(cls, v):
        if v is None:
            return v
        if v <= 0:
            raise ValueError("must be positive")
        return v

    @field_validator("sentiment_metrics_port")
    @classmethod
    def _positive_optional_metrics(cls, v):
        if v is None:
            return v
        if v <= 0:
            raise ValueError("must be positive")
        return v

    @field_validator("sentiment_pos_threshold")
    @classmethod
    def _positive_threshold(cls, v):
        if v <= 0:
            raise ValueError("must be positive")
        return v

    @field_validator("sentiment_neg_threshold")
    @classmethod
    def _negative_threshold(cls, v):
        if v >= 0:
            raise ValueError("must be negative")
        return v

    @model_validator(mode="after")
    def _validate_windows_and_percentiles(self):
        labels = [l.strip().lower() for l in _csv(self.window_labels_raw, ["1m", "5m", "15m"])]
        if not labels:
            raise ValueError("window_labels_raw must not be empty")
        if len(set(labels)) != len(labels):
            raise ValueError("window_labels_raw must be unique")
        for label in labels:
            _parse_window_label(label)
        if not (0 < self.return_percentile_low < 1):
            raise ValueError("return_percentile_low must be in (0,1)")
        if not (0 < self.return_percentile_high < 1):
            raise ValueError("return_percentile_high must be in (0,1)")
        if self.return_percentile_low >= self.return_percentile_high:
            raise ValueError("return_percentile_low must be < return_percentile_high")
        return self

    def safe_dict(self):
        data = self.model_dump()
        for key in ("openai_api_key", "google_api_key"):
            if data.get(key):
                data[key] = "***"
        return data

    @property
    def kafka_brokers(self) -> List[str]:
        return _csv(self.kafka_brokers_raw, ["redpanda:29092"])

    @property
    def symbols(self) -> List[str]:
        return _csv(self.symbols_raw, ["btcusdt", "ethusdt"])
settings = Settings()


def get_thresholds():
    """Return alert thresholds per window."""
    return {
        "1m": settings.alert_threshold_1m,
        "5m": settings.alert_threshold_5m,
        "15m": settings.alert_threshold_15m,
    }


def get_windows():
    labels = _csv(settings.window_labels_raw, ["1m", "5m", "15m"])
    windows = {}
    for label in labels:
        key = label.strip().lower()
        windows[key] = _parse_window_label(key)
    return windows
