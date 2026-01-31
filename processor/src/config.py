from typing import List
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


def _csv(val: str | None, default: List[str]) -> List[str]:
    if not val:
        return default
    return [v.strip() for v in val.split(',') if v.strip()]


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

    ewma_return_alpha: float = 0.25  # smoothing for return z-scores
    vol_z_spike_threshold: float = 3.0  # flag vol spikes
    return_percentile_low: float = 0.05
    return_percentile_high: float = 0.95  # percentiles for return bands

    llm_provider: str = "stub"  # stub|openai|google
    openai_api_key: str | None = None
    google_api_key: str | None = None
    google_model: str = "gemini-2.5-flash"
    openai_model: str = "gpt-3.5-turbo"

    price_topic: str = "prices"
    news_topic: str = "news"
    alerts_topic: str = "alerts"
    summaries_topic: str = "summaries"
    summary_consumer_group: str = "summary-sidecar"
    summary_poll_timeout_ms: int = 500
    summary_batch_max: int | None = None

    def __init__(self, **values):
        # allow CSV env overrides for symbols
        if 'SYMBOLS' in values:
            values['symbols_raw'] = values.get('SYMBOLS')
        super().__init__(**values)

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
