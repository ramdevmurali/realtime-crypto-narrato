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
    kafka_brokers: List[str] = Field(default_factory=lambda: ["redpanda:29092"])
    redis_url: str | None = "redis://redis:6379/0"

    binance_stream: str = "wss://stream.binance.com:9443/stream"
    symbols: List[str] = Field(default_factory=lambda: ["btcusdt", "ethusdt"])
    news_rss: str = "https://www.coindesk.com/arc/outboundfeeds/rss/"

    alert_threshold_1m: float = 0.05
    alert_threshold_5m: float = 0.08
    alert_threshold_15m: float = 0.12

    llm_provider: str = "stub"  # stub|openai|google
    openai_api_key: str | None = None
    google_api_key: str | None = None

    price_topic: str = "prices"
    news_topic: str = "news"
    alerts_topic: str = "alerts"

    def __init__(self, **values):
        # allow CSV env overrides
        if 'KAFKA_BROKERS' in values:
            values['kafka_brokers'] = _csv(values.get('KAFKA_BROKERS'), ["redpanda:29092"])
        if 'SYMBOLS' in values:
            values['symbols'] = _csv(values.get('SYMBOLS'), ["btcusdt", "ethusdt"])
        super().__init__(**values)
settings = Settings()
