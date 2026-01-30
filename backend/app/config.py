from typing import List
import os

from pydantic import field_validator
from pydantic_settings import BaseSettings

# Compat: ConfigDict exists in newer pydantic-settings. Fallback to class Config otherwise.
try:  # pragma: no cover - compatibility shim
    from pydantic_settings import ConfigDict  # type: ignore
except ImportError:  # pragma: no cover - older pydantic-settings
    ConfigDict = None  # type: ignore


def _split_csv(value: str) -> List[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


class Settings(BaseSettings):
    if ConfigDict:
        model_config = ConfigDict(
            env_file="../infra/.env",
            env_file_encoding="utf-8",
            case_sensitive=False,
        )
    else:  # pragma: no cover - legacy fallback
        class Config:
            env_file = "../infra/.env"
            env_file_encoding = "utf-8"
            case_sensitive = False

    database_url: str = "postgresql://postgres:postgres@timescaledb:5432/postgres"
    kafka_brokers: List[str] = ["redpanda:29092"]
    price_topic: str = "prices"
    news_topic: str = "news"
    alerts_topic: str = "alerts"
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    @field_validator("kafka_brokers", mode="before")
    @classmethod
    def parse_brokers(cls, v):
        if isinstance(v, str):
            return _split_csv(v)
        return v


settings = Settings()
