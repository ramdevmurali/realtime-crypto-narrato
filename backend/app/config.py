from typing import List
from pydantic_settings import BaseSettings


def _split_csv(value: str) -> List[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


class Settings(BaseSettings):
    database_url: str = "postgresql://postgres:postgres@timescaledb:5432/postgres"
    kafka_brokers: List[str] = ["redpanda:29092"]
    price_topic: str = "prices"
    news_topic: str = "news"
    alerts_topic: str = "alerts"
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    class Config:
        env_file = "../infra/.env"
        env_file_encoding = "utf-8"
        case_sensitive = False

    @classmethod
    def model_validate_from_env(cls):
        # custom CSV parsing for brokers
        values = {}
        import os

        brokers = os.getenv("KAFKA_BROKERS")
        if brokers:
            values["kafka_brokers"] = _split_csv(brokers)
        return cls(**values)


# Instantiate settings with env overrides (handles CSV for brokers)
settings = Settings.model_validate_from_env()
