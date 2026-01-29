from datetime import timedelta
import json

from .config import settings, get_thresholds
from .db import insert_anomaly
from .utils import llm_summarize, with_retries
from .logging_config import get_logger


async def check_anomalies(processor, symbol: str, ts, metrics):
    """Detect and publish anomalies for a symbol at time ts based on metrics."""
    producer = processor.producer
    assert producer
    log = getattr(processor, "log", get_logger(__name__))
    headline, sentiment = processor.latest_headline
    thresholds = get_thresholds()
    for label, threshold in thresholds.items():
        ret = metrics.get(f"return_{label}") if metrics else None
        if ret is None:
            continue
        if abs(ret) >= threshold:
            key = (symbol, label)
            last_ts = processor.last_alert.get(key)
            if last_ts and ts - last_ts < timedelta(seconds=60):
                continue
            direction = "up" if ret >= 0 else "down"
            api_key = None
            if settings.llm_provider == "openai":
                api_key = settings.openai_api_key
            elif settings.llm_provider == "google":
                api_key = settings.google_api_key
            summary = llm_summarize(settings.llm_provider, api_key, symbol, label, ret, headline, sentiment)
            alert_payload = {
                "time": ts.isoformat(),
                "symbol": symbol,
                "window": label,
                "direction": direction,
                "ret": ret,
                "threshold": threshold,
                "headline": headline,
                "sentiment": sentiment,
                "summary": summary,
            }
            await with_retries(insert_anomaly, ts, symbol, label, direction, ret, threshold, headline, sentiment, summary, log=log, op="insert_anomaly")
            await with_retries(producer.send_and_wait, settings.alerts_topic, json.dumps(alert_payload).encode(), log=log, op="send_alert")
            processor.last_alert[key] = ts
            log.info(
                "alert_emitted",
                extra={
                    "symbol": symbol,
                    "window": label,
                    "direction": direction,
                    "ret": ret,
                    "threshold": threshold,
                },
            )
