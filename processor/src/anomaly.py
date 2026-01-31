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
    if not hasattr(processor, "alerts_emitted"):
        processor.alerts_emitted = 0
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
            # Use the base/stub summary locally; offload richer LLM to summaries topic.
            summary = llm_summarize("stub", None, symbol, label, ret, headline, sentiment)
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
            # Publish summary-request for sidecar to enrich asynchronously.
            await with_retries(
                producer.send_and_wait,
                settings.summaries_topic,
                json.dumps(
                    {
                        "time": ts.isoformat(),
                        "symbol": symbol,
                        "window": label,
                        "direction": direction,
                        "ret": ret,
                        "threshold": threshold,
                        "headline": headline,
                        "sentiment": sentiment,
                    }
                ).encode(),
                log=log,
                op="send_summary_request",
            )
            await with_retries(insert_anomaly, ts, symbol, label, direction, ret, threshold, headline, sentiment, summary, log=log, op="insert_anomaly")
            await with_retries(producer.send_and_wait, settings.alerts_topic, json.dumps(alert_payload).encode(), log=log, op="send_alert")
            processor.last_alert[key] = ts
            processor.alerts_emitted += 1
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
            if processor.alerts_emitted % 50 == 0:
                log.info("alerts_emitted_count", extra={"count": processor.alerts_emitted})
