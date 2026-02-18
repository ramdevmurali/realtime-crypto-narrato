from __future__ import annotations

from datetime import datetime

from ..config import settings, get_thresholds
from ..domain.anomaly import AnomalyEvent, HeadlineContext, detect_anomalies
from ..io.db import insert_anomaly, fetch_anomaly_alert_published, mark_anomaly_alert_published
from ..utils import llm_summarize, with_retries
from ..logging_config import get_logger
from ..io.models.messages import SummaryRequestMsg, AlertMsg
from ..processor_state import ProcessorState


def _get_headline_context(processor: ProcessorState) -> HeadlineContext:
    headline = None
    sentiment = None
    headline_ts = None
    if hasattr(processor, "latest_headline"):
        latest = processor.latest_headline
        if isinstance(latest, tuple):
            if len(latest) >= 1:
                headline = latest[0]
            if len(latest) >= 2:
                sentiment = latest[1]
            if len(latest) >= 3:
                headline_ts = latest[2]
    return HeadlineContext(headline=headline, sentiment=sentiment, headline_ts=headline_ts)


async def check_anomalies(processor: ProcessorState, symbol: str, ts: datetime, metrics):
    """Detect and publish anomalies for a symbol at time ts based on metrics."""
    producer = processor.producer
    assert producer
    log = getattr(processor, "log", get_logger(__name__))
    if not hasattr(processor, "alerts_emitted"):
        processor.alerts_emitted = 0

    headline_ctx = _get_headline_context(processor)
    thresholds = get_thresholds()
    events = detect_anomalies(symbol, ts, metrics or {}, thresholds, processor.last_alert, headline_ctx)
    for event in events:
        if settings.anomaly_hotpath_stub_summary:
            summary = llm_summarize("stub", None, event.symbol, event.window, event.ret, event.headline, event.sentiment)
        else:
            api_key = None
            if settings.llm_provider == "openai":
                api_key = settings.openai_api_key
            elif settings.llm_provider == "google":
                api_key = settings.google_api_key
            summary = llm_summarize(settings.llm_provider, api_key, event.symbol, event.window, event.ret, event.headline, event.sentiment)
        event.summary_stub = summary

        event_id = f"{event.time.isoformat()}:{event.symbol}:{event.window}"
        inserted = await with_retries(
            insert_anomaly,
            event.time,
            event.symbol,
            event.window,
            event.direction,
            event.ret,
            event.threshold,
            event.headline,
            event.sentiment,
            summary,
            log=log,
            op="insert_anomaly",
        )
        if not inserted:
            published = await with_retries(
                fetch_anomaly_alert_published,
                event.time,
                event.symbol,
                event.window,
                log=log,
                op="fetch_anomaly_alert_published",
            )
            if published:
                log.info("anomaly_duplicate_skipped", extra={"event_id": event_id})
                continue
            log.info("anomaly_publish_retry", extra={"event_id": event_id})

        summary_req = SummaryRequestMsg(
            event_id=event_id,
            time=event.time.isoformat(),
            symbol=event.symbol,
            window=event.window,
            direction=event.direction,
            ret=event.ret,
            threshold=event.threshold,
            headline=event.headline,
            sentiment=event.sentiment,
        )

        alert_msg = AlertMsg(
            event_id=event_id,
            time=event.time.isoformat(),
            symbol=event.symbol,
            window=event.window,
            direction=event.direction,
            ret=event.ret,
            threshold=event.threshold,
            headline=event.headline,
            sentiment=event.sentiment,
            summary=summary,
        )

        await with_retries(
            producer.send_and_wait,
            settings.summaries_topic,
            summary_req.to_bytes(),
            log=log,
            op="send_summary_request",
        )
        await with_retries(
            producer.send_and_wait,
            settings.alerts_topic,
            alert_msg.to_bytes(),
            log=log,
            op="send_alert",
        )
        try:
            await with_retries(
                mark_anomaly_alert_published,
                event.time,
                event.symbol,
                event.window,
                log=log,
                op="mark_anomaly_alert_published",
            )
        except Exception as exc:
            log.warning("anomaly_publish_mark_failed", extra={"event_id": event_id, "error": str(exc)})
        processor.record_alert(event.symbol, event.window, event.time)
        log.info(
            "alert_emitted",
            extra={
                "symbol": event.symbol,
                "window": event.window,
                "direction": event.direction,
                "ret": event.ret,
                "threshold": event.threshold,
            },
        )
        if processor.alerts_emitted % settings.alert_log_every == 0:
            log.info("alerts_emitted_count", extra={"count": processor.alerts_emitted})
