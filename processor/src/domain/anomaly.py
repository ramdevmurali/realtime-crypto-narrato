from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Tuple



@dataclass
class HeadlineContext:
    headline: str | None
    sentiment: float | None
    headline_ts: datetime | None


@dataclass
class AnomalyEvent:
    time: datetime
    symbol: str
    window: str
    direction: str
    ret: float
    threshold: float
    headline: str | None
    sentiment: float | None
    summary_stub: str | None = None


def detect_anomalies(
    symbol: str,
    ts: datetime,
    metrics: Dict[str, float] | None,
    thresholds: Dict[str, float],
    last_alerts: Dict[Tuple[str, str], datetime],
    headline_ctx: HeadlineContext,
    *,
    anomaly_cooldown_sec: int,
    headline_max_age_sec: int,
) -> List[AnomalyEvent]:
    headline = headline_ctx.headline
    sentiment = headline_ctx.sentiment
    if headline_ctx.headline_ts is None or ts - headline_ctx.headline_ts > timedelta(seconds=headline_max_age_sec):
        headline = None
        sentiment = None

    events: List[AnomalyEvent] = []
    for label, threshold in thresholds.items():
        ret = metrics.get(f"return_{label}") if metrics else None
        if ret is None:
            continue
        if abs(ret) >= threshold:
            key = (symbol, label)
            last_ts = last_alerts.get(key)
            if last_ts and ts - last_ts < timedelta(seconds=anomaly_cooldown_sec):
                continue
            direction = "up" if ret >= 0 else "down"
            events.append(
                AnomalyEvent(
                    time=ts,
                    symbol=symbol,
                    window=label,
                    direction=direction,
                    ret=ret,
                    threshold=threshold,
                    headline=headline,
                    sentiment=sentiment,
                )
            )

    return events
