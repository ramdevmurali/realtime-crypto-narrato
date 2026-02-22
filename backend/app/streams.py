import asyncio
import json
import logging
from datetime import datetime, timezone

from . import db

logger = logging.getLogger(__name__)
HEADLINE_FRESH_SEC = 900


def _coerce_time(value) -> datetime:
    if isinstance(value, datetime):
        ts = value
    else:
        raw = str(value)
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        ts = datetime.fromisoformat(raw)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts


def serialize_alert_row(row, now: datetime | None = None) -> dict:
    ts = _coerce_time(row["time"])
    current = now or datetime.now(timezone.utc)
    age_sec = max(0, int((current - ts).total_seconds()))
    return {
        "time": ts.isoformat(),
        "symbol": row["symbol"],
        "window": row["window"],
        "direction": row["direction"],
        "return": row["return"],
        "threshold": row["threshold"],
        "summary": row["summary"],
        "headline": row["headline"],
        "sentiment": row["sentiment"],
        "headline_age_sec": age_sec,
        "headline_fresh": age_sec <= HEADLINE_FRESH_SEC,
    }


async def headlines_event_generator(limit: int, interval: float):
    backoff = 1.0
    while True:
        try:
            rows = await db.fetch_headlines(limit)
            payload = {
                "items": [
                    {
                        "time": r["time"].isoformat() if hasattr(r["time"], "isoformat") else str(r["time"]),
                        "title": r["title"],
                        "url": r["url"],
                        "source": r["source"],
                        "sentiment": r["sentiment"],
                    }
                    for r in rows
                ],
                "count": len(rows),
            }
            yield f"data: {json.dumps(payload)}\n\n"
            backoff = 1.0
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            logger.info("headlines_stream_cancelled")
            break
        except Exception as exc:
            logger.warning("headlines_stream_error: %s", exc)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2.0, 30.0)


async def alerts_event_generator(limit: int, interval: float):
    backoff = 1.0
    while True:
        try:
            rows = await db.fetch_alerts(limit)
            now = datetime.now(timezone.utc)
            payload = {
                "items": [serialize_alert_row(r, now=now) for r in rows],
                "count": len(rows),
            }
            yield f"data: {json.dumps(payload)}\n\n"
            backoff = 1.0
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            logger.info("alerts_stream_cancelled")
            break
        except Exception as exc:
            logger.warning("alerts_stream_error: %s", exc)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2.0, 30.0)
