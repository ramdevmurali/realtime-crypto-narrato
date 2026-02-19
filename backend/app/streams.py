import asyncio
import json
import logging

from . import db

logger = logging.getLogger(__name__)


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
            payload = {
                "items": [
                    {
                        "time": r["time"].isoformat() if hasattr(r["time"], "isoformat") else str(r["time"]),
                        "symbol": r["symbol"],
                        "window": r["window"],
                        "direction": r["direction"],
                        "return": r["return"],
                        "threshold": r["threshold"],
                        "summary": r["summary"],
                        "headline": r["headline"],
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
            logger.info("alerts_stream_cancelled")
            break
        except Exception as exc:
            logger.warning("alerts_stream_error: %s", exc)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2.0, 30.0)
