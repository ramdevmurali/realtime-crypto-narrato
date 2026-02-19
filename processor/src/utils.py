from __future__ import annotations
from datetime import datetime, timezone

POSITIVE_WORDS = {"surge", "gain", "up", "bull", "approval", "positive", "green"}
NEGATIVE_WORDS = {"drop", "loss", "down", "bear", "selloff", "crash", "negative"}


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso_datetime(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    if not isinstance(value, str):
        raise TypeError("time must be a datetime or ISO 8601 string")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        if value.endswith("Z"):
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        else:
            from dateutil import parser as dateparser

            parsed = dateparser.isoparse(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def simple_sentiment(text: str) -> float:
    """Tiny keyword-based polarity in [-1,1]."""
    tokens = {t.lower().strip('.,!?') for t in text.split()}
    pos = len(tokens & POSITIVE_WORDS)
    neg = len(tokens & NEGATIVE_WORDS)
    if pos + neg == 0:
        return 0.0
    return (pos - neg) / (pos + neg)
