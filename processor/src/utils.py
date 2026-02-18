from __future__ import annotations
import math
from datetime import datetime, timezone
import random
import asyncio
from typing import Optional
from .logging_config import get_logger
try:
    from .config import settings
except ImportError:
    from config import settings

POSITIVE_WORDS = {"surge", "gain", "up", "bull", "approval", "positive", "green"}
NEGATIVE_WORDS = {"drop", "loss", "down", "bear", "selloff", "crash", "negative"}
log = get_logger(__name__)


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


def llm_summarize(provider: str, api_key: Optional[str], symbol: str, window: str, ret: float, headline: Optional[str], sentiment: Optional[float]) -> str:
    direction = "up" if ret >= 0 else "down"
    magnitude = f"{abs(ret)*100:.2f}%"
    headline_part = f"Headline: {headline}" if headline else "No fresh headlines."
    sentiment_word = "neutral"
    if sentiment is not None:
        if sentiment > settings.sentiment_pos_threshold:
            sentiment_word = "positive"
        elif sentiment < settings.sentiment_neg_threshold:
            sentiment_word = "negative"

    base_summary = f"{symbol.upper()} moved {magnitude} {direction} over {window}. {headline_part} Sentiment {sentiment_word}."

    if provider == "stub" or not api_key:
        return base_summary

    try:
        if provider == "openai":
            import openai  # type: ignore

            openai.api_key = api_key
            prompt = (
                f"Summarize crypto move for dashboard alert: {symbol.upper()} {magnitude} {direction} over {window}. "
                f"Latest headline: {headline or 'none'}. Sentiment {sentiment_word}. Keep it under 50 words."
            )
            resp = openai.ChatCompletion.create(
                model=settings.openai_model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=settings.llm_max_tokens,
                temperature=settings.llm_temperature,
            )
            return resp.choices[0].message["content"].strip()
        if provider == "google":
            import google.generativeai as genai  # type: ignore

            genai.configure(api_key=api_key)
            prompt = (
                f"Summarize crypto move for dashboard alert: {symbol.upper()} {magnitude} {direction} over {window}. "
                f"Latest headline: {headline or 'none'}. Sentiment {sentiment_word}. Keep concise, <50 words."
            )
            resp = genai.GenerativeModel(settings.google_model).generate_content(prompt)
            # google client returns .text on success
            return resp.text.strip() if hasattr(resp, "text") and resp.text else base_summary
    except Exception as exc:
        from .metrics import get_metrics

        metrics = get_metrics("llm")
        metrics.inc("llm_fallbacks")
        fallback_count = metrics.snapshot()["counters"].get("llm_fallbacks", 0)
        if fallback_count == 1 or fallback_count % settings.llm_fallback_log_every == 0:
            log.warning(
                "llm_fallback_used",
                extra={"provider": provider, "error": str(exc), "fallbacks": fallback_count},
            )
        # fall back to stub summary if any API/import/network error
        return base_summary

    return base_summary


async def sleep_backoff(attempt: int, base: float | None = None, cap: float | None = None):
    """Exponential backoff with jitter, capped."""
    if base is None:
        base = settings.retry_backoff_base_sec
    if cap is None:
        cap = settings.retry_backoff_cap_sec
    wait = min(cap, base * (2 ** attempt))
    jitter_span = settings.retry_jitter_max - settings.retry_jitter_min
    jitter = settings.retry_jitter_min + (random.random() * jitter_span)
    wait = wait * jitter
    await asyncio.sleep(wait)


async def with_retries(fn, *args, max_attempts: int | None = None, log=None, op: str | None = None, **kwargs):
    """Run an async fn with bounded retries and backoff."""
    if max_attempts is None:
        max_attempts = settings.retry_max_attempts
    op_name = op or getattr(fn, "__name__", "operation")
    attempt = 0
    while True:
        try:
            return await fn(*args, **kwargs)
        except Exception as exc:
            attempt += 1
            from .metrics import get_metrics

            metrics = get_metrics("processor")
            metrics.inc("retry.total")
            metrics.inc(f"retry.{op_name}")
            if log:
                log.warning("op_failed", extra={"op": op_name, "attempt": attempt, "error": str(exc)})
            if attempt >= max_attempts:
                if log:
                    log.error("op_dropped", extra={"op": op_name, "error": str(exc)})
                raise
            await sleep_backoff(attempt)
