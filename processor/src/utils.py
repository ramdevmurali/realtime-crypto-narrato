from __future__ import annotations
import math
from datetime import datetime, timezone
import random
import asyncio
from typing import Optional
try:
    from .config import settings
except ImportError:
    from config import settings

POSITIVE_WORDS = {"surge", "gain", "up", "bull", "approval", "positive", "green"}
NEGATIVE_WORDS = {"drop", "loss", "down", "bear", "selloff", "crash", "negative"}


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


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
                max_tokens=80,
                temperature=0.3,
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
    except Exception:
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
    wait = wait * (0.5 + random.random())  # jitter 0.5x-1.5x
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
            if log:
                log.warning("op_failed", extra={"op": op_name, "attempt": attempt, "error": str(exc)})
            if attempt >= max_attempts:
                if log:
                    log.error("op_dropped", extra={"op": op_name, "error": str(exc)})
                raise
            await sleep_backoff(attempt)
