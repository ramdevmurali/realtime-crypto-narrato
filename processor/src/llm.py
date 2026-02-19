from __future__ import annotations

from typing import Optional

from .logging_config import get_logger
from .config import settings
from .metrics import get_metrics

log = get_logger(__name__)


def llm_summarize(
    provider: str,
    api_key: Optional[str],
    symbol: str,
    window: str,
    ret: float,
    headline: Optional[str],
    sentiment: Optional[float],
) -> str:
    direction = "up" if ret >= 0 else "down"
    magnitude = f"{abs(ret)*100:.2f}%"
    headline_part = f"Headline: {headline}" if headline else "No fresh headlines."
    sentiment_word = "neutral"
    if sentiment is not None:
        if sentiment > settings.sentiment_pos_threshold:
            sentiment_word = "positive"
        elif sentiment < settings.sentiment_neg_threshold:
            sentiment_word = "negative"

    base_summary = (
        f"{symbol.upper()} moved {magnitude} {direction} over {window}. "
        f"{headline_part} Sentiment {sentiment_word}."
    )

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
