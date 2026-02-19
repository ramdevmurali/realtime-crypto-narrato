from __future__ import annotations

import asyncio
import random

from .config import settings
from .metrics import get_metrics


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
