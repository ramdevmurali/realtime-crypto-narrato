from __future__ import annotations

from dataclasses import dataclass

from ..io.db import insert_price, insert_metric
from ..domain.metrics import compute_metrics
from ..domain.anomaly import check_anomalies
from ..utils import with_retries


@dataclass
class PipelineError(Exception):
    stage: str
    error: Exception


async def process_price(proc, symbol: str, price: float, ts) -> bool:
    try:
        inserted = await with_retries(insert_price, ts, symbol, price, log=proc.log, op="insert_price")
    except Exception as exc:
        proc.log.error("price_insert_failed", extra={"error": str(exc), "symbol": symbol})
        raise PipelineError("insert_price", exc) from exc

    if inserted:
        win = proc.price_windows[symbol]
        win.add(ts, price)
        metrics = compute_metrics(proc.price_windows, symbol, ts)
        if metrics:
            try:
                await with_retries(insert_metric, ts, symbol, metrics, log=proc.log, op="insert_metric")
            except Exception as exc:
                proc.log.error("metric_insert_failed", extra={"error": str(exc), "symbol": symbol})
                raise PipelineError("insert_metric", exc) from exc
        try:
            await check_anomalies(proc, symbol, ts, metrics or {})
        except Exception as exc:
            proc.log.error("anomaly_check_failed", extra={"error": str(exc), "symbol": symbol})
            raise PipelineError("check_anomalies", exc) from exc

    return inserted
