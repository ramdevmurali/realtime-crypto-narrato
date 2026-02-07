import json
from datetime import timedelta

from ..config import settings
from ..io.models.messages import PriceMsg
from .price_pipeline import process_price, PipelineError
from ..logging_config import get_logger


async def consume_prices(proc) -> None:
    """Consume prices from Kafka, compute metrics, check anomalies."""
    assert proc.consumer
    log = getattr(proc, "log", get_logger(__name__))
    async for msg in proc.consumer:
        try:
            data = json.loads(msg.value.decode())
            price_msg = PriceMsg.model_validate(data)
        except (json.JSONDecodeError, Exception):
            proc.bad_price_messages += 1
            if proc.bad_price_messages == 1 or proc.bad_price_messages % proc.bad_price_log_every == 0:
                log.warning(
                    "price_message_decode_failed",
                    extra={
                        "raw": msg.value,
                        "bad_price_messages": proc.bad_price_messages,
                    },
                )
            await proc.send_price_dlq(msg.value)
            await proc.commit_msg(msg)
            continue

        symbol = price_msg.symbol
        price = float(price_msg.price)
        ts = price_msg.time
        last_ts = proc.last_price_ts.get(symbol)
        tolerance = timedelta(seconds=settings.late_price_tolerance_sec)
        if last_ts and ts < last_ts - tolerance:
            proc.late_price_messages += 1
            if proc.late_price_messages == 1 or proc.late_price_messages % proc.late_price_log_every == 0:
                log.warning(
                    "price_message_late",
                    extra={
                        "symbol": symbol,
                        "time": ts.isoformat(),
                        "last_seen": last_ts.isoformat(),
                        "late_price_messages": proc.late_price_messages,
                    },
                )
            await proc.commit_msg(msg)
            continue
        if last_ts is None or ts > last_ts:
            proc.last_price_ts[symbol] = ts

        try:
            await process_price(proc, symbol, price, ts)
        except PipelineError:
            await proc.send_price_dlq(msg.value)
            await proc.commit_msg(msg)
            continue
        except Exception as exc:
            log.error("price_pipeline_failed", extra={"error": str(exc), "symbol": symbol})
            await proc.send_price_dlq(msg.value)
            await proc.commit_msg(msg)
            continue

        await proc.commit_msg(msg)
