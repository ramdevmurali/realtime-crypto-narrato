import json
from ..io.models.messages import PriceMsg
from .price_pipeline import process_price, PipelineError
from ..logging_config import get_logger
from ..processor_state import ProcessorState
from ..utils import parse_iso_datetime
from ..metrics import get_metrics


def handle_price_message(proc: ProcessorState, msg) -> tuple[str, dict]:
    log = getattr(proc, "log", get_logger(__name__))
    try:
        data = json.loads(msg.value.decode())
        price_msg = PriceMsg.model_validate(data)
    except (json.JSONDecodeError, Exception):
        if proc.record_bad_price():
            log.warning(
                "price_message_decode_failed",
                extra={
                    "raw": msg.value,
                    "bad_price_messages": proc.bad_price_messages,
                },
            )
        return "dlq", {"payload": msg.value}

    symbol = price_msg.symbol
    price = float(price_msg.price)
    ts = parse_iso_datetime(price_msg.time)
    if proc.should_drop_late(symbol, ts):
        last_ts = proc.last_price_ts.get(symbol)
        if proc.late_price_messages == 1 or proc.late_price_messages % proc.late_price_log_every == 0:
            log.warning(
                "price_message_late",
                extra={
                    "symbol": symbol,
                    "time": ts.isoformat(),
                    "last_seen": last_ts.isoformat() if last_ts else None,
                    "late_price_messages": proc.late_price_messages,
                },
            )
        return "commit", {}

    return "process", {"symbol": symbol, "price": price, "ts": ts, "raw": msg.value}


async def consume_prices(proc: ProcessorState) -> None:
    """Consume prices from Kafka, compute metrics, check anomalies."""
    assert proc.consumer
    async for msg in proc.consumer:
        action, data = handle_price_message(proc, msg)
        if action == "dlq":
            get_metrics("processor").inc("price_dlq_sent")
            ok = await proc.send_price_dlq(data["payload"])
            if ok:
                await proc.commit_msg(msg)
            else:
                get_metrics("processor").inc("price_dlq_send_failed")
                log = getattr(proc, "log", get_logger(__name__))
                log.warning("price_dlq_commit_skipped", extra={"offset": msg.offset})
            continue
        if action == "commit":
            await proc.commit_msg(msg)
            continue

        try:
            await process_price(proc, data["symbol"], data["price"], data["ts"])
        except PipelineError:
            get_metrics("processor").inc("price_pipeline_failed")
            get_metrics("processor").inc("price_dlq_sent")
            ok = await proc.send_price_dlq(data["raw"])
            if ok:
                await proc.commit_msg(msg)
            else:
                get_metrics("processor").inc("price_dlq_send_failed")
                log = getattr(proc, "log", get_logger(__name__))
                log.warning("price_dlq_commit_skipped", extra={"offset": msg.offset})
            continue
        except Exception as exc:
            log = getattr(proc, "log", get_logger(__name__))
            log.error("price_pipeline_failed", extra={"error": str(exc), "symbol": data["symbol"]})
            get_metrics("processor").inc("price_pipeline_failed")
            get_metrics("processor").inc("price_dlq_sent")
            ok = await proc.send_price_dlq(data["raw"])
            if ok:
                await proc.commit_msg(msg)
            else:
                get_metrics("processor").inc("price_dlq_send_failed")
                log.warning("price_dlq_commit_skipped", extra={"offset": msg.offset})
            continue

        await proc.commit_msg(msg)
