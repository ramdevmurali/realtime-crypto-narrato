#!/usr/bin/env python3
import argparse
import asyncio
import json
import time
import uuid
from datetime import timedelta

import asyncpg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from processor.src.config import settings
from processor.src.io.models.messages import NewsMsg, PriceMsg
from processor.src.utils import now_utc, parse_iso_datetime


async def _anomaly_count(conn, symbol: str, since_ts) -> int:
    return int(
        await conn.fetchval(
            "SELECT count(*) FROM anomalies WHERE symbol=$1 AND time >= $2",
            symbol,
            since_ts,
        )
    )


async def _wait_for_payload(consumer: AIOKafkaConsumer, predicate, timeout_sec: float):
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        batch = await consumer.getmany(timeout_ms=500, max_records=50)
        for records in batch.values():
            for msg in records:
                try:
                    payload = json.loads(msg.value.decode())
                except Exception:
                    continue
                if predicate(payload):
                    return payload
    return None


async def _summary_present(
    conn,
    *,
    symbol: str,
    window: str,
    event_time: str,
) -> bool:
    ts = parse_iso_datetime(event_time)
    row = await conn.fetchrow(
        """
        SELECT summary
        FROM anomalies
        WHERE time=$1 AND symbol=$2 AND window_name=$3
        """,
        ts,
        symbol,
        window,
    )
    return bool(row and row["summary"])


def _evaluate_probe(
    *,
    alert_seen: bool,
    anomaly_delta: int,
    summary_request_seen: bool,
    summary_db_updated: bool,
    summary_dlq_seen: bool,
    check_summaries: bool,
    require_summary_db_update: bool,
    require_no_summary_dlq: bool,
) -> tuple[bool, list[str]]:
    failures: list[str] = []
    if not alert_seen:
        failures.append("no alert observed on alerts topic for probe symbol")
    if anomaly_delta <= 0:
        failures.append("no anomaly row inserted for probe symbol")
    if check_summaries and not summary_request_seen:
        failures.append("no summary request observed for probe symbol")
    if require_summary_db_update and not summary_db_updated:
        failures.append("summary DB update not observed for probe symbol")
    if require_no_summary_dlq and summary_dlq_seen:
        failures.append("probe summary request observed in summaries-deadletter")
    return len(failures) == 0, failures


async def run_probe(
    symbol: str,
    price_start: float,
    price_end: float,
    price_gap_sec: float,
    timeout_sec: float,
    check_summaries: bool,
    require_summary_db_update: bool,
    require_no_summary_dlq: bool,
) -> int:
    started_at = now_utc()
    marker = uuid.uuid4().hex[:10]
    headline = NewsMsg(
        time=started_at.isoformat(),
        title=f"[probe:{marker}] anomaly diagnostic headline",
        url=f"https://example.com/probe/{marker}",
        source="diagnostic",
        sentiment=0.1,
    )
    p1 = PriceMsg(symbol=symbol, price=price_start, time=(started_at + timedelta(seconds=1)).isoformat())
    p2 = PriceMsg(
        symbol=symbol,
        price=price_end,
        time=(started_at + timedelta(seconds=1 + price_gap_sec)).isoformat(),
    )

    conn = await asyncpg.connect(dsn=settings.database_url)
    anomalies_before = await _anomaly_count(conn, symbol, started_at - timedelta(minutes=5))

    alert_consumer = AIOKafkaConsumer(
        settings.alerts_topic,
        bootstrap_servers=settings.kafka_brokers,
        group_id=f"probe-alerts-{uuid.uuid4().hex[:8]}",
        auto_offset_reset="latest",
        enable_auto_commit=False,
    )
    summary_consumer = AIOKafkaConsumer(
        settings.summaries_topic,
        bootstrap_servers=settings.kafka_brokers,
        group_id=f"probe-summaries-{uuid.uuid4().hex[:8]}",
        auto_offset_reset="latest",
        enable_auto_commit=False,
    )
    summary_dlq_consumer = AIOKafkaConsumer(
        settings.summaries_dlq_topic,
        bootstrap_servers=settings.kafka_brokers,
        group_id=f"probe-summaries-dlq-{uuid.uuid4().hex[:8]}",
        auto_offset_reset="latest",
        enable_auto_commit=False,
    )
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_brokers)

    alert_payload = None
    summary_payload = None
    summary_dlq_payload = None
    summary_db_updated = False
    try:
        await alert_consumer.start()
        await summary_consumer.start()
        await summary_dlq_consumer.start()
        await producer.start()

        await producer.send_and_wait(settings.news_topic, headline.to_bytes())
        await producer.send_and_wait(settings.price_topic, p1.to_bytes())
        await asyncio.sleep(max(0.1, price_gap_sec))
        await producer.send_and_wait(settings.price_topic, p2.to_bytes())

        alert_payload = await _wait_for_payload(
            alert_consumer,
            lambda p: p.get("symbol") == symbol,
            timeout_sec=timeout_sec,
        )
        if check_summaries:
            summary_payload = await _wait_for_payload(
                summary_consumer,
                lambda p: p.get("symbol") == symbol,
                timeout_sec=timeout_sec,
            )
        if require_summary_db_update and summary_payload:
            summary_db_updated = await _summary_present(
                conn,
                symbol=symbol,
                window=summary_payload.get("window", ""),
                event_time=summary_payload.get("time", ""),
            )
        if require_no_summary_dlq:
            summary_dlq_payload = await _wait_for_payload(
                summary_dlq_consumer,
                lambda p: p.get("symbol") == symbol,
                timeout_sec=timeout_sec,
            )

        anomalies_after = await _anomaly_count(conn, symbol, started_at - timedelta(minutes=5))
    finally:
        await producer.stop()
        await alert_consumer.stop()
        await summary_consumer.stop()
        await summary_dlq_consumer.stop()
        await conn.close()

    ok, failures = _evaluate_probe(
        alert_seen=bool(alert_payload),
        anomaly_delta=anomalies_after - anomalies_before,
        summary_request_seen=bool(summary_payload),
        summary_db_updated=summary_db_updated,
        summary_dlq_seen=bool(summary_dlq_payload),
        check_summaries=check_summaries,
        require_summary_db_update=require_summary_db_update,
        require_no_summary_dlq=require_no_summary_dlq,
    )
    result = {
        "ok": ok,
        "symbol": symbol,
        "anomaly_test_mode": settings.anomaly_test_mode,
        "topics": {
            "alerts": settings.alerts_topic,
            "summaries": settings.summaries_topic,
            "summaries_dlq": settings.summaries_dlq_topic,
            "news": settings.news_topic,
            "prices": settings.price_topic,
        },
        "anomalies_before": anomalies_before,
        "anomalies_after": anomalies_after,
        "anomaly_delta": anomalies_after - anomalies_before,
        "alert_seen": bool(alert_payload),
        "summary_request_seen": bool(summary_payload),
        "summary_db_updated": summary_db_updated,
        "summary_dlq_seen": bool(summary_dlq_payload),
        "check_summaries": check_summaries,
        "require_summary_db_update": require_summary_db_update,
        "require_no_summary_dlq": require_no_summary_dlq,
        "failures": failures,
    }
    print(json.dumps(result, sort_keys=True))
    if result["ok"]:
        print("probe passed: anomaly persisted and alert observed")
        return 0
    print("probe failed: inspect result JSON for blocker branch")
    for failure in failures:
        print(f" - {failure}")
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Probe anomaly + summary pipeline with bounded diagnostics")
    parser.add_argument("--symbol", default="", help="probe symbol (default: generated diag symbol)")
    parser.add_argument("--price-start", type=float, default=100.0)
    parser.add_argument("--price-end", type=float, default=120.0)
    parser.add_argument("--price-gap-sec", type=float, default=2.0)
    parser.add_argument("--timeout-sec", type=float, default=30.0)
    parser.add_argument("--check-summaries", action="store_true", help="require summary-request observation")
    parser.add_argument(
        "--require-summary-db-update",
        action="store_true",
        help="require anomaly row for probe event to have a summary",
    )
    parser.add_argument(
        "--require-no-summary-dlq",
        action="store_true",
        help="fail if probe summary request appears on summaries-deadletter",
    )
    args = parser.parse_args()

    symbol = args.symbol.strip() or f"diag-{uuid.uuid4().hex[:8]}"
    return asyncio.run(
        run_probe(
            symbol=symbol,
            price_start=args.price_start,
            price_end=args.price_end,
            price_gap_sec=args.price_gap_sec,
            timeout_sec=args.timeout_sec,
            check_summaries=args.check_summaries,
            require_summary_db_update=args.require_summary_db_update,
            require_no_summary_dlq=args.require_no_summary_dlq,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
