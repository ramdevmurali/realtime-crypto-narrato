#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import time
import uuid
from typing import Any, Callable

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from processor.src.config import settings


async def replay_summaries_dlq(
    *,
    source_topic: str,
    target_topic: str,
    brokers: list[str],
    idle_timeout_sec: float,
    max_messages: int | None,
    dry_run: bool,
    consumer_factory: Callable[..., Any] = AIOKafkaConsumer,
    producer_factory: Callable[..., Any] = AIOKafkaProducer,
) -> dict:
    group_id = f"replay-summaries-dlq-{uuid.uuid4().hex[:8]}"
    consumer = consumer_factory(
        source_topic,
        bootstrap_servers=brokers,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    producer = None if dry_run else producer_factory(bootstrap_servers=brokers)

    consumed = 0
    replayed = 0
    started = time.monotonic()
    last_seen = started
    stop_reason = "idle_timeout"

    await consumer.start()
    if producer is not None:
        await producer.start()

    try:
        while True:
            if max_messages is not None and consumed >= max_messages:
                stop_reason = "max_messages"
                break

            batch = await consumer.getmany(timeout_ms=1000, max_records=200)
            had_messages = False
            for records in batch.values():
                for msg in records:
                    had_messages = True
                    consumed += 1
                    if producer is not None:
                        await producer.send_and_wait(target_topic, msg.value)
                    replayed += 1
                    if max_messages is not None and consumed >= max_messages:
                        stop_reason = "max_messages"
                        break
                if stop_reason == "max_messages":
                    break
            if stop_reason == "max_messages":
                break

            if had_messages:
                last_seen = time.monotonic()
                continue

            if idle_timeout_sec <= 0:
                stop_reason = "idle_timeout"
                break
            if time.monotonic() - last_seen >= idle_timeout_sec:
                stop_reason = "idle_timeout"
                break
    finally:
        if producer is not None:
            await producer.stop()
        await consumer.stop()

    elapsed_sec = round(time.monotonic() - started, 3)
    return {
        "ok": True,
        "source_topic": source_topic,
        "target_topic": target_topic,
        "dry_run": dry_run,
        "consumed": consumed,
        "replayed": replayed,
        "idle_timeout_sec": idle_timeout_sec,
        "max_messages": max_messages,
        "stop_reason": stop_reason,
        "elapsed_sec": elapsed_sec,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay summary DLQ topic to summaries topic with bounded idle timeout")
    parser.add_argument("--source-topic", default=settings.summaries_dlq_topic)
    parser.add_argument("--target-topic", default=settings.summaries_topic)
    parser.add_argument("--idle-timeout-sec", type=float, default=5.0)
    parser.add_argument("--max-messages", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    result = asyncio.run(
        replay_summaries_dlq(
            source_topic=args.source_topic,
            target_topic=args.target_topic,
            brokers=settings.kafka_brokers,
            idle_timeout_sec=args.idle_timeout_sec,
            max_messages=args.max_messages,
            dry_run=args.dry_run,
        )
    )
    print(json.dumps(result, sort_keys=True))
    if result["ok"]:
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
