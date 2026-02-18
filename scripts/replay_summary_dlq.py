#!/usr/bin/env python3
import argparse
import asyncio
import base64
import json
from pathlib import Path

from aiokafka import AIOKafkaProducer

from processor.src.config import settings


async def publish_buffer(path: Path, sleep_ms: int) -> int:
    if not path.exists():
        print(f"buffer file not found: {path}")
        return 0

    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_brokers)
    await producer.start()
    sent = 0
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                payload_b64 = record.get("payload_b64")
                if not payload_b64:
                    continue
                payload = base64.b64decode(payload_b64)
                await producer.send_and_wait(settings.summaries_topic, payload)
                sent += 1
                if sleep_ms > 0:
                    await asyncio.sleep(sleep_ms / 1000)
    finally:
        await producer.stop()
    return sent


async def main() -> None:
    parser = argparse.ArgumentParser(description="Replay buffered summary requests to Kafka summaries topic")
    parser.add_argument(
        "--path",
        default=settings.summary_dlq_buffer_path,
        help="Path to summary DLQ buffer JSONL file",
    )
    parser.add_argument("--sleep-ms", type=int, default=0, help="Sleep between messages (ms)")
    args = parser.parse_args()

    sent = await publish_buffer(Path(args.path), args.sleep_ms)
    print(f"replayed {sent} summary requests to {settings.summaries_topic}")


if __name__ == "__main__":
    asyncio.run(main())
