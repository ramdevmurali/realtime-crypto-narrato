import contextlib

from aiokafka import AIOKafkaProducer

from .config import settings
from .db import get_pool


async def healthcheck_db(log) -> None:
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1;")
        log.info("db_health_ok")
    except Exception as exc:
        log.error("db_health_fail", extra={"error": str(exc)})
        raise


async def healthcheck_kafka(log) -> None:
    temp_prod = AIOKafkaProducer(bootstrap_servers=settings.kafka_brokers)
    try:
        await temp_prod.start()
        log.info("kafka_health_ok")
    except Exception as exc:
        log.error("kafka_health_fail", extra={"error": str(exc)})
        raise
    finally:
        with contextlib.suppress(Exception):
            await temp_prod.stop()


async def healthcheck(log) -> None:
    await healthcheck_db(log)
    await healthcheck_kafka(log)
