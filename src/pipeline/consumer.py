"""Consumer: Kafka -> validate -> PostgreSQL, with retries and a dead-letter topic.

Usage: python -m pipeline.consumer
"""
import asyncio
import logging
from functools import partial

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from .config import settings
from .db import create_pool, insert_event
from .processing import build_dlq_payload, parse_message, with_retries

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("consumer")


async def run() -> None:
    pool = await create_pool(settings.database_url)
    consumer = AIOKafkaConsumer(
        settings.topic,
        bootstrap_servers=settings.bootstrap_servers,
        group_id=settings.group_id,
        enable_auto_commit=False,  # commit only after the message is handled
        auto_offset_reset="earliest",
    )
    dlq = AIOKafkaProducer(bootstrap_servers=settings.bootstrap_servers)
    await consumer.start()
    await dlq.start()
    try:
        async for msg in consumer:
            try:
                event = parse_message(msg.value)
                inserted = await with_retries(
                    partial(insert_event, pool, event),
                    attempts=settings.max_attempts,
                    base_delay=settings.base_delay_seconds,
                )
                log.info("event %s %s", event.event_id, "stored" if inserted else "duplicate")
            except Exception as exc:  # noqa: BLE001 - any failure goes to the DLQ
                log.warning("sending offset %s to DLQ: %s", msg.offset, exc)
                await dlq.send_and_wait(
                    settings.dlq_topic,
                    build_dlq_payload(msg.value, exc, msg.topic, msg.offset),
                )
            await consumer.commit()
    finally:
        await consumer.stop()
        await dlq.stop()
        await pool.close()


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("consumer stopped")


if __name__ == "__main__":
    main()
