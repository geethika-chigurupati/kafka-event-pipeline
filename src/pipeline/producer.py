"""Simulated transaction producer.

Usage: python -m pipeline.producer --count 100 --bad-rate 0.05
"""
import argparse
import asyncio
import random
from decimal import Decimal

from aiokafka import AIOKafkaProducer

from .config import settings
from .models import TransactionEvent


def make_event() -> TransactionEvent:
    return TransactionEvent(
        account_id=f"acct-{random.randint(1, 20):03d}",
        amount=Decimal(random.randint(100, 50_000)) / 100,
        currency=random.choice(["USD", "EUR", "GBP"]),
    )


async def run(count: int, bad_rate: float) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=settings.bootstrap_servers)
    await producer.start()
    try:
        for _ in range(count):
            if random.random() < bad_rate:
                value = b'{"account_id": "", "amount": -5}'  # invalid on purpose -> DLQ
            else:
                value = make_event().model_dump_json().encode()
            await producer.send_and_wait(settings.topic, value)
        print(f"sent {count} messages to {settings.topic}")
    finally:
        await producer.stop()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--bad-rate", type=float, default=0.05)
    args = parser.parse_args()
    asyncio.run(run(args.count, args.bad_rate))


if __name__ == "__main__":
    main()
