import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic: str = os.getenv("EVENTS_TOPIC", "transactions")
    dlq_topic: str = os.getenv("DLQ_TOPIC", "transactions.dlq")
    group_id: str = os.getenv("CONSUMER_GROUP", "transactions-sink")
    database_url: str = os.getenv(
        "DATABASE_URL", "postgresql://pipeline:pipeline@localhost:5432/pipeline"
    )
    max_attempts: int = int(os.getenv("MAX_ATTEMPTS", "3"))
    base_delay_seconds: float = float(os.getenv("BASE_DELAY_SECONDS", "0.5"))


settings = Settings()
