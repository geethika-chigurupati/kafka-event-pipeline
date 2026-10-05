import asyncpg

from .models import TransactionEvent

SCHEMA = """
CREATE TABLE IF NOT EXISTS transactions (
    event_id    UUID PRIMARY KEY,
    account_id  TEXT NOT NULL,
    amount      NUMERIC(18, 2) NOT NULL,
    currency    CHAR(3) NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_transactions_account ON transactions (account_id, created_at);
"""


async def create_pool(dsn: str) -> asyncpg.Pool:
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=5)
    async with pool.acquire() as conn:
        await conn.execute(SCHEMA)
    return pool


async def insert_event(pool: asyncpg.Pool, event: TransactionEvent) -> bool:
    """Insert an event. Returns False if it was a duplicate (idempotent write)."""
    status = await pool.execute(
        """
        INSERT INTO transactions (event_id, account_id, amount, currency, created_at)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (event_id) DO NOTHING
        """,
        event.event_id,
        event.account_id,
        event.amount,
        event.currency,
        event.created_at,
    )
    return status.endswith(" 1")
