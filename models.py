from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class TransactionEvent(BaseModel):
    """A single transaction event. `event_id` is the idempotency key."""

    event_id: UUID = Field(default_factory=uuid4)
    account_id: str = Field(min_length=1)
    amount: Decimal = Field(gt=0)
    currency: str = Field(min_length=3, max_length=3)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
