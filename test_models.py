import pytest
from pydantic import ValidationError

from pipeline.models import TransactionEvent


def test_valid_event_gets_defaults():
    event = TransactionEvent(account_id="acct-1", amount="10.50", currency="USD")
    assert event.event_id is not None
    assert event.created_at.tzinfo is not None


@pytest.mark.parametrize(
    "kwargs",
    [
        {"account_id": "", "amount": "5", "currency": "USD"},
        {"account_id": "a", "amount": "-1", "currency": "USD"},
        {"account_id": "a", "amount": "5", "currency": "US"},
    ],
)
def test_invalid_events_rejected(kwargs):
    with pytest.raises(ValidationError):
        TransactionEvent(**kwargs)
