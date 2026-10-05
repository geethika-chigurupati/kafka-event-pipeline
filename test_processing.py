import asyncio
import json

import pytest
from pydantic import ValidationError

from pipeline.processing import build_dlq_payload, parse_message, with_retries


def test_parse_message_valid():
    raw = json.dumps({"account_id": "a1", "amount": "12.30", "currency": "EUR"}).encode()
    event = parse_message(raw)
    assert event.account_id == "a1"


def test_parse_message_not_json():
    with pytest.raises(ValueError):
        parse_message(b"not json")


def test_parse_message_invalid_fields():
    with pytest.raises(ValidationError):
        parse_message(b'{"account_id": "", "amount": -5}')


def test_with_retries_succeeds_after_failures():
    calls = {"n": 0}
    delays: list[float] = []

    async def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise ConnectionError("db down")
        return "ok"

    async def fake_sleep(seconds: float):
        delays.append(seconds)

    result = asyncio.run(with_retries(flaky, attempts=3, base_delay=0.5, sleep=fake_sleep))
    assert result == "ok"
    assert calls["n"] == 3
    assert delays == [0.5, 1.0]  # exponential backoff


def test_with_retries_raises_after_exhaustion():
    async def always_fails():
        raise ConnectionError("db down")

    async def no_sleep(_: float):
        return None

    with pytest.raises(ConnectionError):
        asyncio.run(with_retries(always_fails, attempts=2, sleep=no_sleep))


def test_build_dlq_payload_keeps_context():
    payload = json.loads(build_dlq_payload(b"bad", ValueError("boom"), "transactions", 42))
    assert payload["original_offset"] == 42
    assert payload["error_type"] == "ValueError"
    assert payload["payload"] == "bad"
