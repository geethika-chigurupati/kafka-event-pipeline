"""Pure logic (no Kafka or DB imports) so it is easy to unit test."""
import asyncio
import json
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import TypeVar

from .models import TransactionEvent

T = TypeVar("T")


def parse_message(raw: bytes) -> TransactionEvent:
    """Decode and validate a raw Kafka message.

    Raises ValueError for malformed JSON and pydantic.ValidationError
    (a ValueError subclass) for invalid fields.
    """
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(f"message is not valid JSON: {exc}") from exc
    return TransactionEvent.model_validate(data)


async def with_retries(
    fn: Callable[[], Awaitable[T]],
    *,
    attempts: int = 3,
    base_delay: float = 0.5,
    retry_on: tuple[type[BaseException], ...] = (Exception,),
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> T:
    """Run `fn`, retrying with exponential backoff. Re-raises the last error."""
    last_exc: BaseException | None = None
    for attempt in range(attempts):
        try:
            return await fn()
        except retry_on as exc:
            last_exc = exc
            if attempt < attempts - 1:
                await sleep(base_delay * (2**attempt))
    assert last_exc is not None
    raise last_exc


def build_dlq_payload(raw: bytes, error: BaseException, topic: str, offset: int) -> bytes:
    """Wrap a failed message with enough context to debug or replay it."""
    envelope = {
        "original_topic": topic,
        "original_offset": offset,
        "error_type": type(error).__name__,
        "error": str(error),
        "failed_at": datetime.now(UTC).isoformat(),
        "payload": raw.decode("utf-8", errors="replace"),
    }
    return json.dumps(envelope).encode("utf-8")
