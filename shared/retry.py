"""Retry helpers."""

import time
from typing import Callable, Optional, TypeVar

from shared.logging import get_logger

LOG = get_logger(__name__)

T = TypeVar("T")


def run_with_retry(
    func: Callable[[], T],
    attempts: int = 3,
    backoff_seconds: float = 1.0,
    non_retryable: Optional[Callable[[Exception], bool]] = None,
) -> T:
    """Run a function with basic retry and linear backoff."""
    last_exc: Exception | None = None

    for attempt in range(1, attempts + 1):
        try:
            return func()
        except Exception as exc:  # noqa: BLE001
            if non_retryable and non_retryable(exc):
                LOG.error("non-retryable failure", extra={"attempt": attempt, "error": str(exc)})
                raise
            last_exc = exc
            LOG.warning(
                "retrying after failure",
                extra={"attempt": attempt, "attempts": attempts, "error": str(exc)},
            )
            if attempt < attempts:
                time.sleep(backoff_seconds * attempt)

    if last_exc:
        raise last_exc
    raise RuntimeError("run_with_retry failed without exception")
