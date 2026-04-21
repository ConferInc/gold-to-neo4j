"""Event handlers for realtime customer updates."""

from typing import Any, Dict

from shared.logging import get_logger
from shared.upsert import upsert_event

LOG = get_logger(__name__)


class EventValidationError(ValueError):
    """Raised when event payload is invalid and should not be retried."""


def _validate_payload(event_type: str, payload: Dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        raise EventValidationError(f"{event_type}: payload must be an object")

    for key, value in payload.items():
        if key.endswith("_id") and value is None:
            raise EventValidationError(f"{event_type}: {key} cannot be null")


def handle_event(event: Dict[str, Any], supabase, neo4j) -> None:
    """Map a single outbox event to graph operations and upsert into Neo4j.

    Note: This function does NOT mark the event as processed.
    The caller (process_batch) handles bulk marking after all events
    in a batch succeed, ensuring atomic lock cleanup and processed_at.
    """
    event_type = event.get("event_type")
    payload = event.get("payload", {})

    LOG.info("handling event", extra={"event_type": event_type})
    _validate_payload(event_type, payload)

    # Central mapping table should live here or in config later.
    upsert_event(event_type, payload, neo4j)
