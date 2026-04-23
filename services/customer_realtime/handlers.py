"""Event handlers for realtime customer updates."""

import os
from typing import Any, Dict

from shared.logging import get_logger
from shared.upsert import upsert_event

LOG = get_logger(__name__)


class EventValidationError(ValueError):
    """Raised when event payload is invalid and should not be retried."""


def _validate_payload(event_type: str, payload: Dict[str, Any]) -> None:
    """Validate an outbox event payload before processing.

    Only the row's primary key ("id") is required to be non-null.
    Other ``*_id`` foreign-key columns (e.g. silver_customer_id,
    b2b_customer_id, original_recipe_id) are legitimately nullable
    for B2C-only data and are handled gracefully by the upsert layer.
    """
    if not isinstance(payload, dict):
        raise EventValidationError(f"{event_type}: payload must be an object")

    if "id" in payload and payload["id"] is None:
        raise EventValidationError(f"{event_type}: primary key 'id' cannot be null")


def _embed_after_upsert(event_type: str, payload: Dict[str, Any], neo4j) -> None:
    """Best-effort embedding generation after a realtime upsert.

    This function NEVER raises exceptions — all errors are caught and logged.
    The node upsert is already committed before this runs.
    """
    try:
        # 1. Parse event_type → table_name + operation
        parts = event_type.rsplit(".", 1)
        if len(parts) != 2:
            return
        table_name, operation = parts

        # 2. Skip deletes — deleted nodes don't need embeddings
        if operation == "delete":
            return

        # 3. Look up table config from customers.yaml
        from shared.upsert import _load_config

        config = _load_config("customers.yaml")
        table_cfg = config.get("tables", {}).get(table_name, {})
        label = table_cfg.get("label")

        # 4. Skip join tables (skip_upsert: true) — they're relationships, not nodes
        if not label or table_cfg.get("skip_upsert"):
            return

        # 5. Get the node's primary key from the payload
        pk = table_cfg.get("primary_key", "id")
        node_id = payload.get(pk)
        if not node_id:
            return

        # 6. Call embed_node_inline — LiteLLM API call + Neo4j write
        from shared.semantic_embeddings import embed_node_inline

        success = embed_node_inline(neo4j, label, node_id, payload)
        if success:
            LOG.info(
                "realtime_embedding_written",
                extra={"label": label, "node_id": str(node_id)},
            )

    except Exception as exc:
        LOG.warning(
            "realtime_embedding_skipped",
            extra={"event_type": event_type, "error": str(exc)},
        )


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

    # 1. Upsert node/relationships into Neo4j
    upsert_event(event_type, payload, neo4j)

    # 2. Best-effort semantic embedding (inline, non-blocking)
    if os.getenv("EMBEDDING_REALTIME_ENABLED", "true").lower() == "true":
        _embed_after_upsert(event_type, payload, neo4j)
