"""Realtime customer ingestion worker entry point."""

import os
import signal
import time as _time
from datetime import datetime, timedelta, timezone

from shared.agent_client import triage_failure
from shared.logging import get_logger
from shared.retry import run_with_retry
from shared.supabase_client import SupabaseClient
from shared.neo4j_client import Neo4jClient, is_auth_error, is_non_retryable_write_error
from services.customer_realtime.handlers import EventValidationError, handle_event

LOG = get_logger(__name__)

_SHUTDOWN = False


def _handle_signal(signum, frame):
    global _SHUTDOWN
    LOG.info("shutdown signal received", extra={"signal": signum})
    _SHUTDOWN = True


def _is_non_retryable_error(exc: Exception) -> bool:
    return (
        isinstance(exc, EventValidationError)
        or is_auth_error(exc)
        or is_non_retryable_write_error(exc)
    )


def main() -> None:
    """
    Long-running outbox poller with graceful shutdown.

    Polls gold.outbox_events for pending events and processes them.
    Responds to SIGTERM/SIGINT for graceful shutdown.
    """
    global _SHUTDOWN

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    poll_interval = int(os.getenv("NEO4J_REALTIME_POLL_INTERVAL", "5"))

    supabase = SupabaseClient.from_env()
    neo4j = Neo4jClient.from_env()
    neo4j.verify_auth()

    LOG.info(
        "customer_realtime worker starting",
        extra={"poll_interval": poll_interval},
    )

    while not _SHUTDOWN:
        now = datetime.now(timezone.utc).isoformat()
        pending_events = supabase.fetch_pending_events(now=now)

        if not pending_events:
            _time.sleep(poll_interval)
            continue

        for event in pending_events:
            if _SHUTDOWN:
                break
            try:
                run_with_retry(
                    lambda e=event: handle_event(e, supabase, neo4j),
                    non_retryable=_is_non_retryable_error,
                )
            except EventValidationError as exc:
                event_id = event.get("id")
                if event_id:
                    supabase.mark_event_failed(
                        str(event_id),
                        "validation_error",
                        str(exc),
                    )
                LOG.error(
                    "event validation failed",
                    extra={"event_id": event_id, "error": str(exc)},
                )
            except Exception as exc:
                event_id = event.get("id")
                if is_non_retryable_write_error(exc):
                    if event_id:
                        supabase.mark_event_failed(
                            str(event_id),
                            "neo4j_write_validation_failed",
                            str(exc),
                        )
                    LOG.error(
                        "non-retryable neo4j write failure",
                        extra={"event_id": event_id, "error": str(exc)},
                    )
                    continue
                if is_auth_error(exc):
                    LOG.error(
                        "neo4j auth failure in realtime worker",
                        extra={"error": str(exc)},
                    )
                    raise
                retry_count = int(event.get("retry_count") or 0) + 1
                triage = triage_failure(
                    {
                        "event_id": str(event_id) if event_id else "",
                        "event_type": str(event.get("event_type") or ""),
                        "error": str(exc),
                        "retry_count": retry_count,
                        "payload": event.get("payload", {}),
                    }
                )
                if triage and event_id:
                    classification = triage.get("classification")
                    error_code = triage.get("error_code", "unknown_error")
                    retry_in = int(triage.get("retry_in_seconds") or 0)
                    if classification == "retryable":
                        next_retry_at = (
                            datetime.now(timezone.utc)
                            + timedelta(seconds=retry_in)
                        )
                        supabase.mark_event_retry(
                            str(event_id),
                            error_code,
                            next_retry_at.isoformat(),
                            str(exc),
                            retry_count,
                        )
                        continue
                    if classification == "needs_review":
                        supabase.mark_event_needs_review(
                            str(event_id), error_code, str(exc),
                        )
                        continue
                    supabase.mark_event_failed(
                        str(event_id), error_code, str(exc),
                    )
                    continue
                # Retry helper already handled retry attempts; bubble up.
                raise

        # Brief pause between batches to avoid tight-looping
        if not _SHUTDOWN:
            _time.sleep(1)

    neo4j.close()
    LOG.info("customer_realtime worker stopped gracefully")


if __name__ == "__main__":
    main()

