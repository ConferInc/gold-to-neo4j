"""Realtime customer ingestion worker — parallel outbox processing.

Uses SELECT FOR UPDATE SKIP LOCKED to allow multiple workers to
process outbox events concurrently without coordination.

Each worker:
  1. Claims a batch of pending events atomically
  2. Processes all events in a single Neo4j transaction
  3. Marks the batch as processed on commit, or releases on failure

Scaling: set NEO4J_REALTIME_WORKERS=N to run N parallel workers.
"""

import os
import signal
import threading
import time as _time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from shared.agent_client import triage_failure
from shared.logging import get_logger
from shared.metrics import worker_claimed, worker_processed, worker_idle
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


# ══════════════════════════════════════════════════════
# OutboxWorker — Parallel SKIP LOCKED Worker
# ══════════════════════════════════════════════════════

class OutboxWorker:
    """A single outbox worker that claims and processes events atomically.

    Multiple instances can run concurrently (in threads or containers).
    The database-level SKIP LOCKED guarantees no two workers ever claim
    the same event — no coordination layer needed.

    Args:
        worker_id: Unique identifier for this worker (e.g., "w-abc123-00").
        batch_size: Number of events to claim per poll cycle.
        poll_interval: Seconds to wait when no events are pending.
        lock_timeout: Seconds before a stale lock is considered expired.
    """

    def __init__(
        self,
        worker_id: str,
        batch_size: int = 50,
        poll_interval: int = 5,
        lock_timeout: int = 300,
    ):
        self.worker_id = worker_id
        self.batch_size = batch_size
        self.poll_interval = poll_interval
        self.lock_timeout = lock_timeout
        self.supabase = SupabaseClient.from_env()
        self.neo4j = Neo4jClient.from_env()

    def claim_events(self) -> List[Dict[str, Any]]:
        """Atomically claim a batch of pending events using SKIP LOCKED.

        Returns a list of claimed event dicts, or empty list if none available.
        """
        try:
            result = self.supabase.rpc("claim_outbox_events", {
                "p_worker_id": self.worker_id,
                "p_batch_size": self.batch_size,
                "p_lock_timeout_seconds": self.lock_timeout,
            })
            return result if result else []
        except Exception as exc:
            LOG.error(
                "claim_events_failed",
                extra={"worker_id": self.worker_id, "error": str(exc)},
            )
            return []

    def process_batch(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Process a batch of claimed events, one at a time.

        Each event is processed individually — successful events are
        marked as processed, failed events are handled per their
        error classification (retryable, needs_review, or failed).

        Returns a summary dict with counts.
        """
        processed_ids: List[str] = []
        failed_count = 0
        skipped_count = 0

        for event in events:
            if _SHUTDOWN:
                break

            event_id = event.get("id")
            try:
                handle_event(event, self.supabase, self.neo4j)
                if event_id:
                    processed_ids.append(str(event_id))

            except EventValidationError as exc:
                skipped_count += 1
                if event_id:
                    self.supabase.mark_event_failed(
                        str(event_id), "validation_error", str(exc),
                    )
                LOG.error(
                    "event_validation_failed",
                    extra={
                        "worker_id": self.worker_id,
                        "event_id": event_id,
                        "error": str(exc),
                    },
                )

            except Exception as exc:
                failed_count += 1
                if is_non_retryable_write_error(exc):
                    if event_id:
                        self.supabase.mark_event_failed(
                            str(event_id),
                            "neo4j_write_validation_failed",
                            str(exc),
                        )
                    LOG.error(
                        "non_retryable_neo4j_write",
                        extra={
                            "worker_id": self.worker_id,
                            "event_id": event_id,
                            "error": str(exc),
                        },
                    )
                    continue

                if is_auth_error(exc):
                    LOG.error(
                        "neo4j_auth_failure",
                        extra={"worker_id": self.worker_id, "error": str(exc)},
                    )
                    raise  # Fatal — stop the worker

                # Triage via agent for retry classification
                self._triage_event(event, exc)

        # Batch mark all successfully processed events
        if processed_ids:
            mark_attempts = 3
            for attempt in range(1, mark_attempts + 1):
                try:
                    self.supabase.mark_events_processed_bulk(processed_ids)
                    break
                except Exception as exc:
                    if attempt == mark_attempts:
                        LOG.error(
                            "mark_processed_failed_final",
                            extra={
                                "worker_id": self.worker_id,
                                "event_ids": processed_ids,
                                "error": str(exc),
                                "attempts": mark_attempts,
                            },
                        )
                        raise  # Escalate — events would remain in inconsistent state
                    LOG.warning(
                        "mark_processed_retry",
                        extra={
                            "worker_id": self.worker_id,
                            "attempt": attempt,
                            "error": str(exc),
                        },
                    )
                    _time.sleep(attempt)  # 1s, 2s backoff

        return {
            "worker_id": self.worker_id,
            "claimed": len(events),
            "processed": len(processed_ids),
            "failed": failed_count,
            "skipped": skipped_count,
        }

    def _triage_event(self, event: Dict[str, Any], exc: Exception) -> None:
        """Classify a failed event via the agent and update its status."""
        event_id = event.get("id")
        retry_count = int(event.get("retry_count") or 0) + 1

        triage = triage_failure({
            "event_id": str(event_id) if event_id else "",
            "event_type": str(event.get("event_type") or ""),
            "error": str(exc),
            "retry_count": retry_count,
            "payload": event.get("payload", {}),
        })

        if triage and event_id:
            classification = triage.get("classification")
            error_code = triage.get("error_code", "unknown_error")
            retry_in = int(triage.get("retry_in_seconds") or 0)

            if classification == "retryable":
                next_retry_at = (
                    datetime.now(timezone.utc) + timedelta(seconds=retry_in)
                )
                self.supabase.mark_event_retry(
                    str(event_id), error_code,
                    next_retry_at.isoformat(), str(exc), retry_count,
                )
            elif classification == "needs_review":
                self.supabase.mark_event_needs_review(
                    str(event_id), error_code, str(exc),
                )
            else:
                self.supabase.mark_event_failed(
                    str(event_id), error_code, str(exc),
                )

    def run_loop(self) -> None:
        """Main worker loop — poll, claim, process, repeat.

        Runs until _SHUTDOWN is set (via SIGTERM/SIGINT) or a fatal
        error occurs (e.g., Neo4j auth failure).
        """
        self.neo4j.verify_auth()
        LOG.info(
            "outbox_worker_started",
            extra={
                "worker_id": self.worker_id,
                "batch_size": self.batch_size,
                "poll_interval": self.poll_interval,
                "lock_timeout": self.lock_timeout,
            },
        )

        while not _SHUTDOWN:
            try:
                events = self.claim_events()

                if not events:
                    worker_idle(self.worker_id)
                    _time.sleep(self.poll_interval)
                    continue

                worker_claimed(self.worker_id, len(events))
                result = self.process_batch(events)
                worker_processed(
                    self.worker_id,
                    result["processed"],
                    result["failed"],
                )
                LOG.info("batch_complete", extra=result)

            except Exception as exc:
                # Fatal errors (e.g., Neo4j auth failure) should stop the worker
                # instead of looping forever. Auth errors are raised by
                # process_batch → is_auth_error check.
                if is_auth_error(exc):
                    LOG.error(
                        "FATAL_neo4j_auth_failure — stopping worker",
                        extra={"worker_id": self.worker_id, "error": str(exc)},
                    )
                    break

                LOG.error(
                    "worker_loop_error",
                    extra={"worker_id": self.worker_id, "error": str(exc)},
                    exc_info=True,
                )
                _time.sleep(self.poll_interval)

            # Brief pause between batches to avoid tight-looping
            if not _SHUTDOWN:
                _time.sleep(0.5)

        self.neo4j.close()
        LOG.info(
            "outbox_worker_stopped",
            extra={"worker_id": self.worker_id},
        )


# ══════════════════════════════════════════════════════
# Legacy entry point (backward compatible)
# ══════════════════════════════════════════════════════

def main() -> None:
    """Single-worker entry point (backward compatible).

    .. deprecated:: v0.2.0
        This entry point will be removed in v0.3.0. For multi-worker mode,
        use the orchestrator API startup which reads NEO4J_REALTIME_WORKERS.
        For standalone usage, instantiate OutboxWorker directly.

    When run standalone (python -m services.customer_realtime.service),
    starts a single OutboxWorker. For multi-worker mode, use the
    orchestrator API startup which reads NEO4J_REALTIME_WORKERS.
    """
    global _SHUTDOWN

    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGTERM, _handle_signal)
        signal.signal(signal.SIGINT, _handle_signal)
    else:
        LOG.info("running in daemon thread — signal handlers not registered")

    hostname = os.getenv("HOSTNAME", "standalone")[:8]
    worker_id = f"w-{hostname}-00"
    batch_size = int(os.getenv("NEO4J_WORKER_BATCH_SIZE", "50"))
    poll_interval = int(os.getenv("NEO4J_REALTIME_POLL_INTERVAL", "5"))
    lock_timeout = int(os.getenv("NEO4J_WORKER_LOCK_TIMEOUT", "300"))

    worker = OutboxWorker(worker_id, batch_size, poll_interval, lock_timeout)
    worker.run_loop()


if __name__ == "__main__":
    main()
