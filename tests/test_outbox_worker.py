"""
Tests for the OutboxWorker (services/customer_realtime/service.py).

Covers: event claiming, batch processing, auth error termination,
retry scheduling, and lock-clearing behavior.
"""

from __future__ import annotations

import threading
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, PropertyMock

import pytest


# ═══════════════════════════════════════════════════════
# Claim Events
# ═══════════════════════════════════════════════════════

class TestClaimEvents:
    """Tests for OutboxWorker.claim_events()."""

    def _make_worker(self, mock_supabase_client=None, mock_neo4j_client=None):
        """Build an OutboxWorker with mocked clients."""
        with patch("services.customer_realtime.service.SupabaseClient") as MockSupa, \
             patch("services.customer_realtime.service.Neo4jClient") as MockNeo:
            MockSupa.from_env.return_value = mock_supabase_client or MagicMock()
            MockNeo.from_env.return_value = mock_neo4j_client or MagicMock()
            from services.customer_realtime.service import OutboxWorker
            worker = OutboxWorker(
                worker_id="w-test-00",
                batch_size=50,
                poll_interval=1,
                lock_timeout=600,
            )
        return worker

    def test_claim_calls_rpc_with_correct_params(self):
        """RPC is called with worker_id, batch_size, lock_timeout."""
        mock_supa = MagicMock()
        mock_supa.rpc.return_value = []
        worker = self._make_worker(mock_supabase_client=mock_supa)
        worker.supabase = mock_supa

        worker.claim_events()

        mock_supa.rpc.assert_called_once_with("claim_outbox_events", {
            "p_worker_id": "w-test-00",
            "p_batch_size": 50,
            "p_lock_timeout_seconds": 600,
        })

    def test_claim_returns_empty_when_no_events(self):
        """Empty RPC result returns empty list."""
        mock_supa = MagicMock()
        mock_supa.rpc.return_value = []
        worker = self._make_worker(mock_supabase_client=mock_supa)
        worker.supabase = mock_supa

        result = worker.claim_events()
        assert result == []

    def test_claim_returns_empty_on_rpc_failure(self):
        """Exception during RPC returns empty list (logged, not raised)."""
        mock_supa = MagicMock()
        mock_supa.rpc.side_effect = Exception("connection refused")
        worker = self._make_worker(mock_supabase_client=mock_supa)
        worker.supabase = mock_supa

        result = worker.claim_events()
        assert result == []

    def test_claim_returns_event_list(self, sample_outbox_event):
        """Valid RPC result returns list of event dicts."""
        events = [sample_outbox_event(), sample_outbox_event()]
        mock_supa = MagicMock()
        mock_supa.rpc.return_value = events
        worker = self._make_worker(mock_supabase_client=mock_supa)
        worker.supabase = mock_supa

        result = worker.claim_events()
        assert len(result) == 2


# ═══════════════════════════════════════════════════════
# Process Batch
# ═══════════════════════════════════════════════════════

class TestProcessBatch:
    """Tests for OutboxWorker.process_batch()."""

    def _make_worker(self):
        with patch("services.customer_realtime.service.SupabaseClient") as MockSupa, \
             patch("services.customer_realtime.service.Neo4jClient") as MockNeo:
            MockSupa.from_env.return_value = MagicMock()
            MockNeo.from_env.return_value = MagicMock()
            from services.customer_realtime.service import OutboxWorker
            worker = OutboxWorker("w-test-00", 50, 1, 600)
        return worker

    @patch("services.customer_realtime.service.handle_event")
    def test_successful_events_marked_processed(self, mock_handle, sample_outbox_event):
        """Successfully processed events are bulk-marked as processed."""
        events = [sample_outbox_event(event_id="evt-1"), sample_outbox_event(event_id="evt-2")]
        mock_handle.return_value = None  # Success
        worker = self._make_worker()

        result = worker.process_batch(events)

        assert result["processed"] == 2
        assert result["failed"] == 0
        worker.supabase.mark_events_processed_bulk.assert_called_once_with(["evt-1", "evt-2"])

    @patch("services.customer_realtime.service.handle_event")
    @patch("services.customer_realtime.service.is_auth_error", return_value=True)
    def test_auth_error_raises(self, mock_is_auth, mock_handle, sample_outbox_event):
        """Neo4j auth errors are re-raised to terminate the worker."""
        mock_handle.side_effect = Exception("Unauthorized")
        worker = self._make_worker()

        with pytest.raises(Exception, match="Unauthorized"):
            worker.process_batch([sample_outbox_event()])

    @patch("services.customer_realtime.service.handle_event")
    @patch("services.customer_realtime.service.is_non_retryable_write_error", return_value=True)
    @patch("services.customer_realtime.service.is_auth_error", return_value=False)
    def test_non_retryable_write_error_marks_failed(
        self, mock_auth, mock_non_retry, mock_handle, sample_outbox_event
    ):
        """Non-retryable write errors mark event as failed and continue."""
        mock_handle.side_effect = Exception("constraint violation")
        worker = self._make_worker()

        result = worker.process_batch([sample_outbox_event(event_id="evt-bad")])

        assert result["failed"] == 1  # non-retryable write errors ARE counted as failed
        assert result["processed"] == 0
        worker.supabase.mark_event_failed.assert_called_once()


# ═══════════════════════════════════════════════════════
# Run Loop
# ═══════════════════════════════════════════════════════

class TestRunLoop:
    """Tests for OutboxWorker.run_loop() behavior."""

    def _make_worker(self):
        with patch("services.customer_realtime.service.SupabaseClient") as MockSupa, \
             patch("services.customer_realtime.service.Neo4jClient") as MockNeo:
            MockSupa.from_env.return_value = MagicMock()
            MockNeo.from_env.return_value = MagicMock()
            from services.customer_realtime.service import OutboxWorker
            worker = OutboxWorker("w-test-00", 50, 1, 600)
        return worker

    @patch("services.customer_realtime.service._time")
    @patch("services.customer_realtime.service.is_auth_error", return_value=True)
    def test_loop_breaks_on_auth_error(self, mock_is_auth, mock_time):
        """Auth errors terminate the run_loop instead of infinite-looping."""
        import services.customer_realtime.service as svc

        worker = self._make_worker()
        worker.claim_events = MagicMock(side_effect=Exception("Neo4j auth failed"))

        # Ensure loop can exit
        original_shutdown = svc._SHUTDOWN
        call_count = 0

        def shutdown_after_one(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count > 1:
                svc._SHUTDOWN = True
            raise Exception("Neo4j auth failed")

        worker.claim_events = MagicMock(side_effect=shutdown_after_one)
        svc._SHUTDOWN = False

        worker.run_loop()

        svc._SHUTDOWN = original_shutdown
        # Worker should have stopped after 1 iteration (auth break)
        assert call_count == 1

    @patch("services.customer_realtime.service._time")
    @patch("services.customer_realtime.service.worker_idle")
    def test_loop_sleeps_on_no_events(self, mock_idle, mock_time):
        """No events → worker_idle metric emitted, sleep called."""
        import services.customer_realtime.service as svc

        worker = self._make_worker()

        call_count = 0

        def claim_then_shutdown():
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                svc._SHUTDOWN = True
            return []

        original_shutdown = svc._SHUTDOWN
        svc._SHUTDOWN = False
        worker.claim_events = MagicMock(side_effect=claim_then_shutdown)

        worker.run_loop()

        svc._SHUTDOWN = original_shutdown
        mock_idle.assert_called()
        mock_time.sleep.assert_called()
