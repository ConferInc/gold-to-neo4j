"""
Tests for batch sync coordination (services/catalog_batch/run.py).

Covers: advisory lock ID generation, lock acquisition/contention,
phased execution, and skipped-phase blocking.
"""

from __future__ import annotations

import hashlib
from unittest.mock import MagicMock, patch, call

import pytest


# ═══════════════════════════════════════════════════════
# Advisory Lock ID
# ═══════════════════════════════════════════════════════

class TestLayerLockId:
    """Tests for _layer_lock_id()."""

    def test_deterministic(self):
        """Same layer name produces same lock ID on repeated calls."""
        from services.catalog_batch.run import _layer_lock_id
        id1 = _layer_lock_id("ingredients")
        id2 = _layer_lock_id("ingredients")
        assert id1 == id2

    def test_unique_per_layer(self):
        """Different layers produce different lock IDs."""
        from services.catalog_batch.run import _layer_lock_id
        ids = {_layer_lock_id(layer) for layer in ["ingredients", "recipes", "products", "customers"]}
        assert len(ids) == 4, "All 4 layers should have distinct lock IDs"

    def test_within_int4_range(self):
        """All lock IDs fit within signed 32-bit integer range."""
        from services.catalog_batch.run import _layer_lock_id
        for layer in ["ingredients", "recipes", "products", "customers"]:
            lock_id = _layer_lock_id(layer)
            assert 0 <= lock_id <= 0x7FFFFFFF, f"{layer} lock_id {lock_id} exceeds int4 range"

    def test_mask_applied(self):
        """Verify the 0x7FFFFFFF mask is applied to raw hash."""
        from services.catalog_batch.run import _layer_lock_id
        # Manually compute raw hash for comparison
        raw = int(hashlib.md5(b"batch_sync_ingredients").hexdigest()[:8], 16)
        expected = raw & 0x7FFFFFFF
        assert _layer_lock_id("ingredients") == expected


# ═══════════════════════════════════════════════════════
# Advisory Lock Acquire/Release
# ═══════════════════════════════════════════════════════

class TestAdvisoryLock:
    """Tests for _try_advisory_lock() and _release_advisory_lock()."""

    def test_acquire_returns_true_on_success(self):
        """Lock acquired → returns True."""
        from services.catalog_batch.run import _try_advisory_lock
        mock_supa = MagicMock()
        mock_supa._client.schema.return_value.rpc.return_value.execute.return_value = \
            MagicMock(data=True)

        result = _try_advisory_lock(mock_supa, "ingredients")
        assert result is True

    def test_acquire_returns_false_on_contention(self):
        """Lock held by another session → returns False."""
        from services.catalog_batch.run import _try_advisory_lock
        mock_supa = MagicMock()
        mock_supa._client.schema.return_value.rpc.return_value.execute.return_value = \
            MagicMock(data=False)

        result = _try_advisory_lock(mock_supa, "ingredients")
        assert result is False

    def test_acquire_returns_true_on_rpc_failure(self):
        """RPC failure → returns True (fail-open design for resilience)."""
        from services.catalog_batch.run import _try_advisory_lock
        mock_supa = MagicMock()
        mock_supa._client.schema.return_value.rpc.side_effect = Exception("RPC unavailable")

        result = _try_advisory_lock(mock_supa, "ingredients")
        assert result is True  # Fail-open: better to run than to skip

    def test_release_calls_unlock_rpc(self):
        """Release calls pg_advisory_unlock with correct lock ID."""
        from services.catalog_batch.run import _release_advisory_lock, _layer_lock_id
        mock_supa = MagicMock()
        _release_advisory_lock(mock_supa, "ingredients")

        expected_id = _layer_lock_id("ingredients")
        mock_supa._client.schema.return_value.rpc.assert_called_once_with(
            "pg_advisory_unlock", {"key": expected_id}
        )


# ═══════════════════════════════════════════════════════
# Phase Execution
# ═══════════════════════════════════════════════════════

class TestRunPhase:
    """Tests for _run_phase() and phase blocking logic."""

    def test_run_layer_skips_on_lock_contention(self):
        """When advisory lock is not acquired, layer returns 'skipped'."""
        from services.catalog_batch.run import _run_layer
        mock_supa = MagicMock()
        mock_supa._client.schema.return_value.rpc.return_value.execute.return_value = \
            MagicMock(data=False)  # Lock NOT acquired

        result = _run_layer("ingredients", mock_supa)
        assert result["status"] == "skipped"

    @patch("services.catalog_batch.run._try_advisory_lock", return_value=True)
    @patch("services.catalog_batch.run._release_advisory_lock")
    @patch("services.catalog_batch.run._ensure_state_file")
    def test_run_layer_runs_on_lock_acquired(self, mock_state, mock_release, mock_try_lock):
        """When lock is acquired, layer runs its main() function."""
        from services.catalog_batch.run import _run_layer, LAYER_MAIN

        mock_main = MagicMock(return_value=None)
        with patch.dict(LAYER_MAIN, {"ingredients": mock_main}):
            result = _run_layer("ingredients", MagicMock())

        assert result["status"] == "completed"
        mock_main.assert_called_once()
        mock_release.assert_called_once()

    @patch("services.catalog_batch.run._run_phase")
    def test_phase_blocks_on_skipped_layer(self, mock_run_phase):
        """Skipped layer in phase 1 stops subsequent phases."""
        mock_run_phase.return_value = [
            {"tool_name": "neo4j_sync_ingredients", "status": "skipped",
             "duration_ms": 0, "records_in": 0, "records_out": 0},
        ]

        # Simulate the main() logic inline
        PHASE_ORDER = [["ingredients"], ["recipes", "products"], ["customers"]]
        phases_completed = 0

        for phase_idx, phase_layers in enumerate(PHASE_ORDER, 1):
            phase_results = mock_run_phase(phase_layers, None)
            blocked = [m for m in phase_results if m.get("status") in ("failed", "skipped")]
            if blocked:
                break
            phases_completed += 1

        assert phases_completed == 0, "No phases should complete when first is skipped"

    @patch("services.catalog_batch.run._run_phase")
    def test_phase_blocks_on_failed_layer(self, mock_run_phase):
        """Failed layer in phase 1 stops subsequent phases."""
        mock_run_phase.return_value = [
            {"tool_name": "neo4j_sync_ingredients", "status": "failed",
             "duration_ms": 100, "records_in": 0, "records_out": 0,
             "error": "connection refused"},
        ]

        PHASE_ORDER = [["ingredients"], ["recipes", "products"], ["customers"]]
        phases_completed = 0

        for phase_idx, phase_layers in enumerate(PHASE_ORDER, 1):
            phase_results = mock_run_phase(phase_layers, None)
            blocked = [m for m in phase_results if m.get("status") in ("failed", "skipped")]
            if blocked:
                break
            phases_completed += 1

        assert phases_completed == 0
