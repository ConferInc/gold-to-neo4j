"""
Tests for Neo4j transaction support and Supabase lock-clearing behavior.

Covers: begin_transaction context manager, TX routing in upsert,
and mark_event_retry lock field clearing.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch, call

import pytest


# ═══════════════════════════════════════════════════════
# Neo4j Transaction Context Manager
# ═══════════════════════════════════════════════════════

class TestBeginTransaction:
    """Tests for Neo4jClient.begin_transaction()."""

    def test_returns_context_manager(self):
        """begin_transaction() can be used as a context manager."""
        from shared.neo4j_client import Neo4jClient

        with patch.object(Neo4jClient, "__init__", lambda self: None):
            client = Neo4jClient()
            # Mock the driver and session
            mock_tx = MagicMock()
            mock_tx.closed.return_value = False
            mock_session = MagicMock()
            mock_session.begin_transaction.return_value = mock_tx
            client._driver = MagicMock()
            client._driver.session.return_value = mock_session
            client._database = "neo4j"
            client.database = "neo4j"  # public alias used in _tx_context

            with client.begin_transaction() as tx:
                assert tx is not None

    def test_execute_in_tx_runs_query(self):
        """execute_in_tx routes a query through the transaction object."""
        from shared.neo4j_client import Neo4jClient

        with patch.object(Neo4jClient, "__init__", lambda self: None):
            client = Neo4jClient()
            mock_tx = MagicMock()
            mock_tx.run.return_value = MagicMock(data=MagicMock(return_value=[{"n": 1}]))

            client.execute_in_tx(mock_tx, "MATCH (n) RETURN n", {})
            mock_tx.run.assert_called_once_with("MATCH (n) RETURN n", {})


# ═══════════════════════════════════════════════════════
# Upsert TX Routing
# ═══════════════════════════════════════════════════════

class TestUpsertTxRouting:
    """Tests for upsert_from_config TX parameter routing."""

    def test_routes_to_autocommit_without_tx(self):
        """When tx=None, upsert uses regular neo4j.execute_many()."""
        from shared.upsert import upsert_from_config

        mock_neo4j = MagicMock()
        config = {
            "tables": {
                "test_table": {
                    "label": "TestNode",
                    "primary_key": "id",
                    "columns": ["id", "name"],
                    "include_extra_fields": False,
                }
            }
        }
        data = {"test_table": [{"id": "1", "name": "test"}]}

        upsert_from_config(config, data, mock_neo4j)
        mock_neo4j.execute_many.assert_called()

    def test_routes_to_tx_when_provided(self):
        """When tx= is provided, upsert uses neo4j.execute_many_in_tx()."""
        from shared.upsert import upsert_from_config

        mock_neo4j = MagicMock()
        mock_tx = MagicMock()
        config = {
            "tables": {
                "test_table": {
                    "label": "TestNode",
                    "primary_key": "id",
                    "columns": ["id", "name"],
                    "include_extra_fields": False,
                }
            }
        }
        data = {"test_table": [{"id": "1", "name": "test"}]}

        upsert_from_config(config, data, mock_neo4j, tx=mock_tx)
        mock_neo4j.execute_many_in_tx.assert_called()


# ═══════════════════════════════════════════════════════
# Supabase Lock Clearing on Retry
# ═══════════════════════════════════════════════════════

class TestMarkEventRetry:
    """Tests for SupabaseClient.mark_event_retry() lock clearing."""

    def test_clears_lock_fields(self):
        """mark_event_retry includes locked_by=None and locked_at=None."""
        from shared.supabase_client import SupabaseClient

        with patch.object(SupabaseClient, "__init__", lambda self: None):
            client = SupabaseClient()
            mock_query = MagicMock()
            mock_query.update.return_value = mock_query
            mock_query.eq.return_value = mock_query
            mock_query.execute.return_value = MagicMock(data=[])

            mock_schema = MagicMock()
            mock_schema.from_.return_value = mock_query
            client._client = MagicMock()
            client._client.schema.return_value = mock_schema

            client.mark_event_retry(
                event_id="evt-001",
                error_code="transient_error",
                next_retry_at="2026-04-21T18:00:00Z",
                error_message="timeout",
                retry_count=1,
            )

            # Verify update was called with lock-clearing fields
            update_call = mock_query.update.call_args[0][0]
            assert update_call["locked_by"] is None
            assert update_call["locked_at"] is None
            assert update_call["status"] == "pending"
            assert update_call["retry_count"] == 1


class TestMarkEventsProcessedBulk:
    """Tests for SupabaseClient.mark_events_processed_bulk()."""

    def test_calls_rpc_with_event_ids(self):
        """Bulk mark calls RPC with correct event ID array."""
        from shared.supabase_client import SupabaseClient

        with patch.object(SupabaseClient, "__init__", lambda self: None):
            client = SupabaseClient()
            client._client = MagicMock()
            mock_rpc = MagicMock()
            mock_rpc.execute.return_value = MagicMock(data=None)
            client._client.schema.return_value.rpc.return_value = mock_rpc

            client.mark_events_processed_bulk(["evt-1", "evt-2", "evt-3"])

            client._client.schema.return_value.rpc.assert_called_once_with(
                "mark_events_processed",
                {"p_event_ids": ["evt-1", "evt-2", "evt-3"]},
            )
