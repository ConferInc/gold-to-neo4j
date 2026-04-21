"""
Shared test fixtures for the gold-to-neo4j test suite.

All tests use mocks — no real Supabase/Neo4j connections required.
"""

import os
import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest


# ── Ensure no real connections during tests ────────────

@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    """Set safe defaults for all env vars so clients don't connect to real services."""
    monkeypatch.setenv("SUPABASE_URL", "https://test-project.supabase.co")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-key-1234567890")  # gitleaks:allow
    monkeypatch.setenv("NEO4J_URI", "bolt://localhost:7687")
    monkeypatch.setenv("NEO4J_USER", "neo4j")
    monkeypatch.setenv("NEO4J_PASSWORD", "test-password")
    monkeypatch.setenv("NEO4J_DATABASE", "neo4j")
    monkeypatch.setenv("NEO4J_WORKER_BATCH_SIZE", "50")
    monkeypatch.setenv("NEO4J_REALTIME_POLL_INTERVAL", "5")
    monkeypatch.setenv("NEO4J_WORKER_LOCK_TIMEOUT", "600")


# ── Mock Clients ──────────────────────────────────────

@pytest.fixture
def mock_supabase():
    """
    Return a mock SupabaseClient with chainable query builders.
    Mimics: client.schema("gold").from_("outbox_events").update(...).eq(...).execute()
    """
    mock_client = MagicMock()

    # Chainable query builder
    mock_query = MagicMock()
    mock_query.select.return_value = mock_query
    mock_query.insert.return_value = mock_query
    mock_query.update.return_value = mock_query
    mock_query.eq.return_value = mock_query
    mock_query.order.return_value = mock_query
    mock_query.limit.return_value = mock_query
    mock_query.execute.return_value = MagicMock(data=[])

    # schema("gold").from_("xxx") → mock_query
    mock_schema = MagicMock()
    mock_schema.from_.return_value = mock_query
    mock_schema.rpc.return_value = MagicMock(data=[])
    mock_client.schema.return_value = mock_schema

    return mock_client, mock_query


@pytest.fixture
def mock_neo4j():
    """
    Return a mock Neo4jClient with transaction support.
    Supports: neo4j.begin_transaction() as context manager.
    """
    mock = MagicMock()

    # Mock transaction context manager
    mock_tx = MagicMock()
    mock_tx.__enter__ = MagicMock(return_value=mock_tx)
    mock_tx.__exit__ = MagicMock(return_value=False)
    mock.begin_transaction.return_value = mock_tx

    mock.execute.return_value = []
    mock.execute_many.return_value = None

    return mock, mock_tx


# ── Sample Data Factories ─────────────────────────────

@pytest.fixture
def sample_outbox_event():
    """Factory for outbox event dicts."""
    def _make(
        event_id=None,
        table_name="b2c_customers",
        event_type="INSERT",
        status="pending",
        locked_by=None,
        retry_count=0,
    ):
        return {
            "id": event_id or str(uuid.uuid4()),
            "table_name": table_name,
            "event_type": event_type,
            "status": status,
            "payload": {"customer_id": "cust-001", "name": "Test User"},
            "locked_by": locked_by,
            "locked_at": None,
            "processed_at": None,
            "retry_count": retry_count,
            "next_retry_at": None,
            "error_code": None,
            "error_message": None,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
    return _make
