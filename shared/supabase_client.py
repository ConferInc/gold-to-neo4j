"""Supabase client wrapper."""

from __future__ import annotations

import os
from typing import Any, Dict, Iterable, List, Optional

from supabase import create_client
from postgrest.exceptions import APIError

from shared.logging import get_logger

LOG = get_logger(__name__)


class SupabaseClient:
    def __init__(self, url: str, key: str) -> None:
        self.url = url
        self.key = key
        self._client = create_client(url, key)

    @classmethod
    def from_env(cls) -> "SupabaseClient":
        url = os.getenv("SUPABASE_URL", "")
        key = os.getenv("SUPABASE_KEY", "")
        if not url or not key:
            LOG.warning("missing SUPABASE_URL or SUPABASE_KEY")
        return cls(url, key)

    def fetch_all(
        self,
        schema: str,
        table: str,
        columns: Optional[List[str]] = None,
        batch_size: int = 1000,
        updated_at_column: Optional[str] = None,
        updated_since: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        offset = 0

        while True:
            select_cols = "*" if not columns else ",".join(columns)
            query = self._client.schema(schema).from_(table).select(select_cols)
            if updated_at_column and updated_since:
                query = query.gte(updated_at_column, updated_since)
            if filters:
                for key, value in filters.items():
                    query = query.eq(key, value)
            response = query.range(offset, offset + batch_size - 1).execute()
            batch = response.data or []
            rows.extend(batch)
            if len(batch) < batch_size:
                break
            offset += batch_size

        return rows

    def fetch_pending_events(self, limit: int = 100, now: Optional[str] = None) -> list[dict[str, Any]]:
        """Fetch pending outbox events, optionally respecting next_retry_at.

        NOTE: This is the legacy single-worker fetch method. For parallel
        workers, use claim_outbox_events via the rpc() method instead.
        """
        query = self._client.schema("gold").from_("outbox_events").select("*").eq("status", "pending")
        if now:
            # Filter in database for safety, but fallback to local filtering if needed.
            try:
                query = query.or_(f"next_retry_at.is.null,next_retry_at.lte.{now}")
            except Exception:
                pass
        response = query.limit(limit).execute()
        events = response.data or []
        if now:
            filtered = []
            for event in events:
                next_retry_at = event.get("next_retry_at")
                if not next_retry_at or next_retry_at <= now:
                    filtered.append(event)
            return filtered
        return events

    # ── Parallel Worker Methods (Phase 2) ─────────────────────

    def rpc(self, function_name: str, params: Dict[str, Any]) -> Any:
        """Call a PostgreSQL function via Supabase RPC.

        Uses the 'gold' schema for all outbox-related functions.
        Returns the function result data.
        """
        response = self._client.schema("gold").rpc(function_name, params).execute()
        return response.data

    def mark_events_processed_bulk(self, event_ids: List[str]) -> None:
        """Mark a batch of events as processed using the SQL function.

        Calls gold.mark_events_processed(p_event_ids) which atomically
        sets status='processed', clears locks, and sets processed_at.
        """
        self.rpc("mark_events_processed", {"p_event_ids": event_ids})

    def release_stale_locks(self, timeout_seconds: int = 300) -> int:
        """Release stale locks from crashed workers.

        Returns the number of events released.
        """
        result = self.rpc("release_stale_locks", {"p_timeout_seconds": timeout_seconds})
        return int(result) if result else 0

    def mark_event_processed(self, event_id: str) -> None:
        """Mark an outbox event as processed."""
        _ = (
            self._client.schema("gold").from_("outbox_events")
            .update({"status": "processed"})
            .eq("id", event_id)
            .execute()
        )

    def mark_event_failed(self, event_id: str, error_code: str, error_message: str) -> None:
        """Mark an outbox event as failed with an error payload."""
        payload = {
            "status": "failed",
            "error_code": error_code,
            "error_message": error_message[:500],
        }
        _ = (
            self._client.schema("gold").from_("outbox_events")
            .update(payload)
            .eq("id", event_id)
            .execute()
        )

    def mark_event_retry(
        self,
        event_id: str,
        error_code: str,
        next_retry_at: str,
        error_message: str,
        retry_count: int,
    ) -> None:
        payload = {
            "status": "pending",
            "error_code": error_code,
            "error_message": error_message[:500],
            "next_retry_at": next_retry_at,
            "retry_count": retry_count,
            "locked_by": None,      # Release lock so event is claimable at next_retry_at
            "locked_at": None,
        }
        _ = (
            self._client.schema("gold").from_("outbox_events")
            .update(payload)
            .eq("id", event_id)
            .execute()
        )

    def mark_event_needs_review(self, event_id: str, error_code: str, error_message: str) -> None:
        payload = {
            "status": "failed",
            "needs_review": True,
            "error_code": error_code,
            "error_message": error_message[:500],
        }
        _ = (
            self._client.schema("gold").from_("outbox_events")
            .update(payload)
            .eq("id", event_id)
            .execute()
        )

    def fetch_table_columns(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """Fetch column metadata from information_schema for a table."""
        try:
            response = (
                self._client.schema("information_schema")
                .from_("columns")
                .select("column_name,data_type,udt_name,is_nullable")
                .eq("table_schema", schema)
                .eq("table_name", table)
                .execute()
            )
            return response.data or []
        except APIError as exc:
            message = str(exc)
            if "PGRST106" not in message and "schema must be one of" not in message:
                raise

        # Fallback to a view in the target schema (e.g., gold.schema_columns)
        view_name = os.getenv("SCHEMA_COLUMNS_VIEW", "schema_columns")
        response = (
            self._client.schema(schema)
            .from_(view_name)
            .select("column_name,data_type,udt_name,is_nullable")
            .eq("table_name", table)
            .execute()
        )
        return response.data or []

    def fetch_table_constraints(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """Fetch table constraints for a table."""
        try:
            response = (
                self._client.schema("information_schema")
                .from_("table_constraints")
                .select("constraint_name,constraint_type")
                .eq("table_schema", schema)
                .eq("table_name", table)
                .execute()
            )
            return response.data or []
        except APIError:
            return []

    def fetch_key_column_usage(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """Fetch key column usage for a table."""
        try:
            response = (
                self._client.schema("information_schema")
                .from_("key_column_usage")
                .select("constraint_name,column_name,table_name")
                .eq("table_schema", schema)
                .eq("table_name", table)
                .execute()
            )
            return response.data or []
        except APIError:
            return []

    def fetch_constraint_column_usage(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """Fetch constraint column usage for a table."""
        try:
            response = (
                self._client.schema("information_schema")
                .from_("constraint_column_usage")
                .select("constraint_name,table_name,column_name")
                .eq("table_schema", schema)
                .execute()
            )
            return response.data or []
        except APIError:
            return []

    def count_rows(self, schema: str, table: str, primary_key: str) -> int:
        """Return row count for a table."""
        response = (
            self._client.schema(schema)
            .from_(table)
            .select(primary_key, count="exact")
            .limit(1)
            .execute()
        )
        count = getattr(response, "count", None)
        if count is None:
            return len(response.data or [])
        return int(count)

    def fetch_sample_ids(self, schema: str, table: str, primary_key: str, limit: int = 200) -> List[str]:
        """Fetch a sample of primary key values for checksum comparisons."""
        response = (
            self._client.schema(schema)
            .from_(table)
            .select(primary_key)
            .order(primary_key, desc=False)
            .limit(limit)
            .execute()
        )
        rows = response.data or []
        return [str(row.get(primary_key)) for row in rows if row.get(primary_key) is not None]
