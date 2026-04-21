"""Structured observability metrics for the Gold-to-Neo4j pipeline.

Provides a lightweight, zero-dependency metrics collector that emits
structured JSON log lines compatible with any log aggregator
(Loki, Datadog, CloudWatch, etc.).

Usage::

    from shared.metrics import emit, track_batch

    # Emit a single metric
    emit("outbox_events_claimed", value=50, worker_id="w-orch-00")

    # Track a batch operation (context manager)
    with track_batch("neo4j_upsert", layer="ingredients") as m:
        upsert_from_config(config, data, neo4j, tx=tx)
        m.set(rows_written=1500, tables=5)
    # Automatically emits duration_ms on exit
"""

from __future__ import annotations

import json
import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional

LOG = logging.getLogger("g2n.metrics")


# ═══════════════════════════════════════════════════════
# Simple Metric Emission
# ═══════════════════════════════════════════════════════

def emit(
    metric_name: str,
    *,
    value: float = 1.0,
    unit: str = "count",
    **dimensions: Any,
) -> None:
    """Emit a single structured metric as a JSON log line.

    Args:
        metric_name: Dot-separated metric name (e.g., "outbox.events_claimed").
        value: Numeric value for the metric.
        unit: Unit of measurement (count, ms, bytes, rows, etc.).
        **dimensions: Arbitrary key-value pairs for filtering/grouping.

    Example::

        emit("outbox.events_claimed", value=50, worker_id="w-orch-00")
        emit("batch.duration", value=3200, unit="ms", layer="ingredients")
    """
    record = {
        "_type": "metric",
        "metric": metric_name,
        "value": value,
        "unit": unit,
        "ts": datetime.now(timezone.utc).isoformat(),
        **dimensions,
    }
    LOG.info(json.dumps(record, default=str))


# ═══════════════════════════════════════════════════════
# Batch Tracking Context Manager
# ═══════════════════════════════════════════════════════

@dataclass
class BatchTracker:
    """Accumulates metrics for a batch operation."""
    metric_name: str
    dimensions: Dict[str, Any] = field(default_factory=dict)
    _extra: Dict[str, Any] = field(default_factory=dict)
    _start: float = 0.0

    def set(self, **kwargs: Any) -> None:
        """Set additional metric dimensions to include on emit."""
        self._extra.update(kwargs)


@contextmanager
def track_batch(metric_name: str, **dimensions: Any):
    """Track the duration and outcome of a batch operation.

    Usage::

        with track_batch("neo4j_upsert", layer="ingredients") as m:
            do_work()
            m.set(rows=1500)
        # Emits: {"metric": "neo4j_upsert", "duration_ms": 3200, "rows": 1500, ...}

    On exception, emits with status="failed" and the error message.
    """
    tracker = BatchTracker(metric_name=metric_name, dimensions=dimensions)
    tracker._start = time.monotonic()
    try:
        yield tracker
        duration_ms = int((time.monotonic() - tracker._start) * 1000)
        emit(
            metric_name,
            value=duration_ms,
            unit="ms",
            status="success",
            duration_ms=duration_ms,
            **dimensions,
            **tracker._extra,
        )
    except Exception as exc:
        duration_ms = int((time.monotonic() - tracker._start) * 1000)
        emit(
            metric_name,
            value=duration_ms,
            unit="ms",
            status="failed",
            duration_ms=duration_ms,
            error=str(exc)[:200],
            **dimensions,
            **tracker._extra,
        )
        raise


# ═══════════════════════════════════════════════════════
# Pre-built Metric Helpers
# ═══════════════════════════════════════════════════════

def worker_claimed(worker_id: str, count: int) -> None:
    """Emit when a worker successfully claims outbox events."""
    emit("outbox.events_claimed", value=count, worker_id=worker_id)


def worker_processed(worker_id: str, count: int, failed: int = 0) -> None:
    """Emit after a worker finishes processing a batch."""
    emit(
        "outbox.events_processed",
        value=count,
        worker_id=worker_id,
        failed=failed,
    )


def worker_idle(worker_id: str) -> None:
    """Emit when a worker poll cycle finds no events."""
    emit("outbox.worker_idle", value=1, worker_id=worker_id)


def batch_sync_complete(layer: str, rows: int, duration_ms: int) -> None:
    """Emit after a batch sync layer completes."""
    emit(
        "batch.sync_complete",
        value=rows,
        unit="rows",
        layer=layer,
        duration_ms=duration_ms,
    )


def stale_locks_released(count: int) -> None:
    """Emit when stale locks are released during maintenance."""
    emit("outbox.stale_locks_released", value=count)


def tx_committed(layer: str, rows: int, duration_ms: int) -> None:
    """Emit after a Neo4j transaction commits."""
    emit(
        "neo4j.tx_committed",
        value=duration_ms,
        unit="ms",
        layer=layer,
        rows=rows,
    )
