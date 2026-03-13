"""Agent gateway service with LangGraph workflows and Supabase webhook receiver."""

from __future__ import annotations

import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Literal, Optional

from fastapi import FastAPI, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from services.agent_gateway.graphs import (
    build_embedding_config_graph,
    build_failure_triage_graph,
    build_reconciliation_graph,
    build_schema_drift_graph,
)

# ---------------------------------------------------------------------------
# Debounce state — shared across webhook calls
# ---------------------------------------------------------------------------

_pending_gold_syncs: Dict[str, float] = {}  # table_name → timestamp of most-recent event
_debounce_lock = threading.Lock()
_DEBOUNCE_SECONDS: int = int(os.getenv("WEBHOOK_DEBOUNCE_SECONDS", "30"))

# Thread pool for background layer runs so the webhook endpoint returns quickly.
_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="neo4j_sync")

# ---------------------------------------------------------------------------
# Table → batch-sync layer routing
# Mirrors the 13 tables from implemenation/SUPABASE_WEBHOOK_SETUP_GUIDE.md
# ---------------------------------------------------------------------------

_TABLE_TO_LAYER: Dict[str, str] = {
    # recipes layer
    "recipes":  "recipes",
    "cuisines": "recipes",
    # ingredients layer
    "ingredients": "ingredients",
    # products layer
    "products":            "products",
    "product_categories":  "products",
    "vendors":             "products",
    "certifications":      "products",
    # customers layer
    "b2c_customers":        "customers",
    "b2b_customers":        "customers",
    "households":           "customers",
    "allergens":            "customers",
    "dietary_preferences":  "customers",
    "health_conditions":    "customers",
}


# ---------------------------------------------------------------------------
# Background batch-sync helper
# ---------------------------------------------------------------------------

def _run_layer_sync(layer: str, triggered_by: str) -> None:
    """Run a catalog batch layer synchronously (called from the thread pool)."""
    from shared.logging import get_logger
    from services.catalog_batch.run import _run_layer  # noqa: PLC0415

    log = get_logger(__name__)
    log.info(
        "neo4j_sync started",
        extra={"layer": layer, "triggered_by": triggered_by},
    )
    try:
        _run_layer(layer)
        log.info("neo4j_sync completed", extra={"layer": layer})
    except Exception as exc:
        log.error(
            "neo4j_sync failed",
            extra={"layer": layer, "error": str(exc)},
        )


def _debounced_neo4j_sync(source_table: str, layer: str) -> Dict[str, Any]:
    """
    Debounce Gold→Neo4j sync triggers.

    When a Gold table receives rapid writes (e.g. a batch load of 500 rows),
    Supabase fires one webhook per row.  This function ensures we only trigger
    ONE layer sync after the writes have settled down (no new events for
    _DEBOUNCE_SECONDS).

    Returns immediately with a dict indicating whether the sync was deferred or
    already pending.
    """
    now = time.monotonic()

    with _debounce_lock:
        already_pending = source_table in _pending_gold_syncs
        _pending_gold_syncs[source_table] = now  # always refresh the timestamp

    if already_pending:
        # A timer thread is already running for this table — it will pick up
        # the refreshed timestamp and wait another DEBOUNCE window.
        return {"debounced": True, "table": source_table, "status": "timer_refreshed"}

    # First event for this table in the current window — start a timer thread.
    def _fire_after_debounce() -> None:
        while True:
            time.sleep(_DEBOUNCE_SECONDS)
            with _debounce_lock:
                last_event = _pending_gold_syncs.get(source_table, 0.0)
            elapsed = time.monotonic() - last_event
            if elapsed >= _DEBOUNCE_SECONDS - 1:
                # Quiet window has passed — remove entry and fire the sync.
                with _debounce_lock:
                    _pending_gold_syncs.pop(source_table, None)
                _executor.submit(_run_layer_sync, layer, f"webhook:gold.{source_table}")
                return
            # New events arrived during the sleep — loop and wait again.

    timer = threading.Thread(target=_fire_after_debounce, daemon=True)
    timer.start()

    return {
        "debounced": True,
        "table": source_table,
        "layer": layer,
        "status": "timer_started",
        "will_fire_in_seconds": _DEBOUNCE_SECONDS,
    }


# ---------------------------------------------------------------------------
# APScheduler setup (loaded at startup from Supabase schedule_definitions)
# ---------------------------------------------------------------------------

def _build_scheduler():
    """Return a configured AsyncIOScheduler or None if apscheduler is absent."""
    try:
        from apscheduler.schedulers.background import BackgroundScheduler  # noqa: PLC0415
        from apscheduler.triggers.cron import CronTrigger  # noqa: PLC0415

        return BackgroundScheduler(), CronTrigger
    except ImportError:
        return None, None


# ---------------------------------------------------------------------------
# App lifespan — start/stop scheduler
# ---------------------------------------------------------------------------

@asynccontextmanager
async def _lifespan(app: FastAPI):
    scheduler, CronTrigger = _build_scheduler()

    if scheduler is not None:
        from shared.supabase_client import SupabaseClient  # noqa: PLC0415
        from shared.logging import get_logger  # noqa: PLC0415

        log = get_logger(__name__)
        try:
            supabase = SupabaseClient.from_env()
            schedules = supabase.list_active_schedules()
            for sched in schedules:
                name = sched.get("schedule_name", "")
                cron = sched.get("cron_expression", "")
                flow = sched.get("flow_name", "")
                run_cfg = sched.get("run_config") or {}
                if not cron or not flow:
                    continue
                layer = run_cfg.get("layer", "all")
                triggered_by = f"scheduler:{name}"
                scheduler.add_job(
                    _run_layer_sync,
                    trigger=CronTrigger.from_crontab(cron),
                    id=name,
                    args=[layer, triggered_by],
                    replace_existing=True,
                    misfire_grace_time=300,
                )
                log.info(
                    "schedule registered",
                    extra={"name": name, "cron": cron, "layer": layer},
                )
            scheduler.start()
            log.info("APScheduler started", extra={"job_count": len(schedules)})
        except Exception as exc:
            log.warning("APScheduler startup failed", extra={"error": str(exc)})

    yield  # application runs here

    if scheduler is not None:
        try:
            scheduler.shutdown(wait=False)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------

app = FastAPI(title="Agent Gateway", version="0.1.0", lifespan=_lifespan)


# ---------------------------------------------------------------------------
# Pydantic models — agent tasks
# ---------------------------------------------------------------------------

class SchemaDriftRequest(BaseModel):
    task: Literal["schema_drift_resolver"]
    table: str
    missing: List[str]
    available: List[str]
    schema_contract: Dict[str, Any] = Field(default_factory=dict)
    alias_map: Dict[str, str] = Field(default_factory=dict)
    model: Optional[str] = None


class SchemaDriftResponse(BaseModel):
    aliases: Dict[str, str]
    confidence: float = 0.0
    reason: str = ""


class FailureTriageRequest(BaseModel):
    task: Literal["failure_triage"]
    event_id: str
    event_type: str
    error: str
    retry_count: int = 0
    payload: Dict[str, Any] = Field(default_factory=dict)
    model: Optional[str] = None


class FailureTriageResponse(BaseModel):
    classification: Literal["retryable", "poison", "needs_review"]
    error_code: str = ""
    retry_in_seconds: int = 0


class ReconciliationRequest(BaseModel):
    task: Literal["reconciliation_backfill"]
    entity: str
    source_count: int
    target_count: int
    last_checkpoint: Optional[str] = None
    sampling_window: Optional[str] = None
    checksums: Dict[str, Any] = Field(default_factory=dict)
    drift_threshold: float = 0.005
    model: Optional[str] = None


class ReconciliationResponse(BaseModel):
    action: Literal["backfill", "observe"]
    from_: Optional[str] = Field(default=None, alias="from")
    to: Optional[str] = None
    reason: str = ""


class EmbeddingConfigRequest(BaseModel):
    task: Literal["embedding_config_resolver"]
    expected_labels: List[str]
    available_labels: List[str]
    missing_labels: List[str] = Field(default_factory=list)
    expected_relationship_types: List[str]
    available_relationship_types: List[str]
    missing_relationship_types: List[str] = Field(default_factory=list)
    model: Optional[str] = None


class EmbeddingConfigResponse(BaseModel):
    label_aliases: Dict[str, str] = Field(default_factory=dict)
    relationship_aliases: Dict[str, str] = Field(default_factory=dict)
    reason: str = ""


# ---------------------------------------------------------------------------
# Pydantic model — Supabase webhook payload
# ---------------------------------------------------------------------------

class WebhookPayload(BaseModel):
    type: str                          # "INSERT" | "UPDATE" | "DELETE"
    schema: str                        # e.g. "gold"
    table: str                         # e.g. "recipes"
    record: Optional[Dict[str, Any]] = None
    old_record: Optional[Dict[str, Any]] = None


# ---------------------------------------------------------------------------
# Pre-build LangGraph instances (module-level — avoids cold-start per request)
# ---------------------------------------------------------------------------

schema_drift_graph = build_schema_drift_graph()
failure_triage_graph = build_failure_triage_graph()
reconciliation_graph = build_reconciliation_graph()
embedding_config_graph = build_embedding_config_graph()


# ---------------------------------------------------------------------------
# Routes — agent tasks
# ---------------------------------------------------------------------------

@app.post("/agent")
async def run_agent(payload: Dict[str, Any]) -> Dict[str, Any]:
    task = payload.get("task")
    if task == "schema_drift_resolver":
        request = SchemaDriftRequest.model_validate(payload)
        result = schema_drift_graph.invoke({"payload": request.model_dump()})
        response = SchemaDriftResponse.model_validate(result.get("result", {}))
        return response.model_dump()

    if task == "failure_triage":
        request = FailureTriageRequest.model_validate(payload)
        result = failure_triage_graph.invoke({"payload": request.model_dump()})
        response = FailureTriageResponse.model_validate(result.get("result", {}))
        return response.model_dump()

    if task == "reconciliation_backfill":
        request = ReconciliationRequest.model_validate(payload)
        result = reconciliation_graph.invoke({"payload": request.model_dump()})
        response = ReconciliationResponse.model_validate(result.get("result", {}))
        return response.model_dump(by_alias=True)

    if task == "embedding_config_resolver":
        request = EmbeddingConfigRequest.model_validate(payload)
        result = embedding_config_graph.invoke({"payload": request.model_dump()})
        response = EmbeddingConfigResponse.model_validate(result.get("result", {}))
        return response.model_dump()

    raise HTTPException(status_code=400, detail="unknown task")


# ---------------------------------------------------------------------------
# Route — Supabase webhook receiver
# ---------------------------------------------------------------------------

@app.post("/webhooks/supabase", status_code=status.HTTP_202_ACCEPTED)
async def receive_supabase_webhook(
    request: Request,
    x_webhook_secret: Optional[str] = Header(default=None, alias="x-webhook-secret"),
) -> JSONResponse:
    """
    Receives INSERT/UPDATE/DELETE events fired by pg_net triggers on gold.* tables
    (see sql/webhooks.sql).

    - Validates the shared webhook secret.
    - Ignores non-gold schema events.
    - Debounces per-table to avoid triggering a Neo4j sync on every row during
      a bulk load (waits WEBHOOK_DEBOUNCE_SECONDS after the last event).
    - Returns 202 immediately; the actual sync runs in a background thread.
    """
    from shared.logging import get_logger  # noqa: PLC0415

    log = get_logger(__name__)

    # --- Secret validation ---------------------------------------------------
    expected_secret = os.getenv("WEBHOOK_SECRET", "")
    if expected_secret and x_webhook_secret != expected_secret:
        log.warning("webhook rejected: invalid secret")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid webhook secret",
        )

    # --- Parse payload -------------------------------------------------------
    try:
        body = await request.json()
        event = WebhookPayload.model_validate(body)
    except Exception as exc:
        log.warning("webhook rejected: bad payload", extra={"error": str(exc)})
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"invalid payload: {exc}",
        ) from exc

    # --- Only act on gold schema events -------------------------------------
    if event.schema != "gold":
        log.debug(
            "webhook ignored: non-gold schema",
            extra={"schema": event.schema, "table": event.table},
        )
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content={"accepted": True, "action": "ignored", "reason": "non-gold schema"},
        )

    # --- Route table to layer ------------------------------------------------
    layer = _TABLE_TO_LAYER.get(event.table)
    if layer is None:
        log.warning(
            "webhook: unknown table, defaulting to full sync",
            extra={"schema": event.schema, "table": event.table},
        )
        layer = "all"

    # --- Debounce and dispatch -----------------------------------------------
    result = _debounced_neo4j_sync(event.table, layer)
    log.info(
        "webhook accepted",
        extra={
            "schema": event.schema,
            "table": event.table,
            "op": event.type,
            "layer": layer,
            **result,
        },
    )
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={"accepted": True, **result},
    )


# ---------------------------------------------------------------------------
# Route — health check
# ---------------------------------------------------------------------------

@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("AGENT_PORT", "8000"))
    uvicorn.run("services.agent_gateway.service:app", host="0.0.0.0", port=port, reload=False)
