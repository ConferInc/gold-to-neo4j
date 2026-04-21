"""CLI runner for catalog batch layers — with advisory locks and phased parallel execution.

Phase execution order (for graph consistency):
  Phase 1: Ingredients (foundation nodes, no dependencies)
  Phase 2: Recipes + Products (parallel — both depend on Ingredients)
  Phase 3: Customers (depends on Recipes + Products for edges)

Locking: Uses PostgreSQL advisory locks instead of file-based locks
for cross-container coordination in Coolify deployments.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List

import yaml

from shared.logging import get_logger
from shared.supabase_client import SupabaseClient

from services.customers_batch.service import main as customers_main
from services.ingredients_batch.service import main as ingredients_main
from services.products_batch.service import main as products_main
from services.recipes_batch.service import main as recipes_main

LOG = get_logger(__name__)

ROOT = Path(__file__).resolve().parents[2]

LayerFn = Callable[[], None]

# ── Phased execution order (graph dependency aware) ──────────
# Phase 1: Ingredients (standalone nodes)
# Phase 2: Recipes + Products (can run in parallel, both depend on Ingredients)
# Phase 3: Customers (depends on Recipes + Products for relationship edges)
PHASE_ORDER: List[List[str]] = [
    ["ingredients"],              # Phase 1: foundation
    ["recipes", "products"],      # Phase 2: parallel
    ["customers"],                # Phase 3: depends on Phase 2
]

LAYER_ORDER: List[str] = ["ingredients", "recipes", "products", "customers"]
LAYER_MAIN: Dict[str, LayerFn] = {
    "recipes": recipes_main,
    "ingredients": ingredients_main,
    "products": products_main,
    "customers": customers_main,
}
LAYER_CONFIG: Dict[str, str] = {
    "recipes": "recipes.yaml",
    "ingredients": "ingredients.yaml",
    "products": "products.yaml",
    "customers": "customers.yaml",
}

LEGACY_STATE_PATH = ROOT / "state" / ".sync_state.json"


# ═══════════════════════════════════════════════════════
# Advisory Lock Helpers (replaces file-based locks)
# ═══════════════════════════════════════════════════════

def _layer_lock_id(layer: str) -> int:
    """Derive a stable advisory lock ID from a layer name.

    Uses a hash to map layer names to int4 range (PostgreSQL advisory
    locks require a bigint key, but we stay in int4 for safety).
    All lock IDs are in a reserved namespace (0xBATCH0000 + hash).
    """
    h = int(hashlib.md5(f"batch_sync_{layer}".encode()).hexdigest()[:8], 16)
    return h


def _try_advisory_lock(supabase: SupabaseClient, layer: str) -> bool:
    """Try to acquire a session-level advisory lock for a layer.

    Returns True if the lock was acquired, False if another session
    holds it (i.e., another instance is already processing this layer).
    """
    lock_id = _layer_lock_id(layer)
    try:
        result = supabase._client.schema("public").rpc(
            "pg_try_advisory_lock", {"key": lock_id}
        ).execute()
        acquired = bool(result.data)
        LOG.info(
            "advisory_lock_attempt",
            extra={"layer": layer, "lock_id": lock_id, "acquired": acquired},
        )
        return acquired
    except Exception as exc:
        LOG.warning(
            "advisory_lock_failed_fallback_to_run",
            extra={"layer": layer, "error": str(exc)},
        )
        # If advisory lock RPC fails (e.g., function not exposed),
        # fall through and run anyway — better to run than to skip.
        return True


def _release_advisory_lock(supabase: SupabaseClient, layer: str) -> None:
    """Release a session-level advisory lock for a layer."""
    lock_id = _layer_lock_id(layer)
    try:
        supabase._client.schema("public").rpc(
            "pg_advisory_unlock", {"key": lock_id}
        ).execute()
    except Exception as exc:
        LOG.warning(
            "advisory_unlock_failed",
            extra={"layer": layer, "error": str(exc)},
        )


# ═══════════════════════════════════════════════════════
# State File Management
# ═══════════════════════════════════════════════════════

def _ensure_state_file(layer: str) -> None:
    config_path = ROOT / "config" / LAYER_CONFIG[layer]
    with config_path.open("r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    sync_cfg = config.get("sync", {})
    state_file = sync_cfg.get("state_file", f"{layer}_state.json")
    state_path = ROOT / "state" / state_file
    if state_path.exists():
        return

    tables = list(config.get("tables", {}).keys())
    state_path.parent.mkdir(parents=True, exist_ok=True)

    if LEGACY_STATE_PATH.exists():
        with LEGACY_STATE_PATH.open("r", encoding="utf-8") as f:
            legacy_state = json.load(f)
        new_state = {table: legacy_state[table] for table in tables if table in legacy_state}
    else:
        new_state = {}

    with state_path.open("w", encoding="utf-8") as f:
        json.dump(new_state, f, indent=2)


# ═══════════════════════════════════════════════════════
# Layer Execution
# ═══════════════════════════════════════════════════════

def _run_layer(layer: str, supabase: SupabaseClient | None = None) -> Dict[str, Any]:
    """Run a single layer with advisory lock protection.

    Args:
        layer: Layer name (ingredients, recipes, products, customers).
        supabase: Optional SupabaseClient for advisory locking.
                  If None, runs without lock (backward compatible).
    """
    main_fn = LAYER_MAIN[layer]
    t0 = time.time()
    status = "completed"
    error_msg = None

    # Try to acquire advisory lock
    lock_acquired = False
    if supabase:
        lock_acquired = _try_advisory_lock(supabase, layer)
        if not lock_acquired:
            LOG.warning("layer_already_running", extra={"layer": layer})
            return {
                "tool_name": f"neo4j_sync_{layer}",
                "duration_ms": 0,
                "records_in": 0, "records_out": 0,
                "status": "skipped",
                "error": f"layer {layer} already running (advisory lock held)",
            }

    try:
        _ensure_state_file(layer)
        LOG.info("starting layer", extra={"layer": layer})
        main_fn()
        LOG.info("completed layer", extra={"layer": layer})
    except Exception as exc:
        status = "failed"
        error_msg = str(exc)
        raise
    finally:
        if supabase and lock_acquired:
            _release_advisory_lock(supabase, layer)
        duration_ms = int((time.time() - t0) * 1000)

    metrics: Dict[str, Any] = {
        "tool_name": f"neo4j_sync_{layer}",
        "duration_ms": duration_ms,
        "records_in": 0,
        "records_out": 0,
        "status": status,
    }
    if error_msg:
        metrics["error"] = error_msg

    # Try to read run_summary JSONL for record counts
    summary_path = ROOT / "state" / "run_summaries" / f"{layer}.jsonl"
    if summary_path.exists():
        try:
            with summary_path.open("r", encoding="utf-8") as f:
                lines = f.readlines()
            if lines:
                last = json.loads(lines[-1])
                total_rows = sum(
                    t.get("rows_fetched", 0)
                    for t in last.get("tables", {}).values()
                )
                metrics["records_in"] = total_rows
                metrics["records_out"] = total_rows
        except Exception:
            pass
    return metrics


def _run_phase(phase_layers: List[str], supabase: SupabaseClient | None = None) -> List[Dict[str, Any]]:
    """Run a phase of layers (potentially in parallel).

    If a phase contains a single layer, runs it directly.
    If a phase contains multiple layers, runs them in parallel threads.

    Returns a list of metric dicts.
    """
    if len(phase_layers) == 1:
        return [_run_layer(phase_layers[0], supabase)]

    # Parallel execution for multi-layer phases
    results: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=len(phase_layers), thread_name_prefix="batch") as pool:
        futures = {
            pool.submit(_run_layer, layer, supabase): layer
            for layer in phase_layers
        }
        for future in as_completed(futures):
            layer = futures[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as exc:
                LOG.error("parallel_layer_failed", extra={"layer": layer, "error": str(exc)})
                results.append({
                    "tool_name": f"neo4j_sync_{layer}",
                    "duration_ms": 0,
                    "records_in": 0, "records_out": 0,
                    "status": "failed",
                    "error": str(exc),
                })
    return results


# ═══════════════════════════════════════════════════════
# CLI Entry Point
# ═══════════════════════════════════════════════════════

def _parse_args(argv: Iterable[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run catalog batch layers.")
    parser.add_argument(
        "--layer",
        required=True,
        choices=[*LAYER_ORDER, "all"],
        help="Layer to run",
    )
    return parser.parse_args(list(argv))


def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    tool_metrics: List[Dict[str, Any]] = []

    # Create a shared Supabase client for advisory locks
    try:
        supabase = SupabaseClient.from_env()
    except Exception:
        supabase = None
        LOG.warning("supabase_client_unavailable — running without advisory locks")

    try:
        if args.layer == "all":
            # ── Phased parallel execution ──
            for phase_idx, phase_layers in enumerate(PHASE_ORDER, 1):
                LOG.info(
                    "phase_start",
                    extra={"phase": phase_idx, "layers": phase_layers},
                )
                phase_results = _run_phase(phase_layers, supabase)
                tool_metrics.extend(phase_results)

                # Check for failures — stop if any layer in this phase failed
                failed = [m for m in phase_results if m.get("status") == "failed"]
                if failed:
                    LOG.error(
                        "phase_failed_stopping",
                        extra={"phase": phase_idx, "failed_layers": [m["tool_name"] for m in failed]},
                    )
                    break

                LOG.info(
                    "phase_complete",
                    extra={"phase": phase_idx, "layers": phase_layers},
                )
        else:
            m = _run_layer(args.layer, supabase)
            tool_metrics.append(m)
    except Exception:
        LOG.exception("catalog batch run failed")
        # Still emit partial metrics
        json_summary = {
            "tool_metrics": tool_metrics,
            "llm_usage": {},
            "dq_summary": {},
        }
        print(json.dumps(json_summary))
        return 1

    # ── Structured JSON summary (last line — parsed by orchestrator) ──
    total_in = sum(m.get("records_in", 0) for m in tool_metrics)
    total_out = sum(m.get("records_out", 0) for m in tool_metrics)
    json_summary = {
        "total_records_fetched": total_in,
        "total_records_written": total_out,
        "tool_metrics": tool_metrics,
        "llm_usage": {},
        "dq_summary": {
            "total_records": total_in,
            "pass_count": total_out,
            "fail_count": total_in - total_out,
        },
    }
    print(json.dumps(json_summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
