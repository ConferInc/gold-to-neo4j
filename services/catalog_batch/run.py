"""CLI runner for catalog batch layers."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List

import yaml

from shared.logging import get_logger

from services.customers_batch.service import main as customers_main
from services.ingredients_batch.service import main as ingredients_main
from services.products_batch.service import main as products_main
from services.recipes_batch.service import main as recipes_main

LOG = get_logger(__name__)

ROOT = Path(__file__).resolve().parents[2]
LOCK_DIR = ROOT / "state" / "locks"


LayerFn = Callable[[], None]

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


def _acquire_lock(layer: str) -> Path:
    LOCK_DIR.mkdir(parents=True, exist_ok=True)
    lock_path = LOCK_DIR / f"{layer}.lock"
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as exc:
        raise RuntimeError(f"layer already running: {layer}") from exc

    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(f"pid={os.getpid()}\n")
        f.write(f"started_at={datetime.now(timezone.utc).isoformat()}\n")
    return lock_path


def _release_lock(lock_path: Path) -> None:
    try:
        lock_path.unlink()
    except FileNotFoundError:
        return


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


def _run_layer(layer: str) -> Dict[str, Any]:
    """Run a single layer and return tool metrics."""
    main_fn = LAYER_MAIN[layer]
    lock_path = _acquire_lock(layer)
    t0 = time.time()
    status = "completed"
    error_msg = None
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
        _release_lock(lock_path)
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
    try:
        if args.layer == "all":
            for layer in LAYER_ORDER:
                m = _run_layer(layer)
                tool_metrics.append(m)
        else:
            m = _run_layer(args.layer)
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
