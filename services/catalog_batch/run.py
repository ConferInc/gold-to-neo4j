"""CLI runner for catalog batch layers."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, Iterable, List

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

LAYER_ORDER: List[str] = ["recipes", "ingredients", "products", "customers"]
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


def _run_layer(layer: str) -> None:
    main_fn = LAYER_MAIN[layer]
    lock_path = _acquire_lock(layer)
    try:
        _ensure_state_file(layer)
        LOG.info("starting layer", extra={"layer": layer})
        main_fn()
        LOG.info("completed layer", extra={"layer": layer})
    finally:
        _release_lock(lock_path)


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
    try:
        if args.layer == "all":
            for layer in LAYER_ORDER:
                _run_layer(layer)
        else:
            _run_layer(args.layer)
    except Exception:
        LOG.exception("catalog batch run failed")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
