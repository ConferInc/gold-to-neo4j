"""Status helper for catalog batch layers."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List

import yaml

from services.catalog_batch.run import LAYER_CONFIG, LAYER_ORDER, ROOT


def _parse_args(argv: Iterable[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Show catalog batch status.")
    parser.add_argument(
        "--layer",
        choices=[*LAYER_ORDER, "all"],
        default="all",
        help="Layer to show",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON",
    )
    return parser.parse_args(list(argv))


def _load_config(layer: str) -> Dict[str, Any]:
    config_path = ROOT / "config" / LAYER_CONFIG[layer]
    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _load_state(state_path: Path) -> Dict[str, Any]:
    if not state_path.exists():
        return {}
    with state_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _layer_status(layer: str) -> Dict[str, Any]:
    config = _load_config(layer)
    sync_cfg = config.get("sync", {})
    state_file = sync_cfg.get("state_file", f"{layer}_state.json")
    state_path = ROOT / "state" / state_file
    state = _load_state(state_path)

    tables = list(config.get("tables", {}).keys())
    table_states: List[Dict[str, Any]] = []
    max_ts = None
    for table in tables:
        ts = state.get(table)
        table_states.append({"table": table, "last_checkpoint": ts})
        if ts and (max_ts is None or ts > max_ts):
            max_ts = ts

    return {
        "layer": layer,
        "state_file": state_file,
        "state_path": str(state_path),
        "state_exists": state_path.exists(),
        "last_checkpoint": max_ts,
        "tables": table_states,
        "as_of": datetime.utcnow().isoformat() + "Z",
    }


def _print_plain(statuses: List[Dict[str, Any]]) -> None:
    for status in statuses:
        print(f"layer: {status['layer']}")
        print(f"state_file: {status['state_file']}")
        print(f"state_path: {status['state_path']}")
        print(f"state_exists: {status['state_exists']}")
        print(f"last_checkpoint: {status['last_checkpoint']}")
        print("tables:")
        for table_info in status["tables"]:
            print(f"- {table_info['table']}: {table_info['last_checkpoint']}")
        print(f"as_of: {status['as_of']}")
        print("")


def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    layers = LAYER_ORDER if args.layer == "all" else [args.layer]
    statuses = [_layer_status(layer) for layer in layers]

    if args.json:
        print(json.dumps({"layers": statuses}, indent=2))
    else:
        _print_plain(statuses)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
