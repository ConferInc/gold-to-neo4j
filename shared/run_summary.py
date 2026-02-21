"""Run summary writer for batch jobs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from shared.logging import get_logger

LOG = get_logger(__name__)


def append_run_summary(layer: str, summary: Dict[str, Any], root: Path) -> None:
    """Append a JSONL summary entry for a layer."""
    out_dir = root / "state" / "run_summaries"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{layer}.jsonl"
    try:
        with out_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(summary))
            f.write("\n")
    except Exception:
        LOG.exception("failed to write run summary", extra={"layer": layer, "path": str(out_path)})
