"""Reconciliation job to detect drift and propose backfills."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import yaml
from dotenv import load_dotenv

from shared.agent_client import propose_reconciliation
from shared.logging import get_logger
from shared.neo4j_client import Neo4jClient
from shared.supabase_client import SupabaseClient

LOG = get_logger(__name__)

ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = ROOT / "config"
STATE_DIR = ROOT / "state"


def _hash_values(values: list[str]) -> str:
    digest = hashlib.sha256()
    for val in values:
        digest.update(str(val).encode("utf-8"))
        digest.update(b"|")
    return digest.hexdigest()


def _load_state(state_path: Path) -> Dict[str, Any]:
    if not state_path.exists():
        return {}
    with state_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _append_plan(record: Dict[str, Any]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    out_path = STATE_DIR / "reconcile_plans.jsonl"
    with out_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record))
        f.write("\n")


def main() -> None:
    load_dotenv(ROOT / ".env", override=True)
    supabase = SupabaseClient.from_env()
    neo4j = Neo4jClient.from_env()

    for cfg_path in CONFIG_DIR.glob("*.yaml"):
        with cfg_path.open("r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        schema = config.get("schema", "public")
        tables = config.get("tables", {})
        sync_cfg = config.get("sync", {})
        state_file = sync_cfg.get("state_file", ".sync_state.json")
        state = _load_state(STATE_DIR / state_file)

        for table_name, table_cfg in tables.items():
            label = table_cfg.get("label")
            if not label:
                continue
            primary_key = table_cfg.get("primary_key")
            if not primary_key:
                continue

            source_count = supabase.count_rows(schema, table_name, primary_key)
            target_count = neo4j.count_nodes(label)

            sample_limit = int(sync_cfg.get("reconcile_sample_size", 200))
            source_ids = supabase.fetch_sample_ids(schema, table_name, primary_key, sample_limit)
            target_ids = neo4j.fetch_sample_ids(label, primary_key, sample_limit)

            checksums = {
                "source": _hash_values(source_ids),
                "target": _hash_values(target_ids),
            }

            last_checkpoint = state.get(table_name)
            payload = {
                "entity": table_name,
                "source_count": source_count,
                "target_count": target_count,
                "last_checkpoint": last_checkpoint,
                "sampling_window": "1h",
                "checksums": checksums,
            }
            response = propose_reconciliation(payload)
            if not response:
                continue

            record = {
                "table": table_name,
                "label": label,
                "source_count": source_count,
                "target_count": target_count,
                "checksums": checksums,
                "agent_response": response,
                "recorded_at": datetime.now(timezone.utc).isoformat(),
            }
            if response.get("action") == "backfill":
                _append_plan(record)
                LOG.warning("reconciliation drift detected", extra=record)
            else:
                LOG.info("reconciliation check ok", extra=record)

    neo4j.close()


if __name__ == "__main__":
    main()
