"""Scheduled batch ingestion for products."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import yaml
from dotenv import load_dotenv

from shared.logging import get_logger
from shared.supabase_client import SupabaseClient
from shared.neo4j_client import Neo4jClient, is_auth_error, is_non_retryable_write_error
from shared.upsert import upsert_from_config
from shared.run_summary import append_run_summary
from shared.schema_validation import build_table_plan, normalize_rows

LOG = get_logger(__name__)

ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT / "config" / "products.yaml"


def load_config() -> Dict[str, Any]:
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_state(state_path: Path) -> Dict[str, Any]:
    if not state_path.exists():
        return {}
    with state_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state_path: Path, state: Dict[str, Any]) -> None:
    with state_path.open("w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def _max_checkpoint(state: Dict[str, Any]) -> str | None:
    max_val = None
    for val in state.values():
        if val and (max_val is None or val > max_val):
            max_val = val
    return max_val


def main() -> None:
    started_at = datetime.now(timezone.utc)
    summary: Dict[str, Any] = {
        "layer": "products",
        "started_at": started_at.isoformat(),
        "status": "unknown",
        "tables": {},
    }

    config: Dict[str, Any] = {}
    state: Dict[str, Any] = {}
    state_path = ROOT / "state" / ".sync_state.json"
    before_state: Dict[str, Any] = {}

    try:
        load_dotenv(ROOT / ".env", override=True)
        config = load_config()
        sync_cfg = config.get("sync", {})
        state_path = ROOT / "state" / sync_cfg.get("state_file", ".sync_state.json")
        summary["state_file"] = state_path.name
        summary["state_path"] = str(state_path)
        state = load_state(state_path)
        before_state = dict(state)

        supabase = SupabaseClient.from_env()
        neo4j = Neo4jClient.from_env()
        neo4j.verify_auth()

        schema = config["schema"]
        batch_size = int(sync_cfg.get("page_size", 1000))

        tables_cfg = config.get("tables", {})
        data: Dict[str, List[Dict[str, Any]]] = {}

        for table_name, table_cfg in tables_cfg.items():
            plan = build_table_plan(supabase, schema, table_name, table_cfg)
            columns = table_cfg.get("columns", [])
            filters = plan.filters
            updated_at = plan.updated_at_column

            last_state = state.get(table_name)
            rows = supabase.fetch_all(
                schema,
                table_name,
                columns=plan.select_columns if columns else None,
                batch_size=batch_size,
                updated_at_column=updated_at,
                updated_since=last_state,
                filters=filters,
            )
            normalize_rows(rows, plan.alias_map)
            data[table_name] = rows
            summary["tables"][table_name] = {"rows_fetched": len(rows)}

        # Enrichments
        for table_name, table_cfg in tables_cfg.items():
            enrich_cfg = table_cfg.get("enrich", {})
            if not enrich_cfg:
                continue
            for field_name, enrich in enrich_cfg.items():
                from_table = enrich["from_table"]
                source_key = enrich["source_key"]
                lookup_key = enrich["lookup_key"]
                value_field = enrich["value_field"]

                lookup_rows = data.get(from_table, [])
                lookup_map = {row.get(source_key): row.get(value_field) for row in lookup_rows}
                for row in data.get(table_name, []):
                    row[field_name] = lookup_map.get(row.get(lookup_key))

        upsert_from_config(config, data, neo4j)

        # Update checkpoints for each table based on max updated_at in fetched rows.
        for table_name, table_cfg in tables_cfg.items():
            updated_at_col = table_cfg.get("updated_at")
            if not updated_at_col:
                continue
            rows = data.get(table_name, [])
            max_val = None
            for row in rows:
                val = row.get(updated_at_col)
                if val and (max_val is None or val > max_val):
                    max_val = val
            if max_val is None:
                max_val = datetime.now(timezone.utc).isoformat()
            state[table_name] = max_val

        save_state(state_path, state)

        neo4j.close()
        LOG.info("products_batch completed")
        summary["status"] = "success"
    except Exception as exc:
        if is_auth_error(exc):
            summary["error_type"] = "neo4j_auth_failed"
        elif is_non_retryable_write_error(exc):
            summary["error_type"] = "neo4j_write_validation_failed"
        summary["status"] = "failed"
        summary["error"] = str(exc)
        raise
    finally:
        summary["checkpoint_before"] = _max_checkpoint(before_state)
        summary["checkpoint_after"] = _max_checkpoint(state)
        finished_at = datetime.now(timezone.utc)
        summary["finished_at"] = finished_at.isoformat()
        summary["duration_ms"] = int((finished_at - started_at).total_seconds() * 1000)
        append_run_summary("products", summary, ROOT)


if __name__ == "__main__":
    main()
