"""Shared upsert helpers for Neo4j."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, List

import yaml

from shared.logging import get_logger

LOG = get_logger(__name__)

_CONFIG_ROOT = Path(__file__).resolve().parents[1] / "config"


@lru_cache(maxsize=4)
def _load_config(config_name: str) -> Dict[str, Any]:
    """Load and cache a YAML config file."""
    path = _CONFIG_ROOT / config_name
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _chunk_rows(rows: List[Dict[str, Any]], chunk_size: int) -> Iterable[List[Dict[str, Any]]]:
    if chunk_size <= 0:
        chunk_size = len(rows) or 1
    for idx in range(0, len(rows), chunk_size):
        yield rows[idx : idx + chunk_size]


def upsert_event(event_type: str, payload: Dict[str, Any], neo4j) -> None:
    """
    Config-driven realtime upsert.

    event_type format: "{table_name}.{insert|update|delete}"
    payload: the full row from gold.outbox_events.payload (jsonb)
    """
    parts = event_type.rsplit(".", 1)
    if len(parts) != 2:
        LOG.warning(
            "malformed event_type, expected 'table.op'",
            extra={"event_type": event_type},
        )
        return

    table_name, operation = parts
    config = _load_config("customers.yaml")
    table_cfg = config.get("tables", {}).get(table_name)

    if not table_cfg:
        LOG.warning("no config for table", extra={"table_name": table_name})
        return

    label = table_cfg.get("label")
    pk = table_cfg.get("primary_key", "id")
    pk_val = payload.get(pk)

    if pk_val is None:
        LOG.warning(
            "missing primary key in payload",
            extra={"table_name": table_name, "pk": pk},
        )
        return

    # ── Node tables (have a label, not skip_upsert) ──
    if label and not table_cfg.get("skip_upsert"):
        if operation == "delete":
            neo4j.execute(
                f"MATCH (n:{label} {{{pk}: $pk_val}}) DETACH DELETE n",
                {"pk_val": pk_val},
            )
        else:
            # Build MERGE + SET from configured columns
            columns = table_cfg.get("columns", list(payload.keys()))
            row = {col: payload.get(col) for col in columns}
            set_parts = [f"n.{col} = row.{col}" for col in columns if col != pk]
            set_clause = ", ".join(set_parts) if set_parts else "n._updated = true"
            neo4j.execute(
                f"MERGE (n:{label} {{{pk}: row.{pk}}}) SET {set_clause}",
                {"row": row},
            )

        LOG.info(
            "upsert_event node",
            extra={"label": label, "op": operation, "pk": pk_val},
        )

    # ── Join/relationship tables (skip_upsert: true) ──
    elif table_cfg.get("skip_upsert"):
        _upsert_relationships_for_event(
            table_name, operation, payload, config, neo4j,
        )

    else:
        LOG.debug(
            "table has no label and is not skip_upsert, ignoring",
            extra={"table_name": table_name},
        )


def _upsert_relationships_for_event(
    table_name: str,
    operation: str,
    payload: Dict[str, Any],
    config: Dict[str, Any],
    neo4j,
) -> None:
    """Find relationships using this join table and create/delete edges."""
    tables_cfg = config.get("tables", {})

    for rel in config.get("relationships", []):
        if rel.get("join_table") != table_name:
            continue

        rel_type = rel["type"]
        src_tbl = rel["source_table"]
        tgt_tbl = rel["target_table"]
        from_label = tables_cfg[src_tbl]["label"]
        to_label = tables_cfg[tgt_tbl]["label"]
        from_pk = tables_cfg[src_tbl]["primary_key"]
        to_pk = tables_cfg[tgt_tbl]["primary_key"]
        from_val = payload.get(rel["join_source_key"])
        to_val = payload.get(rel["join_target_key"])

        # Apply filter conditions (e.g. interaction_type: viewed)
        filters = rel.get("filters", {})
        if filters and not all(payload.get(k) == v for k, v in filters.items()):
            continue

        if from_val is None or to_val is None:
            continue

        if operation == "delete":
            neo4j.execute(
                f"MATCH (a:{from_label} {{{from_pk}: $fv}})"
                f"-[r:{rel_type}]->"
                f"(b:{to_label} {{{to_pk}: $tv}}) DELETE r",
                {"fv": from_val, "tv": to_val},
            )
        else:
            neo4j.execute(
                f"MATCH (a:{from_label} {{{from_pk}: $fv}}) "
                f"MATCH (b:{to_label} {{{to_pk}: $tv}}) "
                f"MERGE (a)-[:{rel_type}]->(b)",
                {"fv": from_val, "tv": to_val},
            )

        LOG.info(
            "upsert_event rel",
            extra={
                "rel_type": rel_type, "op": operation,
                "from": from_val, "to": to_val,
            },
        )


def _apply_filters(rows: Iterable[Dict[str, Any]], filters: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not filters:
        return list(rows)
    filtered: List[Dict[str, Any]] = []
    for row in rows:
        match = True
        for key, value in filters.items():
            if row.get(key) != value:
                match = False
                break
        if match:
            filtered.append(row)
    return filtered


def _build_node_rows(
    source_rows: Iterable[Dict[str, Any]],
    key_field: str,
    columns: List[str],
    include_extra_fields: bool,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    skipped_invalid = 0
    for row in source_rows:
        if key_field not in row or row.get(key_field) is None:
            skipped_invalid += 1
            continue
        if columns:
            mapped = {col: row.get(col) for col in columns}
        else:
            mapped = dict(row)

        if key_field not in mapped or mapped.get(key_field) is None:
            mapped[key_field] = row.get(key_field)
        if mapped.get(key_field) is None:
            skipped_invalid += 1
            continue

        if include_extra_fields:
            for key, value in row.items():
                if key not in mapped:
                    mapped[key] = value

        rows.append(mapped)
    if skipped_invalid:
        LOG.warning(
            "skipped rows with missing/null primary key before neo4j upsert",
            extra={"primary_key": key_field, "skipped_count": skipped_invalid},
        )
    return rows


def _build_rel_rows(
    source_rows: Iterable[Dict[str, Any]],
    from_source_key: str,
    to_source_key: str,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for row in source_rows:
        from_val = row.get(from_source_key)
        to_val = row.get(to_source_key)
        if from_val is None or to_val is None:
            continue
        rows.append({"from_key": from_val, "to_key": to_val})
    return rows


def upsert_from_config(config: Dict[str, Any], data: Dict[str, List[Dict[str, Any]]], neo4j) -> None:
    """
    Generic config-driven upsert for schema:
      tables:
        table_name:
          label: Ingredient
          primary_key: id
          columns: [id, name, ...]
      relationships:
        - type: HAS_NUTRITION_VALUE
          source_table: ingredients
          target_table: nutrition_facts
          join_table: nutrition_facts
          join_source_key: entity_id
          join_target_key: id
    """
    sync_cfg = config.get("sync", {})
    write_chunk_size = int(sync_cfg.get("neo4j_write_chunk_size", sync_cfg.get("page_size", 1000)))
    tables = config.get("tables", {})
    relationships = config.get("relationships", [])

    for table_name, table_cfg in tables.items():
        if table_cfg.get("skip_upsert", False):
            continue
        label = table_cfg["label"]
        key_field = table_cfg["primary_key"]
        columns = table_cfg.get("columns", [])
        include_extra_fields = bool(table_cfg.get("include_extra_fields", True))
        table_filters = table_cfg.get("filters", {})

        source_rows = data.get(table_name, [])
        source_rows = _apply_filters(source_rows, table_filters)
        node_rows = _build_node_rows(source_rows, key_field, columns, include_extra_fields)

        LOG.info("upserting nodes label=%s count=%s", label, len(node_rows))
        if not node_rows:
            continue

        for chunk in _chunk_rows(node_rows, write_chunk_size):
            neo4j.execute_many(
                f"""
                UNWIND $rows AS row
                MERGE (n:{label} {{{key_field}: row.{key_field}}})
                SET n += row
                """,
                chunk,
            )

    for rel in relationships:
        rel_type = rel["type"]
        source_table = rel["source_table"]
        target_table = rel["target_table"]
        join_table = rel["join_table"]
        join_source_key = rel["join_source_key"]
        join_target_key = rel["join_target_key"]
        rel_filters = rel.get("filters", {})

        from_label = tables[source_table]["label"]
        to_label = tables[target_table]["label"]
        from_node_key = tables[source_table]["primary_key"]
        to_node_key = tables[target_table]["primary_key"]

        source_rows = data.get(join_table, [])
        source_rows = _apply_filters(source_rows, rel_filters)
        rel_rows = _build_rel_rows(source_rows, join_source_key, join_target_key)

        LOG.info("upserting relationships type=%s count=%s", rel_type, len(rel_rows))
        if not rel_rows:
            continue

        for chunk in _chunk_rows(rel_rows, write_chunk_size):
            neo4j.execute_many(
                f"""
                UNWIND $rows AS row
                MATCH (a:{from_label} {{{from_node_key}: row.from_key}})
                MATCH (b:{to_label} {{{to_node_key}: row.to_key}})
                MERGE (a)-[:{rel_type}]->(b)
                """,
                chunk,
            )


def upsert_batch(layer: str, rows: Iterable[Dict[str, Any]], neo4j) -> None:
    """Upsert a batch of rows into Neo4j."""
    batch: List[Dict[str, Any]] = list(rows)
    LOG.info("upsert_batch", extra={"layer": layer, "count": len(batch)})
    _ = batch
    _ = neo4j
