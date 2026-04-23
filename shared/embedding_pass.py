"""Shared embedding pass — runs semantic embedding generation after batch sync.

Called by each batch service (recipes, customers, ingredients, products) after
the Neo4j transaction commits.  Best-effort: failures are logged but never
propagate to the caller.

Usage::

    from shared.embedding_pass import run_embedding_pass

    # After neo4j tx committed:
    run_embedding_pass(config, data, neo4j, layer="customers")
"""

from __future__ import annotations

import os
from typing import Any, Dict, List

from shared.logging import get_logger

LOG = get_logger(__name__)


def run_embedding_pass(
    config: Dict[str, Any],
    data: Dict[str, List[Dict[str, Any]]],
    neo4j,
    *,
    layer: str = "unknown",
) -> Dict[str, Any]:
    """Generate semantic embeddings for nodes that were just upserted.

    Parameters
    ----------
    config : dict
        The layer's YAML config (e.g. customers.yaml parsed).
    data : dict
        Table-name → rows mapping from the batch fetch.  Each row is a dict
        with the node's properties (already normalised/enriched).
    neo4j : Neo4jClient
        Active Neo4j client (connection still open, post-commit).
    layer : str
        Layer name for logging (e.g. "customers", "recipes").

    Returns
    -------
    dict
        Summary with ``labels_processed``, ``total_embedded``, ``errors``.
    """
    if os.getenv("EMBEDDING_BATCH_ENABLED", "true").lower() != "true":
        LOG.info(
            "batch_embedding_disabled",
            extra={"layer": layer},
        )
        return {"labels_processed": 0, "total_embedded": 0, "skipped": "disabled"}

    summary: Dict[str, Any] = {"labels_processed": 0, "total_embedded": 0, "errors": []}

    try:
        from shared.semantic_embeddings import (
            load_embedding_config,
            get_semantic_rules,
            build_text_from_node,
            write_semantic_embeddings,
        )

        emb_config = load_embedding_config()
        rules = get_semantic_rules(emb_config)

        if not rules:
            LOG.info("no_semantic_rules_found", extra={"layer": layer})
            return summary

        # Read configured write property (default: semanticEmbedding)
        write_property = emb_config.get("semantic", {}).get(
            "write_property", "semanticEmbedding"
        )

        tables_cfg = config.get("tables", {})

        for table_name, rows in data.items():
            if not rows:
                continue

            table_cfg = tables_cfg.get(table_name, {})
            label = table_cfg.get("label")

            # Skip join tables (no label / skip_upsert)
            if not label or table_cfg.get("skip_upsert"):
                continue

            rule = rules.get(label)
            if not rule:
                continue

            properties = list(rule.get("properties") or [])
            if not properties:
                continue  # e.g. MealLog with empty properties list

            separator = rule.get("separator", " ")
            pk = table_cfg.get("primary_key", "id")

            # Build text from in-memory rows (no re-fetch needed)
            embed_rows: List[Dict[str, Any]] = []
            for row in rows:
                node_id = row.get(pk)
                if not node_id:
                    continue
                text = build_text_from_node(row, properties, separator)
                if text:
                    embed_rows.append({"id": node_id, "text": text})

            if not embed_rows:
                continue

            try:
                count = write_semantic_embeddings(
                    neo4j, label, embed_rows,
                    write_property=write_property,
                )
                summary["labels_processed"] += 1
                summary["total_embedded"] += count
                LOG.info(
                    "batch_embedding_written",
                    extra={
                        "layer": layer,
                        "label": label,
                        "nodes_embedded": count,
                        "nodes_attempted": len(embed_rows),
                    },
                )
            except Exception as exc:
                summary["errors"].append({"label": label, "error": str(exc)})
                LOG.warning(
                    "batch_embedding_failed",
                    extra={"layer": layer, "label": label, "error": str(exc)},
                )

    except Exception as exc:
        summary["errors"].append({"phase": "setup", "error": str(exc)})
        LOG.warning(
            "embedding_pass_setup_failed",
            extra={"layer": layer, "error": str(exc)},
        )

    return summary
