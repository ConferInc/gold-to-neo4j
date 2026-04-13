"""Reset all embeddings and vector indexes. Run before re-training GraphSAGE and semantic embeddings."""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

from shared.logging import get_logger
from shared.neo4j_client import Neo4jClient
from shared.semantic_embeddings import load_embedding_config

LOG = get_logger(__name__)


def _sanitize_name(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_]", "_", value)
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned.lower()


def _index_name(label: str, prop: str) -> str:
    return f"vec_{_sanitize_name(label)}_{_sanitize_name(prop)}"


def _cypher_prop(prop: str) -> str:
    safe = prop.replace("`", "``")
    return f"`{safe}`"


def _fetch_index_names(neo4j: Neo4jClient) -> set[str]:
    rows = neo4j.query("SHOW INDEXES YIELD name RETURN name")
    return {row.get("name") for row in rows if row.get("name")}


def drop_vector_indexes(neo4j: Neo4jClient, config: dict) -> int:
    """Drop all vector indexes from config. Returns count dropped."""
    vector_cfg = config.get("vector_indexes", {}) or {}
    existing = _fetch_index_names(neo4j)
    dropped = 0
    for section in ("semantic", "structural"):
        for item in vector_cfg.get(section, []) or []:
            idx_name = _index_name(item["label"], item["property"])
            if idx_name in existing:
                try:
                    neo4j.execute(f"DROP INDEX {idx_name} IF EXISTS")
                    LOG.info("dropped vector index", extra={"index_name": idx_name})
                    dropped += 1
                except Exception as e:
                    LOG.warning("could not drop index", extra={"index_name": idx_name, "error": str(e)})
    return dropped


def remove_embedding_properties(neo4j: Neo4jClient, config: dict) -> int:
    """Remove graphSageEmbedding and semanticEmbedding from all configured labels."""
    vector_cfg = config.get("vector_indexes", {}) or {}
    labels_props: dict[str, set[str]] = {}
    for section in ("semantic", "structural"):
        default_prop = "semanticEmbedding" if section == "semantic" else "graphSageEmbedding"
        for item in vector_cfg.get(section, []) or []:
            label = item["label"]
            p = item.get("property", default_prop)
            labels_props.setdefault(label, set()).add(p)

    total_removed = 0
    for label, props in labels_props.items():
        for prop in props:
            prop_cypher = _cypher_prop(prop)
            cypher = f"""
            MATCH (n:{label})
            WHERE n.{prop_cypher} IS NOT NULL
            WITH n
            REMOVE n.{prop_cypher}
            RETURN count(n) AS removed
            """
            try:
                result = neo4j.query(cypher, {})
                count = result[0]["removed"] if result else 0
                total_removed += count
                LOG.info("removed embedding property", extra={"label": label, "property": prop, "count": count})
            except Exception as e:
                LOG.warning("could not remove property", extra={"label": label, "property": prop, "error": str(e)})
    return total_removed


def main() -> int:
    load_dotenv(ROOT / ".env", override=True)
    config = load_embedding_config()
    neo4j = Neo4jClient.from_env()
    try:
        neo4j.verify_auth()

        LOG.info("dropping vector indexes")
        dropped = drop_vector_indexes(neo4j, config)
        LOG.info("removing embedding properties from nodes")
        removed = remove_embedding_properties(neo4j, config)

        LOG.info(
            "reset complete",
            extra={"indexes_dropped": dropped, "embedding_properties_removed": removed},
        )
        print("\nNext steps:")
        print("  1. python scripts/initial_setup_graphsage.py")
        print("  2. python scripts/initial_setup_semantic_embeddings.py")
        print("  3. python scripts/create_vector_indexes.py")
        return 0
    finally:
        neo4j.close()


if __name__ == "__main__":
    raise SystemExit(main())
