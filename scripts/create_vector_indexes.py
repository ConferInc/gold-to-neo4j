"""Create Neo4j vector indexes for semantic and structural embeddings."""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List

# Add project root for imports
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
    rows = neo4j.query("CALL db.indexes() YIELD name RETURN name")
    return {row.get("name") for row in rows if row.get("name")}


def _create_index(
    neo4j: Neo4jClient,
    *,
    name: str,
    label: str,
    prop: str,
    dimensions: int,
    similarity: str,
) -> None:
    cypher = f"""
    CREATE VECTOR INDEX {name}
    FOR (n:{label})
    ON (n.{_cypher_prop(prop)})
    OPTIONS {{
      indexConfig: {{
        `vector.dimensions`: $dimensions,
        `vector.similarity_function`: $similarity
      }}
    }}
    """
    neo4j.execute(cypher, {"dimensions": dimensions, "similarity": similarity})


def _iter_index_defs(config: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    vector_cfg = config.get("vector_indexes", {}) or {}
    for section in ("semantic", "structural"):
        for item in vector_cfg.get(section, []) or []:
            yield {
                "label": item["label"],
                "property": item["property"],
                "dimensions": int(item["dimensions"]),
                "similarity": item.get("similarity_function", "cosine"),
            }


def main() -> int:
    load_dotenv(ROOT / ".env", override=True)
    config = load_embedding_config()
    neo4j = Neo4jClient.from_env()
    try:
        neo4j.verify_auth()
        existing = _fetch_index_names(neo4j)
        created = 0
        for item in _iter_index_defs(config):
            name = _index_name(item["label"], item["property"])
            if name in existing:
                LOG.info("vector index exists", extra={"name": name})
                continue
            _create_index(
                neo4j,
                name=name,
                label=item["label"],
                prop=item["property"],
                dimensions=item["dimensions"],
                similarity=item["similarity"],
            )
            LOG.info(
                "vector index created",
                extra={"name": name, "label": item["label"], "property": item["property"]},
            )
            created += 1
        LOG.info("vector index creation complete", extra={"created": created})
        return 0
    finally:
        neo4j.close()


if __name__ == "__main__":
    raise SystemExit(main())
