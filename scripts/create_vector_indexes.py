"""Create Neo4j vector and full-text indexes used by ingestion/search."""

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
    rows = neo4j.query("SHOW INDEXES YIELD name RETURN name")
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


def _create_fulltext_index(
    neo4j: Neo4jClient,
    *,
    name: str,
    label: str,
    properties: List[str],
) -> None:
    props = ", ".join(f"n.{_cypher_prop(prop)}" for prop in properties)
    cypher = f"""
    CREATE FULLTEXT INDEX {name}
    FOR (n:{label})
    ON EACH [{props}]
    """
    neo4j.execute(cypher)


def _fulltext_index_name(label: str, properties: List[str]) -> str:
    joined_props = "_".join(_sanitize_name(prop) for prop in properties)
    return f"ft_{_sanitize_name(label)}_{joined_props}"


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


def _iter_fulltext_index_defs(config: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    for item in config.get("fulltext_indexes", []) or []:
        label = item["label"]
        properties = [str(prop) for prop in item.get("properties", []) if prop]
        if not properties:
            continue
        name = item.get("name") or _fulltext_index_name(label, properties)
        yield {
            "name": name,
            "label": label,
            "properties": properties,
        }


def main() -> int:
    load_dotenv(ROOT / ".env", override=True)
    config = load_embedding_config()
    neo4j = Neo4jClient.from_env()
    try:
        neo4j.verify_auth()
        existing = _fetch_index_names(neo4j)
        vector_created = 0
        fulltext_created = 0
        for item in _iter_index_defs(config):
            idx_name = _index_name(item["label"], item["property"])
            if idx_name in existing:
                LOG.info("vector index exists", extra={"index_name": idx_name})
                continue
            _create_index(
                neo4j,
                name=idx_name,
                label=item["label"],
                prop=item["property"],
                dimensions=item["dimensions"],
                similarity=item["similarity"],
            )
            LOG.info(
                "vector index created",
                extra={"index_name": idx_name, "label": item["label"], "property": item["property"]},
            )
            vector_created += 1
            existing.add(idx_name)

        for item in _iter_fulltext_index_defs(config):
            idx_name = item["name"]
            if idx_name in existing:
                LOG.info("fulltext index exists", extra={"index_name": idx_name})
                continue
            _create_fulltext_index(
                neo4j,
                name=idx_name,
                label=item["label"],
                properties=item["properties"],
            )
            LOG.info(
                "fulltext index created",
                extra={"index_name": idx_name, "label": item["label"], "properties": item["properties"]},
            )
            fulltext_created += 1
            existing.add(idx_name)

        LOG.info(
            "index creation complete",
            extra={"vector_indexes_created": vector_created, "fulltext_indexes_created": fulltext_created},
        )
        return 0
    finally:
        neo4j.close()


if __name__ == "__main__":
    raise SystemExit(main())
