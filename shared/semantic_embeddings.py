"""Semantic embedding helpers using Neo4j ai.text.embed (Cypher 25)."""

from __future__ import annotations

import json
import re
import os
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import yaml

from shared.logging import get_logger
from shared.neo4j_client import Neo4jClient

LOG = get_logger(__name__)
ROOT = Path(__file__).resolve().parents[1]

# Rate limit settings
RATE_LIMIT_DELAY_SECONDS = float(os.getenv("EMBEDDING_DELAY_SECONDS", "15"))
RATE_LIMIT_MAX_RETRIES = int(os.getenv("EMBEDDING_MAX_RETRIES", "5"))
RATE_LIMIT_BACKOFF_BASE = float(os.getenv("EMBEDDING_BACKOFF_BASE", "30"))


def load_embedding_config(path: Optional[Path] = None) -> Dict[str, Any]:
    cfg_path = path or (ROOT / "config" / "embedding_config.yaml")
    with cfg_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _cypher_prop(prop: str) -> str:
    safe = prop.replace("`", "``")
    return f"`{safe}`"


def _stringify_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple, set)):
        parts = [_stringify_value(item) for item in value]
        return " ".join(part for part in parts if part)
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=True, separators=(",", ":"))
    return str(value)


def build_text_from_node(node: Dict[str, Any], properties: List[str], separator: str) -> str:
    parts: List[str] = []
    for prop in properties:
        value = _stringify_value(node.get(prop)).strip()
        if value:
            parts.append(value)
    return separator.join(parts).strip()


def get_semantic_rules(config: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    semantic = config.get("semantic", {})
    return dict(semantic.get("label_text_rules", {}) or {})


def iter_label_ids(
    neo4j: Neo4jClient,
    label: str,
    *,
    id_property: str = "id",
    write_property: str = "semanticEmbedding",
    batch_size: int = 500,
    only_missing: bool = True,
) -> Iterable[List[Any]]:
    last_id: Any = None
    id_prop = _cypher_prop(id_property)
    write_prop = _cypher_prop(write_property)

    while True:
        where_parts = [f"n.{id_prop} IS NOT NULL"]
        if only_missing:
            where_parts.append(f"n.{write_prop} IS NULL")
        if last_id is not None:
            where_parts.append(f"n.{id_prop} > $last_id")

        cypher = f"""
        MATCH (n:{label})
        WHERE {' AND '.join(where_parts)}
        RETURN n.{id_prop} AS id
        ORDER BY id
        LIMIT $limit
        """
        rows = neo4j.query(cypher, {"last_id": last_id, "limit": batch_size})
        ids = [row.get("id") for row in rows if row.get("id") is not None]
        if not ids:
            break
        yield ids
        last_id = ids[-1]


def fetch_nodes_by_ids(
    neo4j: Neo4jClient,
    label: str,
    node_ids: List[Any],
    *,
    id_property: str = "id",
    properties: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    if not node_ids:
        return []
    props = list(properties or [])
    if id_property not in props:
        props.append(id_property)
    fields = [f"n.{_cypher_prop(id_property)} AS id"]
    for prop in props:
        if prop == id_property:
            continue
        fields.append(f"n.{_cypher_prop(prop)} AS {_cypher_prop(prop)}")

    cypher = f"""
    MATCH (n:{label})
    WHERE n.{_cypher_prop(id_property)} IN $ids
    RETURN {", ".join(fields)}
    """
    rows = neo4j.query(cypher, {"ids": node_ids})
    return rows


def prepare_semantic_rows(
    neo4j: Neo4jClient,
    label: str,
    node_ids: List[Any],
    *,
    rule: Dict[str, Any],
    id_property: str = "id",
) -> List[Dict[str, Any]]:
    properties = list(rule.get("properties") or [])
    separator = rule.get("separator", " ")
    rows = fetch_nodes_by_ids(
        neo4j,
        label,
        node_ids,
        id_property=id_property,
        properties=properties,
    )

    output: List[Dict[str, Any]] = []
    for row in rows:
        node = {prop: row.get(prop) for prop in properties}
        text = build_text_from_node(node, properties, separator)
        if not text:
            continue
        output.append({"id": row.get("id"), "text": text})
    return output


def _resolve_embedding_params(
    provider: Optional[str],
    model: Optional[str],
    token: Optional[str],
) -> tuple[str, str, str]:
    resolved_provider = provider or os.getenv("EMBEDDING_PROVIDER", "openai")
    resolved_model = model or os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")
    resolved_token = (
        token
        or os.getenv("EMBEDDING_API_TOKEN")
        or os.getenv("LITELLM_API_KEY")
        or os.getenv("OPENAI_API_KEY")
    )
    if not resolved_token:
        raise ValueError(
            "missing embedding token; set EMBEDDING_API_TOKEN, LITELLM_API_KEY, or OPENAI_API_KEY"
        )
    return resolved_provider, resolved_model, resolved_token


def _is_rate_limit_error(exc: Exception) -> bool:
    """Check if exception is a rate limit (429) error."""
    msg = str(exc).lower()
    return "429" in msg or "rate limit" in msg


def _write_single_embedding(
    neo4j: Neo4jClient,
    label: str,
    row: Dict[str, Any],
    *,
    id_property: str,
    write_property: str,
    provider_val: str,
    model_val: str,
    token_val: str,
) -> bool:
    """Write embedding for a single node. Returns True if successful."""
    cypher = f"""CYPHER 25
    MATCH (n:{label})
    WHERE n.{_cypher_prop(id_property)} = $node_id
    WITH n
    WHERE $text IS NOT NULL AND $text <> ''
    WITH n, ai.text.embed($text, $provider, {{token: $token, model: $model}}) AS embedding
    SET n.{_cypher_prop(write_property)} = embedding
    RETURN count(n) AS updated
    """
    result = neo4j.query(
        cypher,
        {
            "node_id": row["id"],
            "text": row["text"],
            "provider": provider_val,
            "model": model_val,
            "token": token_val,
        },
    )
    return bool(result and result[0].get("updated", 0))


def write_semantic_embeddings(
    neo4j: Neo4jClient,
    label: str,
    rows: List[Dict[str, Any]],
    *,
    id_property: str = "id",
    write_property: str = "semanticEmbedding",
    provider: Optional[str] = None,
    model: Optional[str] = None,
    token: Optional[str] = None,
) -> int:
    if not rows:
        return 0
    provider_val, model_val, token_val = _resolve_embedding_params(provider, model, token)

    updated = 0
    for i, row in enumerate(rows):
        retries = 0
        while retries <= RATE_LIMIT_MAX_RETRIES:
            try:
                if _write_single_embedding(
                    neo4j,
                    label,
                    row,
                    id_property=id_property,
                    write_property=write_property,
                    provider_val=provider_val,
                    model_val=model_val,
                    token_val=token_val,
                ):
                    updated += 1
                break
            except Exception as exc:
                if _is_rate_limit_error(exc):
                    retries += 1
                    if retries > RATE_LIMIT_MAX_RETRIES:
                        LOG.error(
                            "max retries exceeded for rate limit",
                            extra={"label": label, "node_id": row.get("id"), "retries": retries},
                        )
                        raise
                    backoff = RATE_LIMIT_BACKOFF_BASE * (2 ** (retries - 1))
                    LOG.warning(
                        "rate limit hit, backing off",
                        extra={"backoff_seconds": backoff, "retry": retries},
                    )
                    time.sleep(backoff)
                else:
                    raise

        if i < len(rows) - 1:
            time.sleep(RATE_LIMIT_DELAY_SECONDS)

    return updated


def normalize_label_name(label: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "", label)
