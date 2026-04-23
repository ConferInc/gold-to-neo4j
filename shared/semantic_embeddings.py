"""Semantic embedding helpers. Generates embeddings in Python (LiteLLM) and writes to Neo4j."""

from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import yaml

from dotenv import load_dotenv

from shared.logging import get_logger
from shared.neo4j_client import Neo4jClient

try:
    from litellm import embedding as litellm_embedding
except ImportError:
    litellm_embedding = None  # type: ignore

try:
    import httpx
except ImportError:
    httpx = None  # type: ignore

LOG = get_logger(__name__)
ROOT = Path(__file__).resolve().parents[1]


def _read_base_url_from_env_file() -> Optional[str]:
    """Read LITELLM_BASE_URL or OPENAI_API_BASE directly from .env file."""
    for p in [ROOT / ".env", Path.cwd() / ".env"]:
        if not p.exists():
            continue
        try:
            raw = p.read_text(encoding="utf-8", errors="replace")
            for line in raw.splitlines():
                line = line.strip()
                if line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                k, v = k.strip(), v.strip().strip('"').strip("'")
                if k == "LITELLM_BASE_URL" and v:
                    return v
                if k == "OPENAI_API_BASE" and v:
                    return v
        except OSError:
            pass
    return None


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


def _embed_via_http(
    base_url: str,
    model: str,
    api_key: str,
    texts: List[str],
) -> List[List[float]]:
    """Call embedding API directly via HTTP (bypasses LiteLLM routing)."""
    if httpx is None:
        raise ImportError("httpx is required for direct HTTP embeddings; pip install httpx")

    url = base_url.rstrip("/") + "/embeddings"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    payload = {"model": model, "input": texts}

    with httpx.Client(timeout=60.0) as client:
        resp = client.post(url, json=payload, headers=headers)
        resp.raise_for_status()
    data = resp.json()

    items = data.get("data") or []
    index_to_embedding: Dict[int, List[float]] = {}
    for item in items:
        idx = item.get("index") if isinstance(item, dict) else getattr(item, "index", None)
        emb = item.get("embedding") if isinstance(item, dict) else getattr(item, "embedding", None)
        if idx is not None and emb is not None:
            index_to_embedding[int(idx)] = list(emb)

    embeddings: List[List[float]] = []
    for i in range(len(texts)):
        emb = index_to_embedding.get(i)
        if emb is None:
            LOG.warning(
                "embedding API returned no vector for index",
                extra={"index": i, "text_preview": (texts[i][:100] if texts[i] else "")[:100]},
            )
            # Return None as placeholder; caller will filter out
            embeddings.append(None)  # type: ignore
        else:
            embeddings.append(emb)
    return embeddings


def embed_texts_python(
    texts: List[str],
    *,
    model: Optional[str] = None,
    api_key: Optional[str] = None,
    api_base: Optional[str] = None,
) -> List[List[float]]:
    """
    Generate embeddings for texts. When LITELLM_BASE_URL is set, uses direct HTTP
    to the LiteLLM proxy (bypasses LiteLLM library routing to api.openai.com).
    """
    if not texts:
        return []

    load_dotenv(ROOT / ".env", override=True)
    load_dotenv(Path.cwd() / ".env", override=True)

    model_val = model or os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")
    api_key_val = api_key or os.getenv("EMBEDDING_API_TOKEN") or os.getenv("LITELLM_API_KEY") or os.getenv("OPENAI_API_KEY")
    if not api_key_val:
        raise ValueError("missing embedding API key; set EMBEDDING_API_TOKEN, LITELLM_API_KEY, or OPENAI_API_KEY")

    api_base_val = (
        api_base
        or os.getenv("LITELLM_BASE_URL")
        or os.getenv("OPENAI_API_BASE")
        or _read_base_url_from_env_file()
    )
    if api_base_val:
        api_base_val = str(api_base_val).strip()

    # When api_base is set, use direct HTTP to LiteLLM proxy (never hit OpenAI directly)
    if api_base_val:
        return _embed_via_http(api_base_val, model_val, api_key_val, texts)

    # Fallback: LiteLLM library (hits OpenAI or configured provider)
    if litellm_embedding is None:
        raise ImportError("litellm is required when LITELLM_BASE_URL is not set; pip install litellm")

    kwargs: Dict[str, Any] = {"model": model_val, "input": texts, "api_key": api_key_val}
    response = litellm_embedding(**kwargs)
    data = getattr(response, "data", None) or []
    index_to_embedding: Dict[int, List[float]] = {}
    for item in data:
        idx = getattr(item, "index", None) if not isinstance(item, dict) else item.get("index")
        emb = getattr(item, "embedding", None) if not isinstance(item, dict) else item.get("embedding")
        if idx is not None and emb is not None:
            index_to_embedding[int(idx)] = list(emb)
    embeddings: List[List[float]] = []
    for i in range(len(texts)):
        emb = index_to_embedding.get(i)
        if emb is None:
            LOG.warning(
                "embedding API returned no vector for index",
                extra={"index": i},
            )
            embeddings.append(None)  # type: ignore
        else:
            embeddings.append(emb)
    return embeddings


def _is_rate_limit_error(exc: Exception) -> bool:
    """Check if exception is a rate limit (429) error."""
    msg = str(exc).lower()
    return "429" in msg or "rate limit" in msg


def _write_embeddings_batch_python(
    neo4j: Neo4jClient,
    label: str,
    rows: List[Dict[str, Any]],
    *,
    id_property: str = "id",
    write_property: str = "semanticEmbedding",
) -> int:
    """Write pre-computed embeddings to Neo4j nodes. rows: [{"id": ..., "embedding": [...]}]."""
    if not rows:
        return 0

    id_prop = _cypher_prop(id_property)
    write_prop = _cypher_prop(write_property)

    write_rows = [{"id": r["id"], "embedding": r["embedding"]} for r in rows if r.get("embedding") is not None]
    if not write_rows:
        return 0

    cypher = f"""
    UNWIND $rows AS row
    MATCH (n:{label})
    WHERE n.{id_prop} = row.id
    SET n.{write_prop} = row.embedding
    """
    neo4j.execute_many(cypher, write_rows)
    return len(write_rows)


# Batch size for embedding API calls (avoids huge single requests)
EMBEDDING_BATCH_SIZE = int(os.getenv("EMBEDDING_BATCH_SIZE", "50"))


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
    api_base: Optional[str] = None,
) -> int:
    """
    Generate embeddings in Python and write to Neo4j nodes.
    Uses LITELLM_BASE_URL for direct HTTP to proxy when set.
    rows: [{"id": ..., "text": ...}]. Returns count of nodes updated.
    """
    if not rows:
        return 0

    load_dotenv(ROOT / ".env", override=True)
    load_dotenv(Path.cwd() / ".env", override=True)

    model_val = model or os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")
    api_key_val = (
        token
        or os.getenv("EMBEDDING_API_TOKEN")
        or os.getenv("LITELLM_API_KEY")
        or os.getenv("OPENAI_API_KEY")
    )
    api_base_val = (
        api_base
        or os.getenv("LITELLM_BASE_URL")
        or os.getenv("OPENAI_API_BASE")
        or _read_base_url_from_env_file()
    )
    if not api_key_val:
        raise ValueError(
            "missing embedding API key; set EMBEDDING_API_TOKEN, LITELLM_API_KEY, or OPENAI_API_KEY"
        )

    total_updated = 0
    batch_size = EMBEDDING_BATCH_SIZE

    for offset in range(0, len(rows), batch_size):
        batch = rows[offset : offset + batch_size]
        texts = [r["text"] for r in batch]

        retries = 0
        while retries <= RATE_LIMIT_MAX_RETRIES:
            try:
                embeddings = embed_texts_python(
                    texts,
                    model=model_val,
                    api_key=api_key_val,
                    api_base=api_base_val,
                )
                for i, row in enumerate(batch):
                    if i < len(embeddings):
                        row["embedding"] = embeddings[i]
                updated = _write_embeddings_batch_python(
                    neo4j,
                    label,
                    batch,
                    id_property=id_property,
                    write_property=write_property,
                )
                total_updated += updated
                break
            except Exception as exc:
                if _is_rate_limit_error(exc):
                    retries += 1
                    if retries > RATE_LIMIT_MAX_RETRIES:
                        LOG.error(
                            "max retries exceeded for rate limit",
                            extra={"label": label, "batch_offset": offset, "retries": retries},
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

        if offset + batch_size < len(rows):
            time.sleep(RATE_LIMIT_DELAY_SECONDS)

    return total_updated


def normalize_label_name(label: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "", label)


def embed_node_inline(
    neo4j: Neo4jClient,
    label: str,
    node_id: Any,
    payload: Dict[str, Any],
    *,
    config: Optional[Dict[str, Any]] = None,
) -> bool:
    """Generate and write a semantic embedding for a single node (best-effort).

    Used by the realtime path to embed a node immediately after upsert.
    The ``payload`` dict contains the node properties from the outbox event,
    so no Neo4j re-fetch is needed.

    Parameters
    ----------
    neo4j : Neo4jClient
        Active Neo4j connection.
    label : str
        The Neo4j node label (e.g. ``"B2C_Customer"``).
    node_id : Any
        Primary key value of the node.
    payload : dict
        Node properties from the outbox event (already in memory).
    config : dict, optional
        Pre-loaded embedding config.  If ``None``, loads from disk.

    Returns
    -------
    bool
        ``True`` if the embedding was written successfully.
    """
    try:
        cfg = config or load_embedding_config()
        rules = get_semantic_rules(cfg)
        rule = rules.get(label)
        if not rule:
            return False

        properties = list(rule.get("properties") or [])
        if not properties:
            return False  # e.g. MealLog with empty properties list

        separator = rule.get("separator", " ")
        text = build_text_from_node(payload, properties, separator)
        if not text:
            return False

        embeddings = embed_texts_python([text])
        if not embeddings or embeddings[0] is None:
            return False

        write_property = cfg.get("semantic", {}).get(
            "write_property", "semanticEmbedding"
        )
        id_property = "id"
        id_prop = _cypher_prop(id_property)
        write_prop = _cypher_prop(write_property)

        safe_label = normalize_label_name(label)
        cypher = f"""
        MATCH (n:`{safe_label}`)
        WHERE n.{id_prop} = $node_id
        SET n.{write_prop} = $embedding
        """
        neo4j.execute(cypher, {"node_id": node_id, "embedding": embeddings[0]})
        return True

    except Exception as exc:
        LOG.warning(
            "embed_node_inline_failed",
            extra={"label": label, "node_id": str(node_id), "error": str(exc)},
        )
        return False
