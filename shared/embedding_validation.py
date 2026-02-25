"""Embedding config validation and agent-assisted resolution."""

from __future__ import annotations

import re
from difflib import SequenceMatcher
from typing import Any, Dict, Iterable, List, Tuple

from shared.agent_client import resolve_embedding_config
from shared.logging import get_logger
from shared.neo4j_client import Neo4jClient

LOG = get_logger(__name__)


class EmbeddingConfigError(RuntimeError):
    """Raised when embedding config cannot be validated or resolved safely."""


def _normalize(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def _similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def _resolve_names(
    expected: Iterable[str],
    available: Iterable[str],
    *,
    threshold: float,
    kind: str,
) -> Tuple[List[str], Dict[str, str], List[str]]:
    expected_list = list(expected)
    available_list = list(available)
    available_set = set(available_list)
    resolved: List[str] = []
    mapping: Dict[str, str] = {}
    missing: List[str] = []
    used: set[str] = set()

    for item in expected_list:
        if item in available_set:
            mapping[item] = item
            resolved.append(item)
            used.add(item)

    for item in expected_list:
        if item in mapping:
            continue
        normalized = _normalize(item)
        matches = [name for name in available_list if _normalize(name) == normalized]
        if len(matches) == 1 and matches[0] not in used:
            mapping[item] = matches[0]
            resolved.append(matches[0])
            used.add(matches[0])

    for item in expected_list:
        if item in mapping:
            continue
        scored = [(name, _similarity(item, name)) for name in available_list if name not in used]
        scored.sort(key=lambda pair: pair[1], reverse=True)
        if not scored:
            missing.append(item)
            continue
        best_name, best_score = scored[0]
        ties = [name for name, score in scored if score == best_score]
        if best_score >= threshold and len(ties) == 1:
            mapping[item] = best_name
            resolved.append(best_name)
            used.add(best_name)
        else:
            missing.append(item)

    if len(resolved) != len(set(resolved)):
        LOG.warning("duplicate mappings detected", extra={"kind": kind, "resolved": resolved})

    return resolved, mapping, missing


def _candidate_hints(missing: Iterable[str], available: Iterable[str], k: int = 5) -> Dict[str, List[Dict[str, Any]]]:
    hints: Dict[str, List[Dict[str, Any]]] = {}
    available_list = list(available)
    for item in missing:
        scored = [(name, _similarity(item, name)) for name in available_list]
        scored.sort(key=lambda pair: pair[1], reverse=True)
        hints[item] = [
            {"name": name, "score": round(score, 4)} for name, score in scored[:k]
        ]
    return hints


def _fetch_labels(neo4j: Neo4jClient) -> List[str]:
    rows = neo4j.query("CALL db.labels() YIELD label RETURN label")
    return [row["label"] for row in rows if row.get("label")]


def _fetch_relationship_types(neo4j: Neo4jClient) -> List[str]:
    rows = neo4j.query("CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType")
    return [row["relationshipType"] for row in rows if row.get("relationshipType")]


def _fetch_label_properties(neo4j: Neo4jClient, label: str) -> List[str]:
    cypher = f"""
    MATCH (n:{label})
    WITH n LIMIT 500
    UNWIND keys(n) AS key
    RETURN DISTINCT key
    """
    rows = neo4j.query(cypher)
    return [row["key"] for row in rows if row.get("key")]


def _fetch_schema_properties(neo4j: Neo4jClient) -> List[Dict[str, Any]]:
    cypher = """
    CALL db.schema.nodeTypeProperties()
    YIELD nodeLabels, propertyName, propertyTypes
    RETURN nodeLabels, propertyName, propertyTypes
    """
    try:
        return neo4j.query(cypher)
    except Exception:
        LOG.warning("db.schema.nodeTypeProperties unavailable; falling back to sampling")
        return []


def _numeric_property(types: Iterable[str]) -> bool:
    for t in types:
        name = str(t).lower()
        if any(token in name for token in ("int", "float", "double", "long", "number", "decimal")):
            return True
    return False


def _validate_feature_properties(
    neo4j: Neo4jClient,
    labels: List[str],
    feature_properties: List[str],
) -> Tuple[bool, Dict[str, List[str]]]:
    missing: Dict[str, List[str]] = {}
    if not feature_properties:
        return True, missing
    # dummyFeature is generated in setup and is the supported universal fallback.
    if set(feature_properties) == {"dummyFeature"}:
        return True, missing

    schema_rows = _fetch_schema_properties(neo4j)
    if schema_rows:
        props_by_label: Dict[str, Dict[str, List[str]]] = {}
        for row in schema_rows:
            node_labels = row.get("nodeLabels") or []
            prop = row.get("propertyName")
            types = row.get("propertyTypes") or []
            for label in node_labels:
                props_by_label.setdefault(str(label), {})[str(prop)] = [str(t) for t in types]

        for label in labels:
            label_props = props_by_label.get(label, {})
            missing_props = [prop for prop in feature_properties if prop not in label_props]
            if missing_props:
                missing[label] = missing_props
                continue
            non_numeric = [
                prop
                for prop in feature_properties
                if not _numeric_property(label_props.get(prop, []))
            ]
            if non_numeric:
                missing[label] = non_numeric
    else:
        for label in labels:
            label_props = set(_fetch_label_properties(neo4j, label))
            missing_props = [prop for prop in feature_properties if prop not in label_props]
            if missing_props:
                missing[label] = missing_props

    return not missing, missing


def resolve_graph_sage_config(neo4j: Neo4jClient, config: Dict[str, Any]) -> Dict[str, Any]:
    cfg = dict(config)
    gs = dict(cfg.get("graph_sage", {}))
    cfg["graph_sage"] = gs

    labels = list(gs.get("node_labels") or [])
    rels = list(gs.get("relationship_types") or [])

    available_labels = _fetch_labels(neo4j)
    available_rels = _fetch_relationship_types(neo4j)

    resolved_labels, label_map, missing_labels = _resolve_names(
        labels,
        available_labels,
        threshold=0.88,
        kind="labels",
    )
    resolved_rels, rel_map, missing_rels = _resolve_names(
        rels,
        available_rels,
        threshold=0.88,
        kind="relationships",
    )

    if missing_labels or missing_rels:
        payload = {
            "expected_labels": labels,
            "available_labels": available_labels,
            "missing_labels": missing_labels,
            "label_hints": _candidate_hints(missing_labels, available_labels),
            "expected_relationship_types": rels,
            "available_relationship_types": available_rels,
            "missing_relationship_types": missing_rels,
            "relationship_hints": _candidate_hints(missing_rels, available_rels),
        }
        agent_response = resolve_embedding_config(payload)
        if agent_response:
            agent_labels = agent_response.get("label_aliases", {}) or {}
            for expected, actual in agent_labels.items():
                if expected in missing_labels and actual in available_labels and actual not in resolved_labels:
                    resolved_labels.append(actual)
                    label_map[expected] = actual
            agent_rels = agent_response.get("relationship_aliases", {}) or {}
            for expected, actual in agent_rels.items():
                if expected in missing_rels and actual in available_rels and actual not in resolved_rels:
                    resolved_rels.append(actual)
                    rel_map[expected] = actual

    missing_labels = [item for item in labels if item not in label_map]
    missing_rels = [item for item in rels if item not in rel_map]
    if missing_labels or missing_rels:
        raise EmbeddingConfigError(
            f"unresolved labels={missing_labels} relationships={missing_rels}"
        )

    resolved_labels_final = [label_map[item] for item in labels]
    resolved_rels_final = [rel_map[item] for item in rels]

    if len(set(resolved_labels_final)) != len(resolved_labels_final):
        raise EmbeddingConfigError("label mapping is ambiguous or duplicated")
    if len(set(resolved_rels_final)) != len(resolved_rels_final):
        raise EmbeddingConfigError("relationship mapping is ambiguous or duplicated")

    if label_map and label_map != {name: name for name in labels}:
        LOG.info("label mapping applied", extra={"mapping": label_map})
    if rel_map and rel_map != {name: name for name in rels}:
        LOG.info("relationship mapping applied", extra={"mapping": rel_map})

    gs["node_labels"] = resolved_labels_final
    gs["relationship_types"] = resolved_rels_final

    total_nodes = 0
    empty_labels: List[str] = []
    for label in resolved_labels_final:
        count = neo4j.count_nodes(label)
        total_nodes += count
        if count == 0:
            empty_labels.append(label)
    if total_nodes == 0:
        raise EmbeddingConfigError("no nodes found for configured labels")
    if empty_labels:
        LOG.warning("labels with zero nodes detected", extra={"labels": empty_labels})

    total_rels = 0
    empty_rels: List[str] = []
    for rel in resolved_rels_final:
        count = neo4j.count_relationships(rel)
        total_rels += count
        if count == 0:
            empty_rels.append(rel)
    if total_rels == 0:
        raise EmbeddingConfigError("no relationships found for configured types")
    if empty_rels:
        LOG.warning("relationship types with zero edges detected", extra={"types": empty_rels})

    feature_props = list(gs.get("feature_properties") or [])
    use_dummy = bool(gs.get("use_dummy_feature_fallback", True))
    valid, missing = _validate_feature_properties(neo4j, resolved_labels_final, feature_props)
    if feature_props and not valid:
        if use_dummy:
            LOG.warning(
                "feature properties missing or non-numeric; falling back to dummyFeature",
                extra={"missing": missing},
            )
            gs["feature_properties"] = []
        else:
            raise EmbeddingConfigError(f"feature properties invalid: {missing}")

    return cfg
