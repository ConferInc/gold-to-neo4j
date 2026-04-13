"""Initial setup: Train GraphSAGE and write structural embeddings to all nodes."""

from __future__ import annotations

import sys
from pathlib import Path

# Add project root for imports
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import yaml
from dotenv import load_dotenv

from shared.embedding_validation import EmbeddingConfigError, resolve_graph_sage_config
from shared.neo4j_client import Neo4jClient
from shared.logging import get_logger

LOG = get_logger(__name__)


def load_config() -> dict:
    with (ROOT / "config" / "embedding_config.yaml").open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def ensure_dummy_feature(neo4j: Neo4jClient, labels: list[str]) -> None:
    """Add dummyFeature=1.0 to nodes that lack feature properties."""
    for label in labels:
        cypher = f"""
        MATCH (n:{label})
        WHERE n.dummyFeature IS NULL
        SET n.dummyFeature = 1.0
        RETURN count(n) AS updated
        """
        neo4j.execute(cypher)
        LOG.info("ensured dummyFeature", extra={"label": label})


def _resolve_feature_properties(gs: dict) -> tuple[dict[str, list[str]], list[str]]:
    """
    Resolve per-label feature properties for projection, and features for train().
    Returns (node_projection_props, train_feature_properties).
    - Projection: uses label_feature_properties per label (with dummyFeature fallback).
    - Train: uses ['dummyFeature'] when label_feature_properties is heterogeneous,
      since GDS requires featureProperties to exist on ALL labels.
    """
    labels = gs.get("node_labels") or []
    label_feat = gs.get("label_feature_properties") or {}
    global_feat = gs.get("feature_properties") or []
    use_dummy = gs.get("use_dummy_feature_fallback", True)
    if not global_feat and use_dummy:
        global_feat = ["dummyFeature"]

    projection: dict[str, list[str]] = {}

    for label in labels:
        props = label_feat.get(label) or global_feat
        if not props and use_dummy:
            props = ["dummyFeature"]
        props = list(dict.fromkeys(props))
        if use_dummy and "dummyFeature" not in props:
            props = props + ["dummyFeature"]
        projection[label] = props

    # Train must use only properties present on ALL labels. With heterogeneous
    # label_feature_properties, that is dummyFeature only.
    if label_feat:
        train_feat = ["dummyFeature"]
    else:
        train_feat = list(global_feat) if global_feat else ["dummyFeature"]

    return projection, train_feat


def project_graph(neo4j: Neo4jClient, cfg: dict) -> None:
    """Project graph in GDS with node properties for GraphSAGE features."""
    gs = cfg["graph_sage"]
    labels = gs["node_labels"]
    rels = gs["relationship_types"]
    graph_name = gs["graph_name"]

    projection_props, train_feat_props = _resolve_feature_properties(gs)
    node_projection = {
        label: {"properties": projection_props.get(label, train_feat_props)}
        for label in labels
    }

    cypher = """
    CALL gds.graph.project($graphName, $nodeProjection, $relationshipTypes)
    YIELD graphName, nodeCount, relationshipCount
    RETURN graphName, nodeCount, relationshipCount
    """
    neo4j.execute(
        cypher,
        {
            "graphName": graph_name,
            "nodeProjection": node_projection,
            "relationshipTypes": rels,
        },
    )
    LOG.info(
        "graph projected",
        extra={"graph": graph_name, "train_feature_properties": train_feat_props},
    )


def _graph_exists(neo4j: Neo4jClient, graph_name: str) -> bool:
    cypher = "CALL gds.graph.exists($graphName) YIELD exists RETURN exists"
    try:
        rows = neo4j.query(cypher, {"graphName": graph_name})
    except Exception:
        return False
    if not rows:
        return False
    return bool(rows[0].get("exists"))


def _drop_graph_if_exists(neo4j: Neo4jClient, graph_name: str) -> None:
    if _graph_exists(neo4j, graph_name):
        LOG.warning("graph already exists; dropping", extra={"graph": graph_name})
        drop_graph(neo4j, graph_name)


def _model_exists(neo4j: Neo4jClient, model_name: str) -> bool:
    cypher_variants = [
        "CALL gds.model.exists($modelName) YIELD exists RETURN exists",
        "CALL gds.beta.model.exists($modelName) YIELD exists RETURN exists",
    ]
    for cypher in cypher_variants:
        try:
            rows = neo4j.query(cypher, {"modelName": model_name})
        except Exception:
            continue
        if rows:
            return bool(rows[0].get("exists"))
    return False


def _drop_model_if_exists(neo4j: Neo4jClient, model_name: str) -> None:
    if not _model_exists(neo4j, model_name):
        return
    cypher_variants = [
        "CALL gds.model.drop($modelName) YIELD modelInfo RETURN modelInfo",
        "CALL gds.beta.model.drop($modelName) YIELD modelInfo RETURN modelInfo",
    ]
    for cypher in cypher_variants:
        try:
            neo4j.execute(cypher, {"modelName": model_name})
            LOG.warning("model already exists; dropped", extra={"model": model_name})
            return
        except Exception:
            continue
    LOG.warning("model exists but could not be dropped", extra={"model": model_name})


def train_and_write(neo4j: Neo4jClient, cfg: dict) -> None:
    """Train GraphSAGE and write embeddings."""
    gs = cfg["graph_sage"]
    graph_name = gs["graph_name"]
    model_name = gs["model_name"]
    write_prop = gs["write_property"]
    emb_dim = gs["embedding_dimension"]
    _, feat_props = _resolve_feature_properties(gs)

    if not feat_props:
        raise ValueError(
            "graph_sage.feature_properties must be non-empty or use_dummy_feature_fallback=true"
        )

    train_cypher = """
    CALL gds.beta.graphSage.train(
        $graphName,
        {
            modelName: $modelName,
            featureProperties: $featureProperties,
            embeddingDimension: $embeddingDimension,
            epochs: 20,
            learningRate: 0.001
        }
    )
    YIELD modelInfo, trainMillis
    RETURN modelInfo.modelName AS modelName, trainMillis
    """
    train_result = neo4j.query(
        train_cypher,
        {
            "graphName": graph_name,
            "modelName": model_name,
            "featureProperties": feat_props,
            "embeddingDimension": emb_dim,
        },
    )
    if train_result:
        LOG.info("GraphSAGE trained", extra={"model": model_name, "trainMillis": train_result[0].get("trainMillis")})
    else:
        LOG.info("GraphSAGE trained", extra={"model": model_name})

    write_cypher = """
    CALL gds.beta.graphSage.write(
        $graphName,
        {
            modelName: $modelName,
            writeProperty: $writeProperty
        }
    )
    YIELD nodeCount, nodePropertiesWritten
    RETURN nodeCount, nodePropertiesWritten
    """
    write_result = neo4j.query(
        write_cypher,
        {
            "graphName": graph_name,
            "modelName": model_name,
            "writeProperty": write_prop,
        },
    )
    if write_result:
        LOG.info("embeddings written", extra={
            "property": write_prop,
            "nodeCount": write_result[0].get("nodeCount"),
            "nodePropertiesWritten": write_result[0].get("nodePropertiesWritten")
        })
    else:
        LOG.warning("embeddings write returned no result", extra={"property": write_prop})


def drop_graph(neo4j: Neo4jClient, graph_name: str) -> None:
    cypher = "CALL gds.graph.drop($graphName, false) YIELD graphName"
    neo4j.execute(cypher, {"graphName": graph_name})
    LOG.info("graph dropped", extra={"graph": graph_name})


def main() -> int:
    load_dotenv(ROOT / ".env", override=True)
    config = load_config()
    neo4j = Neo4jClient.from_env()

    try:
        neo4j.verify_auth()
        try:
            config = resolve_graph_sage_config(neo4j, config)
        except EmbeddingConfigError as exc:
            LOG.error("embedding config validation failed", extra={"error": str(exc)})
            raise

        gs = config["graph_sage"]
        _drop_graph_if_exists(neo4j, gs["graph_name"])
        _drop_model_if_exists(neo4j, gs["model_name"])

        feature_props = list(gs.get("feature_properties") or [])
        if gs.get("use_dummy_feature_fallback", True) and (
            not feature_props or "dummyFeature" in feature_props
        ):
            ensure_dummy_feature(neo4j, gs["node_labels"])

        project_graph(neo4j, config)
        try:
            train_and_write(neo4j, config)
        finally:
            drop_graph(neo4j, gs["graph_name"])

        return 0
    finally:
        neo4j.close()


if __name__ == "__main__":
    raise SystemExit(main())
