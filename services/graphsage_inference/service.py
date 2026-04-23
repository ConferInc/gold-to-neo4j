"""GraphSAGE Inference Service — structural embeddings for new nodes.

Uses the trained GraphSAGE model (kept in GDS Catalog by the retrain
service) to infer embeddings for nodes with ``graphSageEmbedding IS NULL``.

Tier 2 of the two-tier GraphSAGE strategy.  Runs every 6 hours.

GDS 2.26.0 API:
  - ``gds.beta.graphSage.stream()`` for inference (still beta namespace)
  - ``gds.graph.project()`` for native projection (NOT deprecated cypher variant)

Community Edition:
  - Model is in-memory only — gracefully skips if model not loaded.
  - Mutual exclusion with retrain is handled at the orchestrator level.

Usage::

    service = GraphSageInferenceService()
    summary = service.run_once()
    print(summary)
    # {'status': 'success', 'nodes_inferred': 23, 'labels_checked': 21, ...}
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from dotenv import load_dotenv

from shared.logging import get_logger
from shared.neo4j_client import Neo4jClient

LOG = get_logger(__name__)

ROOT = Path(__file__).resolve().parents[2]

# ── Defaults ──────────────────────────────────────────
_DEFAULT_BATCH_LIMIT = 500
_INFERENCE_GRAPH_NAME = "customer_recipe_graph_inference"


class GraphSageInferenceService:
    """Infer structural embeddings for new nodes using a trained model.

    The ``run_once`` method:

    1. Checks if the trained model exists in the GDS Catalog.
    2. Counts nodes missing ``graphSageEmbedding`` across all configured labels.
    3. If nothing to do — returns early (fast path).
    4. Ensures ``dummyFeature`` is set on new nodes.
    5. Projects the full graph using native ``gds.graph.project``.
    6. Streams embeddings via ``gds.beta.graphSage.stream()`` using the
       trained model — filters results to only nodes missing embeddings.
    7. Writes embeddings back via Cypher ``SET``.
    8. Drops the inference graph projection.
    """

    def __init__(self, neo4j: Optional[Neo4jClient] = None):
        load_dotenv(ROOT / ".env", override=True)
        self._neo4j = neo4j
        self._batch_limit = int(
            os.environ.get("GRAPHSAGE_INFERENCE_BATCH_LIMIT", _DEFAULT_BATCH_LIMIT)
        )

    def _get_neo4j(self) -> Neo4jClient:
        if self._neo4j is None:
            self._neo4j = Neo4jClient.from_env()
            self._neo4j.verify_auth()
        return self._neo4j

    def _load_config(self) -> Dict[str, Any]:
        cfg_path = ROOT / "config" / "embedding_config.yaml"
        with cfg_path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    # ── Model check ───────────────────────────────────

    @staticmethod
    def _model_exists(neo4j: Neo4jClient, model_name: str) -> bool:
        """Check if the specified model is loaded in the GDS Catalog."""
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

    # ── Count missing nodes ───────────────────────────

    @staticmethod
    def _count_missing(neo4j: Neo4jClient, labels: List[str]) -> int:
        """Return the total number of nodes missing graphSageEmbedding."""
        total = 0
        for label in labels:
            safe_label = label.replace("`", "")
            cypher = f"""
            MATCH (n:`{safe_label}`)
            WHERE n.graphSageEmbedding IS NULL AND n.id IS NOT NULL
            RETURN count(n) AS cnt
            """
            try:
                rows = neo4j.query(cypher)
                total += (rows[0].get("cnt", 0) if rows else 0)
            except Exception as exc:
                LOG.warning(
                    "inference_count_failed",
                    extra={"label": label, "error": str(exc)},
                )
        return total

    # ── Collect missing node internal IDs ─────────────

    @staticmethod
    def _collect_missing_node_ids(
        neo4j: Neo4jClient, labels: List[str], limit: int
    ) -> set:
        """Return set of internal Neo4j node IDs missing graphSageEmbedding."""
        missing_ids: set = set()
        remaining = limit
        for label in labels:
            if remaining <= 0:
                break
            safe_label = label.replace("`", "")
            cypher = f"""
            MATCH (n:`{safe_label}`)
            WHERE n.graphSageEmbedding IS NULL AND n.id IS NOT NULL
            RETURN id(n) AS nid
            LIMIT $limit
            """
            try:
                rows = neo4j.query(cypher, {"limit": remaining})
                for row in rows:
                    missing_ids.add(row["nid"])
                remaining = limit - len(missing_ids)
            except Exception as exc:
                LOG.warning(
                    "inference_collect_failed",
                    extra={"label": label, "error": str(exc)},
                )
        return missing_ids

    # ── Main run ──────────────────────────────────────

    def run_once(self) -> Dict[str, Any]:
        """Run a single GraphSAGE inference pass.

        Returns
        -------
        dict
            Summary with ``status``, ``nodes_inferred``, ``total_missing``,
            ``duration_ms``.
        """
        start = time.time()
        summary: Dict[str, Any] = {
            "status": "unknown",
            "nodes_inferred": 0,
            "total_missing": 0,
            "errors": [],
        }

        try:
            from scripts.initial_setup_graphsage import (
                ensure_dummy_feature,
                project_graph,
                drop_graph,
                _drop_graph_if_exists,
            )
            from shared.embedding_validation import (
                EmbeddingConfigError,
                resolve_graph_sage_config,
            )

            neo4j = self._get_neo4j()
            config = self._load_config()

            # Validate config
            try:
                config = resolve_graph_sage_config(neo4j, config)
            except EmbeddingConfigError as exc:
                LOG.error("inference_config_invalid", extra={"error": str(exc)})
                summary["status"] = "failed"
                summary["errors"].append({"phase": "config", "error": str(exc)})
                return summary

            gs = config["graph_sage"]
            model_name = gs["model_name"]
            graph_name_base = gs["graph_name"]

            # ── Step 1: Check model exists ────────────
            if not self._model_exists(neo4j, model_name):
                LOG.info(
                    "inference_skipped_no_model",
                    extra={"model": model_name,
                           "note": "model not in GDS Catalog — wait for retrain"},
                )
                summary["status"] = "skipped"
                summary["reason"] = "model_not_loaded"
                return summary

            # ── Step 2: Count missing nodes ───────────
            labels = gs["node_labels"]
            total_missing = self._count_missing(neo4j, labels)
            summary["total_missing"] = total_missing

            if total_missing == 0:
                LOG.info("inference_nothing_to_do",
                         extra={"msg": "all nodes have graphSageEmbedding"})
                summary["status"] = "success"
                summary["nodes_inferred"] = 0
                return summary

            LOG.info(
                "inference_starting",
                extra={"total_missing": total_missing,
                       "model": model_name,
                       "batch_limit": self._batch_limit},
            )

            # ── Step 3: Collect missing node IDs ──────
            missing_ids = self._collect_missing_node_ids(
                neo4j, labels, self._batch_limit
            )
            if not missing_ids:
                summary["status"] = "success"
                return summary

            # ── Step 4: Ensure dummyFeature ───────────
            feature_props = list(gs.get("feature_properties") or [])
            if gs.get("use_dummy_feature_fallback", True) and (
                not feature_props or "dummyFeature" in feature_props
            ):
                ensure_dummy_feature(neo4j, labels)

            # ── Step 5: Project full graph ────────────
            #   Use a separate graph name to avoid collision with retrain
            inference_graph_name = _INFERENCE_GRAPH_NAME
            _drop_graph_if_exists(neo4j, inference_graph_name)

            # Temporarily override graph_name in config for projection
            original_graph_name = gs["graph_name"]
            gs["graph_name"] = inference_graph_name
            try:
                project_graph(neo4j, config)
            finally:
                gs["graph_name"] = original_graph_name

            # ── Step 6+7: Stream + write ──────────────
            nodes_written = 0
            try:
                stream_cypher = """
                CALL gds.beta.graphSage.stream(
                    $graphName,
                    {modelName: $modelName}
                )
                YIELD nodeId, embedding
                RETURN nodeId, embedding
                """
                rows = neo4j.query(
                    stream_cypher,
                    {
                        "graphName": inference_graph_name,
                        "modelName": model_name,
                    },
                )

                if rows:
                    # Filter to only nodes that are missing embeddings
                    write_batch = []
                    for row in rows:
                        node_id = row.get("nodeId")
                        embedding = row.get("embedding")
                        if node_id in missing_ids and embedding:
                            write_batch.append((node_id, embedding))

                    # Batch write embeddings
                    if write_batch:
                        write_cypher = """
                        MATCH (n) WHERE id(n) = $nodeId
                        SET n.graphSageEmbedding = $embedding
                        """
                        for node_id, embedding in write_batch:
                            try:
                                neo4j.execute(
                                    write_cypher,
                                    {
                                        "nodeId": node_id,
                                        "embedding": list(embedding),
                                    },
                                )
                                nodes_written += 1
                            except Exception as exc:
                                summary["errors"].append(
                                    {"nodeId": node_id, "error": str(exc)}
                                )
                                LOG.warning(
                                    "inference_write_failed",
                                    extra={"nodeId": node_id, "error": str(exc)},
                                )

                LOG.info(
                    "inference_write_complete",
                    extra={
                        "nodes_written": nodes_written,
                        "total_streamed": len(rows) if rows else 0,
                        "missing_ids_count": len(missing_ids),
                    },
                )

            finally:
                # ── Step 8: Always drop inference projection ──
                try:
                    drop_graph(neo4j, inference_graph_name)
                except Exception as exc:
                    LOG.warning(
                        "inference_drop_graph_failed",
                        extra={"graph": inference_graph_name, "error": str(exc)},
                    )

            summary["status"] = "success"
            summary["nodes_inferred"] = nodes_written

        except Exception as exc:
            summary["status"] = "failed"
            summary["errors"].append({"phase": "inference", "error": str(exc)})
            LOG.error(
                "inference_failed",
                extra={"error": str(exc)},
            )

        finally:
            duration_ms = int((time.time() - start) * 1000)
            summary["duration_ms"] = duration_ms
            LOG.info(
                "inference_complete",
                extra={
                    "status": summary["status"],
                    "nodes_inferred": summary.get("nodes_inferred", 0),
                    "total_missing": summary.get("total_missing", 0),
                    "error_count": len(summary.get("errors", [])),
                    "duration_ms": duration_ms,
                },
            )

        return summary

    def close(self) -> None:
        """Close the Neo4j connection if we created it."""
        if self._neo4j is not None:
            try:
                self._neo4j.close()
            except Exception:
                pass


def main() -> Dict[str, Any]:
    """Entry point for standalone/CLI execution."""
    service = GraphSageInferenceService()
    try:
        return service.run_once()
    finally:
        service.close()


if __name__ == "__main__":
    result = main()
    import json
    print(json.dumps(result, indent=2))
