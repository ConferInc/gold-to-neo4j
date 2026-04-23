"""GraphSAGE Retrain Service — scheduled retraining of structural embeddings.

Wraps the logic from ``scripts/initial_setup_graphsage.py`` into a service
class suitable for orchestrator flow invocation (every 3 days).

Steps:
  1. Drop existing model + graph projection (if any)
  2. Ensure dummyFeature on all labels
  3. Project the graph with all configured labels and relationships
  4. Train GraphSAGE model
  5. Write structural embeddings to all nodes
  6. Drop the graph projection (cleanup)

After step 6, the **trained model stays in the GDS Catalog** (in-memory).
This is intentional — the ``GraphSageInferenceService`` uses the retained
model to generate structural embeddings for new nodes between retrain
cycles (every 6 hours).  On Neo4j Community Edition the model is lost
if Neo4j restarts; the next retrain cycle recreates it.

Usage::

    service = GraphSageRetrainService()
    summary = service.run_once()
    print(summary)
    # {'status': 'success', 'model_name': 'b2c_customer_model', ...}
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from dotenv import load_dotenv

from shared.logging import get_logger
from shared.neo4j_client import Neo4jClient

LOG = get_logger(__name__)

ROOT = Path(__file__).resolve().parents[2]


class GraphSageRetrainService:
    """Automated GraphSAGE model retraining for structural embeddings."""

    def __init__(self, neo4j: Optional[Neo4jClient] = None):
        load_dotenv(ROOT / ".env", override=True)
        self._neo4j = neo4j

    def _get_neo4j(self) -> Neo4jClient:
        if self._neo4j is None:
            self._neo4j = Neo4jClient.from_env()
            self._neo4j.verify_auth()
        return self._neo4j

    def _load_config(self) -> Dict[str, Any]:
        cfg_path = ROOT / "config" / "embedding_config.yaml"
        with cfg_path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def run_once(self) -> Dict[str, Any]:
        """Run a full GraphSAGE retrain cycle.

        Returns
        -------
        dict
            Summary with ``status``, ``model_name``, ``train_ms``,
            ``nodes_written``, ``duration_ms``.
        """
        start = time.time()
        summary: Dict[str, Any] = {
            "status": "unknown",
            "model_name": None,
            "train_ms": 0,
            "nodes_written": 0,
            "errors": [],
        }

        try:
            # Import the existing GraphSAGE functions from the setup script
            from scripts.initial_setup_graphsage import (
                ensure_dummy_feature,
                project_graph,
                train_and_write,
                drop_graph,
                _drop_graph_if_exists,
                _drop_model_if_exists,
                _model_exists,
            )
            from shared.embedding_validation import (
                EmbeddingConfigError,
                resolve_graph_sage_config,
            )

            neo4j = self._get_neo4j()
            config = self._load_config()

            # Validate and resolve config
            try:
                config = resolve_graph_sage_config(neo4j, config)
            except EmbeddingConfigError as exc:
                LOG.error("graphsage_config_invalid", extra={"error": str(exc)})
                summary["status"] = "failed"
                summary["errors"].append({"phase": "config", "error": str(exc)})
                return summary

            gs = config["graph_sage"]
            graph_name = gs["graph_name"]
            model_name = gs["model_name"]
            summary["model_name"] = model_name

            LOG.info(
                "graphsage_retrain_starting",
                extra={"graph": graph_name, "model": model_name},
            )

            # Step 1: Drop existing model + graph projection
            _drop_graph_if_exists(neo4j, graph_name)
            _drop_model_if_exists(neo4j, model_name)

            # Step 2: Ensure dummyFeature on all labels
            feature_props = list(gs.get("feature_properties") or [])
            if gs.get("use_dummy_feature_fallback", True) and (
                not feature_props or "dummyFeature" in feature_props
            ):
                ensure_dummy_feature(neo4j, gs["node_labels"])

            # Step 3: Project graph
            project_graph(neo4j, config)

            # Step 4+5: Train and write embeddings
            try:
                tw_result = train_and_write(neo4j, config)
                summary["train_ms"] = tw_result.get("train_millis", 0)
                summary["nodes_written"] = tw_result.get("nodes_written", 0)
            finally:
                # Step 6: Drop graph projection (model intentionally
                # stays in GDS Catalog for incremental inference)
                drop_graph(neo4j, graph_name)

            # Verify model remains in catalog for inference service
            model_retained = _model_exists(neo4j, model_name)
            summary["model_retained"] = model_retained
            if model_retained:
                LOG.info(
                    "graphsage_model_retained",
                    extra={"model": model_name,
                           "note": "available for incremental inference"},
                )
            else:
                LOG.warning(
                    "graphsage_model_not_retained",
                    extra={"model": model_name},
                )

            summary["status"] = "success"
            LOG.info(
                "graphsage_retrain_complete",
                extra={"model": model_name,
                       "model_retained": model_retained},
            )

        except Exception as exc:
            summary["status"] = "failed"
            summary["errors"].append({"phase": "retrain", "error": str(exc)})
            LOG.error(
                "graphsage_retrain_failed",
                extra={"error": str(exc)},
            )

        finally:
            duration_ms = int((time.time() - start) * 1000)
            summary["duration_ms"] = duration_ms

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
    service = GraphSageRetrainService()
    try:
        return service.run_once()
    finally:
        service.close()


if __name__ == "__main__":
    result = main()
    import json
    print(json.dumps(result, indent=2))
