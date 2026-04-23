"""Embedding Backfill Service — finds nodes with missing embeddings and generates them.

This service is designed to run as a scheduled orchestrator flow (every 15 minutes)
to ensure eventual consistency.  It acts as a safety net for:

1. Nodes where realtime inline embedding failed (LiteLLM API down).
2. Nodes ingested via batch sync when EMBEDDING_BATCH_ENABLED was false.
3. Nodes that existed before the embedding pipeline was deployed.

Usage::

    service = EmbeddingBackfillService()
    summary = service.run_once()
    print(summary)
    # {'labels_checked': 15, 'total_backfilled': 42, 'errors': []}
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

from shared.logging import get_logger
from shared.neo4j_client import Neo4jClient

LOG = get_logger(__name__)

ROOT = Path(__file__).resolve().parents[2]


class EmbeddingBackfillService:
    """Scans all configured labels for nodes missing semantic embeddings."""

    def __init__(self, neo4j: Optional[Neo4jClient] = None):
        load_dotenv(ROOT / ".env", override=True)
        self._neo4j = neo4j

    def _get_neo4j(self) -> Neo4jClient:
        if self._neo4j is None:
            self._neo4j = Neo4jClient.from_env()
            self._neo4j.verify_auth()
        return self._neo4j

    def run_once(self) -> Dict[str, Any]:
        """Run a single backfill pass across all configured labels.

        Returns
        -------
        dict
            Summary with ``labels_checked``, ``total_backfilled``, ``errors``,
            ``duration_ms``.
        """
        start = time.time()
        summary: Dict[str, Any] = {
            "labels_checked": 0,
            "total_backfilled": 0,
            "errors": [],
        }

        try:
            from shared.semantic_embeddings import (
                load_embedding_config,
                get_semantic_rules,
                iter_label_ids,
                prepare_semantic_rows,
                write_semantic_embeddings,
            )

            neo4j = self._get_neo4j()
            emb_config = load_embedding_config()
            rules = get_semantic_rules(emb_config)

            if not rules:
                LOG.info("backfill_no_rules", extra={"msg": "No semantic rules found"})
                return summary

            # Read configured write property (default: semanticEmbedding)
            write_property = emb_config.get("semantic", {}).get(
                "write_property", "semanticEmbedding"
            )

            for label, rule in rules.items():
                properties = list(rule.get("properties") or [])
                if not properties:
                    continue  # Skip labels with empty property list (e.g. MealLog)

                summary["labels_checked"] += 1
                label_count = 0

                try:
                    # Iterate over batches of node IDs that are missing embeddings
                    for id_batch in iter_label_ids(
                        neo4j,
                        label,
                        write_property=write_property,
                        only_missing=True,
                        batch_size=200,
                    ):
                        if not id_batch:
                            continue

                        # Fetch node properties from Neo4j and build text
                        rows = prepare_semantic_rows(
                            neo4j,
                            label,
                            id_batch,
                            rule=rule,
                        )

                        if not rows:
                            continue

                        # Generate embeddings and write back
                        count = write_semantic_embeddings(
                            neo4j, label, rows,
                            write_property=write_property,
                        )
                        label_count += count

                    if label_count > 0:
                        summary["total_backfilled"] += label_count
                        LOG.info(
                            "backfill_label_complete",
                            extra={"label": label, "backfilled": label_count},
                        )

                except Exception as exc:
                    summary["errors"].append({"label": label, "error": str(exc)})
                    LOG.warning(
                        "backfill_label_failed",
                        extra={"label": label, "error": str(exc)},
                    )

        except Exception as exc:
            summary["errors"].append({"phase": "setup", "error": str(exc)})
            LOG.error(
                "backfill_setup_failed",
                extra={"error": str(exc)},
            )

        finally:
            duration_ms = int((time.time() - start) * 1000)
            summary["duration_ms"] = duration_ms
            LOG.info(
                "backfill_complete",
                extra={
                    "labels_checked": summary["labels_checked"],
                    "total_backfilled": summary["total_backfilled"],
                    "error_count": len(summary["errors"]),
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
    service = EmbeddingBackfillService()
    try:
        return service.run_once()
    finally:
        service.close()


if __name__ == "__main__":
    result = main()
    import json
    print(json.dumps(result, indent=2))
