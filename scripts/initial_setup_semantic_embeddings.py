"""Initial setup: Generate semantic embeddings for configured labels."""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Add project root for imports
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

from shared.logging import get_logger
from shared.neo4j_client import Neo4jClient
from shared.semantic_embeddings import (
    get_semantic_rules,
    iter_label_ids,
    load_embedding_config,
    prepare_semantic_rows,
    write_semantic_embeddings,
)

LOG = get_logger(__name__)


def main() -> int:
    load_dotenv(ROOT / ".env", override=True)
    api_base = os.getenv("LITELLM_BASE_URL") or os.getenv("OPENAI_API_BASE")
    config = load_embedding_config()
    rules = get_semantic_rules(config)
    write_property = config.get("semantic", {}).get("write_property", "semanticEmbedding")

    neo4j = Neo4jClient.from_env()
    try:
        neo4j.verify_auth()
        total_updates = 0
        for label, rule in rules.items():
            LOG.info("semantic embedding start", extra={"label": label})
            batch_updates = 0
            batch_num = 0
            for ids in iter_label_ids(
                neo4j,
                label,
                write_property=write_property,
                batch_size=5,
                only_missing=True,
            ):
                batch_num += 1
                rows = prepare_semantic_rows(neo4j, label, ids, rule=rule)
                LOG.info(
                    "processing batch",
                    extra={"label": label, "batch": batch_num, "rows": len(rows)},
                )
                updated = write_semantic_embeddings(
                    neo4j,
                    label,
                    rows,
                    write_property=write_property,
                    api_base=api_base,
                )
                batch_updates += updated
                LOG.info(
                    "batch complete",
                    extra={"label": label, "batch": batch_num, "updated": updated, "total_so_far": batch_updates},
                )
            LOG.info(
                "semantic embedding complete",
                extra={"label": label, "updated": batch_updates},
            )
            total_updates += batch_updates
        LOG.info("semantic embedding finished", extra={"updated": total_updates})
        return 0
    finally:
        neo4j.close()


if __name__ == "__main__":
    raise SystemExit(main())
