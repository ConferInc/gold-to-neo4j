"""Scan all labels and list nodes missing semantic embeddings."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

from shared.neo4j_client import Neo4jClient
from shared.semantic_embeddings import get_semantic_rules, load_embedding_config


def main() -> int:
    load_dotenv(ROOT / ".env", override=True)
    config = load_embedding_config()
    rules = get_semantic_rules(config)
    write_property = config.get("semantic", {}).get("write_property", "semanticEmbedding")

    neo4j = Neo4jClient.from_env()
    try:
        neo4j.verify_auth()

        labels_with_rules = list(rules.keys())
        if not labels_with_rules:
            print("No semantic label rules in embedding_config.yaml")
            return 0

        print("=" * 60)
        print("SEMANTIC EMBEDDINGS STATUS")
        print("=" * 60)
        print(f"Labels configured for semantic embeddings: {', '.join(labels_with_rules)}")
        print()

        all_missing: list[tuple[str, list]] = []

        for label in labels_with_rules:
            rule = rules[label]
            props = rule.get("properties") or []

            # Total nodes with id
            cypher_total = f"""
            MATCH (n:{label})
            WHERE n.`id` IS NOT NULL
            RETURN count(n) AS total
            """
            total = neo4j.query(cypher_total, {})[0]["total"]

            # Nodes WITH semanticEmbedding
            cypher_with = f"""
            MATCH (n:{label})
            WHERE n.`id` IS NOT NULL AND n.`{write_property}` IS NOT NULL
            RETURN count(n) AS count
            """
            with_emb = neo4j.query(cypher_with, {})[0]["count"]

            # Nodes WITHOUT semanticEmbedding (missing)
            cypher_missing = f"""
            MATCH (n:{label})
            WHERE n.`id` IS NOT NULL AND n.`{write_property}` IS NULL
            RETURN n.`id` AS id
            ORDER BY id
            """
            missing_rows = neo4j.query(cypher_missing, {})
            missing_ids = [r["id"] for r in missing_rows if r.get("id") is not None]

            all_missing.append((label, missing_ids))

            print(f"--- {label} ---")
            print(f"  Total nodes (with id):     {total}")
            print(f"  With semanticEmbedding:    {with_emb}")
            print(f"  Missing semanticEmbedding: {len(missing_ids)}")
            if missing_ids:
                print(f"  Missing IDs (first 20):   {missing_ids[:20]}")
                if len(missing_ids) > 20:
                    print(f"  ... and {len(missing_ids) - 20} more")
            print()

        # Summary
        total_missing = sum(len(ids) for _, ids in all_missing)
        print("=" * 60)
        print("SUMMARY: Nodes missing semantic embeddings")
        print("=" * 60)
        for label, ids in all_missing:
            if ids:
                print(f"  {label}: {len(ids)} nodes - IDs: {ids}")
        print()
        print(f"Total nodes missing embeddings: {total_missing}")
        return 0
    finally:
        neo4j.close()


if __name__ == "__main__":
    raise SystemExit(main())
