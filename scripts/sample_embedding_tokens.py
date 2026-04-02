"""
Token Sampler — measure real avg/max tokens per node label from Neo4j.

Run this whenever you add a new node label or want to refresh the
avg_tokens / max_tokens values in estimate_embedding_cost.py.

Usage:
    python scripts/sample_embedding_tokens.py
    python scripts/sample_embedding_tokens.py --label Recipe
    python scripts/sample_embedding_tokens.py --sample 100
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env", override=True)

from shared.neo4j_client import Neo4jClient
from shared.semantic_embeddings import (
    build_text_from_node,
    get_semantic_rules,
    load_embedding_config,
)

CHARS_PER_TOKEN = 4.0


def sample_label(neo4j: Neo4jClient, label: str, props: list, separator: str, limit: int) -> dict:
    if not props:
        return {"label": label, "props": [], "count": 0, "avg_tokens": 0, "max_tokens": 0, "samples": []}

    total_rows = neo4j.query(f"MATCH (n:`{label}`) RETURN count(n) AS c", {})[0]["c"]

    prop_fields = ", ".join(f"n.`{p}` AS `{p}`" for p in props)
    rows = neo4j.query(
        f"MATCH (n:`{label}`) WHERE n.`id` IS NOT NULL RETURN {prop_fields} LIMIT {limit}",
        {}
    )

    texts = []
    for row in rows:
        node = {p: row.get(p) for p in props}
        t = build_text_from_node(node, props, separator).strip()
        if t:
            texts.append(t)

    if not texts:
        return {"label": label, "props": props, "count": total_rows, "avg_tokens": 0, "max_tokens": 0, "samples": []}

    token_counts = [max(1, int(len(t) / CHARS_PER_TOKEN)) for t in texts]
    avg_tokens = int(sum(token_counts) / len(token_counts))
    max_tokens = max(token_counts)

    return {
        "label": label,
        "props": props,
        "count": total_rows,
        "avg_tokens": avg_tokens,
        "max_tokens": max_tokens,
        "samples": texts[:2],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Sample token counts from Neo4j nodes")
    parser.add_argument("--label", help="Only sample this label (default: all configured labels)")
    parser.add_argument("--sample", type=int, default=50, help="Nodes to sample per label (default: 50)")
    args = parser.parse_args()

    config = load_embedding_config()
    rules = get_semantic_rules(config)

    if args.label:
        if args.label not in rules:
            print(f"[ERROR] Label '{args.label}' not found in embedding_config.yaml")
            return 1
        rules = {args.label: rules[args.label]}

    neo4j = Neo4jClient.from_env()
    try:
        neo4j.verify_auth()
    except Exception as exc:
        print(f"[ERROR] Cannot connect to Neo4j: {exc}")
        return 1

    print()
    print("=" * 80)
    print("  TOKEN SAMPLER — measured from real Neo4j data")
    print(f"  Sample size: {args.sample} nodes per label")
    print("=" * 80)
    print()
    print("  Copy these values into LABEL_REGISTRY in estimate_embedding_cost.py")
    print()
    print(f"  {'Label':<40} {'Nodes':>8} {'Avg tok':>9} {'Max tok':>9}  Props")
    print("  " + "─" * 78)

    for label, rule in rules.items():
        props     = list(rule.get("properties") or [])
        separator = rule.get("separator", " ")
        result    = sample_label(neo4j, label, props, separator, args.sample)

        props_str = ", ".join(props) if props else "(no text properties)"
        print(f"  {label:<40} {result['count']:>8} {result['avg_tokens']:>9} {result['max_tokens']:>9}  {props_str}")

        if result["samples"]:
            for i, s in enumerate(result["samples"]):
                preview = (s[:90] + "...") if len(s) > 90 else s
                print(f"  {'':40}   sample {i+1}: \"{preview}\"")
        print()

    print()
    print("  Paste format for LABEL_REGISTRY:")
    print("  " + "─" * 78)
    for label, rule in rules.items():
        props     = list(rule.get("properties") or [])
        separator = rule.get("separator", " ")
        result    = sample_label(neo4j, label, props, separator, args.sample)
        props_repr = repr(props)
        print(f'  "{label}": LabelDef(')
        print(f'      text_props={props_repr},')
        print(f'      avg_tokens={result["avg_tokens"]}, max_tokens={result["max_tokens"]},')
        print(f'      layer="<fill in>",')
        print(f'      notes="<fill in>",')
        print(f'  ),')
        print()

    neo4j.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
