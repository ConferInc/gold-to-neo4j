#!/usr/bin/env python3
"""Debug script: verify ingredient <-> nutrition_facts mapping for HAS_NUTRITION relationship."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import yaml
from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from shared.supabase_client import SupabaseClient
from shared.neo4j_client import Neo4jClient
from shared.schema_validation import build_table_plan, normalize_rows


def main():
    config = yaml.safe_load((ROOT / "config" / "ingredients.yaml").read_text())
    schema = config["schema"]
    tables_cfg = config["tables"]
    rel = next(r for r in config["relationships"] if r["type"] == "HAS_NUTRITION")

    supabase = SupabaseClient.from_env()
    neo4j = Neo4jClient.from_env()

    # Fetch minimal data (no state - get first batch only)
    plan = build_table_plan(supabase, schema, "ingredients", tables_cfg["ingredients"])
    ingredients = supabase.fetch_all(
        schema, "ingredients",
        columns=plan.select_columns,
        batch_size=500,
        filters=plan.filters,
    )
    normalize_rows(ingredients, plan.alias_map)

    plan_nf = build_table_plan(supabase, schema, "nutrition_facts", tables_cfg["nutrition_facts"])
    nutrition_facts = supabase.fetch_all(
        schema, "nutrition_facts",
        columns=plan_nf.select_columns,
        batch_size=500,
        filters=plan_nf.filters,
    )
    normalize_rows(nutrition_facts, plan_nf.alias_map)

    ingredient_ids = {str(r["id"]) for r in ingredients}
    nf_ids = {str(r["id"]) for r in nutrition_facts}

    rel_rows = []
    for row in nutrition_facts:
        from_val = row.get(rel["join_source_key"])
        to_val = row.get(rel["join_target_key"])
        if from_val is not None and to_val is not None:
            rel_rows.append({"from_key": from_val, "to_key": to_val})

    from_keys = {str(r["from_key"]) for r in rel_rows}
    to_keys = {str(r["to_key"]) for r in rel_rows}

    print("=== Data summary ===")
    print(f"Ingredients fetched: {len(ingredients)}")
    print(f"Nutrition_facts fetched: {len(nutrition_facts)}")
    print(f"Rel rows built: {len(rel_rows)}")
    print(f"Unique from_key (entity_id): {len(from_keys)}")
    print(f"Unique to_key (nf id): {len(to_keys)}")
    print()
    print("=== Overlap check ===")
    from_in_ingredients = from_keys & ingredient_ids
    to_in_nf = to_keys & nf_ids
    print(f"from_key values that exist in ingredients.id: {len(from_in_ingredients)} / {len(from_keys)}")
    print(f"to_key values that exist in nutrition_facts.id: {len(to_in_nf)} / {len(to_keys)}")
    print()
    print("=== Sample from_key (first 3) ===")
    for i, r in enumerate(rel_rows[:3]):
        print(f"  {i}: from_key={r['from_key']!r} (type={type(r['from_key']).__name__})")
    print()
    print("=== Sample ingredient.id (first 3) ===")
    for i, r in enumerate(ingredients[:3]):
        print(f"  {i}: id={r['id']!r} (type={type(r['id']).__name__})")
    print()

    # Neo4j state
    ing_count = neo4j.count_nodes("Ingredient")
    nv_count = neo4j.count_nodes("NutritionValue")
    rel_count = neo4j.count_relationships("HAS_NUTRITION")
    print("=== Neo4j state ===")
    print(f"Ingredient nodes: {ing_count}")
    print(f"NutritionValue nodes: {nv_count}")
    print(f"HAS_NUTRITION relationships: {rel_count}")
    print()

    # Check if a specific entity_id exists as Ingredient in Neo4j
    if rel_rows:
        sample_from = rel_rows[0]["from_key"]
        sample_to = rel_rows[0]["to_key"]
        ing_exists = neo4j.query(
            "MATCH (i:Ingredient {id: $id}) RETURN i.id AS id",
            {"id": sample_from},
        )
        nv_exists = neo4j.query(
            "MATCH (n:NutritionValue {id: $id}) RETURN n.id AS id",
            {"id": sample_to},
        )
        print("=== Sample pair check in Neo4j ===")
        print(f"from_key={sample_from!r}")
        print(f"  Ingredient exists: {bool(ing_exists)}")
        print(f"to_key={sample_to!r}")
        print(f"  NutritionValue exists: {bool(nv_exists)}")
        if ing_exists and nv_exists:
            # Try manual MERGE
            neo4j.execute(
                """
                MATCH (a:Ingredient {id: $from_id})
                MATCH (b:NutritionValue {id: $to_id})
                MERGE (a)-[:HAS_NUTRITION]->(b)
                """,
                {"from_id": sample_from, "to_id": sample_to},
            )
            new_count = neo4j.count_relationships("HAS_NUTRITION")
            print(f"  Manual MERGE run. HAS_NUTRITION count now: {new_count}")

    neo4j.close()


if __name__ == "__main__":
    main()
