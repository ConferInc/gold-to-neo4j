# One-Time Insertion Cost — Semantic Embeddings

**Model:** `text-embedding-3-small` @ $0.02 per 1M tokens  
**Assumption:** Fresh ingestion; only new nodes are embedded (existing nodes = $0)

---

## Cost by layer (example: 100 recipes, 100 products, 100 ingredients, 100 customers)

| Layer     | Nodes | Tokens | Cost (USD) |
|-----------|-------|--------|------------|
| Recipes   | 115   | 19,560 | $0.0004    |
| Ingredients | 1,248 | 9,378 | $0.0002  |
| Products  | 100   | 1,600  | $0.00003   |
| Customers | 752   | 7,076  | $0.0001    |
| **TOTAL** | 2,215 | 37,614 | **$0.0008** |

---

## Quick reference — per 100 nodes of each main entity

| Entity      | Tokens  | Cost      |
|-------------|---------|-----------|
| 100 Recipes | 19,500  | $0.0004   |
| 100 Products | 1,600  | $0.00003  |
| 100 Ingredients | 600 | $0.00001  |
| 100 Customers | 7,076  | $0.0001   |

---

## Notes

- **Recipes** cost the most per node (~195 tokens avg) because of long instruction text.
- **Products** and **Ingredients** are cheap per node (short name/brand text).
- **Customers** includes profile nodes, households, health data, and fixed lookups (Allergens, Dietary_Preferences).
- Only nodes with text properties are embedded; relationships (edges) have no cost.
- Nodes that already have embeddings are never re-billed.

---

*Regenerate for custom scenarios:*
```bash
RECIPES=500 PRODUCTS=1000 python scripts/one_time_insertion_cost_summary.py
```
