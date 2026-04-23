# feat: Embedding Pipeline + GraphSAGE Incremental Inference Service

## Summary

Implements the complete embedding generation pipeline and a two-tier GraphSAGE strategy for structural embeddings. After this change, every node in Neo4j will have both `semanticEmbedding` (via inline/batch/backfill) and `graphSageEmbedding` (via retrain + incremental inference) — closing the 3-day structural embedding gap to 6 hours.

## Problem

1. **Semantic embeddings** were only generated via manual script — nodes created through realtime or batch sync had no `semanticEmbedding`.
2. **Structural embeddings** (`graphSageEmbedding`) were only set during the 3-day retrain cycle — new nodes between cycles had no graph-structure-based embedding, degrading recommendations.

## Solution Architecture

```
                Semantic Embeddings              Structural Embeddings
                ═══════════════════              ═════════════════════
Realtime  →  embed_node_inline()                 (via backfill safety net)
Batch     →  run_embedding_pass()                (via backfill safety net)
Backfill  →  EmbeddingBackfillService (15 min)   ─────────────┐
Retrain   →  ──────────────────────────────────  GraphSageRetrainService (3 days)
Inference →  ──────────────────────────────────  GraphSageInferenceService (6 hours)
```

## Changes

### New Files

| File | Description |
|------|-------------|
| `shared/embedding_pass.py` | DRY post-upsert semantic embedding module, called by all 4 batch services |
| `services/embedding_backfill/__init__.py` | Package init |
| `services/embedding_backfill/service.py` | Periodic scan for nodes with `semanticEmbedding IS NULL` → generates + writes embeddings |
| `services/graphsage_inference/__init__.py` | Package init |
| `services/graphsage_inference/service.py` | **Tier 2**: Uses trained GraphSAGE model from GDS Catalog to infer structural embeddings for new nodes (every 6 hours) |
| `services/graphsage_retrain/__init__.py` | Package init (previously created, now includes model retention changes) |
| `services/graphsage_retrain/service.py` | Updated with model retention verification + `_model_exists` import |

### Modified Files

| File | Change |
|------|--------|
| `shared/semantic_embeddings.py` | Added `embed_node_inline()` — lightweight single-node embedding for realtime path |
| `services/customer_realtime/handlers.py` | Added `_embed_after_upsert()` call after `upsert_event()` — best-effort inline embedding |
| `services/recipes_batch/service.py` | Added `run_embedding_pass()` call after TX commit |
| `services/customers_batch/service.py` | Added `run_embedding_pass()` call after TX commit |
| `services/ingredients_batch/service.py` | Added `run_embedding_pass()` call after TX commit |
| `services/products_batch/service.py` | Added `run_embedding_pass()` call after TX commit |
| `.env.example` | Added `GRAPHSAGE_INFERENCE_CRON`, `GRAPHSAGE_INFERENCE_BATCH_LIMIT` |

## GraphSAGE Inference — Technical Details

### GDS 2.26.0 Compatibility

- Uses `gds.beta.graphSage.stream()` (beta namespace, required for 2.26)
- Uses native `gds.graph.project()` (avoids deprecated Cypher projections)
- Model existence check tries both `gds.model.exists` and `gds.beta.model.exists`

### Graceful Degradation (Community Edition)

- No `gds.model.store/load` available — model lives in GDS Catalog (in-memory only)
- If Neo4j restarts → model is lost → inference service skips execution (`inference_skipped_no_model`)
- Next retrain cycle recreates the model → inference resumes automatically

### Inference Flow

1. Check `gds.model.exists('b2c_customer_model')` → skip if missing
2. Count nodes where `graphSageEmbedding IS NULL` → fast-path if 0
3. Collect missing node IDs (up to `GRAPHSAGE_INFERENCE_BATCH_LIMIT`)
4. Ensure `dummyFeature` on new nodes (required for GraphSAGE)
5. Project full graph using native projection (separate name: `customer_recipe_graph_inference`)
6. Stream via `gds.beta.graphSage.stream()` with trained model
7. Filter stream to only NULL-embedding nodes → batch `SET` write-back
8. Drop inference graph projection in `finally` block

## Environment Variables

| Variable | Default | Container | Read By Code? |
|----------|---------|-----------|:---:|
| `EMBEDDING_REALTIME_ENABLED` | `"true"` | G2N | ✅ |
| `EMBEDDING_BATCH_ENABLED` | `"true"` | G2N | ✅ |
| `GRAPHSAGE_INFERENCE_BATCH_LIMIT` | `500` | G2N | ✅ |
| `GRAPHSAGE_INFERENCE_CRON` | `"0 */6 * * *"` | — | ❌ (doc only) |
| `GRAPHSAGE_RETRAIN_CRON` | `"0 2 */3 * *"` | — | ❌ (doc only) |

> **Note**: Cron schedules are driven by `orchestration.schedule_definitions` SQL table, not env vars.

## SQL Required (Post-Deploy)

```sql
-- Run in Supabase SQL Editor
INSERT INTO orchestration.schedule_definitions
    (schedule_name, cron_expression, flow_name, run_config)
VALUES
    ('embedding_backfill_15m', '*/15 * * * *', 'neo4j_embedding_backfill', '{}'),
    ('graphsage_retrain_3d', '0 2 */3 * *', 'neo4j_graphsage_retrain', '{}'),
    ('graphsage_inference_6h', '0 */6 * * *', 'neo4j_graphsage_inference', '{}')
ON CONFLICT (schedule_name) DO NOTHING;
```

Then run: `python -m orchestrator.cli schedule --register`

## Testing

```bash
# 1. Verify retrain creates model
python -m orchestrator.cli neo4j graphsage-retrain

# 2. Create a test node → verify inference picks it up
python -m orchestrator.cli neo4j graphsage-inference

# 3. Verify model in GDS catalog
# In Neo4j Browser: CALL gds.model.list() YIELD modelName RETURN *
```
