#Event-Driven Embedding & Indexing Plan

## 1. Goals & Requirements

**Goals:**
- Keep semantic (genAI) and structural (GraphSAGE) embeddings in sync with Supabase → Neo4j ingestion.
- Support real-time updates for user preferences, restrictions, and interactions.
- Keep Graph RAG recommendations based on up-to-date embeddings.

**Requirements:**
- Semantic embeddings: create for new nodes with text; do not recompute on relationship changes.
- Structural embeddings: create for new nodes and recompute when relationships change.
- Embedding refresh must not block ingestion.
- Failed embedding refreshes must be retriable.
- Real-time path must stay low-latency for user-facing recommendations.

---

## 2. Current State vs Target State

**Current:**
- Supabase (gold schema) → batch jobs + realtime worker → Neo4j.
- Outbox table `outbox_events` for real-time changes.
- `customer_realtime` worker processes pending events and upserts into Neo4j.
- Embedding and indexing are not refreshed after graph writes.

**Target:**
- Ingestion writes to Neo4j and emits "embedding refresh" events.
- Dedicated workers consume those events and refresh semantic and structural embeddings.
- Neo4j vector indexes stay updated via normal property writes (no separate index sync step).

---

## 3. Event Model

### 3.1 Outbox Event Extension

Extend `outbox_events` so embedding workers know what to refresh:

| Field | Purpose |
|-------|---------|
| `event_type` | e.g. `CUSTOMER_ADDED_ALLERGY`, `RECIPE_CREATED` |
| `table_name` | Source table in Supabase |
| `row_id` | Source row ID |
| `payload` | JSON with entity IDs and relationship endpoints |
| `status` | `pending` / `processed` / `failed` |
| `embedding_refresh_required` | NEW: `boolean`, true if embeddings should be refreshed |
| `embedding_payload` | NEW: JSON describing semantic/structural targets |

### 3.2 Embedding Payload Schema

When `embedding_refresh_required = true`, `embedding_payload` is structured as:

```json
{
  "semantic": {
    "enabled": true,
    "node_ids": ["uuid-1", "uuid-2"],
    "labels": ["Recipe"]
  },
  "structural": {
    "enabled": true,
    "node_ids": ["uuid-1", "uuid-2"],
    "labels": ["B2C_Customer"],
    "relationship_affected": true
  }
}
```

- `semantic.enabled`: whether to run semantic refresh.
- `semantic.node_ids`: node IDs needing semantic embedding.
- `structural.enabled`: whether to run structural refresh.
- `structural.node_ids`: node IDs needing GraphSAGE refresh (new or neighborhood changed).
- `structural.relationship_affected`: whether this is driven by a relationship change.

---

## 4. End-to-End Workflow

### 4.1 Real-Time Path (Customer Updates)

1. User updates preferences/allergies/restrictions → Supabase row insert/update.
2. DB trigger writes to `outbox_events` with `event_type`, `table_name`, `row_id`, `payload`, `embedding_refresh_required = true`, and `embedding_payload`.
3. `customer_realtime` worker fetches pending events, upserts nodes/relationships into Neo4j, marks ingestion as processed (or enqueues embedding events).
4. Embedding workers consume embedding events and, for each event, refresh semantic and/or structural embeddings per `embedding_payload`.
5. Neo4j vector indexes reflect new/updated embeddings via HNSW on embedding properties.
6. Graph RAG recommendations use up-to-date embeddings for similarity search.

### 4.2 Batch Path (Catalog Sync)

1. Batch job (recipes, ingredients, products, customers) runs on schedule.
2. Fetches rows with `updated_at >= last_checkpoint` from Supabase.
3. Upserts into Neo4j.
4. For each batch run, collect new/changed node IDs and labels and emit embedding events with `embedding_payload`.
5. Embedding workers process these events like real-time events.
6. Indexes update as embeddings are written.

---

## 5. Semantic Embedding Flow

### 5.1 Triggers

- New nodes with text properties (Recipe, Ingredient, Product, Cuisine, etc.).
- New B2C_Customer with profile text.
- Rare: node property updates where embedded text changes.

### 5.2 Process

1. Worker receives event with `embedding_payload.semantic.enabled = true` and `node_ids`.
2. For each node ID, load the node from Neo4j and build text per label (e.g. Recipe: title + description + meal_type + cuisine; Ingredient: name + category + nutrients).
3. Call genAI (e.g. `genai.vector.encode()` or batch API) on that text.
4. Write embedding to the node property (e.g. `semanticEmbedding`) via Cypher `SET`.
5. Vector index on `semanticEmbedding` updates automatically.

### 5.3 Text Assembly Examples

- **Recipe**: `{title} {description} {meal_type} {cuisine} {instructions}`
- **Ingredient**: `{name} {category} calories {calories} protein {protein_g}g`
- **Product**: `{name} {brand} {description}`
- **B2C_Customer**: `health goal {health_goal} activity {activity_level} dietary {intolerances}`

---

## 6. Structural Embedding Flow (GraphSAGE)

### 6.1 Triggers

- New nodes of any label.
- Existing nodes whose relationships (incoming or outgoing) are added or removed.

### 6.2 Process

1. Worker receives event with `embedding_payload.structural.enabled = true` and `node_ids`.
2. Optionally collect affected IDs (e.g. 2-hop neighborhood) if needed.
3. Project a Neo4j GDS subgraph that includes the affected nodes and their neighborhood.
4. Run `gds.beta.graphSage.predict.mutate()` on that subgraph using the stored model.
5. Mutate the `graphSageEmbedding` property on nodes in the subgraph.
6. Vector index on `graphSageEmbedding` updates automatically.

### 6.3 Subgraph Scope

- **New nodes**: project graph where `n.id IN $node_ids` (including isolated nodes if needed).
- **Relationship change**: project graph where nodes are in `$node_ids` or within 1–2 hops of them; run predict on those nodes.

---

## 7. Indexing Behavior

Neo4j vector indexes (HNSW) are maintained automatically when the indexed property changes:

- No separate "index sync" step is required.
- `SET n.semanticEmbedding = $vec` and `SET n.graphSageEmbedding = $vec` trigger index updates.

Maintain separate vector indexes for:

- Each label with semantic embeddings (e.g. `CREATE VECTOR INDEX ... FOR (n:Recipe) ON n.semanticEmbedding`).
- Existing `b2c_customer_graphsage` (or equivalent) for `graphSageEmbedding` on B2C_Customer.
- Additional vector indexes for other labels with structural embeddings as needed.

---

## 8. Failure Handling & Retries

### Ingestion

- If Neo4j upsert fails → event remains `pending` and is retried by existing logic.
- Ingestion does not wait for embedding workers.

### Embedding Workers

- If genAI or GDS fails → do not mark the embedding event as done; keep it for retry.
- Retry with backoff; after max retries, mark `embedding_status = failed` and optionally `needs_review`.
- Non-retryable errors (e.g. invalid payload) → mark failed immediately.

### Idempotency

- Semantic: re-running on the same node overwrites the embedding; safe.
- Structural: GraphSAGE predict is deterministic for a given graph; re-running is safe.

---

## 9. How This Serves Your Requirements

| Requirement | How It's Met |
|-------------|--------------|
| Real-time preferences/restrictions | Events emitted on change; embedding workers run shortly after; structural embeddings updated. |
| Customized recommendations | GraphSAGE reflects new relationships (allergies, views, ratings); semantic reflects content. |
| Semantic only for new nodes | Event routing sets `semantic.enabled` only for NODE_CREATED (or similar) events. |
| Structural on relationship changes | Event routing sets `structural.enabled` for relationship events; payload includes affected node IDs. |
| Non-blocking ingestion | Ingestion and embedding are decoupled; embedding failures do not block ingestion. |
| Retriable failures | Embedding events can be retried until success or manual review. |
| Graph RAG accuracy | Both semantic and structural embeddings stay aligned with the current graph and content. |

---

## 10. Rollout Steps

1. Extend outbox schema with embedding fields (`embedding_refresh_required`, `embedding_payload`, `embedding_status`).
2. Update Supabase triggers or application code to populate `embedding_refresh_required` and `embedding_payload`.
3. Implement node-type rules (e.g. config mapping event types and labels to semantic/structural flags).
4. Implement semantic worker (text assembly, genAI, Neo4j write).
5. Implement structural worker (GDS subgraph, predict, mutate).
6. Run embedding workers alongside ingestion (same process or separate).
7. Monitor latency, failure rates, and retries; tune batch sizes and concurrency.
