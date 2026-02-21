# Architecture Overview (Ingestion Pipeline)

This repository implements a **hybrid ingestion pipeline** to move data from the **gold layer** into **Neo4j**.
Customer updates are handled in near real‑time, while catalog data is synchronized on a scheduled interval.

---

## 1) End‑to‑End Workflow 

### Realtime Customer Updates
1. Customer action occurs in Supabase (like/save/profile update).
2. DB trigger writes an event into an **outbox table**.
3. A **worker service** polls the outbox every few seconds.
4. Worker maps the event to the graph model and **upserts** into Neo4j.
5. Event is marked **processed** (or retried on failure).

### Scheduled Catalog Updates
1. A batch job runs on a fixed schedule.
2. It reads rows updated since the last run.
3. It upserts nodes/relationships into Neo4j.
4. The checkpoint timestamp is saved to a state file.

---

## 2) Repo Structure 

```
/services
  /customer_realtime
    service.py
    handlers.py
  /recipes_batch
    service.py
  /ingredients_batch
    service.py
  /products_batch
    service.py

/shared
  supabase_client.py
  neo4j_client.py
  upsert.py
  retry.py
  logging.py
  models.py

/config
  customers.yaml
  recipes.yaml
  ingredients.yaml
  products.yaml

/sql
  outbox_table.sql
  triggers.sql

/state
  .sync_state.json

/docs
  ingestion_overview.md
```

Why this structure:
- Each layer is independently triggerable.
- Shared code prevents duplication.
- Logic changes are made once and reused.

---

## 3) Realtime Customer Pipeline 

### New Event Flow
- DB trigger writes a row into `outbox_events` with:
  - `event_type`, `table_name`, `row_id`, `payload`, `created_at`, `status`

### Worker Responsibilities
- Polls `outbox_events` every few seconds.
- Claims `pending` events and marks `processing`.
- Upserts into Neo4j using idempotent `MERGE`.
- Marks events `processed`, or retries on failure.

### Example Event
```
event_type: RECIPE_LIKED
payload: { user_id: 42, recipe_id: 777 }
```
Worker action in Neo4j:
```
MERGE (u:Customer {id: 42})
MERGE (r:Recipe {id: 777})
MERGE (u)-[:LIKED]->(r)
```

---

## 4) Scheduled Catalog Pipeline (Incremental Batch)

### Batch Behavior
- Uses layer‑specific config (`recipes.yaml`, `ingredients.yaml`, `products.yaml`).
- Fetches rows with `updated_at >= last_seen`.
- Upserts nodes/relationships into Neo4j.
- Saves last processed timestamp in `.sync_state.json`.

### Why Incremental
- Efficient for large tables.
- Supports recovery after failures.
- Safe with multiple writes.

---

## 5) Failure & Retry Strategy

### Realtime (Customer Layer)
- If Neo4j write fails, event remains in outbox and is retried.
- After max retries, event is marked `failed` for manual inspection.

### Batch Jobs (Catalog Layers)
- If a batch fails, the checkpoint is not advanced.
- Next run retries from the last known good timestamp.

---

## 6) Containerization Plan

**Two images are sufficient:**
1. **Customer Layer image** — always running.
2. **Catalog Layers image** — runs per layer on schedule.

Both images reuse shared code, with different entry points.

---

## 7) Event‑to‑Graph Mapping (Deterministic)

The worker does **not guess** relationships. It uses a mapping table:
- `RECIPE_LIKED` → `(Customer)-[:LIKED]->(Recipe)`
- `RECIPE_SAVED` → `(Customer)-[:SAVED]->(Recipe)`
- `ALLERGY_ADDED` → `(Customer)-[:IS_ALLERGIC]->(Allergen)`

This mapping is centralized in the worker logic or config.

---

## 8) New Customer Insert: How It Works

**Case:** A new customer is added in Supabase.

**Flow:**
1. Insert into `b2c_customers` triggers an outbox event (e.g., `CUSTOMER_CREATED`).
2. Worker picks up the event and executes:
   - `MERGE (c:Customer {id: <new_id>})`
   - `SET c += customer_properties` (if included in payload)
3. Related rows (profiles, allergies, conditions) will create relationships when their own events arrive.

This guarantees the customer node exists in Neo4j as soon as the customer is created.

---

## 9) Overview 
A single repo hosts layer‑specific ingestion services with shared logic. Realtime customer actions are captured via DB triggers into an event table and processed by a worker that upserts Neo4j within seconds. Catalog data (recipes/products/ingredients) is updated by scheduled batch jobs using incremental sync and a state file. Reliability is ensured through idempotent writes, retry logic, and durable outbox events. Containerization is minimal: one image for realtime worker and one for batch jobs.
