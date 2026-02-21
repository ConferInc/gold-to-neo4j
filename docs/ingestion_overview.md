# Ingestion Overview

This repo implements a hybrid ingestion pipeline from Supabase (gold layer) to Neo4j.

## Realtime Customer Updates
- Supabase triggers write events into `outbox_events`.
- The realtime worker polls the outbox and upserts nodes/relationships into Neo4j.
- Events are marked processed or retried on failure.

## Scheduled Catalog Updates
- Batch jobs run on a schedule per layer (recipes, ingredients, products).
- Each job reads rows updated since the last checkpoint.
- The checkpoint is stored in `state/.sync_state.json`.

## Reliability
- Idempotent upserts using `MERGE`.
- Retry logic for transient failures.
- Checkpoint-based recovery for batch jobs.
