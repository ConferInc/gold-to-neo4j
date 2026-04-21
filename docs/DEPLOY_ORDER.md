# Deployment Order — Pipeline Hardening

This document describes the correct deployment sequence for the orchestration
pipeline hardening changes (Phases 0–5).

> ⚠️ **Critical**: Deploy **code first**, then run SQL scripts. The outbox workers
> must be running before the old webhook triggers are removed, ensuring continuous
> event processing.

---

## Pre-Deployment Checklist

- [ ] Coolify env vars set for both services (see below)
- [ ] Access to Supabase SQL Editor
- [ ] Verify current `main` branch is stable and deployed

## Required Environment Variables

### Orchestrator Service
```env
NEO4J_REALTIME_WORKERS=3
NEO4J_WORKER_BATCH_SIZE=50
NEO4J_WORKER_LOCK_TIMEOUT=600
NEO4J_REALTIME_AUTOSTART=true
```

### Gold-to-Neo4j Service
```env
NEO4J_REALTIME_WORKERS=1
NEO4J_WORKER_BATCH_SIZE=50
NEO4J_REALTIME_POLL_INTERVAL=5
NEO4J_WORKER_LOCK_TIMEOUT=600
```

---

## Deployment Steps

### Step 1: Set Environment Variables
Add the new env vars to both Coolify services **before** deploying code.
This ensures the workers start with correct configuration on first boot.

### Step 2: Deploy Code (Both Services)
Deploy the `sourav-orchestration` branch to both services via Coolify.

- **Orchestrator**: Workers auto-start on API boot (`NEO4J_REALTIME_AUTOSTART=true`)
- **Gold-to-Neo4j**: Standalone worker starts via Docker entrypoint

**Verify**: Check logs for `outbox_worker_started` entries (should see N workers).

### Step 3: Run Phase 1 SQL (Outbox Enhancement)
Run `sql/phase1_outbox_enhancement.sql` in Supabase SQL Editor.

This adds `locked_by`, `locked_at`, `processed_at` columns and creates the
`claim_outbox_events`, `mark_events_processed`, and `release_stale_locks` functions.

**Verify**: Run the verification queries at the bottom of the script.

### Step 4: Run Phase 0 SQL (Drop Old Triggers)
Run `sql/remove_webhook_triggers.sql` in Supabase SQL Editor.

> **Why after Step 3?** The outbox workers from Step 2 are already live and
> processing events via the outbox path. Dropping the old webhook triggers
> just removes the redundant webhook path — the outbox path is already active.

**Verify**: `SELECT * FROM information_schema.triggers WHERE trigger_schema = 'gold';`
should show only the outbox INSERT trigger remaining.

### Step 5: Run Phase 5 SQL (Monitoring & Maintenance)
Run `sql/outbox_maintenance.sql` in Supabase SQL Editor.

Creates monitoring views and optional pg_cron jobs. If pg_cron is available,
uncomment and run the cron job definitions.

**Verify**: `SELECT * FROM gold.outbox_health;`

---

## Post-Deployment Verification

```sql
-- 1. Check outbox is healthy
SELECT * FROM gold.outbox_health;

-- 2. Check workers are active
SELECT * FROM gold.outbox_worker_activity;

-- 3. Check throughput (wait 5-10 min after deploy)
SELECT * FROM gold.outbox_throughput;

-- 4. Check for failures
SELECT * FROM gold.outbox_failed_summary;
```

---

## Rollback Plan

If issues are detected after deployment:

1. **Re-enable triggers**: Run the original `sql/triggers.sql` to recreate dropped triggers
2. **Stop workers**: Set `NEO4J_REALTIME_AUTOSTART=false` and redeploy
3. **Revert code**: Deploy the `main` branch
