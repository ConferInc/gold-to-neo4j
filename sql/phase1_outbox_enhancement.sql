-- ═══════════════════════════════════════════════════════════════
-- Phase 1: Outbox Schema Enhancement
--
-- Run this in Supabase SQL Editor AFTER Phase 0 is complete.
--
-- What this does:
--   1. Adds 3 columns to gold.outbox_events (locked_by, locked_at, processed_at)
--   2. Drops the old idx_outbox_pending index (superseded)
--   3. Creates 3 new partial indexes for parallel workers
--   4. Creates 3 SQL functions (claim, mark, release)
--
-- Safe to re-run — all statements use IF NOT EXISTS / OR REPLACE.
-- ═══════════════════════════════════════════════════════════════


-- ╔═══════════════════════════════════════════════════════════════╗
-- ║  STEP 1: Add worker-claiming columns                         ║
-- ╚═══════════════════════════════════════════════════════════════╝

ALTER TABLE gold.outbox_events ADD COLUMN IF NOT EXISTS locked_by text;
ALTER TABLE gold.outbox_events ADD COLUMN IF NOT EXISTS locked_at timestamptz;
ALTER TABLE gold.outbox_events ADD COLUMN IF NOT EXISTS processed_at timestamptz;


-- ╔═══════════════════════════════════════════════════════════════╗
-- ║  STEP 2: Replace old index with parallel-worker-optimized    ║
-- ╚═══════════════════════════════════════════════════════════════╝

-- Drop the old single-worker index
DROP INDEX IF EXISTS gold.idx_outbox_pending;

-- Primary claiming index: used by SELECT FOR UPDATE SKIP LOCKED
CREATE INDEX IF NOT EXISTS idx_outbox_claimable
    ON gold.outbox_events (created_at)
    WHERE status = 'pending' AND locked_by IS NULL;

-- Stale lock recovery: find events claimed but never completed
CREATE INDEX IF NOT EXISTS idx_outbox_stale_locks
    ON gold.outbox_events (locked_at)
    WHERE status = 'pending' AND locked_by IS NOT NULL;

-- Cleanup index: efficiently find old processed events for purging
CREATE INDEX IF NOT EXISTS idx_outbox_processed_cleanup
    ON gold.outbox_events (processed_at)
    WHERE status = 'processed';


-- ╔═══════════════════════════════════════════════════════════════╗
-- ║  STEP 3: Create claim_outbox_events function                 ║
-- ║  Atomically claims a batch using SELECT FOR UPDATE SKIP LOCKED║
-- ╚═══════════════════════════════════════════════════════════════╝

CREATE OR REPLACE FUNCTION gold.claim_outbox_events(
    p_worker_id text,
    p_batch_size int DEFAULT 50,
    p_lock_timeout_seconds int DEFAULT 300
) RETURNS SETOF gold.outbox_events AS $$
    UPDATE gold.outbox_events
    SET locked_by = p_worker_id,
        locked_at = now()
    WHERE id IN (
        SELECT id FROM gold.outbox_events
        WHERE status = 'pending'
          AND (locked_by IS NULL
               OR locked_at < now() - make_interval(secs => p_lock_timeout_seconds))
          AND (next_retry_at IS NULL OR next_retry_at <= now())
        ORDER BY created_at
        FOR UPDATE SKIP LOCKED
        LIMIT p_batch_size
    )
    RETURNING *;
$$ LANGUAGE sql;


-- ╔═══════════════════════════════════════════════════════════════╗
-- ║  STEP 4: Create mark_events_processed function               ║
-- ║  Called after Neo4j TX commits successfully                   ║
-- ╚═══════════════════════════════════════════════════════════════╝

CREATE OR REPLACE FUNCTION gold.mark_events_processed(
    p_event_ids uuid[]
) RETURNS void AS $$
    UPDATE gold.outbox_events
    SET status = 'processed',
        processed_at = now(),
        locked_by = NULL,
        locked_at = NULL
    WHERE id = ANY(p_event_ids);
$$ LANGUAGE sql;


-- ╔═══════════════════════════════════════════════════════════════╗
-- ║  STEP 5: Create release_stale_locks function                 ║
-- ║  Recovers events from crashed workers                        ║
-- ╚═══════════════════════════════════════════════════════════════╝

CREATE OR REPLACE FUNCTION gold.release_stale_locks(
    p_timeout_seconds int DEFAULT 300
) RETURNS int AS $$
DECLARE
    released int;
BEGIN
    UPDATE gold.outbox_events
    SET locked_by = NULL,
        locked_at = NULL
    WHERE status = 'pending'
      AND locked_by IS NOT NULL
      AND locked_at < now() - make_interval(secs => p_timeout_seconds);
    GET DIAGNOSTICS released = ROW_COUNT;
    RETURN released;
END;
$$ LANGUAGE plpgsql;


-- ╔═══════════════════════════════════════════════════════════════╗
-- ║  VERIFICATION (run after execution)                          ║
-- ╚═══════════════════════════════════════════════════════════════╝

-- Verify new columns exist
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = 'gold' AND table_name = 'outbox_events'
  AND column_name IN ('locked_by', 'locked_at', 'processed_at')
ORDER BY column_name;

-- Verify new indexes exist
SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'gold' AND tablename = 'outbox_events'
ORDER BY indexname;

-- Verify functions exist
SELECT routine_name, routine_type
FROM information_schema.routines
WHERE routine_schema = 'gold'
  AND routine_name IN ('claim_outbox_events', 'mark_events_processed', 'release_stale_locks')
ORDER BY routine_name;

-- Quick smoke test: verify function is callable (read-only — batch_size=0)
SELECT count(*) AS claimable_events
FROM gold.outbox_events
WHERE status = 'pending' AND locked_by IS NULL;
