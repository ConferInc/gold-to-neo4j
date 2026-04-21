-- ═══════════════════════════════════════════════════════════════
-- Outbox Worker Functions — Parallel Event Claiming
--
-- Three functions for the SKIP LOCKED parallel worker pattern:
--
--   1. gold.claim_outbox_events(worker_id, batch_size, lock_timeout)
--      → Atomically claims a batch of pending events for a worker.
--      → Uses SELECT FOR UPDATE SKIP LOCKED — zero contention.
--
--   2. gold.mark_events_processed(event_ids)
--      → Marks a batch of events as processed after Neo4j TX commit.
--
--   3. gold.release_stale_locks(timeout_seconds)
--      → Recovers events from crashed workers whose locks expired.
--
-- These functions are called via Supabase RPC from the Python
-- OutboxWorker class in services/customer_realtime/service.py.
-- ═══════════════════════════════════════════════════════════════


-- ╔═══════════════════════════════════════════════════════════════╗
-- ║  1. Claim Events — Atomic Batch Claiming                     ║
-- ╚═══════════════════════════════════════════════════════════════╝

-- Usage: SELECT * FROM gold.claim_outbox_events('w-abc123-00', 50, 300);
--
-- Guarantees:
--   • No two workers ever claim the same event (SKIP LOCKED)
--   • Stale locks from crashed workers are reclaimed after timeout
--   • Events with next_retry_at in the future are skipped
--   • Results are ordered by created_at (FIFO)

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
-- ║  2. Mark Events Processed — Batch Completion                 ║
-- ╚═══════════════════════════════════════════════════════════════╝

-- Usage: SELECT gold.mark_events_processed(ARRAY['uuid1', 'uuid2']::uuid[]);
--
-- Called after a Neo4j transaction successfully commits.
-- Clears the lock fields and sets processed_at timestamp.

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
-- ║  3. Release Stale Locks — Crash Recovery                     ║
-- ╚═══════════════════════════════════════════════════════════════╝

-- Usage: SELECT gold.release_stale_locks(300);
--
-- If a worker crashes mid-batch, its claimed events stay locked.
-- This function releases locks older than the timeout, allowing
-- other workers to reclaim them. Can be run periodically via
-- pg_cron or called from the worker startup routine.

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
