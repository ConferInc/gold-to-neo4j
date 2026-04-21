-- ═══════════════════════════════════════════════════════════════
-- gold.outbox_events — Transactional outbox for realtime Neo4j sync
--
-- Each row captures a single domain event (INSERT/UPDATE/DELETE)
-- from a gold schema table, ready for the realtime worker to poll.
--
-- Parallel workers claim events atomically via SKIP LOCKED.
-- See: claim_events.sql for the claim/mark/release functions.
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS gold.outbox_events (
    id            uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type    text        NOT NULL,   -- e.g. "b2c_customers.insert"
    table_name    text        NOT NULL,   -- source table name
    row_id        text        NOT NULL,   -- primary key of the changed row
    payload       jsonb,                  -- full row snapshot (NEW for insert/update)
    status        text        NOT NULL DEFAULT 'pending',
    retry_count   int         NOT NULL DEFAULT 0,
    next_retry_at timestamptz,
    error_code    text,
    error_message text,
    needs_review  boolean     NOT NULL DEFAULT false,
    created_at    timestamptz NOT NULL DEFAULT now(),
    -- ── Parallel worker columns (Phase 1) ──
    locked_by     text,                   -- worker_id that claimed this event
    locked_at     timestamptz,            -- when the claim was made
    processed_at  timestamptz             -- when processing completed
);

-- ── Indexes for parallel worker claiming ──────────────────────

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

-- Index for retry scheduling
CREATE INDEX IF NOT EXISTS idx_outbox_retry
    ON gold.outbox_events (next_retry_at)
    WHERE status = 'pending' AND next_retry_at IS NOT NULL;

