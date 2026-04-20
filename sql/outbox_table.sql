-- ═══════════════════════════════════════════════════════════════
-- gold.outbox_events — Transactional outbox for realtime Neo4j sync
--
-- Each row captures a single domain event (INSERT/UPDATE/DELETE)
-- from a gold schema table, ready for the realtime worker to poll.
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
    created_at    timestamptz NOT NULL DEFAULT now()
);

-- Partial index for the poller: only pending events that are ready
CREATE INDEX IF NOT EXISTS idx_outbox_pending
    ON gold.outbox_events (created_at)
    WHERE status = 'pending';

-- Index for retry scheduling
CREATE INDEX IF NOT EXISTS idx_outbox_retry
    ON gold.outbox_events (next_retry_at)
    WHERE status = 'pending' AND next_retry_at IS NOT NULL;

-- Optional: auto-cleanup events older than 30 days (run via pg_cron)
-- DELETE FROM gold.outbox_events
-- WHERE status IN ('processed', 'failed') AND created_at < now() - interval '30 days';
