-- ═══════════════════════════════════════════════════════════════
-- Outbox Maintenance & Monitoring
--
-- Run this in Supabase SQL Editor AFTER Phases 0-4 are complete.
--
-- What this does:
--   1. Creates a scheduled cleanup function for processed events
--   2. Creates monitoring views for operational visibility
--   3. Sets up pg_cron jobs (if available) for automated maintenance
--
-- Safe to re-run — all statements use OR REPLACE / IF NOT EXISTS.
-- ═══════════════════════════════════════════════════════════════


-- ╔═══════════════════════════════════════════════════════════════╗
-- ║  1. Cleanup Function — Purge old processed events            ║
-- ╚═══════════════════════════════════════════════════════════════╝

-- Removes processed events older than retention_days.
-- Default: 7 days. Returns count of purged events.

CREATE OR REPLACE FUNCTION gold.cleanup_processed_events(
    p_retention_days int DEFAULT 7
) RETURNS int AS $$
DECLARE
    purged int;
BEGIN
    DELETE FROM gold.outbox_events
    WHERE status = 'processed'
      AND processed_at < now() - make_interval(days => p_retention_days);
    GET DIAGNOSTICS purged = ROW_COUNT;
    RETURN purged;
END;
$$ LANGUAGE plpgsql;


-- ╔═══════════════════════════════════════════════════════════════╗
-- ║  2. Outbox Health View — Dashboard-ready monitoring          ║
-- ╚═══════════════════════════════════════════════════════════════╝

CREATE OR REPLACE VIEW gold.outbox_health AS
SELECT
    status,
    count(*) AS event_count,
    min(created_at) AS oldest_event,
    max(created_at) AS newest_event,
    count(*) FILTER (WHERE locked_by IS NOT NULL) AS locked_count,
    count(*) FILTER (WHERE needs_review = true) AS needs_review_count,
    avg(EXTRACT(EPOCH FROM (processed_at - created_at)))::int AS avg_latency_seconds
FROM gold.outbox_events
GROUP BY status
ORDER BY status;


-- ╔═══════════════════════════════════════════════════════════════╗
-- ║  3. Worker Activity View — Per-worker stats                  ║
-- ╚═══════════════════════════════════════════════════════════════╝

CREATE OR REPLACE VIEW gold.outbox_worker_activity AS
SELECT
    locked_by AS worker_id,
    count(*) AS currently_locked,
    min(locked_at) AS oldest_lock,
    max(locked_at) AS newest_lock,
    EXTRACT(EPOCH FROM (now() - min(locked_at)))::int AS oldest_lock_age_seconds
FROM gold.outbox_events
WHERE status = 'pending' AND locked_by IS NOT NULL
GROUP BY locked_by
ORDER BY oldest_lock;


-- ╔═══════════════════════════════════════════════════════════════╗
-- ║  4. Outbox Throughput View — Events/minute over last hour    ║
-- ╚═══════════════════════════════════════════════════════════════╝

CREATE OR REPLACE VIEW gold.outbox_throughput AS
SELECT
    date_trunc('minute', processed_at) AS minute,
    count(*) AS events_processed,
    count(DISTINCT table_name) AS tables_touched,
    avg(EXTRACT(EPOCH FROM (processed_at - created_at)))::int AS avg_latency_s
FROM gold.outbox_events
WHERE status = 'processed'
  AND processed_at > now() - interval '1 hour'
GROUP BY date_trunc('minute', processed_at)
ORDER BY minute DESC;


-- ╔═══════════════════════════════════════════════════════════════╗
-- ║  5. Failed Events Summary — For triage/alerting              ║
-- ╚═══════════════════════════════════════════════════════════════╝

CREATE OR REPLACE VIEW gold.outbox_failed_summary AS
SELECT
    table_name,
    error_code,
    count(*) AS failure_count,
    max(created_at) AS last_failure,
    min(error_message) AS sample_error
FROM gold.outbox_events
WHERE status = 'failed'
GROUP BY table_name, error_code
ORDER BY failure_count DESC;


-- ╔═══════════════════════════════════════════════════════════════╗
-- ║  6. pg_cron Jobs (Optional — requires pg_cron extension)     ║
-- ╚═══════════════════════════════════════════════════════════════╝

-- Uncomment these if pg_cron is available in your Supabase plan.
-- They provide automated maintenance without external cron:

-- Job 1: Purge processed events older than 7 days (runs daily at 3 AM UTC)
-- SELECT cron.schedule(
--     'outbox_cleanup',
--     '0 3 * * *',
--     $$SELECT gold.cleanup_processed_events(7)$$
-- );

-- Job 2: Release stale locks every 5 minutes
-- SELECT cron.schedule(
--     'outbox_release_stale_locks',
--     '*/5 * * * *',
--     $$SELECT gold.release_stale_locks(300)$$
-- );


-- ╔═══════════════════════════════════════════════════════════════╗
-- ║  VERIFICATION                                                ║
-- ╚═══════════════════════════════════════════════════════════════╝

-- Check cleanup function exists
SELECT routine_name FROM information_schema.routines
WHERE routine_schema = 'gold' AND routine_name = 'cleanup_processed_events';

-- Check views exist
SELECT table_name FROM information_schema.views
WHERE table_schema = 'gold'
  AND table_name IN ('outbox_health', 'outbox_worker_activity', 'outbox_throughput', 'outbox_failed_summary')
ORDER BY table_name;

-- Quick health check
SELECT * FROM gold.outbox_health;
