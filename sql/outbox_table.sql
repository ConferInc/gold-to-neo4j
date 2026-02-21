-- Placeholder for outbox table DDL
-- TODO: replace with actual schema.

CREATE TABLE IF NOT EXISTS outbox_events (
    id uuid PRIMARY KEY,
    event_type text NOT NULL,
    table_name text NOT NULL,
    row_id text NOT NULL,
    payload jsonb,
    status text NOT NULL DEFAULT 'pending',
    retry_count int NOT NULL DEFAULT 0,
    next_retry_at timestamptz,
    error_code text,
    error_message text,
    needs_review boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now()
);
