CREATE TABLE IF NOT EXISTS event_outbox (
    event_id UUID PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,

    status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'PUBLISHED', 'FAILED')),

    attempts INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ,
    last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_event_outbox_pending
    ON event_outbox (created_at)
    WHERE status = 'PENDING';