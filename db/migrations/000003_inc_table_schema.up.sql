
-- Main incident record
CREATE TABLE incidents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    title TEXT NOT NULL,
    description TEXT NOT NULL,

    severity TEXT NOT NULL
        CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),

    status TEXT NOT NULL DEFAULT 'OPEN'
        CHECK (
            status IN (
                'OPEN',
                'INVESTIGATING',
                'RESOLVED'
            )
        ),

    affected_service TEXT NOT NULL,

    root_cause TEXT,
    resolution_notes TEXT,

    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    acknowledged_at TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE INDEX idx_incidents_status_created
    ON incidents (status, created_at DESC);

CREATE INDEX idx_incidents_severity_created
    ON incidents (severity, created_at DESC);


-- Evidence collected during incident investigation
CREATE TABLE incident_evidence (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    incident_id UUID NOT NULL
        REFERENCES incidents(id) ON DELETE CASCADE,

    event_type TEXT NOT NULL,
    source TEXT NOT NULL,

    details JSONB NOT NULL DEFAULT '{}'::jsonb,

    observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE INDEX idx_incident_evidence_incident_time
    ON incident_evidence (incident_id, observed_at DESC);
