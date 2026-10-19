from typing import Any
from uuid import UUID

from psycopg import Connection
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from app.config import get_settings

settings = get_settings()

def create_incident(
    connection: Connection,
    title: str,
    description: str,
    severity: str,
    affected_service: str,
) -> dict[str, Any]:
    severity = severity.upper()

    if severity not in {"LOW", "MEDIUM", "HIGH", "CRITICAL"}:
        raise ValueError("Invalid incident severity")

    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            """
            INSERT INTO incidents (
                title,
                description,
                severity,
                affected_service
            )
            VALUES (%s, %s, %s, %s)
            RETURNING *
            """,
            (title, description, severity, affected_service),
        )
        row = cursor.fetchone()

        if row is None:
            raise RuntimeError("Incident creation returned no row")

        return dict(row)

def list_active_incidents(
    connection: Connection,
    limit: int = 20
) -> list[dict[str, Any]]:
    limit = max(1, min(limit, 100))

    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            """
            SELECT *
            FROM incidents
            WHERE status IN ('OPEN', 'INVESTIGATING')
            ORDER BY
                CASE severity
                    WHEN 'CRITICAL' THEN 1
                    WHEN 'HIGH' THEN 2
                    WHEN 'MEDIUM' THEN 3
                    WHEN 'LOW' THEN 4
                END,
                created_at DESC
            LIMIT %s
            """,
            (limit,),
        )

        return [dict(row) for row in cursor.fetchall()]

def get_incident(
    connection: Connection,
    incident_id: UUID
) -> dict[str, Any] | None:
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            """
            SELECT *
            FROM incidents
            WHERE id = %s
            """,
            (incident_id,),
        )
        row = cursor.fetchone()

        if row is None:
            return None

        incident = dict(row)

        cursor.execute(
            """
            SELECT *
            FROM incident_evidence
            WHERE incident_id = %s
            ORDER BY observed_at DESC
            """,
            (incident_id,),
        )
        incident["evidence"] = [
            dict(evidence) for evidence in cursor.fetchall()
        ]

        return incident

def add_incident_evidence(
    connection: Connection,
    incident_id: UUID,
    event_type: str,
    source: str,
    details: dict[str, Any]
) -> dict[str, Any]:
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            """
            INSERT INTO incident_evidence (
                incident_id,
                event_type,
                source,
                details
            )
            VALUES (%s, %s, %s, %s)
            RETURNING *
            """,
            (
                incident_id,
                event_type,
                source,
                Jsonb(details),
            ),
        )
        row = cursor.fetchone()

        if row is None:
            raise RuntimeError("Evidence creation returned no row")

        return dict(row)

def update_incident_status(
    connection: Connection,
    incident_id: UUID,
    status: str,
    root_cause: str | None = None,
    resolution_notes: str | None = None
) -> dict[str, Any] | None:
    status = status.upper()

    if status not in {"OPEN", "INVESTIGATING", "RESOLVED"}:
        raise ValueError("Invalid incident status")

    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            """
            UPDATE incidents
            SET
                status = %s,
                root_cause = COALESCE(%s, root_cause),
                resolution_notes =
                    COALESCE(%s, resolution_notes),
                acknowledged_at = CASE
                    WHEN %s = 'INVESTIGATING'
                    THEN COALESCE(acknowledged_at, NOW())
                    ELSE acknowledged_at
                END,
                resolved_at = CASE
                    WHEN %s = 'RESOLVED' THEN NOW()
                    ELSE NULL
                END,
                updated_at = NOW()
            WHERE id = %s
            RETURNING *
            """,
            (
                status,
                root_cause,
                resolution_notes,
                status,
                status,
                incident_id,
            ),
        )
        row = cursor.fetchone()

        return dict(row) if row else None