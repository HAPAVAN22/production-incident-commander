from typing import Any
from uuid import UUID

from psycopg import Connection
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from app.config import get_settings

from app.models.incident import (
    IncidentCreate,
    IncidentEvidenceCreate,
    IncidentEvidenceResponse,
    IncidentResponse,
    IncidentSeverity,
    IncidentStatus,
    IncidentStatusUpdate,
)

settings = get_settings()

def create_incident(
    connection: Connection,
    payload: IncidentCreate,
) -> dict[str, Any]:

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
            (
                payload.title,
                payload.description,
                payload.severity.value,
                payload.affected_service,
            ),
        )
        row = cursor.fetchone()

        if row is None:
            raise RuntimeError("Incident creation returned no row")

        return IncidentResponse(**row).model_dump(mode="python")

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
            WHERE status IN (%s, %s)
            ORDER BY
                CASE severity
                    WHEN %s THEN 1
                    WHEN %s THEN 2
                    WHEN %s THEN 3
                    WHEN %s THEN 4
                END,
                created_at DESC
            LIMIT %s
            """,
            (
                IncidentStatus.OPEN.value,
                IncidentStatus.INVESTIGATING.value,
                IncidentSeverity.CRITICAL.value,
                IncidentSeverity.HIGH.value,
                IncidentSeverity.MEDIUM.value,
                IncidentSeverity.LOW.value,
                limit,
            ),
        )

        return [
            IncidentResponse(**row).model_dump(mode="python")
            for row in cursor.fetchall()
        ]

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

        incident = IncidentResponse(**row).model_dump(mode="python")
        
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
            IncidentEvidenceResponse(**evidence).model_dump(mode="python")
            for evidence in cursor.fetchall()
        ]

        return incident

def add_incident_evidence(
    connection: Connection,
    incident_id: UUID,
    payload: IncidentEvidenceCreate,
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
                payload.event_type.value,
                payload.source,
                Jsonb(payload.details),
            ),
        )
        row = cursor.fetchone()

        if row is None:
            raise RuntimeError("Evidence creation returned no row")

        return IncidentEvidenceResponse(**row).model_dump(mode="python")

def update_incident_status(
    connection: Connection,
    incident_id: UUID,
    payload: IncidentStatusUpdate,
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
                resolution_notes = COALESCE(%s, resolution_notes),
                acknowledged_at = CASE
                    WHEN %s = %s
                    THEN COALESCE(acknowledged_at, NOW())
                    ELSE acknowledged_at
                END,
                resolved_at = CASE
                    WHEN %s = %s THEN NOW()
                    ELSE NULL
                END,
                updated_at = NOW()
            WHERE id = %s
            RETURNING *
            """,
            (
                payload.status.value,
                payload.root_cause,
                payload.resolution_notes,
                payload.status.value,
                IncidentStatus.INVESTIGATING.value,
                payload.status.value,
                IncidentStatus.RESOLVED.value,
                incident_id,
            ),
        )
        row = cursor.fetchone()

        return IncidentResponse(**row).model_dump(mode="python")