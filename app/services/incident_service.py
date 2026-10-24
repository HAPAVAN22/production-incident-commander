import asyncio
import logging
from datetime import datetime, timezone
from uuid import UUID

from fastapi import HTTPException, status
from psycopg_pool import ConnectionPool

from app.incident_agent import detect_and_investigate, investigate
from app.models.incident import (
    IncidentDetectionRequest,
    IncidentDetectionResponse,
    IncidentInvestigationRequest,
    IncidentInvestigationResponse,
    IncidentResponse,
    IncidentStatus,
    IncidentStatusUpdate,
)
from app.repositories import incident_repository

logger = logging.getLogger(__name__)

class IncidentService:
    def __init__(self, pool: ConnectionPool) -> None:
        self.pool = pool

    async def detect_incident(
        self,
        payload: IncidentDetectionRequest,
    ) -> IncidentDetectionResponse:
        prompt = payload.request or (
            "Detect actionable production incidents using available telemetry. "
            "Inspect existing active incidents, avoid duplicates, and use "
            "evidence to decide whether further investigation is warranted."
        )

        try:
            report = await detect_and_investigate(prompt)
        except Exception as exc:
            logger.exception("Incident detection failed")
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Incident detection agent failed.",
            ) from exc

        return IncidentDetectionResponse(
            checked_at=datetime.now(timezone.utc),
            report=report,
        )

    async def investigate_incident(
        self,
        incident_id: UUID,
        payload: IncidentInvestigationRequest,
    ) -> IncidentInvestigationResponse:
        incident_row = await asyncio.to_thread(
            self._get_incident,
            incident_id,
        )

        if incident_row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Incident {incident_id} was not found.",
            )

        incident = IncidentResponse.model_validate(incident_row)

        if incident.status == IncidentStatus.RESOLVED:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Cannot investigate an incident that is already resolved.",
            )

        if incident.status == IncidentStatus.OPEN:
            updated_row = await asyncio.to_thread(
                self._update_incident_status,
                incident_id,
                IncidentStatusUpdate(
                    status=IncidentStatus.INVESTIGATING,
                ),
            )

            if updated_row is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Incident {incident_id} was not found.",
                )

        prompt = payload.request or (
            "Investigate this incident using evidence-driven diagnostic "
            "selection. Review existing evidence, choose relevant diagnostics "
            "dynamically, persist new evidence, and distinguish confirmed "
            "facts from hypotheses. Do not resolve the incident."
        )

        try:
            report = await investigate(str(incident_id), prompt)
        except Exception as exc:
            logger.exception(
                "Incident investigation failed",
                extra={"incident_id": str(incident_id)},
            )
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Incident investigation agent failed.",
            ) from exc

        # Read back durable state; do not trust the agent's prose for status.
        updated_row = await asyncio.to_thread(
            self._get_incident,
            incident_id,
        )

        if updated_row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Incident {incident_id} was not found.",
            )

        updated_incident = IncidentResponse.model_validate(updated_row)

        return IncidentInvestigationResponse(
            incident_id=updated_incident.id,
            incident_status=updated_incident.status,
            report=report,
        )

    def _get_incident(
        self,
        incident_id: UUID,
    ) -> dict | None:
        with self.pool.connection() as connection:
            return incident_repository.get_incident(
                connection,
                incident_id,
            )

    def _update_incident_status(
        self,
        incident_id: UUID,
        payload: IncidentStatusUpdate,
    ) -> dict | None:
        with self.pool.connection() as connection:
            return incident_repository.update_incident_status(
                connection,
                incident_id,
                payload,
            )