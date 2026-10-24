from uuid import UUID

from psycopg_pool import ConnectionPool

from app.models.incident import (
    IncidentDetectionRequest,
    IncidentDetectionResponse,
    IncidentInvestigationRequest,
    IncidentInvestigationResponse,
)
from app.services.incident_service import IncidentService

async def detect_incident_handler(
    payload: IncidentDetectionRequest,
    pool: ConnectionPool,
) -> IncidentDetectionResponse:
    service = IncidentService(pool)
    return await service.detect_incident(payload)

async def investigate_incident_handler(
    incident_id: UUID,
    payload: IncidentInvestigationRequest,
    pool: ConnectionPool,
) -> IncidentInvestigationResponse:
    service = IncidentService(pool)
    return await service.investigate_incident(incident_id, payload)