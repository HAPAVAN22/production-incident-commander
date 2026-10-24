from uuid import UUID

from fastapi import APIRouter, Depends, status
from psycopg_pool import ConnectionPool

from app.api.handlers.incident_handler import (
    detect_incident_handler,
    investigate_incident_handler,
)
from app.db.connection import get_pool
from app.models.incident import (
    IncidentDetectionRequest,
    IncidentDetectionResponse,
    IncidentInvestigationRequest,
    IncidentInvestigationResponse,
)

router = APIRouter(
    prefix="/incidents",
    tags=["incidents"],
)

@router.post(
    "/detect",
    response_model=IncidentDetectionResponse,
    status_code=status.HTTP_200_OK,
    summary="Detect production incidents",
)
async def detect_incidents(
    payload: IncidentDetectionRequest,
    pool: ConnectionPool = Depends(get_pool),
) -> IncidentDetectionResponse:
    return await detect_incident_handler(payload, pool)


@router.post(
    "/{incident_id}/investigate",
    response_model=IncidentInvestigationResponse,
    status_code=status.HTTP_200_OK,
    summary="Investigate a production incident",
)
async def investigate_incident(
    incident_id: UUID,
    payload: IncidentInvestigationRequest,
    pool: ConnectionPool = Depends(get_pool),
) -> IncidentInvestigationResponse:
    return await investigate_incident_handler(
        incident_id,
        payload,
        pool,
    )