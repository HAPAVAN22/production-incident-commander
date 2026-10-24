from enum import StrEnum
from uuid import UUID
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, ConfigDict

class IncidentStatus(StrEnum):
    OPEN = "OPEN"
    INVESTIGATING = "INVESTIGATING"
    RESOLVED = "RESOLVED"

class IncidentSeverity(StrEnum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"

class IncidentEvidenceType(StrEnum):
    DIAGNOSTIC_RESULT = "DIAGNOSTIC_RESULT"
    DIAGNOSTIC_COLLECTION_FAILED = "DIAGNOSTIC_COLLECTION_FAILED"


class IncidentCreate(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    title: str = Field(min_length=1, max_length=200)
    description: str = Field(min_length=1)
    severity: IncidentSeverity
    affected_service: str = Field(min_length=1)

class IncidentResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    title: str
    description: str
    severity: IncidentSeverity
    status: IncidentStatus
    affected_service: str

    root_cause: str | None
    resolution_notes: str | None

    started_at: datetime
    acknowledged_at: datetime | None
    resolved_at: datetime | None

    created_at: datetime
    updated_at: datetime

class IncidentStatusUpdate(BaseModel):
    status: IncidentStatus
    root_cause: str | None = None
    resolution_notes: str | None = None

class IncidentEvidenceCreate(BaseModel):
    event_type: IncidentEvidenceType
    source: str = Field(min_length=1)
    details: dict[str, Any] = Field(default_factory=dict)

class IncidentEvidenceResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    incident_id: UUID
    event_type: IncidentEvidenceType
    source: str
    details: dict[str, Any]
    observed_at: datetime
    created_at: datetime

class IncidentDetectionRequest(BaseModel):
    request: str | None = None

class IncidentInvestigationRequest(BaseModel):
    request: str | None = None

class IncidentDetectionResponse(BaseModel):
    checked_at: datetime
    report: str

class IncidentInvestigationResponse(BaseModel):
    incident_id: UUID
    incident_status: IncidentStatus
    report: str