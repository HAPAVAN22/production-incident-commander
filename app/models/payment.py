from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

class PaymentMethod(StrEnum):
    UPI = "UPI"
    CARD = "CARD"
    NETBANKING = "NETBANKING"
    WALLET = "WALLET"

class PaymentStatus(StrEnum):
    PENDING = "PENDING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"

class PaymentCreate(BaseModel):
    customer_id: UUID
    amount: Decimal = Field(
        gt=Decimal("0"),
        max_digits=12,
        decimal_places=2,
    )
    currency: str = Field(
        min_length=3,
        max_length=3,
        examples=["INR"],
    )
    payment_method: PaymentMethod

    @field_validator("currency")
    @classmethod
    def normalize_currency(cls, value: str) -> str:
        value = value.upper()

        if not value.isalpha() or len(value) != 3:
            raise ValueError(
                "Currency must be a 3-letter alphabetic code"
            )
        
        return value
    
class PaymentResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    transaction_id: UUID
    customer_id: UUID
    amount: Decimal
    currency: str
    payment_status: PaymentStatus
    payment_method: PaymentMethod
    created_at: datetime
    updated_at: datetime


class HealthResponse(BaseModel):
    status: str
    database: str