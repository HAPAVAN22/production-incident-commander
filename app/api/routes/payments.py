import os

from uuid import UUID

from fastapi import(
    APIRouter,
    Depends,
    Request,
    Response,
    status,
)

from psycopg_pool import ConnectionPool

from app.api.handlers.payment_handler import (
    create_payment_handler,
    get_payment_handler,
)
from app.db.connection import get_connection, get_pool
from app.models.payment import PaymentCreate, PaymentResponse

router = APIRouter(
    prefix="/payments",
    tags=["payments"],
)

SIMULATION_ENABLED = (
    os.getenv("ENABLE_FAILURE_SIMULATION", "false").lower()
    == "true"
)

@router.post(
    "",
    response_model=PaymentResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_payment(
    payload: PaymentCreate,
    request: Request,
    response: Response,
    pool: ConnectionPool = Depends(get_pool),
) -> PaymentResponse:

    if (
        SIMULATION_ENABLED
        and request.headers.get("X-Simulate-Failure", "").lower() == "true"
    ):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Simulated payment service failure",
        )
    
    payment = create_payment_handler(payload, request, pool)

    response.headers["X-Request-ID"] = str(request.state.request_id)
    response.headers["X-Trace-ID"] = str(request.state.trace_id)

    return payment

@router.get(
    "/{transaction_id}",
    response_model=PaymentResponse,
)
def read_payment(
    transaction_id: UUID,
    pool: ConnectionPool = Depends(get_pool),
) -> PaymentResponse:
    return get_payment_handler(transaction_id, pool)