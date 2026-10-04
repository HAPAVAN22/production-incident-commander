from uuid import UUID

from fastapi import Request
from psycopg_pool import ConnectionPool

from app.events.publisher import LoggingEventPublisher
from app.models.payment import PaymentCreate, PaymentResponse
from app.services.payment_service import PaymentService

def _service(pool: ConnectionPool) -> PaymentService:
    return PaymentService(
        pool=pool,
        publisher=LoggingEventPublisher(),
    )

def create_payment_handler(
    payload: PaymentCreate,
    request: Request,
    pool: ConnectionPool,
) -> PaymentResponse:
    return _service(pool).create_payment(
        payload,
        request_id=request.state.request_id,
        trace_id=request.state.trace_id,
    )

def get_payment_handler(
    transaction_id: UUID,
    pool: ConnectionPool,
) -> PaymentResponse:
    return _service(pool).get_payment(transaction_id)