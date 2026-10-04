from uuid import UUID

from fastapi import Request
from psycopg_pool import ConnectionPool

from app.events.publisher import EventPublisher
from app.models.payment import PaymentCreate, PaymentResponse
from app.services.payment_service import PaymentService

def _service(
        pool: ConnectionPool,
        #publisher: EventPublisher
    ) -> PaymentService:
    return PaymentService(
        pool=pool,
        #publisher=publisher,
    )

def create_payment_handler(
    payload: PaymentCreate,
    request: Request,
    pool: ConnectionPool,
) -> PaymentResponse:

    #publisher: EventPublisher = request.app.state.event_publisher
    
    return _service(
        pool=pool,
        #publisher=publisher,
    ).create_payment(
        payload,
        request_id=request.state.request_id,
        trace_id=request.state.trace_id,
    )

def get_payment_handler(
    transaction_id: UUID,
    pool: ConnectionPool,
) -> PaymentResponse:
    return _service(
        pool=pool
    ).get_payment(transaction_id)