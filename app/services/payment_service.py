import logging
from time import perf_counter
from uuid import UUID, uuid4

from fastapi import HTTPException, status
from psycopg_pool import ConnectionPool

from app.events.publisher import EventPublisher
from app.models.payment import PaymentCreate, PaymentResponse
from app.repositories import event_repository, payment_repository

logger = logging.getLogger(__name__)

class PaymentService:
    def __init__(
        self,
        pool: ConnectionPool,
        publisher: EventPublisher,
    ) -> None:
        self.pool = pool
        self.publisher = publisher
        
    def create_payment(
        self,
        payload: PaymentCreate,
        *,
        request_id: UUID,
        trace_id: UUID,
    ) -> PaymentResponse:
        transaction_id = uuid4()
        start_time = perf_counter()

        with self.pool.connection() as connection:
            payment = payment_repository.create_payment(
                connection,
                transaction_id=transaction_id,
                payload=payload,
            )

            latency_ms = int((perf_counter() - start_time) * 1000)

            event_repository.create_payment_event(
                connection,
                transaction_id=transaction_id,
                request_id=request_id,
                trace_id=trace_id,
                latency_ms=latency_ms,
            )

        self.publisher.publish_payment_created(
            transaction_id=payment.transaction_id,
            customer_id=payment.customer_id,
            amount=str(payment.amount),
            currency=payment.currency,
        )

        logger.info(
            "payment_created",
            extra={
                "transaction_id": str(payment.transaction_id),
                "request_id": str(request_id),
                "trace_id": str(trace_id),
            },
        )

        return payment
    
    def get_payment(
        self,
        transaction_id: UUID,
    ) -> PaymentResponse:
        with self.pool.connection() as connection:
            payment = payment_repository.get_payment(
                connection,
                transaction_id=transaction_id,
            )

        if not payment:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Payment with transaction_id {transaction_id} not found",
            )

        return payment