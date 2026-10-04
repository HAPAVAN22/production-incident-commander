import logging
from datetime import datetime, timezone
from time import perf_counter
from uuid import UUID, uuid4

from fastapi import HTTPException, status
from psycopg_pool import ConnectionPool

from app.models.payment import PaymentCreate, PaymentResponse
from app.repositories import (
    customer_repository,
    event_repository,
    outbox_repository,
    payment_repository,
)
logger = logging.getLogger(__name__)

class PaymentService:
    def __init__(
        self,
        pool: ConnectionPool,
    ) -> None:
        self.pool = pool
        
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
            if not customer_repository.customer_exists(
                connection,
                payload.customer_id,
            ):
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Customer not found",
                )
            
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

            event_id = uuid4()

            event_payload = {
                "event_id": str(event_id),
                "event_type": "payment.created",
                "transaction_id": str(payment.transaction_id),
                "customer_id": str(payment.customer_id),
                "amount": str(payment.amount),
                "currency": payment.currency,
                "status": payment.payment_status,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "request_id": str(request_id),
                "trace_id": str(trace_id),
            }

            outbox_repository.create_outbox_event(
                connection,
                event_id=event_id,
                aggregate_id=payment.transaction_id,
                event_type="payment.created",
                payload=event_payload,
            )

        
        # self.publisher.publish_payment_created(
        #     transaction_id=payment.transaction_id,
        #     customer_id=payment.customer_id,
        #     amount=str(payment.amount),
        #     currency=payment.currency,
        #     status=payment.payment_status,
        #     request_id=request_id,
        #     trace_id=trace_id,
        # )

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