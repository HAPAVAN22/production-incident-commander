import logging
from typing import Protocol
from uuid import UUID

logger = logging.getLogger(__name__)

class EventPublisher(Protocol):
    def publish_payment_created(
        self,
        *,
        transaction_id: UUID,
        customer_id: UUID,
        amount: str,
        currency: str,
    ) -> None:
        ...

class LoggingEventPublisher:
    """Temporary publisher until Kafka is added."""

    def publish_payment_created(
        self,
        *,
        transaction_id: UUID,
        customer_id: UUID,
        amount: str,
        currency: str,
    ) -> None:
        logger.info(
            "payment_event_ready",
            extra={
                "event_type": "PAYMENT_CREATED",
                "transaction_id": str(transaction_id),
                "customer_id": str(customer_id),
                "amount": amount,
                "currency": currency,
            },
        )