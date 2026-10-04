import json
import os
import logging
from datetime import datetime, timezone
from typing import Any, Protocol
from uuid import UUID

from kafka import KafkaProducer, future
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)

class EventPublisher(Protocol):
    def publish_payment_created(
        self,
        *,
        transaction_id: UUID,
        customer_id: UUID,
        amount: str,
        currency: str,
        status: str,
        request_id: UUID,
        trace_id: UUID,
    ) -> None:
        ...

    def publish_outbox_event(
        self,
        *,
        event_id: UUID,
        aggregate_id: UUID,
        payload: dict[str, Any],
    ) -> None:
        ...

    def close(self) -> None:
        ...

class KafkaEventPublisher:
    def __init__(self) -> None:
        self.topic = os.getenv(
            "KAFKA_PAYMENT_TOPIC",
            "payment-events",
        )

        bootstrap_servers = os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "localhost:9092",
        )

        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=lambda k: str(k).encode("utf-8"),
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            acks="all",
            retries=5,
        )

    def publish_payment_created(
            self,
            *,
            transaction_id: UUID,
            customer_id: UUID,
            amount: str,
            currency: str,
            status: str,
            request_id: UUID,
            trace_id: UUID,
    ) -> None:
        event = {
            "event_type": "payment.created",
            "transaction_id": str(transaction_id),
            "customer_id": str(customer_id),
            "amount": amount,
            "currency": currency,
            "status": status,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "request_id": str(request_id),
            "trace_id": str(trace_id),
        }

        try:
            future = self.producer.send(
                self.topic,
                key=str(transaction_id),
                value=event,
            )

            metadata = future.get(timeout=10)

            logger.info(
                "payment_event_published",
                extra={
                    "event_type": event["event_type"],
                    "transaction_id": event["transaction_id"],
                    "topic": metadata.topic,
                    "partition": metadata.partition,
                    "offset": metadata.offset,
                },
            )

        except (KafkaError, TimeoutError):
            logger.exception(
                "payment_event_publish_failed",
                extra={
                    "transaction_id": str(transaction_id),
                    "request_id": str(request_id),
                    "trace_id": str(trace_id),
                },
            )
            raise

    def publish_outbox_event(
        self,
        *,
        event_id: UUID,
        aggregate_id: UUID,
        payload: dict[str, Any],
    ) -> None:
        try:
            message = {
                "event_id": str(event_id),
                "aggregate_id": str(aggregate_id),
                "payload": payload,
            }

            future = self.producer.send(
                self.topic,
                value=message,
            )

            metadata = future.get(timeout=10)

            logger.info(
                "outbox_event_published",
                extra={
                    "event_id": event_id,
                    "aggregate_id": aggregate_id,
                    "event_type": payload.get("event_type"),
                    "topic": metadata.topic,
                    "partition": metadata.partition,
                    "offset": metadata.offset,
                },
            )

        except (KafkaError, TimeoutError):
            logger.exception(
                "outbox_event_publish_failed",
                extra={
                    "event_type": payload.get("event_type"),
                    "aggregate_id": payload.get("aggregate_id"),
                },
            )
            raise

    def close(self) -> None:
        """Flush pending records and close the producer."""
        self.producer.flush(timeout=10)
        self.producer.close(timeout=10)

# class LoggingEventPublisher:
#     """Temporary publisher until Kafka is added."""

    # def publish_payment_created(
    #     self,
    #     *,
    #     transaction_id: UUID,
    #     customer_id: UUID,
    #     amount: str,
    #     currency: str,
    # ) -> None:
    #     logger.info(
    #         "payment_event_ready",
    #         extra={
    #             "event_type": "PAYMENT_CREATED",
    #             "transaction_id": str(transaction_id),
    #             "customer_id": str(customer_id),
    #             "amount": amount,
    #             "currency": currency,
    #         },
    #     )