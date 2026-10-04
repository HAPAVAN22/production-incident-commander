import json
import logging
import os

from kafka import KafkaConsumer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

logger = logging.getLogger(__name__)

def main() -> None:
    topic = os.getenv(
        "KAFKA_PAYMENT_TOPIC",
        "payment-events",
    )

    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092",
    )

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id="payment-event-consumer",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    logger.info(
        "payment_event_consumer_started",
        extra={"topic": topic},
    )

    try:
        for message in consumer:
            event = message.value

            logger.info(
                "payment_event_received",
                extra={
                    "event_type": event.get("event_type"),
                    "transaction_id": event.get("transaction_id"),
                    "customer_id": event.get("customer_id"),
                    "amount": event.get("amount"),
                    "status": event.get("status"),
                    "timestamp": event.get("timestamp"),
                    "request_id": event.get("request_id"),
                    "trace_id": event.get("trace_id"),
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                },
            )
    except KafkaError as e:
        logger.error("Error occurred while consuming payment events", extra={"error": str(e)})
        raise
    finally:
        consumer.close()

if __name__ == "__main__":
    main()