import json
import logging
import os
import time

from prometheus_client import start_http_server

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from app.metrics import (
    kafka_messages_consumed_total,
    kafka_message_processing_failures_total,
    kafka_message_processing_latency_seconds,
    kafka_consumer_lag
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

logger = logging.getLogger(__name__)

def update_consumer_lag(
    consumer: KafkaConsumer,
    topic: str,
) -> None:
    partitions = consumer.assignment()

    if not partitions:
        return

    end_offsets = consumer.end_offsets(partitions)

    for partition in partitions:
        committed_offset = consumer.committed(partition)

        if committed_offset is None:
            continue

        end_offset = end_offsets[partition]

        lag = max(0, end_offset - committed_offset)

        kafka_consumer_lag.labels(
            topic=topic,
            partition=str(partition.partition),
        ).set(lag)

def main() -> None:
    start_http_server(8001)

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
        group_id="payment-event-consumer-new",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    logger.info(
        "payment_event_consumer_started",
        extra={"topic": topic},
    )

    try:
        for message in consumer:
            started = time.perf_counter()

            try:
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

                processing_duration = time.perf_counter() - started

                kafka_message_processing_latency_seconds.labels(
                    topic=message.topic,
                ).observe(processing_duration)

                kafka_messages_consumed_total.labels(
                    topic=message.topic,
                ).inc()

                consumer.commit()

                update_consumer_lag(
                    consumer,
                    topic,
                )
            except KafkaError as e:
                kafka_message_processing_failures_total.labels(
                    topic=message.topic,
                ).inc()

                logger.exception(
                    "kafka_message_processing_failed",
                    extra={
                        "topic": message.topic,
                        "partition": message.partition,
                        "offset": message.offset,
                    },
                )
    except KafkaError:
        logger.exception("kafka_consumer_error")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()