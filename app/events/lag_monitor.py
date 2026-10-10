
import logging
import os
import time

from prometheus_client import start_http_server
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError

from app.metrics import kafka_consumer_lag

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

    group_id = os.getenv(
        "KAFKA_CONSUMER_GROUP_ID",
        "payment-event-consumer-new",
    )

    poll_interval = float(
        os.getenv("LAG_MONITOR_INTERVAL_SECONDS", "5")
    )

    start_http_server(8002)

    # This client reads offsets; it does not process messages.
    monitor = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=False,
        request_timeout_ms=10000,
    )

    known_partitions = set()

    logger.info(
        "consumer_lag_monitor_started topic=%s group_id=%s",
        topic,
        group_id,
    )

    try:
        while True:
            try:
                partition_ids = monitor.partitions_for_topic(topic)

                if partition_ids is None:
                    logger.warning("topic_not_available topic=%s", topic)
                    time.sleep(poll_interval)
                    continue

                partitions = {
                    TopicPartition(topic, partition_id)
                    for partition_id in partition_ids
                }

                # Manual assignment does not join the group as a
                # subscribed message-processing consumer.
                monitor.assign(list(partitions))

                end_offsets = monitor.end_offsets(partitions)
                beginning_offsets = monitor.beginning_offsets(partitions)

                current_partitions = {
                    (partition.topic, str(partition.partition))
                    for partition in partitions
                }

                # Remove metrics for partitions that no longer exist.
                for old_topic, old_partition in (
                    known_partitions - current_partitions
                ):
                    kafka_consumer_lag.remove(
                        old_topic,
                        old_partition,
                    )

                known_partitions = current_partitions

                for partition in partitions:
                    committed_offset = monitor.committed(partition)

                    if committed_offset is None:
                        # Match the consumer's earliest reset policy
                        # until this group has a committed offset.
                        committed_offset = beginning_offsets[partition]

                    lag = max(
                        0,
                        end_offsets[partition] - committed_offset,
                    )

                    kafka_consumer_lag.labels(
                        topic=partition.topic,
                        partition=str(partition.partition),
                    ).set(lag)

                    logger.info(
                        "consumer_lag topic=%s partition=%s lag=%s",
                        partition.topic,
                        partition.partition,
                        lag,
                    )

            except (KafkaError, Exception):
                logger.exception("consumer_lag_monitor_update_failed")

            time.sleep(poll_interval)

    finally:
        monitor.close()


if __name__ == "__main__":
    main()
