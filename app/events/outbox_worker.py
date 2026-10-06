import logging
import threading
import time

from psycopg_pool import ConnectionPool

from app.config import get_settings
from app.events.publisher import EventPublisher
from app.repositories import outbox_repository
from app.metrics import (
    outbox_events_published_total,
    outbox_events_failed_total,
    outbox_publish_latency_seconds,
    outbox_events_pending,
)

logger = logging.getLogger(__name__)

settings = get_settings()

POLL_INTERVAL_SECONDS = settings.outbox_poll_interval_seconds
BATCH_SIZE = settings.outbox_batch_size

def process_pending_events(
    pool: ConnectionPool,
    publisher: EventPublisher,
) -> int:
    with pool.connection() as connection:
        events = outbox_repository.get_pending_events(
            connection,
            limit=BATCH_SIZE,
        )

    pending_count = outbox_repository.get_pending_event_count(
            connection,
        )

    outbox_events_pending.set(pending_count)

    published_count = 0

    for event in events:
        event_id = event["event_id"]
        aggregate_id = event["aggregate_id"]
        event_type = event["event_type"]

        try:
            with pool.connection() as connection:
                outbox_repository.mark_event_attempted(
                    connection,
                    event_id=event_id,
                )

            publisher.publish_outbox_event(
                event_id=event_id,
                aggregate_id=aggregate_id,
                payload=event["payload"],
            )

            publish_duration = perf_counter() - publish_started

            outbox_publish_latency_seconds.labels(
                event_type=event_type,
            ).observe(publish_duration)

            outbox_events_published_total.labels(
                event_type=event_type,
            ).inc()

            with pool.connection() as connection:
                outbox_repository.mark_event_published(
                    connection,
                    event_id=event_id,
                )

            published_count += 1

        except Exception as e:
            logger.error(
                "Failed to publish event %s: %s",
                event_id,
                str(e),
            )

            outbox_events_failed_total.labels(
                event_type=event["event_type"],
            ).inc()

            try:
                with pool.connection() as connection:
                    outbox_repository.mark_event_failed(
                        connection,
                        event_id=event_id,
                        error=str(e),
                    )

            except Exception:
                logger.exception(
                    "outbox_failure_recording_failed",
                    extra={"event_id": str(event_id)},
                )

    return published_count

def run_worker(
    pool: ConnectionPool,
    publisher: EventPublisher,
    stop_event: threading.Event,
) -> None:
    poll_interval = POLL_INTERVAL_SECONDS

    logger.info("Starting outbox worker with poll interval %s seconds", poll_interval)

    while not stop_event.is_set():
        try:
            published_count = process_pending_events(pool, publisher)
            if published_count > 0:
                logger.info("Published %d events from outbox", published_count)
        except Exception:
            logger.exception("outbox_worker_error")

        stop_event.wait(poll_interval)

    logger.info("Outbox worker stopped")