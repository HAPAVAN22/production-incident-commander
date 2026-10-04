from uuid import UUID, uuid4
from psycopg import Connection

def create_payment_event(
    connection: Connection,
    *,
    transaction_id: UUID,
    request_id: UUID,
    trace_id: UUID,
    latency_ms: int,
) -> UUID:
    event_id = uuid4()

    connection.execute(
        """
        INSERT INTO service_events (
            event_id, transaction_id, service_name, event_type,
            event_status, latency_ms, error_code, error_message,
            request_id, trace_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            event_id,
            transaction_id,
            "payment-api",
            "PAYMENT_CREATED",
            "SUCCESS",
            max(0, latency_ms),
            None,
            None,
            request_id,
            trace_id,
        ),
    )

    return event_id