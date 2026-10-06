from typing import Any
from uuid import UUID

from psycopg import Connection
from psycopg.types.json import Jsonb

from app.config import get_settings

MAX_ATTEMPTS = get_settings().outbox_max_attempts

def create_outbox_event(
    connection: Connection,
    *,
    event_id: UUID,
    aggregate_id: UUID,
    event_type: str,
    payload: dict[str, Any],
) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO event_outbox (event_id, aggregate_id, event_type, payload)
            VALUES (%s, %s, %s, %s)
            """,
            (event_id, aggregate_id, event_type, Jsonb(payload)),
        )


def get_pending_events(
    connection: Connection,
    *,
    limit: int = 50,
) -> list[dict[str, Any]]:
    from psycopg.rows import dict_row

    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            """
            SELECT event_id, aggregate_id, event_type, payload,
                   attempts
            FROM event_outbox
            WHERE status = 'PENDING'
            ORDER BY created_at
            LIMIT %s
            """,
            (limit,),
        )
        return cursor.fetchall()

def mark_event_attempted(
    connection: Connection,
    *,
    event_id: UUID,
) -> None:
    connection.execute(
        """
        UPDATE event_outbox
        SET attempts = attempts + 1
        WHERE event_id = %s
        """,
        (event_id,),
    )

def mark_event_published(
    connection: Connection,
    *,
    event_id: UUID,
) -> None:
    connection.execute(
        """
        UPDATE event_outbox
        SET status = 'PUBLISHED',
            published_at = NOW(),
            last_error = NULL
        WHERE event_id = %s
        """,
        (event_id,),
    )

def mark_event_failed(
    connection: Connection,
    *,
    event_id: UUID,
    error: str,
) -> None:
    connection.execute(
        """
        UPDATE event_outbox
        SET
            last_error = %s,
            status = CASE
                WHEN attempts >= %s THEN 'FAILED'
                ELSE 'PENDING'
            END
        WHERE event_id = %s
        """,
        (
            error,
            MAX_ATTEMPTS,
            event_id,
        ),
    )

def get_pending_event_count(
    connection,
) -> int:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM event_outbox
            WHERE status = 'PENDING'
            """
        )

        result = cursor.fetchone()

    return result[0]