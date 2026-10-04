from uuid import UUID

from psycopg import Connection
from psycopg.rows import dict_row

from app.models.payment import PaymentCreate, PaymentResponse

_PAYMENT_COLUMNS="""
    transaction_id, customer_id, amount, currency, payment_status,
    payment_method, created_at, updated_at
"""

def create_payment(
    connection: Connection,
    transaction_id: UUID,
    payload: PaymentCreate,
) -> PaymentResponse:
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            f"""
            INSERT INTO payment_orders (
                transaction_id, customer_id, amount, currency,
                payment_status, payment_method
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING {_PAYMENT_COLUMNS}
            """,
            (
                transaction_id,
                payload.customer_id,
                payload.amount,
                payload.currency,
                "PENDING",
                payload.payment_method.value,
            ),
        )

        row = cursor.fetchone()

    if row is None:
        raise RuntimeError("Payment insert returned no row")

    return PaymentResponse(**row)

def get_payment(
    connection: Connection,
    transaction_id: UUID,
) -> PaymentResponse | None:
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            f"""
            SELECT {_PAYMENT_COLUMNS}
            FROM payment_orders
            WHERE transaction_id = %s
            """,
            (transaction_id,),
        )

        row = cursor.fetchone()

    return PaymentResponse(**row) if row else None