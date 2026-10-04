from uuid import UUID

from psycopg import Connection

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
    row = connection.execute(
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
    ).fetchone()

    return PaymentResponse(**row)

def get_payment(
    connection: Connection,
    transaction_id: UUID,
) -> PaymentResponse | None:
    row = connection.execute(
        f"""
        SELECT {_PAYMENT_COLUMNS}
        FROM payment_orders
        WHERE transaction_id = %s
        """,
        (transaction_id,),
    ).fetchone()

    return PaymentResponse(**row) if row else None