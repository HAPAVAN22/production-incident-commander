from uuid import UUID

from psycopg import Connection

def customer_exists(
    connection: Connection,
    customer_id: UUID,
) -> bool:
    row = connection.execute(
        "SELECT 1 FROM customers WHERE customer_id = %s",
        (customer_id,),
    ).fetchone()

    return row is not None