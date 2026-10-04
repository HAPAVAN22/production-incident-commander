from collections.abc import Iterator
from fastapi import Request
from psycopg import Connection
from psycopg_pool import ConnectionPool

def get_pool(request: Request) -> ConnectionPool:
    return request.app.state.db_pool

def get_connection(pool: ConnectionPool) -> Iterator[Connection]:
    with pool.connection() as connection:
        yield connection