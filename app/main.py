import logging
from contextlib import asynccontextmanager
from time import perf_counter
from uuid import UUID, uuid4

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from psycopg_pool import ConnectionPool

from app.api.routes.payments import router as payments_router
from app.config import get_settings
from app.models.payment import HealthResponse

settings = get_settings()

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = ConnectionPool(
        conninfo=settings.database_url,
        min_size=settings.db_pool_min_size,
        max_size=max(
            settings.db_pool_max_size,
            settings.db_pool_min_size,
        ),
        open=False,
        kwargs={"autocommit": False}
    )

    pool.open()
    pool.wait(timeout=10)
    app.state.db_pool = pool

    logger.info(
        "Database connection pool created",
        extra={"app_env": settings.app_env},
    )

    try:
        yield
    finally:
        pool.close()
        logger.info("Database connection pool closed")

app = FastAPI(
    title=settings.app_name,
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(payments_router, prefix="/api/v1")

@app.middleware("http")
async def request_context_middleware(
    request: Request,
    call_next,
):
    def parse_uuid_header(header_name: str) -> UUID:
        raw = request.headers.get(header_name)

        if raw:
            try:
                return UUID(raw)
            except ValueError:
                pass
        
        return uuid4()
    
    request.state.request_id = parse_uuid_header("x-request-id")
    request.state.trace_id = parse_uuid_header("x-trace-id")

    started = perf_counter()

    try:
        response = await call_next(request)
    except Exception:
        logger.exception(
            "unhandled_request_error",
            extra={
                "request_id": str(request.state.request_id),
                "path": request.url.path,
            },
        )

        response = JSONResponse(
            status_code=500,
            content={
                "detail": "Internal server error",
                "request_id": str(request.state.request_id),
            },
        )

    response.headers["X-Request-ID"] = str(request.state.request_id)
    response.headers["X-Trace-ID"] = str(request.state.trace_id)

    logger.info(
        "http_request_completed",
        extra={
            "request_id": str(request.state.request_id),
            "trace_id": str(request.state.trace_id),
            "method": request.method,
            "path": request.url.path,
            "status_code": response.status_code,
            "duration_ms": int(
                (perf_counter() - started) * 1000
            ),
        },
    )

    return response

@app.get(
    "/health",
    response_model=HealthResponse,
    tags=["operations"],
)
def health_check(request: Request) -> HealthResponse:
    pool: ConnectionPool = request.app.state.db_pool

    with pool.connection() as connection:
        connection.execute("SELECT 1")

    return HealthResponse(
        status="ok",
        database="reachable",
    )

@app.get("/", tags=["operations"])
def root() -> dict[str, str]:
    return {
        "name": settings.app_name,
        "docs": "/docs",
        "health": "/health",
    }