import logging
from contextlib import asynccontextmanager
import threading
from time import perf_counter
from uuid import UUID, uuid4

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from psycopg_pool import ConnectionPool

from prometheus_client import make_asgi_app

from app.api.routes.payments import router as payments_router
from app.config import get_settings
from app.events.outbox_worker import run_worker
from app.events.publisher import KafkaEventPublisher
from app.models.payment import HealthResponse
from app.metrics import http_requests_total, http_request_duration_seconds

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

    publisher = None
    worker_thread = None
    stop_event = threading.Event()

    try:
        pool.open()
        pool.wait(timeout=10)
        app.state.db_pool = pool

        logger.info("Database connection pool created")

        publisher = KafkaEventPublisher()
        # app.state.event_publisher = publisher

        # logger.info("Kafka event publisher created")

        worker_thread = threading.Thread(
            target=run_worker,
            kwargs={
                "pool": pool,
                "publisher": publisher,
                "stop_event": stop_event,
            },
            name="OutboxWorkerThread",
            daemon=False,
        )
        worker_thread.start()

        logger.info("Outbox worker thread started")

        yield

    finally:
        # if publisher is not None:
        #     publisher.close()
        #     logger.info("Kafka event publisher closed")

        stop_event.set()

        if worker_thread is not None:
            worker_thread.join(timeout=15)

            if worker_thread.is_alive():
                logger.error("Outbox worker thread did not terminate within timeout")

        if publisher is not None:
            try:
                publisher.close()
            except Exception:
                logger.exception("kafka_publisher_close_failed")

        pool.close()
        logger.info("Database connection pool closed")

app = FastAPI(
    title=settings.app_name,
    version="1.0.0",
    lifespan=lifespan,
)

metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

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

    duration_seconds = perf_counter() - started

    http_requests_total.labels(
        method=request.method,
        path=request.url.path,
        status_code=str(response.status_code),
    ).inc()

    http_request_duration_seconds.labels(
        method=request.method,
        path=request.url.path,
    ).observe(duration_seconds)

    duration_ms = int(duration_seconds * 1000)

    logger.info(
        "http_request_completed",
        extra={
            "request_id": str(request.state.request_id),
            "trace_id": str(request.state.trace_id),
            "method": request.method,
            "path": request.url.path,
            "status_code": response.status_code,
            "duration_ms": duration_ms,
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