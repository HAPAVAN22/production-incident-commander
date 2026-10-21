import logging
import os
import sys

from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from psycopg_pool import ConnectionPool
from psycopg.types.json import Jsonb

from mcp.server.fastmcp import FastMCP

from app.config import get_settings
from app.repositories import incident_repository
from app.mcp_utilities import (
    _json_safe,
    _db_query,
    _prometheus_query,
    _prometheus_metric_names,
    _instant_value
)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    stream=sys.stderr,
)

logger = logging.getLogger(
    "prod-inc-commander.mcp"
)

settings = get_settings()

db_pool = ConnectionPool(
    conninfo=settings.database_url,
    min_size=1,
    max_size=5,
    open=True,
)

PROMETHEUS_URL = os.getenv(
    "PROMETHEUS_URL",
    "http://localhost:9090",
).rstrip("/")


MCP_INSTRUCTIONS = """
Tools for investigating incidents in a payment platform.

Use incident details and existing evidence to decide which diagnostic tools
are relevant. Do not run every diagnostic automatically.

You may discover available Prometheus metrics and execute read-only PromQL
instant queries when predefined health summaries are insufficient.

Inspect actual tool results. Distinguish zero from missing data, unavailable
sources, and potentially stale metrics. Correlate independent evidence where
useful, and explain uncertainty.

Persist diagnostic observations against the existing incident using
record_incident_evidence. Never fabricate measurements or evidence IDs.

Do not execute remediation or resolve incidents autonomously.
"""

mcp = FastMCP(
    "Production Incident Commander",
    instructions=MCP_INSTRUCTIONS,
)

def collect_service_health(window: str = "5m") -> dict[str, Any]:
    """
    API availability, HTTP 5xx rate, p95 latency, and Kafka consumer lag.

    Args:
        window: Prometheus range window, e.g. 5m, 30m, 1h.
    """
    import re

    # Validate the window before interpolating it into PromQL.
    match = re.fullmatch(r"([1-9]\d*)(m|h)", window)

    if not match:
        raise ValueError(
            "Invalid window. Use a duration such as 5m, 30m, or 1h."
        )

    amount = int(match.group(1))
    unit = match.group(2)
    window_minutes = amount * (60 if unit == "h" else 1)

    if window_minutes > 1440:
        raise ValueError("Window cannot exceed 24h.")

    checks = {
        "api_targets_up": _prometheus_query(
            'up{job="production-incident-commander"}'
        ),
        f"http_5xx_per_second_{window}": _prometheus_query(
            f'sum(rate(http_requests_total{{status_code=~"5.."}}[{window}]))'
        ),
        f"http_p95_latency_seconds_{window}": _prometheus_query(
            "histogram_quantile(0.95, "
            "sum by (le) ("
            f"rate(http_request_duration_seconds_bucket[{window}])"
            "))"
        ),
        "kafka_consumer_lag": _prometheus_query(
            "sum(kafka_consumer_lag)"
        ),
    }

    api_up_result = checks["api_targets_up"]
    api_up = None

    if api_up_result.get("available"):
        values = [
            item.get("value")
            for item in api_up_result.get("results", [])
        ]

        api_up = bool(values) and all(
            value == "1" for value in values
        )

    return {
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "window": window,
        "api_targets_up": api_up,
        f"http_5xx_per_second_{window}": _instant_value(
            checks[f"http_5xx_per_second_{window}"]
        ),
        f"http_p95_latency_seconds_{window}": _instant_value(
            checks[f"http_p95_latency_seconds_{window}"]
        ),
        "kafka_consumer_lag": _instant_value(
            checks["kafka_consumer_lag"]
        ),
        "sources": checks,
        "notes": [
            "HTTP error rate and p95 latency use the requested window.",
            "API target availability and Kafka consumer lag are instant "
            "queries, not historical measurements over that window.",
            "A null value means the query was unavailable or has no usable sample.",
            "Consumer lag may be stale if the lag monitor stops updating its gauge.",
        ],
    }

def collect_kafka_consumer_health() -> dict[str, Any]:
    """
    Kafka consumer throughput, processing failures, processing latency, consumer lag and exporter availability
    """

    queries = {
        "messages_consumer_per_second_5m": (
            "sum(rate(kafka_messages_consumed_total[5m]))"
        ),
        "processing_failures_per_second_5m": (
            "sum(rate("
            "kafka_message_processing_failures_total[5m]"
            "))"
        ),
        "processing_p95_seconds_5m": (
            "histogram_quantile(0.95, "
            "sum by (le) ("
            "rate(kafka_message_processing_latency_seconds_bucket[5m])"
            "))"
        ),
        "consumer_lag": (
            "sum(kafka_consumer_lag)"
        ),
        "consumer exporter_up": (
            'up{job="payments-events-consumer"}'
        ),
        "lag_monitor_up": (
            'up{job="kafka-lag-monitor"}'
        ),
    }

    results = {
        name: _prometheus_query(query)
        for name, query in queries.items()
    }

    return {
        "checked_at_utc": datetime.now(
            timezone.utc
        ).isoformat(),

        "metrics": {
            name: _instant_value(result)
            for name, result in results.items()
        },

        "sources": results,

        # "interpretation": (
        #     "Zero throughput alone does not prove an outage. "
        #     "Check exporter availability, lag, and whether "
        #     "the consumer is expected to receive messages."
        # ),
    }

def collect_outbox_backlog() -> dict[str, Any]:
    """
    Outbox counts by status, oldest pending event, recent events that exhausted their retries
    """
    counts = _db_query(
        """
        SELECT
            status,
            COUNT(*)::BIGINT AS event_count
        FROM event_outbox
        GROUP BY status
        ORDER BY status
        """
    )

    oldest_pending = _db_query(
        """
        SELECT
            event_id,
            aggregate_id,
            event_type,
            attempts,
            created_at,
            EXTRACT(
                EPOCH FROM (NOW() - created_at)
            )::BIGINT AS age_seconds,
            last_error
        FROM event_outbox
        WHERE status = 'PENDING'
        ORDER BY created_at ASC
        LIMIT 1
        """
    )

    failed = _db_query(
        """
        SELECT
            event_id,
            aggregate_id,
            event_type,
            attempts,
            created_at,
            last_error
        FROM event_outbox
        WHERE status = 'FAILED'
        ORDER BY created_at DESC
        LIMIT 10
        """
    )

    return {
        "status_counts": {
            row["status"]: row["event_count"]
            for row in counts
        },

        "oldest_pending_event": (
            oldest_pending[0]
            if oldest_pending
            else None
        ),

        "recent_failed_events": failed,

        "max_attempts_configured": (
            settings.outbox_max_attempts
        ),
    }

def collect_failed_payments(
    limit: int = 20
) -> dict[str, Any]:
    """
    List recent payment orders whose payment_status is FAILED.
    """

    safe_limit = max(
        1,
        min(limit, 100),
    )

    rows = _db_query(
        """
        SELECT
            transaction_id,
            customer_id,
            amount,
            currency,
            payment_status,
            payment_method,
            created_at,
            updated_at
        FROM payment_orders
        WHERE UPPER(payment_status) = 'FAILED'
        ORDER BY updated_at DESC
        LIMIT %s
        """,
        (safe_limit,),
    )

    return {
        "count_returned": len(rows),
        "limit": safe_limit,
        "payments": rows,
    }

@mcp.tool()
def get_service_health(window: str = "5m") -> dict:
    """Get service health using the specified Prometheus lookback window."""
    return collect_service_health(window=window)

@mcp.tool()
def get_kafka_consumer_health() -> dict[str, Any]:
    return collect_kafka_consumer_health()

@mcp.tool()
def get_outbox_backlog() -> dict[str, Any]:
    return collect_outbox_backlog()

@mcp.tool()
def get_failed_payments(
    limit: int = 20,
) -> dict[str, Any]:
    return collect_failed_payments(limit=limit)

@mcp.tool()
def create_incident(
    title: str,
    description: str,
    severity: str,
    affected_service: str,
) -> dict[str, Any]:
    """Create and persist a new incident."""

    with db_pool.connection() as connection:
        incident = incident_repository.create_incident(
            connection=connection,
            title=title,
            description=description,
            severity=severity,
            affected_service=affected_service,
        )

        return _json_safe(incident)

@mcp.tool()
def list_active_incidents(
    limit: int = 20,
) -> dict[str, Any]:
    """List open and investigating incidents."""

    with db_pool.connection() as connection:
        incidents = incident_repository.list_active_incidents(
            connection=connection,
            limit=limit,
        )

        return {
            "count": len(incidents),
            "incidents": _json_safe(incidents),
        }

@mcp.tool()
def get_incident(
    incident_id: str,
) -> dict[str, Any]:
    """Retrieve an incident and its associated evidence."""

    try:
        parsed_id = UUID(incident_id)
    except ValueError:
        return {"error": "Invalid incident UUID"}

    with db_pool.connection() as connection:
        incident = incident_repository.get_incident(
            connection=connection,
            incident_id=parsed_id,
        )

        if incident is None:
            return {"error": "Incident not found"}

        return _json_safe(incident)

@mcp.tool()
def update_incident_status(
    incident_id: str,
    status: str,
    root_cause: str | None = None,
    resolution_notes: str | None = None,
) -> dict[str, Any]:
    """Update an incident's status and investigation findings."""

    try:
        parsed_id = UUID(incident_id)
    except ValueError:
        return {"error": "Invalid incident UUID"}

    with db_pool.connection() as connection:
        incident = incident_repository.update_incident_status(
            connection=connection,
            incident_id=parsed_id,
            status=status,
            root_cause=root_cause,
            resolution_notes=resolution_notes,
        )

        if incident is None:
            return {"error": "Incident not found"}

        return _json_safe(incident)

# @mcp.tool()
# def investigate_incident(
#     incident_id: str,
# ) -> dict[str, Any]:
#     """Collect diagnostic evidence for an existing incident."""

#     try:
#         parsed_id = UUID(incident_id)
#     except ValueError:
#         return {"error": "Invalid incident UUID"}

#     return run_incident_investigation(parsed_id)

@mcp.tool()
def record_incident_evidence(
    incident_id: str,
    event_type: str,
    source: str,
    details: dict[str, Any]
) -> dict[str, Any]:
    try:
        parsed_id = UUID(incident_id)
    except (TypeError, ValueError):
        return {"error": "Invalid incident UUID"}

    if event_type not in {
        "DIAGNOSTIC_RESULT",
        "DIAGNOSTIC_COLLECTION_FAILED",
    }:
        return {"error": "Unsupported evidence event_type"}

    if not source or not source.strip():
        return {"error": "source must be non-empty"}

    if not isinstance(details, dict):
        return {"error": "details must be an object"}

    with db_pool.connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT id FROM incidents WHERE id = %s",
                (parsed_id,),
            )

            if cursor.fetchone() is None:
                return {"error": "Incident not found"}

            cursor.execute(
                """
                INSERT INTO incident_evidence (
                    incident_id,
                    event_type,
                    source,
                    details
                )
                VALUES (%s, %s, %s, %s)
                RETURNING
                    id,
                    incident_id,
                    event_type,
                    source,
                    observed_at,
                    created_at
                """,
                (
                    parsed_id,
                    event_type,
                    source.strip(),
                    Jsonb(details),
                ),
            )

            row = cursor.fetchone() 

    return {
        "id": str(row[0]),
        "incident_id": str(row[1]),
        "event_type": row[2],
        "source": row[3],
        "observed_at": row[4].isoformat(),
        "created_at": row[5].isoformat(),
    }


@mcp.tool()
def query_prometheus(query: str) -> dict[str, Any]:
    """
    Execute a read-only Prometheus instant query using PromQL.

    Use this to investigate metrics not covered by predefined health tools.
    The result preserves query availability and the individual time series.
    """
    if not query or not query.strip():
        return {
            "available": False,
            "error": "PromQL query cannot be empty",
        }

    # Only instant queries are supported by this helper.
    # Production deployments should also validate query cost and access.
    return _prometheus_query(query.strip())


@mcp.tool()
def list_prometheus_metrics(
    name_filter: str = "",
    limit: int = 100,
) -> dict[str, Any]:
    """
    Discover metric names exposed by the configured Prometheus server.

    Optionally filter metric names by a substring.
    """
    return _prometheus_metric_names(
        name_filter=name_filter,
        limit=limit,
    )

if __name__ == "__main__":
    mcp.run(transport="stdio")