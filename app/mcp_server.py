import logging
import os
import sys

from datetime import datetime, timezone
from typing import Any

from mcp.server.fastmcp import FastMCP

from app.config import get_settings
from app.mcp_utilities import (
    _json_safe,
    _db_query,
    _prometheus_query,
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

PROMETHEUS_URL = os.getenv(
    "PROMETHEUS_URL",
    "http://localhost:9090",
).rstrip("/")

mcp = FastMCP(
    "Production Incident Commander",
    instructions=(
        "Read-only incident-investigation tools for a payment system. "
        "Correlate evidence from PostgreSQL and Prometheus. "
        "Do not claim a root cause unless the returned evidence "
        "supports it. Distinguish observed facts from hypotheses."
    )
)

@mcp.tool()
def get_service_health() -> dict[str, Any]:
    """
    API availability, HTTP 5xx rate, p95 latency, Kafka consumer lag
    """
    checks = {
        "api_targets_up": _prometheus_query(
            'up{job="production-incident-commander"}'
        ),
        "http_5xx_per_second_5m": _prometheus_query(
            'sum(rate('
            'http_requests_total{status_code=~"5.."}'
            '[5m]))'
        ),
        "http_p95_latency_seconds_5m": _prometheus_query(
            'histogram_quantile(0.95, '
            'sum by (le) ('
            'rate(http_request_duration_seconds_bucket[5m])'
            '))'
        ),
        "kafka_consumer_lag": _prometheus_query(
            "sum(kafka_consumer_lag)"
        )
    }

    api_up_result = checks["api_targets_up"]

    api_up = None

    if api_up_result.get("available"):
        values = [
            item.get("value")
            for item in api_up_result.get("results", [])
        ]

        api_up = bool(values) and all(
            value == "1"
            for value in values
        )

    return {
        "checked_at_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "api_targets_up": api_up,
        "http_5xx_per_second_5m": _instant_value(
            checks["http_5xx_per_second_5m"]
        ),
        "http_p95_latency_seconds_5m": _instant_value(
            checks["http_p95_latency_seconds_5m"]
        ),
        "kafka_consumer_lag": _instant_value(
            checks["kafka_consumer_lag"]
        ),
        "sources": checks,
        "notes": [
            "A null value means the query was unavailable "
            "or has no usable sample.",
            "Consumer lag may be stale if the lag monitor "
            "stops updating its gauge.",
        ],
    }

@mcp.tool()
def get_kafka_consumer_health() -> dict[str, Any]:
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

        "interpretation": (
            "Zero throughput alone does not prove an outage. "
            "Check exporter availability, lag, and whether "
            "the consumer is expected to receive messages."
        ),
    }

@mcp.tool()
def get_outbox_backlog() -> dict[str, Any]:
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

@mcp.tool()
def get_failed_payments(
    limit: int = 20,
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

if __name__ == "__main__":
    mcp.run(transport="stdio")