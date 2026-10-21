import json
import os
import sys
import logging

from datetime import datetime
from decimal import Decimal
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen
from uuid import UUID

import psycopg
from psycopg.rows import dict_row

from app.config import get_settings

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

def _json_safe(value: Any) -> Any:
    """
    Convert common database values into JSON-serializable values.
    """
    if isinstance(value, dict):
        return {
            str(key): _json_safe(item)
            for key, item in value.items()
        }

    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]

    if isinstance(value, (UUID, datetime, Decimal)):
        return str(value)

    return value

def _db_query(
    sql: str,
    params: tuple[Any, ...] = ()
) -> list[dict[str, Any]]:
    with psycopg.connect(
        settings.database_url,
        autocommit=True,
        row_factory=dict_row,
    ) as conn:
        conn.execute(
            "SET default_transaction_read_only = on"
        )

        with conn.cursor() as cursor:
            cursor.execute(sql, params)

            return[
                _json_safe(row)
                for row in cursor.fetchall()
            ]

def _prometheus_query(
    query: str
) -> dict[str, Any]:
    url = (
        f"{PROMETHEUS_URL}/api/v1/query?"
        f"{urlencode({'query': query})}"
    )

    try:
        with urlopen(url, timeout=4) as response:
            body = json.loads(
                response.read().decode("utf-8")
            )
    except (
        HTTPError,
        URLError,
        TimeoutError,
        OSError,
        json.JSONDecodeError,
    ) as exception:
        logger.warning(
            "prometheus_query_failed: %s",
            exception,
        )

        return {
            "available": False,
            "query": query,
            "error": str(exception),
        }

    if body.get("status") != "success":
        return {
            "available": False,
            "query": query,
            "error": body.get(
                "error",
                "Prometheus query failed",
            )
        }

    results = []

    for item in body.get("data", {}).get("result", []):
        metric = item.get("metric", {})
        value = item.get("value", [None, None])

        results.append({
            "labels": metric,
            "timestamp": value[0] if len(value) > 0 else None,
            "value": value[1] if len(value) > 1 else None,
        })

    return {
        "available": True,
        "query": query,"result_type": body.get("data", {}).get("resultType"),
        "result_count": len(results),
        "no_matching_series": len(results) == 0,
        "results": results,
    }

def _prometheus_metric_names(
    name_filter: str = "",
    limit: int = 100
) -> dict[str, Any]:
    """Discover metric names available in Prometheus."""
    safe_limit = max(1, min(int(limit), 500))
    url = f"{PROMETHEUS_URL}/api/v1/label/__name__/values"

    try:
        with urlopen(url, timeout=4) as response:
            body = json.loads(response.read().decode("utf-8"))
    except (
        HTTPError,
        URLError,
        TimeoutError,
        OSError,
        json.JSONDecodeError,
    ) as exc:
        logger.warning("Prometheus metric discovery failed: %s", exc)
        return {
            "available": False,
            "error": str(exc),
            "metrics": [],
        }

    if body.get("status") != "success":
        return {
            "available": False,
            "error": body.get("error", "Metric discovery failed"),
            "metrics": [],
        }

    metrics = body.get("data", [])
    if name_filter.strip():
        needle = name_filter.strip().lower()
        metrics = [
            name for name in metrics
            if needle in name.lower()
        ]

    metrics = sorted(metrics)
    return {
        "available": True,
        "count_returned": min(len(metrics), safe_limit),
        "truncated": len(metrics) > safe_limit,
        "metrics": metrics[:safe_limit],
    }

def _instant_value(
    result: dict[str, Any],
) -> float | None:
    if not result.get("available"):
        return None

    values = result.get("results", [])

    if not values:
        return None

    try:
        usable_values = [
            float(item["value"])
            for item in values
            if item.get("value") is not None
        ]
    except (KeyError, TypeError, ValueError):
        return None

    if not usable_values:
        return None

    return sum(usable_values)

    