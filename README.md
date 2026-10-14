# production-incident-commander

{
  "checked_at_utc": "2026-10-14T18:44:59.357665+00:00",
  "api_targets_up": true,
  "http_5xx_per_second_5m": 1.0163049185770376,
  "http_p95_latency_seconds_5m": 0.7680555555555554,
  "kafka_consumer_lag": 0,
  "sources": {
    "api_targets_up": {
      "available": true,
      "query": "up{job=\"production-incident-commander\"}",
      "results": [
        {
          "labels": {
            "__name__": "up",
            "instance": "host.docker.internal:8000",
            "job": "production-incident-commander"
          },
          "value": "1"
        }
      ]
    },
    "http_5xx_per_second_5m": {
      "available": true,
      "query": "sum(rate(http_requests_total{status_code=~\"5..\"}[5m]))",
      "results": [
        {
          "labels": {},
          "value": "1.0163049185770376"
        }
      ]
    },
    "http_p95_latency_seconds_5m": {
      "available": true,
      "query": "histogram_quantile(0.95, sum by (le) (rate(http_request_duration_seconds_bucket[5m])))",
      "results": [
        {
          "labels": {},
          "value": "0.7680555555555554"
        }
      ]
    },
    "kafka_consumer_lag": {
      "available": true,
      "query": "sum(kafka_consumer_lag)",
      "results": []
    }
  },
  "notes": [
    "A null value means the query was unavailable or has no usable sample.",
    "Consumer lag may be stale if the lag monitor stops updating its gauge."
  ]
}