from prometheus_client import Counter, Histogram


http_requests_total = Counter(
    "http_requests_total",
    "Total number of HTTP requests",
    ["method", "path", "status_code"],
)

http_request_duration_seconds = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "path"],
)

payment_requests_total = Counter(
    "payment_requests_total",
    "Total number of payment requests",
    ["operation"],
)

payment_failures_total = Counter(
    "payment_failures_total",
    "Total number of failed payment requests",
    ["operation"],
)