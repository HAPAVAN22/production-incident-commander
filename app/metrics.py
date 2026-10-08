from prometheus_client import Counter, Gauge, Histogram

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

outbox_events_published_total = Counter(
    "outbox_events_published_total",
    "Total number of outbox events successfully published to Kafka",
    ["event_type"],
)

outbox_events_failed_total = Counter(
    "outbox_events_failed_total",
    "Total number of outbox event publish failures",
    ["event_type"],
)

outbox_publish_latency_seconds = Histogram(
    "outbox_publish_latency_seconds",
    "Time taken to publish an outbox event to Kafka",
    ["event_type"],
)

outbox_events_pending = Gauge(
    "outbox_events_pending",
    "Number of outbox events currently pending publication",
)

kafka_messages_consumed_total = Counter(
    "kafka_messages_consumed_total",
    "Total number of Kafka messages successfully consumed",
    ["topic"],
)

kafka_message_processing_failures_total = Counter(
    "kafka_message_processing_failures_total",
    "Total number of Kafka message processing failures",
    ["topic"],
)

kafka_message_processing_latency_seconds = Histogram(
    "kafka_message_processing_latency_seconds",
    "Time taken to process a Kafka message",
    ["topic"],
)

kafka_consumer_lag = Gauge(
    "kafka_consumer_lag",
    "Current Kafka consumer lag",
    ["topic", "partition"],
)