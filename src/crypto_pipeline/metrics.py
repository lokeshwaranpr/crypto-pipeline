from prometheus_client import Counter, Histogram, start_http_server

messages_published = Counter(
    "crypto_messages_published_total",
    "Messages published to Kafka",
    ["coin"],
)

messages_consumed = Counter(
    "crypto_messages_consumed_total",
    "Messages consumed from Kafka",
    ["coin"],
)

db_writes = Counter(
    "crypto_db_writes_total",
    "Rows written to the database",
    ["table"],
)

db_write_duration = Histogram(
    "crypto_db_write_duration_seconds",
    "Time spent writing to the database",
    ["table"],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)

spike_alerts = Counter(
    "crypto_spike_alerts_total",
    "Price spike alerts triggered",
    ["coin", "direction"],
)


def start_metrics_server(port: int) -> None:
    start_http_server(port)
