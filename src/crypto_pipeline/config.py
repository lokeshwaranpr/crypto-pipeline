from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "crypto-prices"
    kafka_consumer_group: str = "smart-consumer-group"

    # PostgreSQL
    postgres_host: str = "localhost"
    postgres_port: int = 5433
    postgres_db: str = "cryptodb"
    postgres_user: str = "crypto"
    postgres_password: str = "crypto123"

    # Producer
    fetch_interval_seconds: int = 15
    coingecko_coins: str = "bitcoin,ethereum,solana"
    coingecko_api_url: str = "https://api.coingecko.com/api/v3/simple/price"

    # Spike detector
    spike_window_size: int = 10
    spike_threshold_pct: float = 0.5

    # Resilience
    max_retries: int = 5
    dlq_topic: str = "crypto-prices-dlq"

    # Logging
    log_level: str = "INFO"
    log_format: str = "console"  # "console" or "json"

    # Metrics
    producer_metrics_port: int = 8000
    consumer_metrics_port: int = 8001


settings = Settings()
