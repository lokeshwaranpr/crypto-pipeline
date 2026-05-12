import json
import signal
import sys
import time
from datetime import datetime, timezone

import requests
import structlog
from kafka import KafkaProducer
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from crypto_pipeline.config import settings
from crypto_pipeline.logging_config import configure_logging
from crypto_pipeline.metrics import messages_published, start_metrics_server
from crypto_pipeline.schemas.price import PriceTick

log = structlog.get_logger()


def _build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def _log_retry(retry_state) -> None:
    log.warning(
        "api.retrying",
        attempt=retry_state.attempt_number,
        error=str(retry_state.outcome.exception()),
    )


@retry(
    retry=retry_if_exception_type(requests.RequestException),
    wait=wait_exponential(multiplier=2, min=2, max=60),
    stop=stop_after_attempt(settings.max_retries),
    before_sleep=_log_retry,
    reraise=True,
)
def _fetch_prices() -> dict:
    coins = settings.coingecko_coins
    url = f"{settings.coingecko_api_url}?ids={coins}&vs_currencies=usd"
    response = requests.get(url, timeout=10)
    if response.status_code == 429:
        raise requests.HTTPError("rate limited", response=response)
    response.raise_for_status()
    return response.json()


def main() -> None:
    configure_logging(settings.log_level, settings.log_format)
    start_metrics_server(settings.producer_metrics_port)

    producer = _build_producer()

    def _shutdown(sig, frame):
        log.info("producer.shutdown", reason="signal")
        producer.flush()
        producer.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    try:
        signal.signal(signal.SIGTERM, _shutdown)
    except (OSError, AttributeError):
        pass

    log.info("producer.started", bootstrap_servers=settings.kafka_bootstrap_servers)

    while True:
        try:
            data = _fetch_prices()

            for coin, price_info in data.items():
                tick = PriceTick(
                    coin=coin,
                    price_usd=price_info["usd"],
                    timestamp=datetime.now(timezone.utc),
                )
                producer.send(settings.kafka_topic, value=tick.model_dump(mode="json"))
                messages_published.labels(coin=coin).inc()
                log.info("message.published", coin=coin, price_usd=tick.price_usd)

            producer.flush()

        except Exception as exc:
            log.error("producer.fetch_failed", error=str(exc), exc_info=True)

        time.sleep(settings.fetch_interval_seconds)


if __name__ == "__main__":
    main()
