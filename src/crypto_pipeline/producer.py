import json
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

from crypto_pipeline.config import settings
from crypto_pipeline.schemas.price import PriceTick


def _build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def _fetch_prices() -> dict:
    coins = settings.coingecko_coins
    url = f"{settings.coingecko_api_url}?ids={coins}&vs_currencies=usd"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


def main() -> None:
    producer = _build_producer()

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
                print(f"Published: {tick}")

            producer.flush()

        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 429:
                print("Rate limited — waiting 30s")
                time.sleep(30)
                continue
            print(f"HTTP error: {exc}")
        except Exception as exc:
            print(f"Error: {exc}")

        time.sleep(settings.fetch_interval_seconds)


if __name__ == "__main__":
    main()
