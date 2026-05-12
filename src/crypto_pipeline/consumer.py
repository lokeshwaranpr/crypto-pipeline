import json

from kafka import KafkaConsumer

from crypto_pipeline.config import settings
from crypto_pipeline.schemas.price import PriceTick


def main() -> None:
    consumer = KafkaConsumer(
        settings.kafka_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="basic-consumer-group",
    )

    print("Listening for crypto prices...\n")

    for message in consumer:
        tick = PriceTick.model_validate(message.value)
        print(f"{tick.coin.upper():>10} | ${tick.price_usd:>10,.2f} | {tick.timestamp}")


if __name__ == "__main__":
    main()
