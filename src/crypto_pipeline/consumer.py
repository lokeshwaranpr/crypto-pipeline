import json

import structlog
from kafka import KafkaConsumer

from crypto_pipeline.config import settings
from crypto_pipeline.logging_config import configure_logging
from crypto_pipeline.schemas.price import PriceTick

log = structlog.get_logger()


def main() -> None:
    configure_logging(settings.log_level, settings.log_format)
    consumer = KafkaConsumer(
        settings.kafka_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="basic-consumer-group",
    )

    log.info("consumer.started", topic=settings.kafka_topic)

    for message in consumer:
        tick = PriceTick.model_validate(message.value)
        log.info("price.received", coin=tick.coin, price_usd=tick.price_usd, timestamp=str(tick.timestamp))


if __name__ == "__main__":
    main()
