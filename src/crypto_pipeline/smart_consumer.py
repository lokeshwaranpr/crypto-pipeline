import json
from collections import deque

from kafka import KafkaConsumer

from crypto_pipeline.config import settings
from crypto_pipeline.db.queries import create_tables, insert_alert, insert_tick
from crypto_pipeline.schemas.price import PriceTick


class SpikeDetector:
    def __init__(self, window_size: int, threshold_pct: float) -> None:
        self.window_size = window_size
        self.threshold_pct = threshold_pct
        self._windows: dict[str, deque] = {}

    def check(self, coin: str, price: float) -> dict | None:
        if coin not in self._windows:
            self._windows[coin] = deque(maxlen=self.window_size)

        window = self._windows[coin]
        result = None

        if len(window) >= 3:
            avg = sum(window) / len(window)
            change_pct = ((price - avg) / avg) * 100

            if abs(change_pct) >= self.threshold_pct:
                result = {
                    "avg_price": round(avg, 2),
                    "change_pct": round(change_pct, 4),
                    "direction": "UP" if change_pct > 0 else "DOWN",
                }

        window.append(price)
        return result


def _build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        settings.kafka_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id=settings.kafka_consumer_group,
    )


def main() -> None:
    create_tables()
    consumer = _build_consumer()
    detector = SpikeDetector(
        window_size=settings.spike_window_size,
        threshold_pct=settings.spike_threshold_pct,
    )

    print("Smart consumer running...\n")

    for message in consumer:
        tick = PriceTick.model_validate(message.value)

        insert_tick(tick.coin, tick.price_usd, tick.timestamp)
        print(f"  STORED | {tick.coin.upper():>10} | ${tick.price_usd:>10,.2f}")

        spike = detector.check(tick.coin, tick.price_usd)
        if spike:
            insert_alert(
                tick.coin, tick.price_usd, spike["avg_price"],
                spike["change_pct"], spike["direction"], tick.timestamp,
            )
            print(
                f"  *** ALERT: {tick.coin.upper()} {spike['direction']} "
                f"{spike['change_pct']}% (avg: ${spike['avg_price']:,.2f} -> "
                f"now: ${tick.price_usd:,.2f}) ***"
            )


if __name__ == "__main__":
    main()
