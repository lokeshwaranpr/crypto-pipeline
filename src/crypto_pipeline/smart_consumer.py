import json
import signal
import threading
from collections import deque
from datetime import datetime, timezone

import structlog
from kafka import KafkaConsumer, KafkaProducer
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from crypto_pipeline.config import settings
from crypto_pipeline.db.queries import insert_alert, insert_tick
from crypto_pipeline.logging_config import configure_logging
from crypto_pipeline.metrics import messages_consumed, spike_alerts, start_metrics_server
from crypto_pipeline.schemas.price import PriceTick

log = structlog.get_logger()
_shutdown = threading.Event()


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


class DeadLetterQueue:
    def __init__(self) -> None:
        self._producer = KafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def send(self, original: dict, error: str) -> None:
        payload = {
            "original": original,
            "error": error,
            "failed_at": datetime.now(timezone.utc).isoformat(),
        }
        self._producer.send(settings.dlq_topic, value=payload)
        self._producer.flush()
        log.warning("dlq.sent", error=error)


def _install_shutdown_handler() -> None:
    def _handler(sig, frame):
        log.info("consumer.shutdown", reason="signal")
        _shutdown.set()

    signal.signal(signal.SIGINT, _handler)
    try:
        signal.signal(signal.SIGTERM, _handler)
    except (OSError, AttributeError):
        pass


def _log_db_retry(retry_state) -> None:
    log.warning(
        "db.retrying",
        attempt=retry_state.attempt_number,
        error=str(retry_state.outcome.exception()),
    )


@retry(
    retry=retry_if_exception_type(Exception),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    stop=stop_after_attempt(settings.max_retries),
    before_sleep=_log_db_retry,
    reraise=True,
)
def _write_to_db(tick: PriceTick, spike: dict | None) -> None:
    insert_tick(tick.coin, tick.price_usd, tick.timestamp)
    if spike:
        insert_alert(
            tick.coin, tick.price_usd, spike["avg_price"],
            spike["change_pct"], spike["direction"], tick.timestamp,
        )


def _process_message(
    raw: dict,
    detector: SpikeDetector,
    dlq: DeadLetterQueue,
) -> None:
    try:
        tick = PriceTick.model_validate(raw)
    except Exception as exc:
        log.error("message.invalid", error=str(exc))
        dlq.send(raw, f"validation_error: {exc}")
        return

    # spike detection is pure in-memory — do it before DB write
    spike = detector.check(tick.coin, tick.price_usd)

    try:
        _write_to_db(tick, spike)
    except Exception as exc:
        log.error("db.write_failed", coin=tick.coin, error=str(exc))
        dlq.send(raw, f"db_error: {exc}")
        return

    messages_consumed.labels(coin=tick.coin).inc()
    log.info("tick.stored", coin=tick.coin, price_usd=tick.price_usd)

    if spike:
        spike_alerts.labels(coin=tick.coin, direction=spike["direction"]).inc()
        log.warning(
            "spike.detected",
            coin=tick.coin,
            direction=spike["direction"],
            change_pct=spike["change_pct"],
            avg_price=spike["avg_price"],
            current_price=tick.price_usd,
        )


def main() -> None:
    configure_logging(settings.log_level, settings.log_format)
    start_metrics_server(settings.consumer_metrics_port)
    _install_shutdown_handler()

    consumer = KafkaConsumer(
        settings.kafka_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id=settings.kafka_consumer_group,
    )
    dlq = DeadLetterQueue()
    detector = SpikeDetector(
        window_size=settings.spike_window_size,
        threshold_pct=settings.spike_threshold_pct,
    )

    log.info("consumer.started", topic=settings.kafka_topic, group=settings.kafka_consumer_group)

    try:
        while not _shutdown.is_set():
            records = consumer.poll(timeout_ms=1000)
            for tp, messages in records.items():
                for message in messages:
                    if _shutdown.is_set():
                        break
                    _process_message(message.value, detector, dlq)
    finally:
        consumer.close()
        log.info("consumer.stopped")


if __name__ == "__main__":
    main()
