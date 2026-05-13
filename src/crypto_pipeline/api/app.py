import asyncio
import json
import threading
from contextlib import asynccontextmanager
from typing import Set

import structlog
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from kafka import KafkaConsumer

from crypto_pipeline.config import settings
from crypto_pipeline.db.queries import get_alerts, get_latest_prices, get_price_history
from crypto_pipeline.logging_config import configure_logging

log = structlog.get_logger()

_active_ws: Set[WebSocket] = set()
_event_loop: asyncio.AbstractEventLoop | None = None


def _kafka_broadcast_thread() -> None:
    try:
        consumer = KafkaConsumer(
            settings.kafka_topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
            group_id="api-live-feed",
        )
    except Exception as exc:
        log.warning("api.kafka_feed.unavailable", error=str(exc))
        return

    while True:
        records = consumer.poll(timeout_ms=500)
        for tp, messages in records.items():
            for message in messages:
                for ws in list(_active_ws):
                    asyncio.run_coroutine_threadsafe(
                        ws.send_json(message.value), _event_loop
                    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _event_loop
    _event_loop = asyncio.get_event_loop()
    thread = threading.Thread(target=_kafka_broadcast_thread, daemon=True)
    thread.start()
    log.info("api.started", host=settings.api_host, port=settings.api_port)
    yield
    log.info("api.stopped")


app = FastAPI(title="Crypto Pipeline API", version="0.1.0", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/prices/latest")
def latest_prices():
    return get_latest_prices()


@app.get("/prices/{coin}/history")
def price_history(coin: str, limit: int = 50):
    return get_price_history(coin, limit)


@app.get("/alerts")
def alerts(coin: str | None = None, limit: int = 20):
    return get_alerts(coin, limit)


@app.websocket("/ws/prices")
async def websocket_prices(websocket: WebSocket):
    await websocket.accept()
    _active_ws.add(websocket)
    log.info("ws.connected", clients=len(_active_ws))
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        _active_ws.discard(websocket)
        log.info("ws.disconnected", clients=len(_active_ws))


def run() -> None:
    configure_logging(settings.log_level, settings.log_format)
    uvicorn.run(
        "crypto_pipeline.api.app:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=False,
    )
