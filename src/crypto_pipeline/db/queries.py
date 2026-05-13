from datetime import datetime

from crypto_pipeline.db.connection import get_conn
from crypto_pipeline.metrics import db_write_duration, db_writes


def create_tables() -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS price_ticks (
                    id SERIAL PRIMARY KEY,
                    coin VARCHAR(20) NOT NULL,
                    price_usd DOUBLE PRECISION NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS price_alerts (
                    id SERIAL PRIMARY KEY,
                    coin VARCHAR(20) NOT NULL,
                    current_price DOUBLE PRECISION NOT NULL,
                    avg_price DOUBLE PRECISION NOT NULL,
                    change_pct DOUBLE PRECISION NOT NULL,
                    direction VARCHAR(4) NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL
                );
            """)


def insert_tick(coin: str, price: float, timestamp: datetime) -> None:
    with db_write_duration.labels(table="price_ticks").time():
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO price_ticks (coin, price_usd, timestamp) VALUES (%s, %s, %s)",
                    (coin, price, timestamp),
                )
    db_writes.labels(table="price_ticks").inc()


def insert_alert(
    coin: str,
    current_price: float,
    avg_price: float,
    change_pct: float,
    direction: str,
    timestamp: datetime,
) -> None:
    with db_write_duration.labels(table="price_alerts").time():
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO price_alerts
                       (coin, current_price, avg_price, change_pct, direction, timestamp)
                       VALUES (%s, %s, %s, %s, %s, %s)""",
                    (coin, current_price, avg_price, change_pct, direction, timestamp),
                )
    db_writes.labels(table="price_alerts").inc()


def get_latest_prices() -> list[dict]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT ON (coin) coin, price_usd, timestamp
                FROM price_ticks
                ORDER BY coin, timestamp DESC
            """)
            return [
                {"coin": r[0], "price_usd": r[1], "timestamp": r[2].isoformat()}
                for r in cur.fetchall()
            ]


def get_price_history(coin: str, limit: int = 50) -> list[dict]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT coin, price_usd, timestamp
                FROM price_ticks
                WHERE coin = %s
                ORDER BY timestamp DESC
                LIMIT %s
                """,
                (coin, limit),
            )
            return [
                {"coin": r[0], "price_usd": r[1], "timestamp": r[2].isoformat()}
                for r in cur.fetchall()
            ]


def get_alerts(coin: str | None = None, limit: int = 20) -> list[dict]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            if coin:
                cur.execute(
                    """
                    SELECT coin, current_price, avg_price, change_pct, direction, timestamp
                    FROM price_alerts
                    WHERE coin = %s
                    ORDER BY timestamp DESC
                    LIMIT %s
                    """,
                    (coin, limit),
                )
            else:
                cur.execute(
                    """
                    SELECT coin, current_price, avg_price, change_pct, direction, timestamp
                    FROM price_alerts
                    ORDER BY timestamp DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
            return [
                {
                    "coin": r[0],
                    "current_price": r[1],
                    "avg_price": r[2],
                    "change_pct": r[3],
                    "direction": r[4],
                    "timestamp": r[5].isoformat(),
                }
                for r in cur.fetchall()
            ]
