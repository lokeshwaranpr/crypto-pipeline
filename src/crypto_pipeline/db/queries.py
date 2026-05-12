from datetime import datetime

from crypto_pipeline.db.connection import get_conn


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
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO price_ticks (coin, price_usd, timestamp) VALUES (%s, %s, %s)",
                (coin, price, timestamp),
            )


def insert_alert(
    coin: str,
    current_price: float,
    avg_price: float,
    change_pct: float,
    direction: str,
    timestamp: datetime,
) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO price_alerts
                   (coin, current_price, avg_price, change_pct, direction, timestamp)
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                (coin, current_price, avg_price, change_pct, direction, timestamp),
            )
