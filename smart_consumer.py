import json
from datetime import datetime, timezone
from collections import deque

import psycopg2
from kafka import KafkaConsumer

# --- Database Setup ---

def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="cryptodb",
        user="crypto",
        password="crypto123"
    )


def create_tables(conn):
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS price_ticks (
            id SERIAL PRIMARY KEY,
            coin VARCHAR(20) NOT NULL,
            price_usd DOUBLE PRECISION NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)

    cursor.execute("""
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

    conn.commit()
    cursor.close()
    print("Tables created successfully.")


def insert_tick(conn, coin, price, timestamp):
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO price_ticks (coin, price_usd, timestamp) VALUES (%s, %s, %s)",
        (coin, price, timestamp)
    )
    conn.commit()
    cursor.close()


def insert_alert(conn, coin, current_price, avg_price, change_pct, direction, timestamp):
    cursor = conn.cursor()
    cursor.execute(
        """INSERT INTO price_alerts (coin, current_price, avg_price, change_pct, direction, timestamp)
           VALUES (%s, %s, %s, %s, %s, %s)""",
        (coin, current_price, avg_price, change_pct, direction, timestamp)
    )
    conn.commit()
    cursor.close()


# --- Spike Detection ---

class SpikeDetector:
    """
    Tracks a rolling window of recent prices per coin.
    If the latest price deviates more than the threshold
    from the rolling average, it flags a spike.
    """

    def __init__(self, window_size=10, threshold_pct=0.5):
        self.window_size = window_size
        self.threshold_pct = threshold_pct
        self.price_windows = {}  # coin -> deque of recent prices

    def check(self, coin, price):
        if coin not in self.price_windows:
            self.price_windows[coin] = deque(maxlen=self.window_size)

        window = self.price_windows[coin]

        result = None

        if len(window) >= 3:  # need at least 3 data points
            avg_price = sum(window) / len(window)
            change_pct = ((price - avg_price) / avg_price) * 100

            if abs(change_pct) >= self.threshold_pct:
                direction = "UP" if change_pct > 0 else "DOWN"
                result = {
                    "avg_price": round(avg_price, 2),
                    "change_pct": round(change_pct, 4),
                    "direction": direction
                }

        window.append(price)
        return result


# --- Main Consumer Loop ---

def main():
    conn = get_db_connection()
    create_tables(conn)

    consumer = KafkaConsumer(
        "crypto-prices",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="smart-consumer-group"
    )

    detector = SpikeDetector(window_size=10, threshold_pct=0.5)

    print("Smart consumer running...\n")

    for message in consumer:
        data = message.value
        coin = data["coin"]
        price = data["price_usd"]
        timestamp = data["timestamp"]

        # Store every tick
        insert_tick(conn, coin, price, timestamp)
        print(f"  STORED | {coin.upper():>10} | ${price:>10,.2f}")

        # Check for spike
        spike = detector.check(coin, price)
        if spike:
            insert_alert(conn, coin, price, spike["avg_price"],
                         spike["change_pct"], spike["direction"], timestamp)
            print(f"  *** ALERT: {coin.upper()} {spike['direction']} {spike['change_pct']}% "
                  f"(avg: ${spike['avg_price']:,.2f} -> now: ${price:,.2f}) ***")


if __name__ == "__main__":
    main()