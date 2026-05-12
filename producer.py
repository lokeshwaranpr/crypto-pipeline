import json
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC = "crypto-prices"
API_URL = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,solana&vs_currencies=usd"


def fetch_and_publish():
    while True:
        try:
            response = requests.get(API_URL)

            if response.status_code != 200:
                print(f"API rate limited (status {response.status_code}). Waiting...")
                time.sleep(30)
                continue

            data = response.json()

            for coin, price_info in data.items():
                message = {
                    "coin": coin,
                    "price_usd": price_info["usd"],
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }

                producer.send(TOPIC, value=message)
                print(f"Published: {message}")

            producer.flush()

        except Exception as e:
            print(f"Error: {e}")

        time.sleep(15)


if __name__ == "__main__":
    fetch_and_publish()