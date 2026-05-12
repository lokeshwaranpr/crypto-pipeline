import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "crypto-prices",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="crypto-consumer-group"
)

print("Listening for crypto prices...\n")

for message in consumer:
    data = message.value
    print(f"{data['coin'].upper():>10} | ${data['price_usd']:>10,.2f} | {data['timestamp']}")