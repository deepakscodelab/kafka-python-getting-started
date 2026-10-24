import json
from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "analytics-service",
    "auto.offset.reset": "earliest"
})

consumer.subscribe(["orders"])

print("Analytics Service started...")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"Kafka error: {msg.error()}")
            continue

        event = json.loads(msg.value().decode("utf-8"))

        print(
            f"[ANALYTICS] Recording order "
            f"{event['order_id']} "
            f"with amount {event['total_amount']}"
        )

finally:
    consumer.close()