import json
from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "notification-service",
    "auto.offset.reset": "earliest"
})

consumer.subscribe(["orders"])

print("Notification Service started...")

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
            f"[NOTIFICATION] Processing order "
            f"{event['order_id']}"
        )

finally:
    consumer.close()