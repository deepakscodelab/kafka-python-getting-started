import json
from confluent_kafka import Consumer, KafkaException

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "notification-service",
    "auto.offset.reset": "earliest"
})

consumer.subscribe(["orders"])

print("Notification Service started...")
print("Waiting for order events...")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            raise KafkaException(msg.error())

        order_event = json.loads(msg.value().decode("utf-8"))

        print("Received order event:")
        print(json.dumps(order_event, indent=2))

        print(
            f"Processing order {order_event['order_id']} "
            f"for customer {order_event['customer_id']}"
        )

except KeyboardInterrupt:
    print("Consumer stopped.")

finally:
    consumer.close()