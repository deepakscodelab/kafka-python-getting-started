import json
from datetime import datetime, timezone

from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

order_event = {
    "event_id": "evt-1001",
    "event_type": "OrderCreated",
    "order_id": "ORD-1001",
    "customer_id": "CUST-101",
    "amount": 1499.00,
    "created_at": datetime.now(timezone.utc).isoformat()
}


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(
            f"Message delivered to topic={msg.topic()}, "
            f"partition={msg.partition()}, "
            f"offset={msg.offset()}"
        )


producer.produce(
    topic="orders",
    key=order_event["order_id"],
    value=json.dumps(order_event).encode("utf-8"),
    callback=delivery_report
)

# Wait for the delivery callback to complete.
producer.flush()

print("Producer finished.")