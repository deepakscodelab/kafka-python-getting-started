import json
from confluent_kafka import Producer

# Connect to Kafka
producer = Producer({
    "bootstrap.servers": "localhost:9092"
})


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(
            f"Message delivered to {msg.topic()} "
            f"[partition {msg.partition()}] "
            f"at offset {msg.offset()}"
        )


# Create an order event
order_event = {
    "event_type": "OrderCreated",
    "order_id": 103,
    "customer_id": 503,
    "total_amount": 2500
}

# Publish the event
producer.produce(
    topic="orders",
    key=str(order_event["order_id"]),
    value=json.dumps(order_event),
    callback=delivery_report
)

# Wait for outstanding messages to be delivered
producer.flush()