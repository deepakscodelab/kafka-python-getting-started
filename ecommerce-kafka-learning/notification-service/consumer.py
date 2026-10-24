import json

from confluent_kafka import Consumer, KafkaException


consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "notification-service",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})

consumer.subscribe(["orders"])


def process_order_created(event: dict) -> None:
    order_id = event["order_id"]
    customer_id = event["customer_id"]
    amount = event["total_amount"]

    print("\nOrder confirmation event received!")
    print(f"Order ID: {order_id}")
    print(f"Customer ID: {customer_id}")
    print(f"Amount: ₹{amount}")

    # In a real application, call your email or SMS provider here.
    print(f"Preparing confirmation for order {order_id}")


def main() -> None:
    print("Notification Service started...")
    print("Waiting for order events...")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())

            try:
                event = json.loads(msg.value().decode("utf-8"))

                if event.get("event_type") != "OrderCreated":
                    print("Ignoring unsupported event type")
                    continue

                process_order_created(event)

                # Commit only after processing succeeds.
                consumer.commit(
                    message=msg,
                    asynchronous=False,
                )

            except Exception as exc:
                print(f"Event processing failed: {exc}")
                # Do not commit this message.
                # A restart or reassignment can cause it to be delivered again.

    except KeyboardInterrupt:
        print("\nNotification Service stopping...")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()