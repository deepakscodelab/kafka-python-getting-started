import json

from confluent_kafka import Producer

from events import OrderCreatedEvent


producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "acks": "all",
    "enable.idempotence": True,
})


def delivery_report(err, msg):
    if err is not None:
        print(f"Kafka delivery failed: {err}")
        return

    print(
        f"Event delivered successfully | "
        f"topic={msg.topic()} | "
        f"partition={msg.partition()} | "
        f"offset={msg.offset()}"
    )


def publish_order_created(event: OrderCreatedEvent) -> None:
    try:
        producer.produce(
            topic="orders",
            key=event["order_id"],
            value=json.dumps(event),
            callback=delivery_report,
        )

        # Serve delivery callbacks and send queued messages.
        remaining = producer.flush(timeout=10)

        if remaining:
            raise RuntimeError(
                f"{remaining} Kafka message(s) remain undelivered"
            )

    except BufferError as exc:
        raise RuntimeError(
            "Kafka producer queue is full"
        ) from exc