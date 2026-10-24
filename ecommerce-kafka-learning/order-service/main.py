from uuid import uuid4

from events import OrderCreatedEvent
from producer import publish_order_created


def create_order(
    order_id: str,
    customer_id: str,
    total_amount: float,
) -> OrderCreatedEvent:
    # In a real application, validate the request
    # and save the order in PostgreSQL first.

    order_event: OrderCreatedEvent = {
        "event_id": str(uuid4()),
        "event_type": "OrderCreated",
        "event_version": 1,
        "order_id": order_id,
        "customer_id": customer_id,
        "total_amount": total_amount,
        "currency": "INR",
    }

    publish_order_created(order_event)

    return order_event


if __name__ == "__main__":
    event = create_order(
        order_id="ORD-1001",
        customer_id="CUST-101",
        total_amount=2500.0,
    )

    print("Order created:")
    print(event)