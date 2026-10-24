from typing import TypedDict


class OrderCreatedEvent(TypedDict):
    event_id: str
    event_type: str
    event_version: int
    order_id: str
    customer_id: str
    total_amount: float
    currency: str