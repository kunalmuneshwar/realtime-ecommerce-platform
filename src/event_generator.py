
import random
import uuid
import time
from datetime import datetime, timezone

PRODUCTS = [
    {"product_id": "P101", "name": "Laptop", "price": 55000},
    {"product_id": "P102", "name": "Headphones", "price": 2500},
    {"product_id": "P103", "name": "Smartphone", "price": 25000},
    {"product_id": "P104", "name": "Keyboard", "price": 1500},
    {"product_id": "P105", "name": "Mouse", "price": 800}
]

CUSTOMERS = [f"C{i}" for i in range(1001, 1101)]

# Orders waiting for their payment event
pending_payments = []


def current_timestamp():
    return datetime.now(timezone.utc).isoformat()


def generate_event():

    # Occasionally complete a previously placed order
    if pending_payments and random.random() < 0.35:

        order = pending_payments.pop(0)

        payment_status = random.choices(
            ["payment_success", "payment_failed"],
            weights=[90, 10]
        )[0]

        return {
            "event_id": str(uuid.uuid4()),
            "event_type": payment_status,
            "order_id": order["order_id"],
            "customer_id": order["customer_id"],
            "product_id": order["product_id"],
            "product_name": order["product_name"],
            "price": order["price"],
            "timestamp": current_timestamp()
        }

    # Generate a browsing, cart, or order event
    product = random.choice(PRODUCTS)
    customer_id = random.choice(CUSTOMERS)

    event_type = random.choices(
        ["product_viewed", "add_to_cart", "order_placed"],
        weights=[55, 30, 15]
    )[0]

    order_id = None

    if event_type == "order_placed":
        order_id = str(uuid.uuid4())

        # Save order details for its future payment event
        pending_payments.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "product_id": product["product_id"],
            "product_name": product["name"],
            "price": product["price"]
        })

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "order_id": order_id,
        "customer_id": customer_id,
        "product_id": product["product_id"],
        "product_name": product["name"],
        "price": product["price"],
        "timestamp": current_timestamp()
    }


if __name__ == "__main__":

    while True:
        event = generate_event()
        print(event, flush=True)
        time.sleep(2)
