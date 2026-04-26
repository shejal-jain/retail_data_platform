# ingestion/sources/orders_source.py

import random
import uuid
from datetime import datetime, UTC


def generate_order():
    """
    Generates a single order record (simulates one transaction)
    """

    return {
        # Unique order ID (UUID ensures no duplicates)
        "order_id": str(uuid.uuid4()),

        # Random customer ID (simulating existing customers)
        "customer_id": random.randint(1, 100),

        # Random product ID (simulating existing products)
        "product_id": random.randint(1, 50),

        # Quantity of product purchased
        "quantity": random.randint(1, 5),

        # Price per order (random float with 2 decimal places)
        "price": round(random.uniform(100, 1000), 2),

        # Timestamp of order (timezone-aware UTC)
        "order_timestamp": datetime.now(UTC).isoformat()
    }


def generate_orders(n=10):
    """
    Generates multiple orders

    Parameters:
    n (int): number of orders to generate

    Returns:
    list: list of order dictionaries
    """

    orders = []

    for _ in range(n):
        order = generate_order()
        orders.append(order)

    return orders