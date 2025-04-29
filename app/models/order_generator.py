import uuid
import random
from datetime import datetime, timezone

PAYMENT_METHODS = ["Credit Card", "PayPal", "Cash", "Bank Transfer"]

def generate_order(user, product):
    return {
        "order_id": str(uuid.uuid4()),
        "user_id": user["user_id"],
        "user_name": user["name"],
        "user_email": user["email"],
        "product_id": product["product_id"],
        "category": product["category"],
        "price": product["price"],
        "quantity": random.randint(1, 5),
        "payment_method": random.choice(PAYMENT_METHODS),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
