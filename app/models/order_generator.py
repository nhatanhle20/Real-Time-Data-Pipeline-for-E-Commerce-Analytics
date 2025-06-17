import uuid
import random
from datetime import datetime, timezone

PAYMENT_METHODS = ["Credit Card", "PayPal", "COD", "Bank Transfer"]
SHIPPING_METHODS = ["Free", "Standard", "Fast Express", " Next Day Shipping"]

def generate_order(user, product, address):
    return {
        "order_id": str(uuid.uuid4()),
        "user_id": user["user_id"],
        "user_name": user["name"],
        "user_email": user["email"],
        "street": address["street"],
        "city": address["city"],
        "state": address["state"],
        "postal_code": address["postal_code"],
        "country": address["country"],
        "product_id": product["product_id"],
        "category": product["category"],
        "price": product["price"],
        "quantity": random.randint(1, 5),
        "shipping_method": random.choice(SHIPPING_METHODS),
        "payment_method": random.choice(PAYMENT_METHODS),
        "timestamp": datetime.now(timezone.utc).isoformat(),    
    }
