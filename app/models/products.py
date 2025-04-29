import uuid
import random
from faker import Faker
from faker_commerce import Provider

faker = Faker()
faker.add_provider(Provider)

CATEGORIES = ["Electronics", "Clothing", "Books", "Beauty"]

def generate_products(n=50):
    return [
        {
            "product_id": str(uuid.uuid4()),
            "category": faker.ecommerce_category(),
            "price": round(random.uniform(10, 500), 2)
        }
        for _ in range(n)
    ]
