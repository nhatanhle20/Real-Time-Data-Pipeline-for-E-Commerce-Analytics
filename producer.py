from kafka import KafkaProducer
from faker import Faker
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

fake = Faker()

def generate_transaction():
    return {
        "order_id": fake.uuid4(),
        "user_id": fake.uuid4(),
        "product_id": fake.uuid4(),
        "category": random.choice(["Electronics", "Clothing", "Books"]),
        "price": round(random.uniform(10.0, 500.0), 2),
        "quantity": random.randint(1, 5),
        "payment_method": random.choice(["Credit Card", "Paypal", "COD"]),
        "order_status": "Completed",
        "timestamp": fake.iso8601()
    }

while True:
    data = generate_transaction()
    print(f"Sending: {data}")
    producer.send("ecommerce-transactions", data)
    time.sleep(1)
