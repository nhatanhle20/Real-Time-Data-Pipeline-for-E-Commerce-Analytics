import time
import json
import random
from kafka import KafkaProducer
from models.users import generate_users
from models.products import generate_products
from models.order_generator import generate_order
from models.address import generate_addresses

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5,
    max_request_size=1_000_000
)


users = generate_users()
products = generate_products()
addresses = generate_addresses()


if __name__ == "__main__":
    while True:
        user = random.choice(users)
        product = random.choice(products)
        address = random.choice(addresses)
        
        order = generate_order(user, product, address)
        print(f"Sending: {order}")
        producer.send("ecommerce-transactions", order)
        producer.flush()
        time.sleep(1)
