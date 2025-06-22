import time
import json
import random
from kafka import KafkaProducer
from models.users import generate_users
from models.products import generate_products
from models.order_generator import generate_order
from models.address import generate_addresses, load_us_cities

producer = KafkaProducer(
    bootstrap_servers="kafka:29092", # "localhost:9092" to run producer.py on local machine; "kafka:29092" to run on airflow 
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5,
    max_request_size=1_000_000
)


users = generate_users()
products = generate_products()

city_state_pool = load_us_cities()
addresses = generate_addresses(n=50, city_state_pool=city_state_pool)


if __name__ == "__main__":
    start_time = time.time()
    run_duration = 60  # run for 60 seconds

    while time.time() - start_time < run_duration:
        user = random.choice(users)
        product = random.choice(products)
        address = random.choice(addresses)

        order = generate_order(user, product, address)
        print(f"Sending: {order}")
        producer.send("ecommerce-transactions", order)
        producer.flush()
        time.sleep(random.uniform(1, 5))
