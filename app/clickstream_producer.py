import time
import json
import random
import uuid
from faker import Faker
from kafka import KafkaProducer
from datetime import datetime, timezone

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

pages = ["/home", "/cart", "/checkout", "/search", 
         "/product/trending", "/product/on_sale", "/product/new_released", ]

actions = ["click", "back", "view", "scroll", "add_to_cart", "remove from cart"]

if __name__ == "__main__":
    start_time = time.time()
    run_duration = 60  # run for 60 seconds

    while time.time() - start_time < run_duration:
        click_event = {
            "event_id": str(uuid.uuid4()),
            "user_id": str(uuid.uuid4()),
            "page_url": random.choice(pages),
            "action": random.choice(actions),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        print(f"Sending clickstream: {click_event}")
        producer.send("user-clickstream", click_event)
        time.sleep(random.uniform(1, 5))
