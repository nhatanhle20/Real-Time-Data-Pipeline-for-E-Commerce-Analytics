import time
import json
import random
import uuid
from kafka import KafkaProducer
from datetime import datetime, timezone
from models.products import generate_products

products = generate_products()

producer = KafkaProducer(
    bootstrap_servers="kafka:29092", # "localhost:9092" to run on local machine; "kafka:29092" to run on airflow 
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

change_type = ["sold", "restock"]

if __name__ == "__main__":
    start_time = time.time()
    run_duration = 60  # run for 60 seconds
    product = random.choice(products)

    while time.time() - start_time < run_duration:
        inventory_event = {
            "event_id": str(uuid.uuid4()),
            "product_id": product["product_id"],
            "change_type": random.choice(change_type),
            "quantity_change": random.randint(0, 100),  
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        print(f"Sending inventory update: {inventory_event}")
        producer.send("inventory-updates", inventory_event)
        producer.flush()
        time.sleep(random.uniform(1, 5))
