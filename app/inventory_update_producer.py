import time
import json
import random
import uuid
from kafka import KafkaProducer
from datetime import datetime, timezone

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

products = ["P001", "P002", "P003", "P004", "P005"]

if __name__ == "__main__":
    start_time = time.time()
    run_duration = 60  # run for 60 seconds
    
    while time.time() - start_time < run_duration:
        inventory_event = {
            "event_id": str(uuid.uuid4()),
            "product_id": random.choice(products),
            "change": random.randint(-100, 100),  # -20 means sold, +50 means restocked
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        print(f"Sending inventory update: {inventory_event}")
        producer.send("inventory-updates", inventory_event)
        time.sleep(random.uniform(1, 5))
