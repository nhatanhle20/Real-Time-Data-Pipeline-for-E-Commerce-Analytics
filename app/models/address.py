import os
import csv
import random
import uuid
from faker import Faker

faker = Faker('en_US')

# Determine the absolute path to the CSV file
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # goes from models → app
CSV_PATH = os.path.join(BASE_DIR, 'data', 'us_coordinates.csv')

def load_us_cities(file_path=CSV_PATH):
    with open(file_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return [(row["city"], row["state"]) for row in reader]

def generate_addresses(n=50, city_state_pool=None):
    if not city_state_pool:
        raise ValueError("No city/state data provided")

    return [
        {
            "address_id": str(uuid.uuid4()),
            "street": faker.street_address(),
            "city": city,
            "state": state,
            "postal_code": faker.zipcode(),
            "country": "United States"
        }
        for city, state in random.choices(city_state_pool, k=n)
    ]

# # Example test (remove in production)
# if __name__ == "__main__":
#     city_state_list = load_us_cities()
#     addresses = generate_addresses(5, city_state_list)
#     for a in addresses:
#         print(a)
