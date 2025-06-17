import uuid
from faker import Faker

faker = Faker()

def generate_addresses(n=50):
    return [
        {
            "address_id": str(uuid.uuid4()),
            "street": faker.street_address(),
            "city": faker.city(),
            "state": faker.state(),
            "postal_code": faker.postcode(),
            "country": faker.country()
        }
        for _ in range(n)
    ]
