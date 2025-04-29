import uuid
from faker import Faker

faker = Faker()

def generate_users(n=50):
    return [
        {"user_id": str(uuid.uuid4()), 
         "name": faker.name(), 
         "email": faker.email()}
        for _ in range(n)
    ]