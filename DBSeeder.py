from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.id import ID
from faker import Faker
import random

# Initialize the Faker library
fake = Faker()

# Setup Appwrite client
client = Client()
client.set_endpoint('ENDPOINT') # Your Appwrite Endpoint (e.g. https://[HOSTNAME_OR_IP]/v1' for self-hosted or 'https://appwrite.io/v1' for cloud)
client.set_project('<PROJECT-ID') # Your project ID
client.set_key('<API-KEY>') # Your secret API key

databases = Databases(client)

database_id = '<DATABASE-ID>' # Your database ID
collection_id = '<COLLECTION-ID>' # Your collection ID

def seed_users(count=10):
    for _ in range(count):
        name = fake.name()
        email = fake.email()
        age = random.randint(18, 100)  
        
        response = databases.create_document(
            database_id=database_id,
            collection_id=collection_id,
            document_id=ID.unique(), 
            data={
                'Name': name,
                'email': email,
                'age': age
            }
        )
        print(f"Inserted: {response}")

seed_users(450)