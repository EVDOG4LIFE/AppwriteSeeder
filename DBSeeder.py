import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from appwrite.client import Client
from appwrite.id import ID
from appwrite.services.databases import Databases
from faker import Faker

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize the Faker library
fake = Faker()

# Setup Appwrite client
client = Client()
client.set_endpoint('https://cloud.appwrite.io/v1')  # Your API Endpoint
client.set_project('1234')  # Your project ID here
client.set_key('1234')  # Your secret API key here
databases = Databases(client)
database_id = '1234'  # Your database ID here
collection_id = '1234'  # Your collection ID here

response_times = []

def create_user_document():
    """Create a user document with random data."""
    name = fake.name()
    email = fake.email()
    age = random.randint(18, 100)
    document_id = ID.unique()
    try:
        start_time = time.perf_counter()
        response = databases.create_document(
            database_id=database_id,
            collection_id=collection_id,
            document_id=document_id,
            data={
                'Name': name,
                'email': email,  # Ensure this matches your collection's schema
                'age': age
            }
        )
        end_time = time.perf_counter()
        response_time_ms = (end_time - start_time) * 1000
        response_times.append(response_time_ms)
        logging.info(f"Inserted: {response['$id']} - Response Time: {response_time_ms:.2f} ms")
        return response['$id'], email  # Return both document ID and email
    except Exception as e:
        logging.error(f"Failed to insert document: {e}")
        return None, None

def verify_document(doc_id, expected_email):
    """Verify a single document."""
    try:
        start_time = time.perf_counter()
        document = databases.get_document(database_id=database_id, collection_id=collection_id, document_id=doc_id)
        end_time = time.perf_counter()
        response_time_ms = (end_time - start_time) * 1000
        response_times.append(response_time_ms)
        # Check if 'email' is in document and not None, then verify its equality to expected_email
        if document and 'email' in document and document['email'] is not None and document['email'] == expected_email:
            logging.info(f"Verified document: {doc_id}, email: {document.get('email')} - Response Time: {response_time_ms:.2f} ms")
            return True
        else:
            # Log if email is missing, None, or doesn't match the expected_email
            logging.error(f"Document {doc_id} verification failed: email mismatch or email is null.")
            return False
    except Exception as e:
        logging.error(f"Failed to verify document {doc_id}: {e}")
        return False

def verify_documents(document_ids):
    """Verify multiple documents concurrently."""
    verified_count = 0
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(verify_document, doc_id, expected_email) for doc_id, expected_email in document_ids]
        for future in as_completed(futures):
            if future.result():
                verified_count += 1
    return verified_count

def seed_users_parallel(count=150):
    """Seed users concurrently."""
    document_ids_with_email = []
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(create_user_document) for _ in range(count)]
        for future in as_completed(futures):
            doc_id, email = future.result()
            if doc_id and email:
                document_ids_with_email.append((doc_id, email))  # Store tuples of document ID and email
    logging.info(f"Total documents inserted: {len(document_ids_with_email)}")
    verified_count = verify_documents(document_ids_with_email)
    logging.info(f"Total documents verified: {verified_count}")

    # Calculate and print response time summary
    if response_times:
        logging.info(f"Slowest request: {max(response_times):.2f} ms")
        logging.info(f"Fastest request: {min(response_times):.2f} ms")
        logging.info(f"Average request time: {sum(response_times) / len(response_times):.2f} ms")
    else:
        logging.info("No response times recorded.")

if __name__ == '__main__':
    seed_users_parallel(50)  # Replace with a reasonable number for testing