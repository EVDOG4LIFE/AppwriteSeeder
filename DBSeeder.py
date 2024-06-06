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

write_response_times = []
read_response_times = []

write_start_time = 0
write_end_time = 0
read_start_time = 0
read_end_time = 0

# Categorization dictionaries
write_latency_categories = {
    '<1000ms': 0,
    '1000-2000ms': 0,
    '2000-3000ms': 0,
    '3000-4000ms': 0,
    '4000-5000ms': 0,
    '>5000ms': 0
}

read_latency_categories = {
    '<1000ms': 0,
    '1000-2000ms': 0,
    '2000-3000ms': 0,
    '3000-4000ms': 0,
    '4000-5000ms': 0,
    '>5000ms': 0
}

def categorize_latency(response_time_ms, category_dict):
    """Categorize the response time into the appropriate latency category."""
    if response_time_ms < 1000:
        category_dict['<1000ms'] += 1
    elif response_time_ms < 2000:
        category_dict['1000-2000ms'] += 1
    elif response_time_ms < 3000:
        category_dict['2000-3000ms'] += 1
    elif response_time_ms < 4000:
        category_dict['3000-4000ms'] += 1
    elif response_time_ms < 5000:
        category_dict['4000-5000ms'] += 1
    else:
        category_dict['>5000ms'] += 1

def create_user_document():
    """Create a user document with random data."""
    global write_start_time
    global write_end_time

    if not write_start_time:
        write_start_time = time.perf_counter()

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
        write_response_times.append(response_time_ms)
        categorize_latency(response_time_ms, write_latency_categories)
        logging.info(f"Inserted: {response['$id']} - Response Time: {response_time_ms:.2f} ms")
        return response['$id'], email  # Return both document ID and email
    except Exception as e:
        logging.error(f"Failed to insert document: {e}")
        return None, None
    finally:
        write_end_time = time.perf_counter()

def verify_document(doc_id, expected_email):
    """Verify a single document."""
    global read_start_time
    global read_end_time

    if not read_start_time:
        read_start_time = time.perf_counter()

    try:
        start_time = time.perf_counter()
        document = databases.get_document(database_id=database_id, collection_id=collection_id, document_id=doc_id)
        end_time = time.perf_counter()
        response_time_ms = (end_time - start_time) * 1000
        read_response_times.append(response_time_ms)
        categorize_latency(response_time_ms, read_latency_categories)
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
    finally:
        read_end_time = time.perf_counter()

def verify_documents(document_ids):
    """Verify multiple documents concurrently."""
    verified_count = 0
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(verify_document, doc_id, expected_email) for doc_id, expected_email in document_ids]
        for future in as_completed(futures):
            if future.result():
                verified_count += 1
    return verified_count

def seed_users_parallel(count=500):
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

    # Calculate and print response time summary for writes
    if write_response_times:
        logging.info(f"Write Requests - Slowest request: {max(write_response_times):.2f} ms")
        logging.info(f"Write Requests - Fastest request: {min(write_response_times):.2f} ms")
        logging.info(f"Write Requests - Average request time: {sum(write_response_times) / len(write_response_times):.2f} ms")
        total_write_time = write_end_time - write_start_time
        write_tps = len(write_response_times) / total_write_time
        logging.info(f"Write Requests - Transactions per second (TPS): {write_tps:.2f}")
        logging.info(f"Write Requests - Latency Categories: {write_latency_categories}")
    else:
        logging.info("No write response times recorded.")

    # Calculate and print response time summary for reads
    if read_response_times:
        logging.info(f"Read Requests - Slowest request: {max(read_response_times):.2f} ms")
        logging.info(f"Read Requests - Fastest request: {min(read_response_times):.2f} ms")
        logging.info(f"Read Requests - Average request time: {sum(read_response_times) / len(read_response_times):.2f} ms")
        total_read_time = read_end_time - read_start_time
        read_tps = len(read_response_times) / total_read_time
        logging.info(f"Read Requests - Transactions per second (TPS): {read_tps:.2f}")
        logging.info(f"Read Requests - Latency Categories: {read_latency_categories}")
    else:
        logging.info("No read response times recorded.")

if __name__ == '__main__':
    seed_users_parallel(1000)  # Replace with a reasonable number for testing
