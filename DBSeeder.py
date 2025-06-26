import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from appwrite.client import Client
from appwrite.id import ID
from appwrite.services.databases import Databases
from appwrite.exception import AppwriteException
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

# Configuration
DATABASE_ID = 'performance_test_db'
DATABASE_NAME = 'Performance Test Database'
COLLECTION_ID = 'users_collection'
COLLECTION_NAME = 'Users Collection'

# Performance tracking
write_response_times = []
read_response_times = []
upsert_response_times = []

write_start_time = 0
write_end_time = 0
read_start_time = 0
read_end_time = 0
upsert_start_time = 0
upsert_end_time = 0

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

upsert_latency_categories = {
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

def ensure_database_exists():
    """Create database if it doesn't exist."""
    try:
        # Try to get the database first
        databases.get(database_id=DATABASE_ID)
        logging.info(f"Database '{DATABASE_ID}' already exists")
        return True
    except AppwriteException as e:
        if e.code == 404:  # Database not found
            try:
                logging.info(f"Creating database '{DATABASE_ID}'...")
                databases.create(
                    database_id=DATABASE_ID,
                    name=DATABASE_NAME,
                    enabled=True
                )
                logging.info(f"Database '{DATABASE_ID}' created successfully")
                return True
            except AppwriteException as create_error:
                logging.error(f"Failed to create database: {create_error}")
                return False
        else:
            logging.error(f"Error checking database: {e}")
            return False

def ensure_collection_exists():
    """Create collection if it doesn't exist."""
    try:
        # Try to get the collection first
        databases.get_collection(database_id=DATABASE_ID, collection_id=COLLECTION_ID)
        logging.info(f"Collection '{COLLECTION_ID}' already exists")
        return True
    except AppwriteException as e:
        if e.code == 404:  # Collection not found
            try:
                logging.info(f"Creating collection '{COLLECTION_ID}'...")
                databases.create_collection(
                    database_id=DATABASE_ID,
                    collection_id=COLLECTION_ID,
                    name=COLLECTION_NAME,
                    permissions=["read(\"any\")", "write(\"any\")"],  # Allow all users to read/write
                    document_security=False,
                    enabled=True
                )
                logging.info(f"Collection '{COLLECTION_ID}' created successfully")
                return True
            except AppwriteException as create_error:
                logging.error(f"Failed to create collection: {create_error}")
                return False
        else:
            logging.error(f"Error checking collection: {e}")
            return False

def ensure_attributes_exist():
    """Create required attributes if they don't exist."""
    required_attributes = [
        {'key': 'Name', 'type': 'string', 'size': 255, 'required': True},
        {'key': 'email', 'type': 'email', 'required': True},
        {'key': 'age', 'type': 'integer', 'required': True, 'min': 0, 'max': 150}
    ]
    
    try:
        # Get existing attributes
        existing_attributes = databases.list_attributes(
            database_id=DATABASE_ID,
            collection_id=COLLECTION_ID
        )
        existing_keys = {attr['key'] for attr in existing_attributes['attributes']}
        
        for attr in required_attributes:
            if attr['key'] not in existing_keys:
                logging.info(f"Creating attribute '{attr['key']}'...")
                try:
                    if attr['type'] == 'string':
                        databases.create_string_attribute(
                            database_id=DATABASE_ID,
                            collection_id=COLLECTION_ID,
                            key=attr['key'],
                            size=attr['size'],
                            required=attr['required']
                        )
                    elif attr['type'] == 'email':
                        databases.create_email_attribute(
                            database_id=DATABASE_ID,
                            collection_id=COLLECTION_ID,
                            key=attr['key'],
                            required=attr['required']
                        )
                    elif attr['type'] == 'integer':
                        databases.create_integer_attribute(
                            database_id=DATABASE_ID,
                            collection_id=COLLECTION_ID,
                            key=attr['key'],
                            required=attr['required'],
                            min=attr.get('min'),
                            max=attr.get('max')
                        )
                    
                    logging.info(f"Attribute '{attr['key']}' created successfully")
                    # Wait a bit for attribute to be ready
                    time.sleep(1)
                    
                except AppwriteException as create_error:
                    logging.error(f"Failed to create attribute '{attr['key']}': {create_error}")
                    return False
            else:
                logging.info(f"Attribute '{attr['key']}' already exists")
        
        return True
        
    except AppwriteException as e:
        logging.error(f"Error managing attributes: {e}")
        return False

def setup_database_infrastructure():
    """Setup database, collection, and attributes if they don't exist."""
    logging.info("Setting up database infrastructure...")
    
    if not ensure_database_exists():
        return False
    
    if not ensure_collection_exists():
        return False
    
    # Wait a bit for collection to be ready
    time.sleep(2)
    
    if not ensure_attributes_exist():
        return False
    
    # Wait for attributes to be fully ready
    logging.info("Waiting for attributes to be ready...")
    time.sleep(5)
    
    logging.info("Database infrastructure setup complete!")
    return True

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
            database_id=DATABASE_ID,
            collection_id=COLLECTION_ID,
            document_id=document_id,
            data={
                'Name': name,
                'email': email,
                'age': age
            }
        )
        end_time = time.perf_counter()
        response_time_ms = (end_time - start_time) * 1000
        write_response_times.append(response_time_ms)
        categorize_latency(response_time_ms, write_latency_categories)
        logging.info(f"Inserted: {response['$id']} - Response Time: {response_time_ms:.2f} ms")
        return response['$id'], email
    except Exception as e:
        logging.error(f"Failed to insert document: {e}")
        return None, None
    finally:
        write_end_time = time.perf_counter()

def upsert_user_documents(count=100):
    """Upsert user documents using the new upsert method."""
    global upsert_start_time
    global upsert_end_time
    
    upsert_start_time = time.perf_counter()
    
    # Prepare documents for upsert
    documents = []
    for _ in range(count):
        document_data = {
            '$id': ID.unique(),  # Use $id for document ID
            'Name': fake.name(),
            'email': fake.email(),
            'age': random.randint(18, 100)
        }
        documents.append(document_data)
    
    try:
        start_time = time.perf_counter()
        response = databases.upsert_documents(
            database_id=DATABASE_ID,
            collection_id=COLLECTION_ID,
            documents=documents
        )
        end_time = time.perf_counter()
        response_time_ms = (end_time - start_time) * 1000
        upsert_response_times.append(response_time_ms)
        categorize_latency(response_time_ms, upsert_latency_categories)
        
        upsert_end_time = time.perf_counter()
        
        logging.info(f"Upserted {len(documents)} documents - Response Time: {response_time_ms:.2f} ms")
        return [doc['$id'] for doc in documents], [doc['email'] for doc in documents]
        
    except Exception as e:
        logging.error(f"Failed to upsert documents: {e}")
        upsert_end_time = time.perf_counter()
        return [], []

def verify_document(doc_id, expected_email):
    """Verify a single document."""
    global read_start_time
    global read_end_time

    if not read_start_time:
        read_start_time = time.perf_counter()

    try:
        start_time = time.perf_counter()
        document = databases.get_document(
            database_id=DATABASE_ID, 
            collection_id=COLLECTION_ID, 
            document_id=doc_id
        )
        end_time = time.perf_counter()
        response_time_ms = (end_time - start_time) * 1000
        read_response_times.append(response_time_ms)
        categorize_latency(response_time_ms, read_latency_categories)
        
        if document and 'email' in document and document['email'] is not None and document['email'] == expected_email:
            logging.info(f"Verified document: {doc_id}, email: {document.get('email')} - Response Time: {response_time_ms:.2f} ms")
            return True
        else:
            logging.error(f"Document {doc_id} verification failed: email mismatch or email is null.")
            return False
    except Exception as e:
        logging.error(f"Failed to verify document {doc_id}: {e}")
        return False
    finally:
        read_end_time = time.perf_counter()

def verify_documents(document_info_list):
    """Verify multiple documents concurrently."""
    verified_count = 0
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(verify_document, doc_id, expected_email) 
                  for doc_id, expected_email in document_info_list]
        for future in as_completed(futures):
            if future.result():
                verified_count += 1
    return verified_count

def seed_users_parallel(count=500):
    """Seed users concurrently using create_document."""
    document_ids_with_email = []
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(create_user_document) for _ in range(count)]
        for future in as_completed(futures):
            doc_id, email = future.result()
            if doc_id and email:
                document_ids_with_email.append((doc_id, email))
    
    logging.info(f"Total documents inserted: {len(document_ids_with_email)}")
    verified_count = verify_documents(document_ids_with_email)
    logging.info(f"Total documents verified: {verified_count}")
    
    return document_ids_with_email

def test_upsert_performance(count=1000):
    """Test upsert performance."""
    logging.info(f"Testing upsert performance with {count} documents...")
    
    batch_size = 100
    all_doc_ids = []
    all_emails = []
    
    for i in range(0, count, batch_size):
        batch_count = min(batch_size, count - i)
        doc_ids, emails = upsert_user_documents(batch_count)
        all_doc_ids.extend(doc_ids)
        all_emails.extend(emails)
        
        # Small delay between batches
        time.sleep(0.1)
    
    # Verify some of the upserted documents
    if all_doc_ids and all_emails:
        sample_size = min(100, len(all_doc_ids))
        sample_docs = list(zip(all_doc_ids[:sample_size], all_emails[:sample_size]))
        verified_count = verify_documents(sample_docs)
        logging.info(f"Verified {verified_count} out of {sample_size} sampled upserted documents")
    
    return list(zip(all_doc_ids, all_emails))

def print_performance_summary():
    """Print comprehensive performance summary."""
    
    # Write performance summary
    if write_response_times:
        logging.info("=== WRITE PERFORMANCE (create_document) ===")
        logging.info(f"Slowest request: {max(write_response_times):.2f} ms")
        logging.info(f"Fastest request: {min(write_response_times):.2f} ms")
        logging.info(f"Average request time: {sum(write_response_times) / len(write_response_times):.2f} ms")
        if write_end_time and write_start_time:
            total_write_time = write_end_time - write_start_time
            write_tps = len(write_response_times) / total_write_time
            logging.info(f"Transactions per second (TPS): {write_tps:.2f}")
        logging.info(f"Latency Categories: {write_latency_categories}")
    
    # Upsert performance summary
    if upsert_response_times:
        logging.info("=== UPSERT PERFORMANCE (upsert_documents) ===")
        logging.info(f"Slowest request: {max(upsert_response_times):.2f} ms")
        logging.info(f"Fastest request: {min(upsert_response_times):.2f} ms")
        logging.info(f"Average request time: {sum(upsert_response_times) / len(upsert_response_times):.2f} ms")
        if upsert_end_time and upsert_start_time:
            total_upsert_time = upsert_end_time - upsert_start_time
            # Calculate documents per second based on total documents upserted
            total_docs_upserted = len(upsert_response_times) * 100  # Assuming 100 docs per batch
            upsert_dps = total_docs_upserted / total_upsert_time
            logging.info(f"Documents per second: {upsert_dps:.2f}")
        logging.info(f"Latency Categories: {upsert_latency_categories}")
    
    # Read performance summary
    if read_response_times:
        logging.info("=== READ PERFORMANCE (get_document) ===")
        logging.info(f"Slowest request: {max(read_response_times):.2f} ms")
        logging.info(f"Fastest request: {min(read_response_times):.2f} ms")
        logging.info(f"Average request time: {sum(read_response_times) / len(read_response_times):.2f} ms")
        if read_end_time and read_start_time:
            total_read_time = read_end_time - read_start_time
            read_tps = len(read_response_times) / total_read_time
            logging.info(f"Transactions per second (TPS): {read_tps:.2f}")
        logging.info(f"Latency Categories: {read_latency_categories}")

def run_comprehensive_test(create_count=500, upsert_count=1000):
    """Run comprehensive performance test with both create and upsert methods."""
    
    # Setup infrastructure
    if not setup_database_infrastructure():
        logging.error("Failed to setup database infrastructure. Exiting.")
        return
    
    logging.info("=== Starting Comprehensive Performance Test ===")
    
    # Test individual document creation
    logging.info(f"Testing individual document creation with {create_count} documents...")
    create_results = seed_users_parallel(create_count)
    
    # Test batch upsert
    logging.info(f"Testing batch upsert with {upsert_count} documents...")
    upsert_results = test_upsert_performance(upsert_count)
    
    # Print comprehensive summary
    print_performance_summary()
    
    logging.info("=== Performance Test Complete ===")
    logging.info(f"Total documents created individually: {len(create_results)}")
    logging.info(f"Total documents upserted: {len(upsert_results)}")

if __name__ == '__main__':
    # Run comprehensive test
    run_comprehensive_test(create_count=500, upsert_count=1000)
    
    # Or test just upsert performance
    # test_upsert_performance(1000)
