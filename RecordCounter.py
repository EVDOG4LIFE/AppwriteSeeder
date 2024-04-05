import logging
import concurrent.futures
from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.query import Query

# Initialize logging
logging.basicConfig(level=logging.INFO)

# Setup Appwrite client
client = Client()
client.set_endpoint('https://cloud.appwrite.io/v1')  # Your API Endpoint
client.set_project('1234')  # Your project ID here
client.set_key('1234')  # Your secret API key here
databases = Databases(client)
database_id = '1234'  # Your database ID here
collection_id = '1234'  # Your collection ID here

def retrieve_documents(page):
    try:
        page_size = 1000
        response = databases.list_documents(
            database_id,
            collection_id,
            queries=[
                Query.limit(page_size),
                Query.offset(page * page_size)
            ]
        )
        documents = response['documents']
        logging.info(f"Retrieved {len(documents)} documents (page: {page + 1})")
        return documents
    except Exception as e:
        logging.error(f"Something messed up: {e}")
        return []

def process_documents(documents):
    for document in documents:
        # Perform any desired operations with each document
        logging.info(f"Document ID: {document['$id']}")
        # Add more logging or processing as needed

def scan_collection():
    total_documents = 0
    page = 0
    max_workers = 150  # Adjust the number of workers based on your system and API rate limits

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        while True:
            futures = [executor.submit(retrieve_documents, page + i) for i in range(max_workers)]
            completed_futures = concurrent.futures.as_completed(futures)

            documents = []
            for future in completed_futures:
                documents.extend(future.result())

            total_documents += len(documents)

            if not documents:
                break

            process_documents(documents)
            page += max_workers

    logging.info(f"Total documents scanned: {total_documents}")

if __name__ == "__main__":
    scan_collection()