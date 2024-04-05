from appwrite.client import Client
from appwrite.services.storage import Storage
from appwrite.query import Query

def main():
    # Initialize the Appwrite client
    client = Client()
    client.set_endpoint('https://cloud.appwrite.io/v1')  # Your API Endpoint
    client.set_project('1234')  # Your project ID
    client.set_key('1234')  # Your secret API key

    # Initialize the Storage service
    storage = Storage(client)

    # Your bucket ID
    bucket_id = '1234'

    # Retrieve files based on MIME type
    mime_type = 'image/png'  # Specify the desired MIME type
    try:
        response = storage.list_files(bucket_id, search=f'mimeType:{mime_type}')
        files = response['files']
        print(f"List of files with MIME type '{mime_type}':")
        for file in files:
            print(file)
    except Exception as e:
        print(f"Error listing files with MIME type '{mime_type}':", str(e))

if __name__ == '__main__':
    main()
