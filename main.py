# main.py

from pymongo import MongoClient
from large_document_handler import save_document, read_document, delete_document
from _secrets import MONGODB_CONNECTION_STRING
import random
import string

# Connect to MongoDB using the provided connection string
client = MongoClient(MONGODB_CONNECTION_STRING)
db = client['test_db']
collection = db['test_collection']

large_document_size = 150,000  # Each 16MB document will have approximately 150,000 fields
small_document_size = 100  # 100 fields, small enough to be inserted directly

# Function to generate a document with many fields
def generate_document(num_fields):
    print(f"Generating a document with {num_fields} fields...")
    document = {'big_document_id': 'large_doc'}
    for i in range(num_fields):
        field_name = f'field_{i}'
        field_value = ''.join(random.choices(string.ascii_letters + string.digits, k=100))  # 100 characters per field
        document[field_name] = field_value
    print("Document generated.")
    return document

# Function to generate and save multiple documents
def generate_and_save_documents(num_large_docs, num_small_docs):
    # Generate and save large document
    for i in range(num_large_docs):
        doc_id = f'large_doc_{i}'
        print(f"\nGenerating large document {doc_id}...")
        large_document = generate_document(large_document_size)
        large_document['big_document_id'] = doc_id
        save_document(collection, large_document)

    # Generate and save small documents
    for i in range(num_small_docs):
        doc_id = f'small_doc_{i}'
        print(f"\nGenerating small document {doc_id}...")
        small_document = generate_document(small_document_size)
        small_document['big_document_id'] = doc_id
        save_document(collection, small_document)

# Function to read and verify multiple documents
def read_and_verify_documents(num_large_docs, num_small_docs):
    # Read and verify large document
    for i in range(num_large_docs):
        doc_id = f'large_doc_{i}'
        print(f"\nReading large document {doc_id}...")
        retrieved_document = read_document(collection, doc_id)
        print(f"Retrieved document {doc_id} has {len(retrieved_document) - 1} fields")  # Subtract 1 for the 'big_document_id' field

    # Read and verify small documents
    for i in range(num_small_docs):
        doc_id = f'small_doc_{i}'
        print(f"\nReading small document {doc_id}...")
        retrieved_document = read_document(collection, doc_id)
        print(f"Retrieved document {doc_id} has {len(retrieved_document) - 1} fields")  # Subtract 1 for the 'big_document_id' field

# Function to delete multiple documents
def delete_documents(num_large_docs, num_small_docs):
    # Delete large documents
    for i in range(num_large_docs):
        doc_id = f'large_doc_{i}'
        print(f"\nDeleting document {doc_id}...")
        delete_document(collection, doc_id)
        print(f"Deleted document {doc_id}")

    # Delete small documents
    for i in range(num_small_docs):
        doc_id = f'small_doc_{i}'
        print(f"\nDeleting document {doc_id}...")
        delete_document(collection, doc_id)
        print(f"Deleted document {doc_id}")

# Number of large and small documents to generate
num_large_docs = 1
num_small_docs = 5

# Generate and save documents
generate_and_save_documents(num_large_docs, num_small_docs)

# Read and verify documents
read_and_verify_documents(num_large_docs, num_small_docs)

# Delete documents
delete_documents(num_large_docs, num_small_docs)
