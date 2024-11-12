# large_document_handler.py

import bson
import sys

# Constants
CHUNK_SIZE = 14 * 1024 * 1024  # 14MB for safety

# Function to split a document into chunks by fields
def split_document(document, max_chunk_size):
    print(f"Splitting document into chunks with a maximum size of {max_chunk_size} bytes per chunk...")
    fields = list(document.keys())
    fields.remove('big_document_id')
    chunk_documents = []
    big_document_id = document['big_document_id']
    current_chunk = {}
    current_chunk_size = 0
    chunk_index = 0

    for field in fields:
        field_size = sys.getsizeof(field) + sys.getsizeof(document[field])
        if current_chunk_size + field_size > max_chunk_size:
            current_chunk['big_document_id'] = big_document_id
            current_chunk['chunk_index'] = chunk_index
            if chunk_index > 0:
                current_chunk['next_id'] = f'{big_document_id}_chunk_{chunk_index + 1}'
            chunk_documents.append(current_chunk)
            print(f"Created chunk {chunk_index} with size {current_chunk_size} bytes.")
            current_chunk = {}
            current_chunk_size = 0
            chunk_index += 1
        current_chunk[field] = document[field]
        current_chunk_size += field_size

    if current_chunk:
        current_chunk['big_document_id'] = big_document_id
        current_chunk['chunk_index'] = chunk_index
        chunk_documents.append(current_chunk)
        print(f"Created chunk {chunk_index} with size {current_chunk_size} bytes.")

    print("Document split into chunks.")
    return chunk_documents

# Function to save a document with error handling
def save_document(collection, document):
    print("Attempting to save document...")
    try:
        collection.insert_one(document)
        print("Document saved successfully.")
    except bson.errors.BSONError:
        print("Document too large, splitting into smaller chunks...")
        split_and_save_document(collection, document)

# Function to split and save a document into chunks
def split_and_save_document(collection, document):
    chunk_documents = split_document(document, CHUNK_SIZE)
    session = collection.database.client.start_session()
    try:
        session.start_transaction()
        collection.insert_many(chunk_documents, session=session)
        session.commit_transaction()
        print("Chunks saved successfully.")
    except Exception as e:
        session.abort_transaction()
        raise RuntimeError("Failed to save document chunks", e)
    finally:
        session.end_session()
    print("Chunks saved successfully.")

# Function to read a document by piecing together chunks
def read_document(collection, big_document_id):
    print(f"Reading document with big_document_id: {big_document_id}...")
    document = collection.find_one({'big_document_id': big_document_id})
    if document and 'chunk_index' not in document:
        print("Document found as a single document.")
        return document

    print("Document not found as a single document. Retrieving chunks...")
    chunks = collection.find({'big_document_id': big_document_id, 'chunk_index': {'$exists': True}}).sort('chunk_index')
    document = {'big_document_id': big_document_id}
    for chunk in chunks:
        print(f"Processing chunk {chunk['chunk_index']}...")
        for key, value in chunk.items():
            if key not in ['_id', 'big_document_id', 'chunk_index', 'next_id']:
                document[key] = value
    print("Document reassembled from chunks.")
    return document

# Function to delete a document and its chunks
def delete_document(collection, big_document_id):
    print(f"Deleting document with big_document_id: {big_document_id}...")
    session = collection.database.client.start_session()
    try:
        session.start_transaction()
        collection.delete_many({'big_document_id': big_document_id}, session=session)
        session.commit_transaction()
        print("Document deleted successfully.")
    except Exception as e:
        session.abort_transaction()
        raise RuntimeError(f"Failed to delete documents for big_document_id: {big_document_id}", e)
    finally:
        session.end_session()
