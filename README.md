# Large Document Handler

This project demonstrates handling large documents in MongoDB by splitting them into smaller chunks, saving, reading, and deleting them.

## Prerequisites

- Python 3.13
- MongoDB

## Installation

1. Clone the repository:

    ```sh
    git clone https://github.com/ptmfitch/large-document-handler.git
    cd large-document-handler
    ```

2. Create a virtual environment:

    ```sh
    python -m venv .env
    ```

3. Activate the virtual environment:

    - On Windows:

        ```sh
        .env\Scripts\activate
        ```

    - On macOS/Linux:

        ```sh
        source .env/bin/activate
        ```

4. Install the required packages:

    ```sh
    pip install -r requirements.txt
    ```

5. Set up your MongoDB connection string:

    - Copy [_secrets.example.py](http://_vscodecontentref_/2) to [_secrets.py](http://_vscodecontentref_/3):

        ```sh
        cp _secrets.example.py _secrets.py
        ```

    - Edit [_secrets.py](http://_vscodecontentref_/4) and update the [MONGODB_CONNECTION_STRING](http://_vscodecontentref_/5) with your MongoDB connection string.

## Running the Script

To run the [main.py](http://_vscodecontentref_/6) script, use the following command:

```sh
python main.py
```

This script will:

- Generate and save large and small documents.
- Read and verify the saved documents.
- Delete the documents.

## main.py Step by Step Flow

1. Connect to MongoDB using the connection string from `_secrets.py`.
2. Generate a large document with 150,000 fields and split it into smaller chunks of 14MB in size.
3. Save the chunks to MongoDB in a single transaction.
4. Read the chunks from MongoDB and reconstruct the original document by using the big_document_id and chunk_index to identify and order related chunks.
5. Verify the integrity of the reconstructed document by counting the number of fields in the resulting documents.
6. Delete the chunks from MongoDB in a single transaction.
