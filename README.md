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

This script will:

Generate and save large and small documents.
Read and verify the saved documents.
Delete the documents.
Usage Examples

