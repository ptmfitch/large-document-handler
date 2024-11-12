package large-document-handler.java;

public package org.example;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

public class Main {
    public static void main(String[] args) {
        MongoClient client = MongoClients.create("mongodb://localhost:27017");
        MongoDatabase db = client.getDatabase("test_db");
        MongoCollection<Document> collection = db.getCollection("test_collection");

        int numLargeDocs = 1;
        int numSmallDocs = 5;

        generateAndSaveDocuments(numLargeDocs, numSmallDocs, collection);
        readAndVerifyDocuments(numLargeDocs, numSmallDocs, collection);
        deleteDocuments(numLargeDocs, numSmallDocs, collection);
    }

    private static void generateAndSaveDocuments(int numLargeDocs, int numSmallDocs, MongoCollection<Document> collection) {
        for (int i = 0; i < numLargeDocs; i++) {
            String docId = "large_doc_" + i;
            System.out.printf("\nGenerating large document %s...\n", docId);
            Document largeDocument = DocumentHandler.generateDocument(150000); // 150,000 fields to exceed 16MB
            largeDocument.append("big_document_id", docId);
            DocumentHandler.saveDocument(collection, largeDocument);
        }

        for (int i = 0; i < numSmallDocs; i++) {
            String docId = "small_doc_" + i;
            System.out.printf("\nGenerating small document %s...\n", docId);
            Document smallDocument = DocumentHandler.generateDocument(100); // 100 fields, small enough to be inserted directly
            smallDocument.append("big_document_id", docId);
            DocumentHandler.saveDocument(collection, smallDocument);
        }
    }

    private static void readAndVerifyDocuments(int numLargeDocs, int numSmallDocs, MongoCollection<Document> collection) {
        for (int i = 0; i < numLargeDocs; i++) {
            String docId = "large_doc_" + i;
            System.out.printf("\nReading large document %s...\n", docId);
            Document retrievedDocument = DocumentHandler.readDocument(collection, docId);
            System.out.printf("Retrieved document %s has %d fields\n", docId, retrievedDocument.size() - 1); // Subtract 1 for the 'big_document_id' field
        }

        for (int i = 0; i < numSmallDocs; i++) {
            String docId = "small_doc_" + i;
            System.out.printf("\nReading small document %s...\n", docId);
            Document retrievedDocument = DocumentHandler.readDocument(collection, docId);
            System.out.printf("Retrieved document %s has %d fields\n", docId, retrievedDocument.size() - 1); // Subtract 1 for the 'big_document_id' field
        }
    }

    private static void deleteDocuments(int numLargeDocs, int numSmallDocs, MongoCollection<Document> collection) {
        for (int i = 0; i < numLargeDocs; i++) {
            String docId = "large_doc_" + i;
            System.out.printf("\nDeleting document %s...\n", docId);
            DocumentHandler.deleteDocument(collection, docId);
            System.out.printf("Deleted document %s\n", docId);
        }

        for (int i = 0; i < numSmallDocs; i++) {
            String docId = "small_doc_" + i;
            System.out.printf("\nDeleting document %s...\n", docId);
            DocumentHandler.deleteDocument(collection, docId);
            System.out.printf("Deleted document %s\n", docId);
        }
    }
}
