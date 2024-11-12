package org.example;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import org.bson.Document;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.BsonCodec;
import org.bson.codecs.BsonCodecProvider;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
public class DocumentHandler {
    private static final int CHUNK_SIZE = 14 * 1024 * 1024; // 14MB for safety
    private static final Random random = new Random();
    // Function to generate a document with many fields
    public static Document generateDocument(int numFields) {
        System.out.printf("Generating a document with %d fields...%n", numFields);
        Document document = new Document("big_document_id", "large_doc");
        for (int i = 0; i < numFields; i++) {
            String fieldName = "field_" + i;
            String fieldValue = generateRandomString(100); // 100 characters per field
            document.append(fieldName, fieldValue);
        }
        System.out.println("Document generated.");
        return document;
    }
    private static String generateRandomString(int length) {
        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder result = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            result.append(characters.charAt(random.nextInt(characters.length())));
        }
        return result.toString();
    }
    // Function to split a document into chunks by fields
    public static List<Document> splitDocument(Document document, int maxChunkSize) {
        System.out.printf("Splitting document into chunks with a maximum size of %d bytes per chunk...%n", maxChunkSize);
        List<Document> chunkDocuments = new ArrayList<>();
        String bigDocumentId = document.getString("big_document_id");
        Document currentChunk = new Document();
        int currentChunkSize = 0;
        int chunkIndex = 0;
        for (String field : document.keySet()) {
            if (field.equals("big_document_id")) continue;
            int fieldSize = field.getBytes().length + document.get(field).toString().getBytes().length;
            if (currentChunkSize + fieldSize > maxChunkSize) {
                currentChunk.append("big_document_id", bigDocumentId);
                currentChunk.append("chunk_index", chunkIndex);
                if (chunkIndex > 0) {
                    currentChunk.append("next_id", bigDocumentId + "_chunk_" + (chunkIndex + 1));
                }
                chunkDocuments.add(currentChunk);
                System.out.printf("Created chunk %d with size %d bytes.%n", chunkIndex, currentChunkSize);
                currentChunk = new Document();
                currentChunkSize = 0;
                chunkIndex++;
            }
            currentChunk.append(field, document.get(field));
            currentChunkSize += fieldSize;
        }
        if (!currentChunk.isEmpty()) {
            currentChunk.append("big_document_id", bigDocumentId);
            currentChunk.append("chunk_index", chunkIndex);
            chunkDocuments.add(currentChunk);
            System.out.printf("Created chunk %d with size %d bytes.%n", chunkIndex, currentChunkSize);
        }
        System.out.println("Document split into chunks.");
        return chunkDocuments;
    }
    // Function to save a document with error handling
    public static void saveDocument(MongoCollection<Document> collection, Document document) {
        System.out.println("Attempting to save document...");
        try {
            collection.insertOne(document);
            System.out.println("Document saved successfully.");
        } catch (Exception e) {
            System.out.println("Document too large, splitting into smaller chunks...");
            splitAndSaveDocument(collection, document);
        }
    }
    // Function to split and save a document into chunks
    public static void splitAndSaveDocument(MongoCollection<Document> collection, Document document) {
        List<Document> chunkDocuments = splitDocument(document, CHUNK_SIZE);
        try (ClientSession session = collection.getDatabase().getClient().startSession()) {
            session.startTransaction();
            collection.insertMany(chunkDocuments, new InsertManyOptions().session(session));
            session.commitTransaction();
            System.out.println("Chunks saved successfully.");
        } catch (Exception e) {
            session.abortTransaction();
            throw new RuntimeException("Failed to save document chunks", e);
        }
    }
    // Function to read a document by piecing together chunks
    public static Document readDocument(MongoCollection<Document> collection, String bigDocumentId) {
        System.out.printf("Reading document with big_document_id: %s...%n", bigDocumentId);
        Document document = collection.find(Filters.eq("big_document_id", bigDocumentId)).first();
        if (document != null && !document.containsKey("chunk_index")) {
            System.out.println("Document found as a single document.");
            return document;
        }
        System.out.println("Document not found as a single document. Retrieving chunks...");
        List<Document> chunks = collection.find(Filters.eq("big_document_id", bigDocumentId))
                .sort(Sorts.ascending("chunk_index"))
                .into(new ArrayList<>());
        document = new Document("big_document_id", bigDocumentId);
        for (Document chunk : chunks) {
            System.out.printf("Processing chunk %d...%n", chunk.getInteger("chunk_index"));
            for (String key : chunk.keySet()) {
                if (!key.equals("_id") && !key.equals("big_document_id") && !key.equals("chunk_index") && !key.equals("next_id")) {
                    document.append(key, chunk.get(key));
                }
            }
        }
        System.out.println("Document reassembled from chunks.");
        return document;
    }
    // Function to delete a document and its chunks
    public static void deleteDocument(MongoCollection<Document> collection, String bigDocumentId) {
        System.out.printf("Deleting document with big_document_id: %s...%n", bigDocumentId);
        try (ClientSession session = collection.getDatabase().getClient().startSession()) {
            session.startTransaction();
            collection.deleteMany(Filters.eq("big_document_id", bigDocumentId), new DeleteOptions().session(session));
            session.commitTransaction();
            System.out.println("Document deleted successfully.");
        } catch (Exception e) {
            session.abortTransaction();
            throw new RuntimeException("Failed to delete documents for big_document_id: " + bigDocumentId, e);
        }
    }
}
