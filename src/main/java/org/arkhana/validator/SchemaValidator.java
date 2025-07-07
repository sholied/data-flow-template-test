package org.arkhana.validator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

public class SchemaValidator extends DoFn<String, String> {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaValidator.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Define output tags for valid and invalid records
    private static final TupleTag<String> VALID_RECORDS_TAG = new TupleTag<String>() {};
    private static final TupleTag<String> INVALID_RECORDS_TAG = new TupleTag<String>() {};


    private final String bigQuerySchemaPath;
    private TableSchema expectedSchema;
    private static final ObjectMapper mapper = new ObjectMapper(); // Thread-safe for read operations

    public SchemaValidator(String bigQuerySchemaPath) {
        this.bigQuerySchemaPath = bigQuerySchemaPath;
    }

    @Setup
    public void setup() throws IOException {
        // Load schema from GCS during setup (once per worker)
        LOG.info("Loading BigQuery schema from GCS: {}", bigQuerySchemaPath);
        try {
            Storage storage = StorageOptions.getDefaultInstance().getService();
            String bucketName = bigQuerySchemaPath.split("/")[2]; // Assuming gs://bucket/path
            String blobName = bigQuerySchemaPath.substring(bigQuerySchemaPath.indexOf(bucketName) + bucketName.length() + 1);

            BlobId blobId = BlobId.of(bucketName, blobName);
            Blob blob = storage.get(blobId);

            if (blob == null) {
                throw new IOException("Schema file not found: " + bigQuerySchemaPath);
            }

            String schemaJson = new String(blob.getContent(), StandardCharsets.UTF_8);

            // Parse the JSON schema into TableSchema object
            // The JSON schema should be an array of TableFieldSchema objects
            JsonNode schemaNode = mapper.readTree(schemaJson);
            if (!schemaNode.isArray()) {
                throw new IllegalArgumentException("BigQuery schema JSON must be an array of field definitions.");
            }

            List<TableFieldSchema> fields = new ArrayList<>();
            for (JsonNode fieldNode : schemaNode) {
                TableFieldSchema field = new TableFieldSchema()
                        .setName(fieldNode.get("name").asText())
                        .setType(fieldNode.get("type").asText())
                        .setMode(fieldNode.has("mode") ? fieldNode.get("mode").asText() : "NULLABLE");
                // Add nested fields if applicable (for RECORD types) - omitted for brevity
                fields.add(field);
            }
            expectedSchema = new TableSchema().setFields(fields);
            LOG.info("Schema loaded successfully with {} fields.", fields.size());

        } catch (Exception e) {
            LOG.error("Failed to load or parse BigQuery schema from {}: {}", bigQuerySchemaPath, e.getMessage());
            throw e; // Re-throw to fail setup and prevent pipeline from running with invalid schema
        }
    }

    @ProcessElement
    public void processElement(@Element String jsonLine, ProcessContext c) {
        JsonNode record;
        try {
            record = mapper.readTree(jsonLine);
            // Perform validation based on expectedSchema
            validateRecord(record);
            // If valid, output the record as JSON string
            c.output(VALID_RECORDS_TAG, jsonLine);

        } catch (Exception e) {
            // If parsing or validation fails, output to dead-letter queue
            LOG.warn("Invalid record encountered: {}. Error: {}", jsonLine, e.getMessage());
            c.output(INVALID_RECORDS_TAG,
                    String.format("{\"original_data\": %s, \"error_message\": \"%s\", \"processing_time\": \"%s\"}",
                            jsonLine, e.getMessage().replace("\"", "'"), java.time.Instant.now()));
        }
    }

    private void validateRecord(JsonNode record) throws IllegalArgumentException, DateTimeParseException {
        // Basic validation: check for required fields and data types
        for (TableFieldSchema field : expectedSchema.getFields()) {
            String fieldName = field.getName();
            String fieldType = field.getType();
            String fieldMode = field.getMode();
            JsonNode node = record.get(fieldName);

            // Check for required fields
            if ("REQUIRED".equalsIgnoreCase(fieldMode) && (node == null || node.isNull())) {
                throw new IllegalArgumentException("Missing or null required field: " + fieldName);
            }

            if (node != null && !node.isNull()) {
                // Validate data types (simplified for example)
                switch (fieldType) {
                    case "STRING":
                        if (!node.isTextual()) {
                            throw new IllegalArgumentException(String.format("Field '%s' expected to be STRING, but found: %s", fieldName, node.getNodeType()));
                        }
                        break;
                    case "INTEGER":
                        if (!node.isIntegralNumber()) {
                            throw new IllegalArgumentException(String.format("Field '%s' expected to be INTEGER, but found: %s", fieldName, node.getNodeType()));
                        }
                        break;
                    case "FLOAT":
                    case "NUMERIC":
                    case "BIGNUMERIC":
                        if (!node.isNumber()) {
                            throw new IllegalArgumentException(String.format("Field '%s' expected to be numeric, but found: %s", fieldName, node.getNodeType()));
                        }
                        break;
                    case "BOOLEAN":
                        if (!node.isBoolean()) {
                            throw new IllegalArgumentException(String.format("Field '%s' expected to be BOOLEAN, but found: %s", fieldName, node.getNodeType()));
                        }
                        break;
                    case "DATE":
                        if (!node.isTextual()) {
                            throw new IllegalArgumentException(String.format("Field '%s' expected to be DATE (string), but found: %s", fieldName, node.getNodeType()));
                        }
                        // Attempt to parse to validate format
                        LocalDate.parse(node.asText());
                        break;
                    // Add more types (TIMESTAMP, DATETIME, RECORD, ARRAY) as needed
                    default:
                        // Unknown type, or no specific validation
                        break;
                }
            }
        }
        // Add custom business logic validations here if needed (e.g., total_price == quantity * unit_price)
    }
}
