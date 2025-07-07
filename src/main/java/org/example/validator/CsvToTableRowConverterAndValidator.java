package org.example.validator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
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

public class CsvToTableRowConverterAndValidator extends DoFn<String, TableRow> {

    private static final Logger LOG = LoggerFactory.getLogger(CsvToTableRowConverterAndValidator.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String[] csvHeaders;
    private final String bigQuerySchemaPath;
    private final TupleTag<TableRow> validOutputTag;
    private final TupleTag<String> invalidRecordsTag;
    private transient TableSchema expectedSchema;

    // Constructor: Sekarang menerima path skema GCS
    public CsvToTableRowConverterAndValidator(String[] csvHeaders,
                                              String bigQuerySchemaPath, // Terima path GCS untuk skema
                                              TupleTag<TableRow> validOutputTag,
                                              TupleTag<String> invalidRecordsTag) {
        this.csvHeaders = csvHeaders;
        this.bigQuerySchemaPath = bigQuerySchemaPath; // Simpan path skema
        this.validOutputTag = validOutputTag;
        this.invalidRecordsTag = invalidRecordsTag;
    }

    @Setup
    public void setup() throws IOException {
        LOG.info("Worker: Loading BigQuery schema from GCS: {}", bigQuerySchemaPath);
        try {
            String schemaJson = readGcsFileToString(bigQuerySchemaPath); // Helper method to read GCS file
            JsonNode schemaNode = MAPPER.readTree(schemaJson);

            if (!schemaNode.isArray()) {
                throw new IllegalArgumentException("BigQuery schema JSON must be an array of field definitions.");
            }

            List<TableFieldSchema> fields = new ArrayList<>();
            for (JsonNode fieldNode : schemaNode) {
                TableFieldSchema field = new TableFieldSchema()
                        .setName(fieldNode.get("name").asText())
                        .setType(fieldNode.get("type").asText())
                        .setMode(fieldNode.has("mode") ? fieldNode.get("mode").asText() : "NULLABLE");
                fields.add(field);
            }
            this.expectedSchema = new TableSchema().setFields(fields);
            LOG.info("Worker: Schema loaded successfully with {} fields.", fields.size());

        } catch (Exception e) {
            LOG.error("Worker: Failed to load or parse BigQuery schema from {}: {}", bigQuerySchemaPath, e.getMessage());
            throw e; // Re-throw to fail setup and prevent pipeline from running with invalid schema
        }
    }

    private String readGcsFileToString(String gcsPath) throws IOException {
        Storage storage = StorageOptions.getDefaultInstance().getService();
        String pathWithoutPrefix = gcsPath.substring("gs://".length());
        int firstSlash = pathWithoutPrefix.indexOf('/');
        String bucketName = pathWithoutPrefix.substring(0, firstSlash);
        String blobName = pathWithoutPrefix.substring(firstSlash + 1);

        BlobId blobId = BlobId.of(bucketName, blobName);
        Blob blob = storage.get(blobId);

        if (blob == null) {
            throw new IOException("Schema file not found at " + gcsPath);
        }
        return new String(blob.getContent(), StandardCharsets.UTF_8);
    }

    // Metode @ProcessElement
    @ProcessElement
    public void processElement(@Element String csvLine, ProcessContext c) {
        if (csvLine.equals(String.join(",", csvHeaders))) {
            LOG.info("Skipping CSV header row: {}", csvLine);
            return;
        }

        try {
            String[] values = csvLine.split(",");
            if (values.length != csvHeaders.length) {
                throw new IllegalArgumentException(String.format(
                        "Mismatched number of columns. Expected %d, found %d. Line: '%s'",
                        csvHeaders.length, values.length, csvLine));
            }
            TableRow row = new TableRow();
            for (int i = 0; i < csvHeaders.length; i++) {
                String header = csvHeaders[i];
                String value = values[i].trim().replace("\"", "");
                TableFieldSchema fieldSchema = expectedSchema.getFields().stream()
                        .filter(f -> f.getName().equalsIgnoreCase(header))
                        .findFirst()
                        .orElse(null);
                if (fieldSchema == null) {
                    LOG.warn("Field '{}' from CSV not found in BigQuery schema. Skipping this column for the current record.", header);
                    continue;
                }
                Object convertedValue = convertValueBasedOnSchema(value, fieldSchema);
                row.set(fieldSchema.getName(), convertedValue); // Set nilai ke TableRow
            }

            c.output(validOutputTag, row);

        } catch (Exception e) {
            LOG.warn("Invalid record encountered: {}. Error: {}", csvLine, e.getMessage());
            c.output(invalidRecordsTag,
                    String.format("{\"original_data\": \"%s\", \"error_message\": \"%s\", \"processing_time\": \"%s\"}",
                            csvLine.replace("\"", "\\\""), // Escape quotes di original_data untuk JSON valid
                            e.getMessage().replace("\"", "'"), // Ganti double quote jadi single quote di error_message untuk menghindari masalah parsing
                            java.time.Instant.now()));
        }
    }

    private Object convertValueBasedOnSchema(String value, TableFieldSchema fieldSchema) {
        String fieldName = fieldSchema.getName();
        String fieldType = fieldSchema.getType();
        String fieldMode = fieldSchema.getMode();
        if ("REQUIRED".equalsIgnoreCase(fieldMode) && (value == null || value.isEmpty())) {
            throw new IllegalArgumentException("Missing or empty required field: " + fieldName);
        }

        if (value.isEmpty()) {
            return null;
        }

        try {
            switch (fieldType.toUpperCase()) {
                case "STRING":
                    return value;
                case "INTEGER":
                    return Long.parseLong(value); // BigQuery INTEGER maps to Long di Java
                case "FLOAT":
                case "NUMERIC":
                case "BIGNUMERIC": // Untuk numerik yang lebih besar, bisa gunakan BigDecimal
                    return Double.parseDouble(value);
                case "BOOLEAN":
                    return Boolean.parseBoolean(value);
                case "DATE":
                    LocalDate.parse(value);
                    return value; // Kirim sebagai string, BigQuery akan mengonversi
                case "TIMESTAMP":
                    // Asumsi format ISO 8601 atau sesuai kebutuhan BigQuery
                    return value;
                // Tambahkan case untuk tipe data BigQuery lainnya (RECORD, ARRAY, BYTES, dll.)
                default:
                    LOG.warn("Unsupported BigQuery type '{}' for field '{}'. Treating as STRING.", fieldType, fieldName);
                    return value;
            }
        } catch (NumberFormatException | DateTimeParseException e) {
            // Tangani error konversi tipe data
            throw new IllegalArgumentException(String.format("Failed to convert field '%s' with value '%s' to type %s: %s",
                    fieldName, value, fieldType, e.getMessage()), e);
        }
    }
}