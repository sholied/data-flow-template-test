package org.example.component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.example.pipeline.TransactionTemplateOptions;
import org.example.validator.CsvToTableRowConverterAndValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class TransactionDataflowTemplate {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionDataflowTemplate.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final TupleTag<TableRow> VALID_RECORDS_TAG = new TupleTag<TableRow>() {};
    private static final TupleTag<String> INVALID_RECORDS_TAG = new TupleTag<String>() {};


    public static void mainJob(String[] args) {
        TransactionTemplateOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(TransactionTemplateOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        TableSchema bigQuerySchema; // Still needed for BigQueryIO.write().withSchema()
        String[] csvHeaders;

        try {
            // --- Memuat BigQuery Schema ---
            // This part is crucial and remains on the client-side
            LOG.info("Loading BigQuery schema for pipeline construction from GCS: {}", options.getBigQuerySchemaPath());
            String schemaJson = readGcsFileToString(options.getBigQuerySchemaPath());
            JsonNode schemaNode = MAPPER.readTree(schemaJson);

            List<TableFieldSchema> fields = new ArrayList<>();
            for (JsonNode fieldNode : schemaNode) {
                TableFieldSchema field = new TableFieldSchema()
                        .setName(fieldNode.get("name").asText())
                        .setType(fieldNode.get("type").asText())
                        .setMode(fieldNode.has("mode") ? fieldNode.get("mode").asText() : "NULLABLE");
                fields.add(field);
            }
            bigQuerySchema = new TableSchema().setFields(fields); // <--- bigQuerySchema initialized here
            LOG.info("BigQuery schema successfully loaded for BigQueryIO.write.");

            LOG.info("Attempting to read CSV headers from input file: {}", options.getInputFilePattern());
            csvHeaders = readCsvHeaderFromGcs(options.getInputFilePattern());
            LOG.info("CSV Headers successfully loaded: {}", String.join(", ", csvHeaders));

        } catch (IOException e) {
            LOG.error("Fatal error: Could not load BigQuery schema or CSV headers from GCS: {}", e.getMessage());
            throw new RuntimeException("Failed to setup pipeline due to schema or header loading error.", e);
        }

        // Read input CSV lines from GCS
        PCollectionTuple results = pipeline
                .apply("ReadInputFiles", TextIO.read().from(options.getInputFilePattern()))
                .apply("ParseCsvAndValidate", ParDo.of(new CsvToTableRowConverterAndValidator(
                                csvHeaders,
                                options.getBigQuerySchemaPath(),
                                VALID_RECORDS_TAG,
                                INVALID_RECORDS_TAG))
                        .withOutputTags(VALID_RECORDS_TAG, TupleTagList.of(INVALID_RECORDS_TAG)));

        // Write valid records (now TableRow) to BigQuery
        results.get(VALID_RECORDS_TAG)
                .apply("WriteValidToBigQuery",
                        BigQueryIO.writeTableRows()
                                .to(options.getOutputBigQueryTable())
                                .withSchema(bigQuerySchema) // <--- Use the client-loaded schema here (it's fine)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        pipeline.run().waitUntilFinish();
    }

    // Metode helper untuk membaca file dari GCS sebagai String (untuk skema)
    @NotNull
    private static String readGcsFileToString(String gcsPath) throws IOException {
        Storage storage = StorageOptions.getDefaultInstance().getService();
        String pathWithoutPrefix = gcsPath.substring("gs://".length());
        int firstSlash = pathWithoutPrefix.indexOf('/');
        String bucketName = pathWithoutPrefix.substring(0, firstSlash);
        String blobName = pathWithoutPrefix.substring(firstSlash + 1);

        BlobId blobId = BlobId.of(bucketName, blobName);
        Blob blob = storage.get(blobId);

        if (blob == null) {
            throw new IOException("File not found at " + gcsPath);
        }
        return new String(blob.getContent(), StandardCharsets.UTF_8);
    }

    // Metode helper baru untuk membaca baris pertama CSV dari GCS dan memecahnya menjadi header
    @NotNull
    private static String[] readCsvHeaderFromGcs(String gcsPath) throws IOException {
        Storage storage = StorageOptions.getDefaultInstance().getService();
        if (gcsPath.contains("*")) {
            LOG.warn("InputFilePattern contains wildcard ({}). Attempting to read header from a single resolved path. " +
                    "Ensure all CSV files have identical headers.", gcsPath);
        }

        String pathWithoutPrefix = gcsPath.substring("gs://".length());
        int firstSlash = pathWithoutPrefix.indexOf('/');
        String bucketName = pathWithoutPrefix.substring(0, firstSlash);
        String blobName = pathWithoutPrefix.substring(firstSlash + 1);

        BlobId blobId = BlobId.of(bucketName, blobName);
        Blob blob = storage.get(blobId);

        if (blob == null) {
            throw new IOException("CSV input file not found at " + gcsPath);
        }

        // Baca hanya baris pertama
        try (BufferedReader reader = new BufferedReader(Channels.newReader(blob.reader(), StandardCharsets.UTF_8))) {
            String headerLine = reader.readLine();
            if (headerLine == null || headerLine.trim().isEmpty()) {
                throw new IOException("CSV file is empty or missing header row: " + gcsPath);
            }
            // Basic split, adjust if your CSV has quoted commas
            return headerLine.split(",");
        }
    }
}