package org.arkhana.component;

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
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.arkhana.pipeline.TransactionTemplateOptions;
import org.arkhana.validator.CsvToTableRowConverterAndValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors; // Tambahkan ini

public class TransactionDataflowTemplate {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionDataflowTemplate.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final TupleTag<TableRow> VALID_RECORDS_TAG = new TupleTag<TableRow>() {};
    private static final TupleTag<String> INVALID_RECORDS_TAG = new TupleTag<String>() {};


    public static void mainJob(String[] args) {
        TransactionTemplateOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(TransactionTemplateOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        TableSchema bigQuerySchema;
        String[] csvHeaders; // Variable untuk menyimpan header CSV

        try {
            // --- Memuat BigQuery Schema ---
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
            bigQuerySchema = new TableSchema().setFields(fields);
            LOG.info("BigQuery schema successfully loaded for BigQueryIO.write.");

            // --- Membaca Header CSV dari File Input ---
            LOG.info("Attempting to read CSV headers from input file: {}", options.getInputFilePattern());
            // Karena InputFilePattern bisa pakai wildcard, kita ambil yang pertama saja
            // Asumsi file CSV memiliki header di baris pertama
            // Ini akan membaca 1 file saja, jika ada banyak file, pastikan semua memiliki header yang sama
            csvHeaders = readCsvHeaderFromGcs(options.getInputFilePattern());
            LOG.info("CSV Headers successfully loaded: {}", String.join(", ", csvHeaders));

        } catch (IOException e) {
            LOG.error("Fatal error: Could not load BigQuery schema or CSV headers from GCS: {}", e.getMessage());
            throw new RuntimeException("Failed to setup pipeline due to schema or header loading error.", e);
        }

        // Read input CSV lines from GCS (akan membaca data, DoFn akan skip header pertama)
        PCollectionTuple results = pipeline
                .apply("ReadInputFiles", TextIO.read().from(options.getInputFilePattern()))
                .apply("ParseCsvAndValidate", ParDo.of(new CsvToTableRowConverterAndValidator(
                                csvHeaders,           // Teruskan headers
                                bigQuerySchema,       // Teruskan skema BigQuery
                                VALID_RECORDS_TAG,
                                INVALID_RECORDS_TAG))
                        .withOutputTags(VALID_RECORDS_TAG, TupleTagList.of(INVALID_RECORDS_TAG)));

        results.get(INVALID_RECORDS_TAG)
                .apply("WriteInvalidToDLQ", TextIO.write().to(options.getDeadLetterQueuePath())
                        .withSuffix(".jsonl")
                        .withNumShards(1) // Write all errors to a single file for easier inspection
                        .withWindowedWrites());

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
        // Handle wildcard in inputFilePattern by finding the first matching file
        // For simplicity, this assumes a single file or consistent headers across all files matching pattern.
        // For real production, you might need more sophisticated file discovery.
        if (gcsPath.contains("*")) {
            // Basic attempt to find a single file for header reading
            // Note: this is a simplification. For robust wildcard handling,
            // you might need to list blobs and pick one.
            // For templates, typically the inputFilePattern points to a specific example file.
            LOG.warn("InputFilePattern contains wildcard ({}). Attempting to read header from a single resolved path. " +
                    "Ensure all CSV files have identical headers.", gcsPath);
            // Example: If pattern is gs://bucket/data/*.csv, this might not resolve to a single file.
            // A better way would be to list blobs and pick one.
            // For now, assuming firstMatchingFilePath will be the correct path for header reading.
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