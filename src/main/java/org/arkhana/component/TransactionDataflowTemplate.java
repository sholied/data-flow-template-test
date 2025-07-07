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
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.arkhana.pipeline.TransactionTemplateOptions;
import org.arkhana.validator.SchemaValidator;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class TransactionDataflowTemplate {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionDataflowTemplate.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TupleTag<String> VALID_RECORDS_TAG = new TupleTag<String>() {};
    private static final TupleTag<String> INVALID_RECORDS_TAG = new TupleTag<String>() {};

    public static void mainJob(String[] args) {
        TransactionTemplateOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(TransactionTemplateOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        TableSchema bigQuerySchema;
        try {
            // Load BigQuery schema once for the pipeline to use for BigQueryIO.write
            // This is done on the client side before pipeline construction
            LOG.info("Loading BigQuery schema for pipeline construction from GCS: {}", options.getBigQuerySchemaPath());
            String schemaJson = getString(options);
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

        } catch (IOException e) {
            LOG.error("Fatal error: Could not load BigQuery schema from GCS: {}", e.getMessage());
            throw new RuntimeException("Failed to load BigQuery schema for pipeline setup.", e);
        }

        // Read input JSON lines from GCS
        PCollectionTuple results = pipeline
                .apply("ReadInputFiles", TextIO.read().from(options.getInputFilePattern()))
                .apply("ValidateAndParse", ParDo.of(new SchemaValidator(
                                options.getBigQuerySchemaPath(),
                                VALID_RECORDS_TAG,
                                INVALID_RECORDS_TAG))
                        .withOutputTags(VALID_RECORDS_TAG, TupleTagList.of(INVALID_RECORDS_TAG)));

        // Write valid records to BigQuery
        results.get(VALID_RECORDS_TAG)
                .apply("WriteValidToBigQuery",
                        BigQueryIO.<String>write() // Specify the input type for write
                                .to(options.getOutputBigQueryTable())
                                .withFormatFunction(jsonString -> {
                                    try {
                                        // Use ObjectMapper to convert JSON string to TableRow
                                        // This is a common pattern for writing arbitrary JSON to BigQuery
                                        return MAPPER.readValue(jsonString, TableRow.class);
                                    } catch (Exception e) {
                                        // This catch is mostly for safety; ideally, validation in DoFn
                                        // should ensure this doesn't fail for valid records.
                                        LOG.error("Error converting JSON string to TableRow for BigQuery write: {}", jsonString, e);
                                        throw new RuntimeException("Failed to convert JSON to TableRow", e);
                                    }
                                })
                                .withSchema(bigQuerySchema) // Use the loaded schema
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        // Write invalid records to Dead-Letter Queue in GCS
        results.get(INVALID_RECORDS_TAG)
                .apply("WriteInvalidToDLQ", TextIO.write().to(options.getDeadLetterQueuePath())
                        .withSuffix(".jsonl")
                        .withNumShards(1) // Write all errors to a single file for easier inspection
                        .withWindowedWrites()); // Required for streaming or unbounded PCollections

        pipeline.run().waitUntilFinish();
    }

    @NotNull
    private static String getString(TransactionTemplateOptions options) {
        Storage storage = StorageOptions.getDefaultInstance().getService();
        String schemaPath = options.getBigQuerySchemaPath();

        // Parse bucket and blob name correctly from GCS path
        String pathWithoutPrefix = schemaPath.substring("gs://".length());
        int firstSlash = pathWithoutPrefix.indexOf('/');
        String bucketName = pathWithoutPrefix.substring(0, firstSlash);
        String blobName = pathWithoutPrefix.substring(firstSlash + 1);

        BlobId blobId = BlobId.of(bucketName, blobName);
        Blob blob = storage.get(blobId);

        if (blob == null) {
            throw new RuntimeException("Schema file not found at " + options.getBigQuerySchemaPath());
        }

        return new String(blob.getContent(), StandardCharsets.UTF_8);
    }
}
