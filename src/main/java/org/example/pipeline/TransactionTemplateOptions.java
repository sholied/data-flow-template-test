package org.example.pipeline;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * Interface for the template's custom options.
 */
public interface TransactionTemplateOptions extends PipelineOptions {
    @Description("Path of the GCS input file(s) (e.g., gs://your-bucket/input/*.jsonl)")
    @Validation.Required
    String getInputFilePattern();
    void setInputFilePattern(String value);

    @Description("BigQuery output table (e.g., project_id:dataset_id.table_id)")
    @Validation.Required
    String getOutputBigQueryTable();
    void setOutputBigQueryTable(String value);

    @Description("Path to the GCS JSON schema file for BigQuery validation (e.g., gs://your-bucket/schemas/transactions_schema.json)")
    @Validation.Required
    String getBigQuerySchemaPath();
    void setBigQuerySchemaPath(String value);

    @Description("Path for dead-letter queue for invalid records (e.g., gs://your-bucket/dlq/invalid_transactions/)")
    @Validation.Required
    String getDeadLetterQueuePath();
    void setDeadLetterQueuePath(String value);
}
