package org.arkhana.validator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Locale;

public class CsvToTableRowConverterAndValidator extends DoFn<String, TableRow> {

    private static final Logger LOG = LoggerFactory.getLogger(CsvToTableRowConverterAndValidator.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String[] csvHeaders; // Sekarang diterima dari konstruktor
    private final TableSchema expectedSchema;
    private final TupleTag<TableRow> validOutputTag;
    private final TupleTag<String> invalidRecordsTag;
    private transient CSVFormat csvFormat; // transient karena tidak bisa diserialisasi

    // Format tanggal yang diharapkan untuk validasi
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd").withLocale(Locale.ENGLISH);

    public CsvToTableRowConverterAndValidator(String[] csvHeaders, // Terima headers di sini
                                              TableSchema expectedSchema,
                                              TupleTag<TableRow> validOutputTag,
                                              TupleTag<String> invalidRecordsTag) {
        this.csvHeaders = csvHeaders;
        this.expectedSchema = expectedSchema;
        this.validOutputTag = validOutputTag;
        this.invalidRecordsTag = invalidRecordsTag;
    }

    @Setup
    public void setup() {
        csvFormat = CSVFormat.DEFAULT.builder()
                .setHeader(csvHeaders)
                .setSkipHeaderRecord(true)
//                .setDelimiter(",") // Pastikan delimiter sesuai dengan format CSV
//                .setIgnoreHeaderCase(true) // Mengabaikan kasus header
//                .setTrim(true) // Menghapus spasi di awal dan akhir nilai
                .build();
    }

    @ProcessElement
    public void processElement(@Element String csvLine, ProcessContext c) {
        // Logika untuk melewatkan header jika ada di CSV
        // Kita bandingkan dengan header yang sudah dibaca di client side
        if (csvLine.equals(String.join(",", csvHeaders))) { // Bandingkan string utuh
            LOG.info("Skipping CSV header row: {}", csvLine);
            return; // Lewati baris header
        }

        try {
            CSVRecord csvRecord;
            try (CSVParser parser = CSVParser.parse(csvLine, csvFormat)) {
                csvRecord = parser.getRecords().get(0); // Ambil record pertama (dan satu-satunya) dari baris ini
            }
            TableRow row = new TableRow();
            // Iterasi berdasarkan nama header (lebih robust)
            for (String header : csvHeaders) {
                String value = csvRecord.get(header); // Ambil nilai berdasarkan nama header

                TableFieldSchema fieldSchema = expectedSchema.getFields().stream()
                        .filter(f -> f.getName().equalsIgnoreCase(header))
                        .findFirst()
                        .orElse(null);

                if (fieldSchema == null) {
                    LOG.warn("Field '{}' from CSV not found in BigQuery schema. Skipping.", header);
                    continue;
                }

                Object convertedValue = convertValueBasedOnSchema(value, fieldSchema);
                row.set(fieldSchema.getName(), convertedValue);
            }

            c.output(validOutputTag, row);

        } catch (Exception e) {
            LOG.warn("Invalid record encountered: {}. Error: {}", csvLine, e.getMessage());
            c.output(invalidRecordsTag,
                    String.format("{\"original_data\": \"%s\", \"error_message\": \"%s\", \"processing_time\": \"%s\"}",
                            csvLine.replace("\"", "\\\""),
                            e.getMessage().replace("\"", "'"),
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
                    return Long.parseLong(value);
                case "FLOAT":
                case "NUMERIC":
                case "BIGNUMERIC":
                    return Double.parseDouble(value);
                case "BOOLEAN":
                    return Boolean.parseBoolean(value);
                case "DATE":
                    LocalDate.parse(value);
                    return value;
                case "TIMESTAMP":
                    return value;
                default:
                    LOG.warn("Unsupported BigQuery type '{}' for field '{}'. Treating as STRING.", fieldType, fieldName);
                    return value;
            }
        } catch (NumberFormatException | DateTimeParseException e) {
            throw new IllegalArgumentException(String.format("Failed to convert field '%s' with value '%s' to type %s: %s",
                    fieldName, value, fieldType, e.getMessage()), e);
        }
    }
}