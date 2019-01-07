package com.springml.dataflow.patterns;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.google.api.services.bigquery.model.TableReference;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

class TableRefPartition implements SerializableFunction<ValueInSingleWindow<TableRow>, TableDestination> {

    private final String projectId;
    private final String datasetId;
    private final String pattern;
    private final String table;

    public static TableRefPartition perDay(String projectId, String datasetId, String tablePrefix) {
        return new TableRefPartition(projectId, datasetId, "20181118", tablePrefix + "$");
    }

    private TableRefPartition(String projectId, String datasetId, String pattern, String table) {
        this.projectId = projectId;
        this.datasetId = datasetId;
        this.pattern = pattern;
        this.table = table;
    }

    @Override
    public TableDestination apply(ValueInSingleWindow<TableRow> input) {
        DateTimeFormatter partition = DateTimeFormat.forPattern(pattern).withZoneUTC();

        TableReference reference = new TableReference();
        reference.setProjectId(this.projectId);
        reference.setDatasetId(this.datasetId);

        reference.setTableId(table + input.getWindow().maxTimestamp().toString(partition));
        return new TableDestination(reference, null);
    }
}

public class PartitionedWritetoBQ {

    public interface Options extends PipelineOptions {

        @Description("The QPS which the benchmark should output to Pub/Sub.")
        @Validation.Required
        Long getQps();
        void setQps(Long value);

        @Description("Set Dataset")
        String getDataset();
        void setDataset(String value);

    }

    @SuppressWarnings("serial")
    static class FormatBigQueryFn extends DoFn<Long, TableRow> {
        static long increment = 0;

        @ProcessElement
        public void processElement(@Element Long number, OutputReceiver<TableRow> out) {

            TableRow row = new TableRow();

            Random rand = new Random();
            int n = rand.nextInt(1000);

            row.set("event_id", n);
            row.set("message", "test");
            row.set("event_ts", Instant.now().toString());

            out.output(row);
        }
    }

    @SuppressWarnings("serial")
    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        Pipeline pipeline = Pipeline.create(options);

        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("event_id").setType("INT64"));
        fields.add(new TableFieldSchema().setName("message").setType("STRING"));
        fields.add(new TableFieldSchema().setName("event_ts").setType("STRING"));

        TableSchema schema = new TableSchema().setFields(fields);

        PCollection<Long> input = pipeline.apply(GenerateSequence.from(0L).to(10000L));

        PCollection<TableRow> BQformat = input.apply(ParDo.of(new FormatBigQueryFn()));

        BQformat.apply(BigQueryIO.writeTableRows()
                .to(TableRefPartition.perDay(
                        "dataflowtesting-218212",
                        "sourcedata",
                        "streamdemo"))
                .withSchema(schema)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

// For ingestion partition tables with current date
//        BQformat.apply(BigQueryIO.writeTableRows()
//                .to(options.getDataset())
//                        .withSchema(schema)
//                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
//                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        pipeline.run();
    }
}
