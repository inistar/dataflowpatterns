package com.springml.dataflow.patterns;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Arrays;

import com.google.cloud.bigquery.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import com.google.cloud.bigquery.JobInfo.CreateDisposition;
import com.google.cloud.bigquery.JobInfo.WriteDisposition;

public class ParquettoBQ {

    static BigQuery BIGQUERY = BigQueryOptions.getDefaultInstance().getService();

    public interface Options extends PipelineOptions{

        @Description("The QPS which the benchmark should output to Pub/Sub.")
        @Required
        Long getQps();
        void setQps(Long value);

        @Description("The path to the schema to generate.")
        String getSchemaLocation();
        void setSchemaLocation(String value);

        @Description("Set Dataset")
        String getDataset();
        void setDataset(String value);

    }

//    @SuppressWarnings("serial")
//    static class FormatBigQueryFn extends DoFn<GenericRecord, TableRow> {
//        static long increment = 0;
//
//        @ProcessElement
//        public void processElement(@Element GenericRecord word, OutputReceiver<TableRow> out) {
//
//            String play_id = word.get("play_id").toString();
//			Long dateTime = (Long) word.get("dateTime")/1000;
//
////            java.time.ZonedDateTime utc = Instant.ofEpochMilli((Long)word.get("dateTimer")).atZone(java.time.ZoneOffset.UTC);
//
//            java.sql.Timestamp ts = new java.sql.Timestamp(dateTime);
//
//            String event = word.get("event").toString();
//
////            TableRow row = new TableRow();
////
////            row.set("qtr", play_id);
////            row.set("sales", 11);
////            row.set("year", ts.toString());
////
////            out.output(row);
//
//
//            TableId tid = TableId.of("dataflowtesting-218212", "examples", "mavropartition"+"$"+"20181125");
//
////            LoadJobConfiguration loadConf = LoadJobConfiguration.newBuilder(tid, "gs://dataflow-temp-ini/avrofile.avro", FormatOptions.avro())
////                    .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
////                    .setAutodetect(Boolean.TRUE)
////                    .build();
//
//
//            try {
//
////                WriteChannelConfiguration wcc = WriteChannelConfiguration.newBuilder(tid, FormatOptions.avro());
//                WriteChannelConfiguration wcc = WriteChannelConfiguration.newBuilder(tid, FormatOptions.avro()).build();
////                BIGQUERY.create(JobInfo.of(loadConf));
//                TableDataWriteChannel test = BIGQUERY.writer(wcc);
//
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//    }


    public static void main(String[] args) {

        Options options = PipelineOptionsFactory.fromArgs(args)
                .withValidation().as(Options.class);

        final List<String> LINES = Arrays.asList("gs://dataflow-temp-ini/twitter.avro");

        Pipeline pipeline = Pipeline.create(options);
        PCollection<String> records = pipeline.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of());

        records.apply("Load into BQ", ParDo.of(new DoFn<String, Void>() {
            String projectId = "dataflowtesting-218212";
            String datasetId = "examples";
            String tableName = "newavro";

            private TableId getTableToOverwrite(String tableToOverwrite, String dayPartition) {
                return TableId.of(projectId, datasetId, tableToOverwrite+"$"+dayPartition);
            }

            @ProcessElement
            public void processElement(ProcessContext c) throws IOException {

                String dayPartition = "20181125";
                String gcs_filename = c.element();

                LoadJobConfiguration loadConf = LoadJobConfiguration.newBuilder(getTableToOverwrite(tableName, dayPartition), gcs_filename, FormatOptions.avro())
                        .setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                        .setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
                        .setTimePartitioning(TimePartitioning.newBuilder(TimePartitioning.Type.DAY).setField("dateTime").build())
                        .setAutodetect(Boolean.TRUE)
                        .build();

                try {
                    BIGQUERY.create(Job.of(loadConf));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }));

        pipeline.run();


    }
}
