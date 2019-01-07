package com.springml.dataflow.patterns;

import com.google.cloud.bigquery.*;
import org.apache.avro.LogicalTypes;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.hbase.shaded.org.apache.commons.net.ntp.TimeStamp;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroParquetReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
//import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

public class AvrotoBQ {

    static BigQuery BIGQUERY = BigQueryOptions.getDefaultInstance().getService();

    public interface Options extends PipelineOptions {

        @Description("The QPS which the benchmark should output to Pub/Sub.")
        @Validation.Required
        Long getQps();
        void setQps(Long value);

        @Description("The path to the schema to generate.")
        String getSchemaLocation();
        void setSchemaLocation(String value);

        @Description("Set Dataset")
        String getDataset();
        void setDataset(String value);

    }

    public static void main(String[] args) throws IOException {

        Schema schema = new Schema.Parser().parse ("{\"type\":\"record\",\"name\":\"Root\", \"fields\":[{\"name\":\"play_id\",\"type\":[\"null\",\"string\"]},{\"name\":\"dateTime\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}]},{\"name\":\"event\",\"type\":[\"null\",\"string\"]}]}");

        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);

		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>( new File("/Users/iniyavansathiamurthi/Downloads/avro_testing/avro_sample"), reader);

		Schema dateType = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));

		SchemaBuilder.FieldAssembler fieldAssembler = SchemaBuilder.record("record").namespace("root").fields().name("DATE_FOR_PARTITIONING").type(dateType).noDefault();

		for (Schema.Field field : schema.getFields()) {
		    fieldAssembler = fieldAssembler.name(field.name()).type(field.schema()).withDefault(field.defaultVal());
		}

        Schema dateSchema = (Schema) fieldAssembler.endRecord();

        ParquetWriter<GenericRecord> pw = AvroParquetWriter.<GenericRecord>builder(new org.apache.hadoop.fs.Path("/Users/iniyavansathiamurthi/Downloads/avro_testing/test.parquet")).withSchema(dateSchema) .build();

        while (dataFileReader.hasNext()) {
            GenericRecord record = dataFileReader.next();
            GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(dateSchema);

            Long temp = 0L;
            for (Schema.Field field : record.getSchema().getFields()) {
                if(field.name().equals("dateTime")){
                    temp = (Long) record.get(field.name());
                }
                genericRecordBuilder.set(field.name(), record.get(field.name()));
            }

            TimeStamp ts = new TimeStamp(temp/1000);
            System.out.println(ts.getDate());

            genericRecordBuilder.set("DATE_FOR_PARTITIONING", temp/1000);
            pw.write(genericRecordBuilder.build());
        }

        pw.close();

        ParquetReader<GenericRecord> test = null;
        Path path =    new Path("/Users/iniyavansathiamurthi/Downloads/avro_testing/test.parquet");

        test = AvroParquetReader
                .<GenericRecord>builder(path)
                .withConf(new Configuration())
                .build();
        GenericRecord record;
        while ((record = test.read()) != null) {
            System.out.println(record);
        }



//        PCollection<GenericRecord> records =
//                pipeline.apply(AvroIO.readGenericRecords(schema1).from("gs://dataflow-temp-ini/avrofile.avro"));

//		String subscriptionId = "projects/dataflowtesting-218212/subscriptions/PubSubTesting";
//		PCollection<String> text = pipeline.apply("GetMessages", PubsubIO.readStrings().fromSubscription(subscriptionId));

//		PCollection<Long> numbers = pipeline.apply("Trigger",
//				GenerateSequence.from(0L).withRate(options.getQps(), Duration.standardSeconds(1L)));

//				GenerateSequence
//					.from(0L).to(1000));//withRate(options.getQps(), Duration.standardSeconds(1L)));
//					.withMaxReadTime(Duration.standardSeconds(1L)));

//		PCollection<String> formatted = text.apply("Format", ParDo.of(new FormatTextFn()));

//        PCollection<TableRow>rows = records.apply("FormatToBigQuery", ParDo.of(new FormatBigQueryFn()));

//        rows.apply("WriteToBigQuery", BigQueryIO.writeTableRows().to(options.getDataset())
//        	.withSchema(schema)
//        	.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
//        	.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

//        rows.apply("BQWrite", BigQueryIO.writeTableRows()
//                .to(TableRefPartition.perDay(
//                        "dataflowtesting-218212",
//                        "examples",
//                        "manualpartition"))
//                .withSchema(schema)
//                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
//                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
//
//
//        KrogerWritetoBQ.Options options = PipelineOptionsFactory.fromArgs(args)
//                .withValidation().as(KrogerWritetoBQ.Options.class);

//        final List<String> LINES = Arrays.asList("gs://dataflow-temp-ini/avrofile.avro");

//        Pipeline pipeline = Pipeline.create(options);
//        PCollection<String> records = pipeline.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of());
//
//        records.apply("Load into BQ", ParDo.of(new DoFn<String, Void>() {
//            String projectId = "dataflowtesting-218212";
//            String datasetId = "examples";
//            String tableName = "newavro";
//
//            private TableId getTableToOverwrite(String tableToOverwrite, String dayPartition) {
//                return TableId.of(projectId, datasetId, tableToOverwrite+"$"+dayPartition);
//            }
//
//            @ProcessElement
//            public void processElement(ProcessContext c) throws IOException {
//
//                String dayPartition = "20181125";
//                String gcs_filename = c.element();
//
//                LoadJobConfiguration loadConf = LoadJobConfiguration.newBuilder(getTableToOverwrite(tableName, dayPartition), gcs_filename, FormatOptions.avro())
//                        .setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
//                        .setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE)
//                        .setTimePartitioning(TimePartitioning.newBuilder(TimePartitioning.Type.DAY).setField("dateTime").build())
//                        .setAutodetect(Boolean.TRUE)
//                        .build();
//
//                try {
//                    BIGQUERY.create(Job.of(loadConf));
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//        }));
//
//        pipeline.run();


    }
}
