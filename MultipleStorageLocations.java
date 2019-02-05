package com.springml.dataflow.patterns;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Mutation;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MultipleStorageLocations {

    public interface Options extends PipelineOptions {

        @Description("INPUT File")
        String getInputFile();
        void setInputFile(String value);

        @Description("OUTPUT BigQuery Dataset")
        String getOutputDataset();
        void setOutputDataset(String value);

        @Description("OUTPUT Bigtable Instance")
        String getOutputBigtableInstance();
        void setOutputBigtableInstance(String value);

        @Description("OUTPUT Bigtable Table")
        String getOutputBigtableTable();
        void setOutputBigtableTable(String value);
    }

    private static TableSchema createBigQuerySchema() {

        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("event_id").setType("INT64"));
        fields.add(new TableFieldSchema().setName("message").setType("STRING"));
        fields.add(new TableFieldSchema().setName("event_ts").setType("STRING"));

        return new TableSchema().setFields(fields);
    }

    private static CloudBigtableTableConfiguration getCloudBigTableConfiguration(Options options) {

        String PROJECT_ID = "dataflowtesting-218212";
        String INSTANCE_ID = options.getOutputBigtableInstance();
        String TABLE_ID = options.getOutputBigtableTable();

        return new CloudBigtableTableConfiguration.Builder()
                .withProjectId(PROJECT_ID)
                .withInstanceId(INSTANCE_ID)
                .withTableId(TABLE_ID)
                .build();
    }

    public static class FormatBigQueryFn extends DoFn<Long, TableRow> {

        static long increment = 0;

        @ProcessElement
        public void processElement(@Element Long number, OutputReceiver<TableRow> out){
            TableRow row = new TableRow();

            Random rand = new Random();
            int n = rand.nextInt(1000);

            row.set("event_id", n);
            row.set("message", "test message");
            row.set("event_ts", Instant.now().toString());

            out.output(row);
        }
    }
    private static final byte[] FAMILY = Bytes.toBytes("cf");
    private static final byte[] QUALIFIER = Bytes.toBytes("qualifier");
    // This is a random value so that there will be some changes in the table
    // each time the job runs.
    private static final byte[] VALUE = Bytes.toBytes("value_" + (60 * Math.random()));
    // [START bigtable_dataflow_connector_process_element]

    static final DoFn<Long, Mutation> MUTATION_TRANSFORM = new DoFn<Long, Mutation>() {
        private static final long serialVersionUID = 1L;

        @ProcessElement
        public void processElement(DoFn<String, Mutation>.ProcessContext c) throws Exception {
            c.output(new Put(c.element().getBytes()).addColumn(FAMILY, QUALIFIER, VALUE));
        }
    };

    public static void main(String[] args){

        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        Pipeline pipeline = Pipeline.create(options);

        PCollection<Long> input = pipeline.apply(GenerateSequence.from(0L).to(1000L));
//        PCollection<TableRow> BQformat = input.apply(ParDo.of(new FormatBigQueryFn()));
//
////       BIGQUERY
////       Schema is only necessary for the first time when the table is not created. Change
////       CREATE_IF_NEEDED if a new table needs to be created and uncomment the schema parts.
////        TableSchema schema = createBigQuerySchema();
//        BQformat.apply(BigQueryIO.writeTableRows()
//                .to(options.getOutputDataset())
////                .withSchema(schema)
//                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
//                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));

//      BIGTABLE

        CloudBigtableTableConfiguration config = getCloudBigTableConfiguration(options);
//        pipeline.apply(Create.of("Hello", "World"))
                input.apply(ParDo.of(MUTATION_TRANSFORM))
                .apply(CloudBigtableIO.writeToTable(config));

//        input.apply("BigTableTransform", ParDo.of(new FormatBigTableFn()))
//                .apply("WriteToBigTable", CloudBigtableIO.writeToTable(config));

        pipeline.run();
    }
}
