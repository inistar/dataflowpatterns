package com.springml.dataflow.patterns;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MultipleStorageLocations {

    public interface Options extends PipelineOptions {

        @Description("INPUT File")
        String getInputFile();
        void setInputFile(String value);

        @Description("OUTPUT Dataset")
        String getOutputDataset();
        void setOutputDataset(String Value);

    }

    static class FormatBigQueryFn extends DoFn<Long, TableRow> {

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

    public static void main(String[] args){

        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        Pipeline pipeline = Pipeline.create(options);

        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("event_id").setType("INT64"));
        fields.add(new TableFieldSchema().setName("message").setType("STRING"));
        fields.add(new TableFieldSchema().setName("event_ts").setType("STRING"));

        TableSchema schema = new TableSchema().setFields(fields);

        PCollection<Long> input = pipeline.apply(GenerateSequence.from(0L).to(1000L));
        PCollection<TableRow> BQformat = input.apply(ParDo.of(new FormatBigQueryFn()));

        BQformat.apply(BigQueryIO.writeTableRows()
                .to(options.getOutputDataset())
                .withSchema(schema)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        pipeline.run();
    }
}
