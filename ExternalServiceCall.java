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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ExternalServiceCall {

    public interface Options extends PipelineOptions {

        @Description("OUTPUT BigQuery Dataset")
        String getOutputDataset();

        void setOutputDataset(String value);

    }

    private static TableSchema createBigQuerySchema() {

        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("event_id").setType("INT64"));
        fields.add(new TableFieldSchema().setName("message").setType("STRING"));

        return new TableSchema().setFields(fields);
    }

    public static class FormatBigQueryFn extends DoFn<Long, TableRow> {

        static long increment = 0;

        private void startBundle(){

        }

        @ProcessElement
        public void processElement(@Element Long number, OutputReceiver<TableRow> out) {
            TableRow row = new TableRow();

            Random rand = new Random();
            int n = rand.nextInt(100);
            increment += 1;

            String message = getMessage("test");

            row.set("event_id", increment);
            row.set("message", message);

            out.output(row);
        }

        private String getMessage(String key){

            String message = "Error";

            try{
                message = new RestIO().get("http://159.65.111.13");
            }
            catch (IOException e){
                System.out.println("Error" + e);
            }

            return message;
        }
    }

    public static void main(String[] args) {

        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        Pipeline pipeline = Pipeline.create(options);

        PCollection<Long> input = pipeline.apply(GenerateSequence.from(0L).to(10L));
        PCollection<TableRow> BQformat = input.apply(ParDo.of(new FormatBigQueryFn()));

//        TableSchema schema = createBigQuerySchema();
        BQformat.apply(BigQueryIO.writeTableRows()
                .to(options.getOutputDataset())
//                .withSchema(schema)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));

        pipeline.run();
    }
}
