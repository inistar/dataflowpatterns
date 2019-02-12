package com.springml.dataflow.patterns;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;


import static org.apache.beam.sdk.transforms.View.asMap;

public class ChangingLookupCache {

    public interface Options extends PipelineOptions {

        @Description("INPUT Pubsub Topic")
        String getInputPubsubTopic();
        void setInputPubsubTopic(String value);
    }

    @Test
    public static void should_show_side_input_in_global_window(String[] args) {

        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        Pipeline pipeline = Pipeline.create(options);

        PCollection<Integer> processedMeanNumbers = pipeline.apply(Create.of(Arrays.asList(2, 50, 20)));
        // Here the parameter shared by all functions must be computed
        PCollectionView<Integer> minNumber = processedMeanNumbers.apply(Min.integersGlobally().asSingletonView());
        PCollection<Integer> processedNumbers = pipeline.apply(Create.of(Arrays.asList(1, 2, 3, 4, 5, 6)));



        PCollection<Integer> numbersFromChars = processedNumbers.apply(
                ParDo.of(new DoFn<Integer, Integer>() {
                    @ProcessElement
                    public void processElement(ProcessContext processContext) {
                        int minNumberToFilter = processContext.sideInput(minNumber);
                        int processedElement = processContext.element();
                        System.out.println("MinNumbertoFilter: " + minNumberToFilter + " Processed Element: " + processedElement);
                        if (processedElement > minNumberToFilter) {
                            System.out.println(processedElement);
                            processContext.output(processedElement);
                        }
                    }
                }).withSideInputs(minNumber));

        PAssert.that(numbersFromChars).containsInAnyOrder(3, 4, 5, 6);
        pipeline.run().waitUntilFinish();
    }

    public static void main(String[] args){

        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> mainStream = pipeline.apply("Read from Pubsub",
                PubsubIO.readStrings().fromTopic(options.getInputPubsubTopic()));

        String query = "SELECT event_id, event_ts FROM `dataflowtesting-218212.testing.apple`";

        final PCollectionView<Map<String, String>> sideInput = pipeline
                .apply(String.format("Updating every %s seconds", 10),
                        GenerateSequence.from(0).withRate(1, Duration.standardSeconds(10)))
                .apply("Assign to Fixed Window", Window.<Long>into(FixedWindows.of(Duration.standardSeconds(10))))
                .apply(new ReadSlowChangingTable("Read BigQuery Table", query, "event_id", "event_ts"))
                .apply("View as Map", View.<String, String>asMap());


        mainStream
                .apply("Assign to Fixed Window", Window.<String>into(FixedWindows.of(Duration.standardSeconds(20))))
                .apply("Enrich MainInput with SideInput", ParDo.of(new DoFn<String, String>() {

                    @ProcessElement
                    public void processElement(ProcessContext c) {

                        System.out.println("Pubsub Message: " + c.element());

                        String event_id = c.element();

                        Map<String, String> enrichData = c.sideInput(sideInput);

                        String output = event_id + ", " + enrichData.get(event_id);

                        System.out.println(output);

                        c.output(output);
                    }

                }).withSideInputs(sideInput));

        pipeline.run().waitUntilFinish();
    }
}
