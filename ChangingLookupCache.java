package com.springml.dataflow.patterns;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;
import org.junit.Test;

import java.util.Arrays;

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

        mainStream.apply("Print the Data", ParDo.of(new DoFn<String, String>(){

            @ProcessElement
            public void processElement(ProcessContext c){
                System.out.println(c.element());
                c.output("test");
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}
