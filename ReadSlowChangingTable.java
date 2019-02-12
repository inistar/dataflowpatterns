package com.springml.dataflow.patterns;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryResponse;
import com.google.cloud.bigquery.TableResult;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.UUID;

public class ReadSlowChangingTable extends PTransform<PCollection<Long>, PCollection<KV<String, String>>> {

    private final String query;
    private final String key;
    private final String value;

    ReadSlowChangingTable(String name, String query, String key, String value){
        super(name);
        this.query = query;
        this.key = key;
        this.value = value;
    }

    public PCollection<KV<String, String>> expand(PCollection<Long> input){

        return input.apply("Read BigQuery Table", ParDo.of(new DoFn<Long, KV<String, String>>(){

            @ProcessElement
            public void processElement(ProcessContext c) throws InterruptedException{

                System.out.println("Generate Sequence: " + c.element());

                BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

                QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).setUseLegacySql(false).build();

                JobId jobId = JobId.of(UUID.randomUUID().toString());
                Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

                queryJob = queryJob.waitFor();

                if(queryJob == null){
                    throw new RuntimeException("Job no longer exists");
                } else if (queryJob.getStatus().getError() != null){
                    throw new RuntimeException(queryJob.getStatus().getError().toString());
                }

                QueryResponse response = bigquery.getQueryResults(jobId);
                TableResult result = queryJob.getQueryResults();

                for(FieldValueList row : result.iterateAll()){

                    String keyInstance = row.get(key).getStringValue();
                    String valueInstance = row.get(value).getStringValue();

                    c.output(KV.of(keyInstance, valueInstance));
                }
            }
        }));
    }
}
