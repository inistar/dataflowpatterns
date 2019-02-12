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
import java.util.UUID;

public class BQTest {

    public static void main(String[] args) throws InterruptedException {

        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

        String query = "SELECT event_id, event_ts FROM `dataflowtesting-218212.testing.apple`";

        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(query)
                        .setUseLegacySql(false)
                        .build();

        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        queryJob = queryJob.waitFor();

        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }

        QueryResponse response = bigquery.getQueryResults(jobId);

        TableResult result = queryJob.getQueryResults();

        for (FieldValueList row : result.iterateAll()) {
            String url = row.get("event_id").getStringValue();
            String viewCount = row.get("event_ts").getStringValue();
            System.out.printf("url: %s views: %s%n", url, viewCount);
        }
    }
}
