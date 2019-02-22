package com.springml.dataflow.patterns;

//import com.google.api.services.storage.Storage;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryResponse;
import com.google.cloud.bigquery.TableResult;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.stream.Stream;

import org.json.*;

public class BQTest {

    public static void downloadFromStorage(){

        String bucketName = "dataflow-results-ini";
        String srcFilename = "upload.txt";
        Path destFilePath = Paths.get("./src/main/resources/download.txt");

        Storage storage = StorageOptions.getDefaultInstance().getService();

        Blob blob = storage.get(BlobId.of(bucketName, srcFilename));

        blob.downloadTo(destFilePath);

        System.out.println("DOWNLOAD DONE!");
    }

    public static void readParse() throws IOException {

        File file = new File("./src/main/resources/download.txt");

        BufferedReader br = new BufferedReader(new FileReader(file));

        String st;
        while ((st = br.readLine()) != null) {

//            System.out.println(st);
            JSONObject obj = new JSONObject(st);
            for (Iterator<String> it = obj.keys(); it.hasNext(); ) {
                String element = it.next();

                System.out.println(element);

            }
//            break;
        }
    }


    public static void main(String[] args) throws InterruptedException, IOException {

//        downloadFromStorage();

        readParse();



//        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
//
//        String query = "SELECT event_id, event_ts FROM `dataflowtesting-218212.testing.apple`";
//
//        QueryJobConfiguration queryConfig =
//                QueryJobConfiguration.newBuilder(query)
//                        .setUseLegacySql(false)
//                        .build();
//
//        JobId jobId = JobId.of(UUID.randomUUID().toString());
//        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
//
//        queryJob = queryJob.waitFor();
//
//        if (queryJob == null) {
//            throw new RuntimeException("Job no longer exists");
//        } else if (queryJob.getStatus().getError() != null) {
//            throw new RuntimeException(queryJob.getStatus().getError().toString());
//        }
//
//        QueryResponse response = bigquery.getQueryResults(jobId);
//
//        TableResult result = queryJob.getQueryResults();
//
//        for (FieldValueList row : result.iterateAll()) {
//            String url = row.get("event_id").getStringValue();
//            String viewCount = row.get("event_ts").getStringValue();
//            System.out.printf("url: %s views: %s%n", url, viewCount);
//        }
    }
}
