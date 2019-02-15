package com.springml.dataflow.patterns;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;

public class RestIO {

    OkHttpClient client = new OkHttpClient();

    private String run(String url) throws IOException{

        Request request = new Request.Builder()
                .url(url)
                .build();

        try(Response response = client.newCall(request).execute()){
            return response.body().string();
        }
    }

    public String get(String url) throws IOException{

        RestIO example = new RestIO();
        String response = example.run(url);
        System.out.println("Response: " + response);

        return response;
    }
}
