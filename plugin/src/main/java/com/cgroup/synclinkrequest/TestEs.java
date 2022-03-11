package com.cgroup.synclinkrequest;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.GetIndexRequest;

import java.io.IOException;

/**
 * Created by zzq on 2022/3/11.
 */
public class TestEs {

    public static void main(String[] args) throws IOException {

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")));

//
//        IndexRequest indexRequest = new IndexRequest(".monitoring-es-6-2022.03.07", "doc");
//
//
//        String json = "";
//
//        GetIndexRequest getIndexRequest = new GetIndexRequest(".monitoring-es-6-2022.03.07");

        RestClient lowLevelClient = client.getLowLevelClient();

//        Request request = new Request("PUT", ".monitoring-es-6-2022.03.07/_settings");
//        String json = "{\n" +
//                "  \"routing.allocation.require.group_type\":\"g1\"\n" +
//                "}";
//        request.setJsonEntity(json);

        Request request = new Request("GET", "/_cluster/state");
        String json = "{\n" +
                "  \"routing.allocation.require.group_type\":\"g1\"\n" +
                "}";
        request.setJsonEntity(null);

        Response response = lowLevelClient.performRequest(request);

        HttpEntity entity = response.getEntity();

        try {
            String s = EntityUtils.toString(entity);

            System.out.println(s);
        } catch (IOException e) {
            e.printStackTrace();
        }


        System.out.println();
        try {
            lowLevelClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
