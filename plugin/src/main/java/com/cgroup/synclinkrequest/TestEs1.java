package com.cgroup.synclinkrequest;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.List;

/**
 * Created by zzq on 2022/3/12.
 */
public class TestEs1 {
    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")));


        LinkRequest l1 = new LinkRequest("GET", "/_cat/indices?v", null) {
            @Override
            public LinkResponseState doExec(String ret) {


                return LinkResponseState.Success;
            }
        };

        LinkRequest l11 = new LinkRequest() {
            @Override
            public LinkResponseState doExec(String ret) {
                System.out.println("哈哈哈");

                return LinkResponseState.Success;
            }
        };


        LinkRequest l2 = new LinkRequest("PUT", ".monitoring-es-6-2022.03.07/_settings",
                "{\n" +
                        "  \"index.number_of_replicas\": \"0\",\n" +
                        "  \"auto_expand_replicas\":\"false\"\n" +
                        "}") {
            @Override
            public LinkResponseState doExec(String ret) {
                System.out.println(ret);

                return LinkResponseState.Success;
            }
        };

        LinkRequest l3 = new LinkRequest("PUT", ".monitoring-es-6-2022.03.07/_settings") {
            @Override
            public LinkResponseState doExec(String ret) {
                System.out.println(ret);

                return LinkResponseState.Success;
            }
        };
        l3.setJsonContent("{\n" +
                "  \"routing.allocation.require.group_type\":\"g1\"\n" +
                "}");

        LinkExecutor executor = new LinkExecutor(client.getLowLevelClient(), l1, l11, l2, l3);
        List<LinkResponse> responses = executor.exec();

        //client.close();

        for (int i = 0; i < responses.size(); i++) {
            LinkResponse linkResponse = responses.get(i);
            System.out.println(linkResponse.getData());
            System.out.println(linkResponse.getMsg());
            System.out.println(linkResponse.getState());
            System.out.println("============");
        }
    }
}
