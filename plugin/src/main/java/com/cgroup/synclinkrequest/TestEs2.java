package com.cgroup.synclinkrequest;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.List;

/**
 * Created by zzq on 2022/3/12.
 */
public class TestEs2 {
    public static void main(String[] args) {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")));


        LinkRequest l1 = new LinkRequest("GET", "/_cat/indices?v", null) {
            @Override
            public LinkResponseState processResponse(String data, LinkResponse preLinkResponse) {


                return LinkResponseState.Success;
            }
        };

        LinkRequest l2 = new LinkRequest("POST", "_reindex?wait_for_completion=false",
                "{\n" +
                        "  \"source\": {\n" +
                        "    \"size\": 2,\n" +
                        "    \"index\": \".monitoring-es-6-2022.03*\"\n" +
                        "  },\n" +
                        "  \"dest\": {\n" +
                        "    \"op_type\": \"create\",\n" +
                        "    \"index\": \".monitoring-es-6-2022.03_dest\"\n" +
                        "  }\n" +
                        "}") {
            @Override
            public LinkResponseState processResponse(String data, LinkResponse preLinkResponse) {
                System.out.println(data);

                return LinkResponseState.Success;
            }
        };

        LinkRequest l3 = new LinkRequest("PUT", ".monitoring-es-6-2022.03.07/_settings") {
            @Override
            public LinkResponseState processResponse(String data, LinkResponse preLinkResponse) {
                System.out.println(preLinkResponse);

                return LinkResponseState.Success;
            }
        };
        l3.setJsonContent("{\n" +
                "  \"routing.allocation.require.group_type\":\"g1\"\n" +
                "}");

        LinkExecutor executor = new LinkExecutor(client.getLowLevelClient(), l1, l2, l3);
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
