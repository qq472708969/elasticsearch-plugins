package com.cgroup.synclinkrequest;

import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by zzq on 2022/3/12.
 */
public class TestEs2 {
    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")));

//
//        LinkRequest l1 = new LinkRequest("GET", "/_cat/indices?v", null) {
//            @Override
//            public LinkResponseState processResponse(String data, LinkResponse preLinkResponse) {
//
//
//                return LinkResponseState.Success;
//            }
//        };

        LinkRequest l1 = new LinkRequest("DELETE", "/.monitoring-es-6-2022.03_dest") {
            @Override
            public LinkResponseState processResponse(String data, LinkResponse preLinkResponse) {
                System.out.println(data);

                if (data != null) {
                    return LinkResponseState.Success;
                } else {
                    return LinkResponseState.Fail;
                }
            }

            @Override
            protected LinkResponseState processIOException(String exceptionMsg, LinkResponse preLinkResponse) {
                return LinkResponseState.Success;
            }
        };

        LinkRequest l11 = new LinkRequest() {
            @Override
            public LinkResponseState processResponse(String data, LinkResponse preLinkResponse) {
                System.out.println("hahahah");
                return LinkResponseState.Success;
            }
        };

        LinkRequest l2 = new LinkRequest("POST", "_reindex?wait_for_completion=false",
                "{\n" +
                        "  \"source\": {\n" +
                        "    \"size\": 2000,\n" +
                        "    \"index\": \".monitoring-es-6-2022.03*\"\n" +
                        "  },\n" +
                        "  \"dest\": {\n" +
                        "    \"op_type\": \"create\",\n" +
                        "    \"index\": \".monitoring-es-6-2022.03_dest\"\n" +
                        "  }\n" +
                        "}") {
            @Override
            public LinkResponseState processResponse(String data, LinkResponse preLinkResponse) {
                System.out.println("删除的结果：" + preLinkResponse.getData());

                if (data != null) {
                    return LinkResponseState.Success;
                } else {
                    return LinkResponseState.Fail;
                }
            }
        };

        LinkRequest l3 = new LinkRequest("GET") {
            @Override
            protected void processPreRequest(LinkRequest currLinkRequest, LinkResponse preLinkResponse) {
                currLinkRequest.setJsonContent(null);
                JSONObject jsonObject = JSONObject.parseObject(preLinkResponse.getData());
                String task = jsonObject.getString("task");
                currLinkRequest.setUri("_tasks/".concat(task));
            }

            @Override
            public LinkResponseState processResponse(String data, LinkResponse preLinkResponse) {

                JSONObject jsonObject = JSONObject.parseObject(data);
                Boolean ret = jsonObject.getBoolean("completed");
                if (ret) {
                    return LinkResponseState.Success;
                } else {
                    System.out.println(">>>>" + data);
                    System.out.println(">>>>" + ret);

                    return LinkResponseState.Repeat;
                }
            }
        };

        LinkRequest l4 = new LinkRequest("GET", "/_cluster/health") {
            @Override
            public LinkResponseState processResponse(String data, LinkResponse preLinkResponse) {

                System.out.println("health".concat(data));
                return LinkResponseState.Success;
            }
        };


        LinkExecutor executor = new LinkExecutor(client.getLowLevelClient(), l1, l11, l2, l3, l4);
        executor.setSleepSecond(30);
        List<LinkResponse> responses = executor.exec();

        client.close();

    }
}
