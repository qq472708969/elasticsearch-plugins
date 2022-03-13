package com.cgroup.synclinkrequest.loadsegment;

import com.alibaba.fastjson.JSONObject;
import com.cgroup.synclinkrequest.core.LinkExecutor;
import com.cgroup.synclinkrequest.core.LinkRequest;
import com.cgroup.synclinkrequest.core.LinkResponse;
import com.cgroup.synclinkrequest.core.LinkResponseState;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.List;

/**
 * Created by zzq on 2022/3/13.
 */
public class Load {
    public static void main(String[] args) throws IOException {

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")));


        LinkRequest l1 = new LinkRequest("DELETE", "/.monitoring-es-6-2022.03.07_bak") {
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
                return LinkResponseState.Success;//如果遇到返回异常信息错误，则直接忽略掉
            }
        };

        LinkRequest l2 = new LinkRequest("PUT") {

            @Override
            protected void processPreRequest(LinkRequest currLinkRequest, LinkResponse preLinkResponse) {
                currLinkRequest.setJsonContent(Contents.content);
                currLinkRequest.setUri("/.monitoring-es-6-2022.03.07_bak");
            }

            @Override
            protected LinkResponseState processResponse(String currData, LinkResponse preLinkResponse) {
//                {
//                    "acknowledged" : true,
//                        "shards_acknowledged" : true,
//                        "index" : ".monitoring-es-6-2022.03.07_bak"
//                }
                JSONObject currDataJson = JSONObject.parseObject(currData);
                Boolean acknowledged = currDataJson.getBoolean("acknowledged");
                Boolean shardsAcknowledged = currDataJson.getBoolean("shards_acknowledged");
                if (acknowledged != null
                        && shardsAcknowledged != null
                        && shardsAcknowledged && acknowledged) {
                    return LinkResponseState.Success;
                }

                return LinkResponseState.Fail;
            }
        };


        LinkExecutor executor = new LinkExecutor(client.getLowLevelClient(), l1, l2);
        executor.setSleepSecond(30);
        List<LinkResponse> responses = executor.exec();

        client.close();

        for (int i = 0; i < responses.size(); i++) {
            LinkResponse linkResponse = responses.get(i);
            System.out.println(linkResponse.getRequestInfo());
            System.out.println("=====");
        }

    }
}
