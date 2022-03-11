package com.cgroup.synclinkrequest;

import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zzq on 2022/3/11.
 */
public class LinkExecutor {

    private final List<LinkRequest> requests;
    private final RestClient restClient;

    public LinkExecutor(List<LinkRequest> requests, RestClient restClient) {
        this.requests = requests;
        this.restClient = restClient;
    }

    public List<LinkResponse> exec() throws IOException {
        List<LinkResponse> resList = new ArrayList<>();
        for (int i = 0; i < requests.size(); i++) {
            LinkRequest linkRequest = requests.get(i);
            LinkResponse linkResponse = linkRequest.execRequest(restClient);
            //如果没有后续的请求，则直接结束掉
            if (linkResponse.getState().equals(ResLinkState.None)) {
                break;
            }
        }
        return resList;
    }
}
