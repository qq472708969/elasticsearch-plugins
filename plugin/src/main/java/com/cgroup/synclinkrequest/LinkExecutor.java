package com.cgroup.synclinkrequest;

import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.client.RestClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by zzq on 2022/3/11.
 */
public class LinkExecutor {

    private List<LinkRequest> requests;
    private RestClient restClient;

    public LinkExecutor(RestClient restClient, List<LinkRequest> requests) {
        if (CollectionUtils.isEmpty(requests)) {
            return;
        }
        this.requests = requests;
        this.restClient = restClient;
    }

    public LinkExecutor(RestClient restClient, LinkRequest... requests) {
        if (requests == null || requests.length == 0) {
            return;
        }
        this.requests = new ArrayList<>(Arrays.asList(requests));
        this.restClient = restClient;
    }

    /**
     * RestClient执行低级别API
     *
     * @return
     */
    public List<LinkResponse> exec() {
        List<LinkResponse> resList = new ArrayList<>(5);
        LinkResponse tmp = new LinkResponse();
        for (int i = 0; i < requests.size(); i++) {
            LinkRequest linkRequest = requests.get(i);
            LinkResponse linkResponse = linkRequest.exec(restClient, tmp);
            resList.add(linkResponse);
            //缓存前一个执行器的结果
            tmp = linkResponse;
            //请求执行非正常情况，则直接停止
            if (!linkResponse.getState().equals(LinkResponseState.Success)) {
                break;
            }
        }
        return resList;
    }
}
