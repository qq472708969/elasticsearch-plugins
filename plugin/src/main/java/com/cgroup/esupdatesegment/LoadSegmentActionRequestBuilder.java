package com.cgroup.esupdatesegment;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Created by zzq on 2021/6/11.
 */
public class LoadSegmentActionRequestBuilder extends ActionRequestBuilder<LoadSegmentActionRequest, LoadSegmentActionResponse, LoadSegmentActionRequestBuilder> {
    protected LoadSegmentActionRequestBuilder(ElasticsearchClient client, Action<LoadSegmentActionRequest, LoadSegmentActionResponse, LoadSegmentActionRequestBuilder> action, LoadSegmentActionRequest request) {
        super(client, action, request);
    }
}
