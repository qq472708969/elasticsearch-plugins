package com.cgroup.esupdatesegment;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Created by zzq on 2021/6/11.
 */
public class LoadSegmentAction extends Action<LoadSegmentActionRequest, LoadSegmentActionResponse, LoadSegmentActionRequestBuilder> {
    public static final LoadSegmentAction instance = new LoadSegmentAction(Constants.load_segment_name);

    protected LoadSegmentAction(String name) {
        super(name);
    }

    @Override
    public LoadSegmentActionResponse newResponse() {
        return new LoadSegmentActionResponse();
    }

    @Override
    public LoadSegmentActionRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new LoadSegmentActionRequestBuilder(client, this, new LoadSegmentActionRequest());
    }
}
