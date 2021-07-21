package com.cgroup.essearch;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Created by zzq on 2021/7/20.
 */
public class SlotSearchAction extends Action<SlotSearchRequest, SlotSearchResponse, SlotSearchRequestBuilder> {
    public static final SlotSearchAction INSTANCE = new SlotSearchAction();
    public static final String NAME = "indices:data/read/search_by_slot";

    private SlotSearchAction() {
        super(NAME);
    }

    @Override
    public SlotSearchResponse newResponse() {
        return new SlotSearchResponse();
    }

    @Override
    public SlotSearchRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new SlotSearchRequestBuilder(client, this);
    }
}
