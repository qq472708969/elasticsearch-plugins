package com.cgroup.querynodesstatsinfo;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Created by zzq on 2021/8/31.
 */
public class NodesStatsInfoAction extends Action<NodesStatsInfoRequest, NodesStatsResponse, NodesStatsInfoRequestBuilder> {

    public static final NodesStatsInfoAction instance = new NodesStatsInfoAction();
    public static final String NAME = "cluster:monitor/nodes/stats_info";

    protected NodesStatsInfoAction() {
        super(NAME);
    }

    @Override
    public NodesStatsResponse newResponse() {
        return new NodesStatsResponse(null, null, null);
    }

    @Override
    public NodesStatsInfoRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new NodesStatsInfoRequestBuilder(client, this);
    }
}
