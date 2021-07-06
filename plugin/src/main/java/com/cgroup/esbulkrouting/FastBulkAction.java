package com.cgroup.esbulkrouting;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.TransportRequestOptions;

/**
 * Created by zzq on 2021/7/5.
 */
public class FastBulkAction extends Action<BulkRequest, BulkResponse, FastBulkRequestBuilder> {

    public static final FastBulkAction INSTANCE = new FastBulkAction();
    public static final String NAME = "indices:data/write/fast_bulk";

    public FastBulkAction() {
        super(NAME);
    }

    @Override
    public BulkResponse newResponse() {
        return new BulkResponse(null, 3000L);
    }

    @Override
    public FastBulkRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new FastBulkRequestBuilder(client, this);
    }

    @Override
    public TransportRequestOptions transportOptions(Settings settings) {
        return TransportRequestOptions.builder().withType(TransportRequestOptions.Type.BULK).build();
    }

}
