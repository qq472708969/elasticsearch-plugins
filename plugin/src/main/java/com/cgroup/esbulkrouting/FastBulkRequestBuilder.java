package com.cgroup.esbulkrouting;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.WriteRequestBuilder;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;

/**
 * Created by zzq on 2021/7/5.
 */
public class FastBulkRequestBuilder extends ActionRequestBuilder<FastBulkRequest, BulkResponse, FastBulkRequestBuilder>
        implements WriteRequestBuilder<FastBulkRequestBuilder> {

    public FastBulkRequestBuilder(ElasticsearchClient client, FastBulkAction action, @Nullable String globalIndex, @Nullable String globalType) {
        super(client, action, new FastBulkRequest(globalIndex, globalType));
    }

    public FastBulkRequestBuilder(ElasticsearchClient client, FastBulkAction action) {
        super(client, action, new FastBulkRequest());
    }

    /**
     * Adds an {@link IndexRequest} to the list of actions to execute. Follows the same behavior of {@link IndexRequest}
     * (for example, if no id is provided, one will be generated, or usage of the create flag).
     */
    public FastBulkRequestBuilder add(IndexRequest request) {
        super.request.add(request);
        return this;
    }

    /**
     * Adds an {@link IndexRequest} to the list of actions to execute. Follows the same behavior of {@link IndexRequest}
     * (for example, if no id is provided, one will be generated, or usage of the create flag).
     */
    public FastBulkRequestBuilder add(IndexRequestBuilder request) {
        super.request.add(request.request());
        return this;
    }

    /**
     * Adds an {@link DeleteRequest} to the list of actions to execute.
     */
    public FastBulkRequestBuilder add(DeleteRequest request) {
        super.request.add(request);
        return this;
    }

    /**
     * Adds an {@link DeleteRequest} to the list of actions to execute.
     */
    public FastBulkRequestBuilder add(DeleteRequestBuilder request) {
        super.request.add(request.request());
        return this;
    }


    /**
     * Adds an {@link UpdateRequest} to the list of actions to execute.
     */
    public FastBulkRequestBuilder add(UpdateRequest request) {
        super.request.add(request);
        return this;
    }

    /**
     * Adds an {@link UpdateRequest} to the list of actions to execute.
     */
    public FastBulkRequestBuilder add(UpdateRequestBuilder request) {
        super.request.add(request.request());
        return this;
    }

    /**
     * Adds a framed data in binary format
     */
    public FastBulkRequestBuilder add(byte[] data, int from, int length, XContentType xContentType) throws Exception {
        request.add(data, from, length, null, null, xContentType);
        return this;
    }

    /**
     * Adds a framed data in binary format
     */
    public FastBulkRequestBuilder add(byte[] data, int from, int length, @Nullable String defaultIndex, @Nullable String defaultType,
                                      XContentType xContentType) throws Exception {
        request.add(data, from, length, defaultIndex, defaultType, xContentType);
        return this;
    }

    /**
     * Sets the number of shard copies that must be active before proceeding with the write.
     * See {@link ReplicationRequest#waitForActiveShards(ActiveShardCount)} for details.
     */
    public FastBulkRequestBuilder setWaitForActiveShards(ActiveShardCount waitForActiveShards) {
        request.waitForActiveShards(waitForActiveShards);
        return this;
    }

    /**
     * A shortcut for {@link #setWaitForActiveShards(ActiveShardCount)} where the numerical
     * shard count is passed in, instead of having to first call {@link ActiveShardCount#from(int)}
     * to get the ActiveShardCount.
     */
    public FastBulkRequestBuilder setWaitForActiveShards(final int waitForActiveShards) {
        return setWaitForActiveShards(ActiveShardCount.from(waitForActiveShards));
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to {@code 1m}.
     */
    public final FastBulkRequestBuilder setTimeout(TimeValue timeout) {
        request.timeout(timeout);
        return this;
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to {@code 1m}.
     */
    public final FastBulkRequestBuilder setTimeout(String timeout) {
        request.timeout(timeout);
        return this;
    }

    /**
     * The number of actions currently in the bulk.
     */
    public int numberOfActions() {
        return request.numberOfActions();
    }

    public FastBulkRequestBuilder pipeline(String globalPipeline) {
        request.pipeline(globalPipeline);
        return this;
    }

    public FastBulkRequestBuilder routing(String globalRouting) {
        request.routing(globalRouting);
        return this;
    }
}
