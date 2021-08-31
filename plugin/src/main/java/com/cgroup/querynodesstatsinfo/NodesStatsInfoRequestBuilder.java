package com.cgroup.querynodesstatsinfo;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.unit.TimeValue;

/**
 * Created by zzq on 2021/8/31.
 */
public class NodesStatsInfoRequestBuilder
        extends ActionRequestBuilder<NodesStatsInfoRequest, NodesStatsResponse, NodesStatsInfoRequestBuilder> {
    public NodesStatsInfoRequestBuilder(ElasticsearchClient client, NodesStatsInfoAction action) {
        super(client, action, new NodesStatsInfoRequest());
    }

    @SuppressWarnings("unchecked")
    public final NodesStatsInfoRequestBuilder setNodesIds(String... nodesIds) {
        request.nodesIds(nodesIds);
        return this;
    }

    @SuppressWarnings("unchecked")
    public final NodesStatsInfoRequestBuilder setTimeout(TimeValue timeout) {
        request.timeout(timeout);
        return this;
    }

    @SuppressWarnings("unchecked")
    public final NodesStatsInfoRequestBuilder setTimeout(String timeout) {
        request.timeout(timeout);
        return this;
    }

    /**
     * Sets all the request flags.
     */
    public NodesStatsInfoRequestBuilder all() {
        request.all();
        return this;
    }

    /**
     * Clears all stats flags.
     */
    public NodesStatsInfoRequestBuilder clear() {
        request.clear();
        return this;
    }

    /**
     * Should the node indices stats be returned.
     */
    public NodesStatsInfoRequestBuilder setIndices(boolean indices) {
        request.indices(indices);
        return this;
    }

    public NodesStatsInfoRequestBuilder setBreaker(boolean breaker) {
        request.breaker(breaker);
        return this;
    }

    public NodesStatsInfoRequestBuilder setScript(boolean script) {
        request.script(script);
        return this;
    }

    /**
     * Should the node indices stats be returned.
     */
    public NodesStatsInfoRequestBuilder setIndices(CommonStatsFlags indices) {
        request.indices(indices);
        return this;
    }

    /**
     * Should the node OS stats be returned.
     */
    public NodesStatsInfoRequestBuilder setOs(boolean os) {
        request.os(os);
        return this;
    }

    /**
     * Should the node OS stats be returned.
     */
    public NodesStatsInfoRequestBuilder setProcess(boolean process) {
        request.process(process);
        return this;
    }

    /**
     * Should the node JVM stats be returned.
     */
    public NodesStatsInfoRequestBuilder setJvm(boolean jvm) {
        request.jvm(jvm);
        return this;
    }

    /**
     * Should the node thread pool stats be returned.
     */
    public NodesStatsInfoRequestBuilder setThreadPool(boolean threadPool) {
        request.threadPool(threadPool);
        return this;
    }

    /**
     * Should the node file system stats be returned.
     */
    public NodesStatsInfoRequestBuilder setFs(boolean fs) {
        request.fs(fs);
        return this;
    }

    /**
     * Should the node Transport stats be returned.
     */
    public NodesStatsInfoRequestBuilder setTransport(boolean transport) {
        request.transport(transport);
        return this;
    }

    /**
     * Should the node HTTP stats be returned.
     */
    public NodesStatsInfoRequestBuilder setHttp(boolean http) {
        request.http(http);
        return this;
    }

    /**
     * Should the discovery stats be returned.
     */
    public NodesStatsInfoRequestBuilder setDiscovery(boolean discovery) {
        request.discovery(discovery);
        return this;
    }

    /**
     * Should ingest statistics be returned.
     */
    public NodesStatsInfoRequestBuilder setIngest(boolean ingest) {
        request.ingest(ingest);
        return this;
    }

    public NodesStatsInfoRequestBuilder setAdaptiveSelection(boolean adaptiveSelection) {
        request.adaptiveSelection(adaptiveSelection);
        return this;
    }
}
