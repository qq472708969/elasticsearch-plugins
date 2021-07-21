package com.cgroup.essearch.base;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.node.ResponseCollectorService;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.transport.Transport;

import java.util.Objects;
import java.util.function.BiFunction;

/**
 * Created by zzq on 2021/7/21.
 */
public class SearchExecutionStatsCollector implements ActionListener<SearchPhaseResult> {
    private final ActionListener<SearchPhaseResult> listener;
    private final String nodeId;
    private final ResponseCollectorService collector;
    private final long startNanos;

    SearchExecutionStatsCollector(ActionListener<SearchPhaseResult> listener,
                                  ResponseCollectorService collector,
                                  String nodeId) {
        this.listener = Objects.requireNonNull(listener, "listener cannot be null");
        this.collector = Objects.requireNonNull(collector, "response collector cannot be null");
        this.startNanos = System.nanoTime();
        this.nodeId = nodeId;
    }

    public static BiFunction<Transport.Connection, SearchActionListener, ActionListener> makeWrapper(ResponseCollectorService service) {
        return (connection, originalListener) -> new SearchExecutionStatsCollector(originalListener, service, connection.getNode().getId());
    }

    @Override
    public void onResponse(SearchPhaseResult response) {
        QuerySearchResult queryResult = response.queryResult();
        if (nodeId != null && queryResult != null) {
            final long serviceTimeEWMA = queryResult.serviceTimeEWMA();
            final int queueSize = queryResult.nodeQueueSize();
            final long responseDuration = System.nanoTime() - startNanos;
            // EWMA/queue size may be -1 if the query node doesn't support capturing it
            if (serviceTimeEWMA > 0 && queueSize >= 0) {
                collector.addNodeStatistics(nodeId, queueSize, responseDuration, serviceTimeEWMA);
            }
        }
        listener.onResponse(response);
    }

    @Override
    public void onFailure(Exception e) {
        listener.onFailure(e);
    }
}
