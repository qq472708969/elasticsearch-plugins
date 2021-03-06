package com.cgroup.querynodesstatsinfo;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.NoSuchNodeException;
import org.elasticsearch.action.admin.cluster.node.stats.*;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Created by zzq on 2021/8/31.
 */
public class NodesStatsInfoTransportAction extends HandledTransportAction<NodesStatsInfoRequest, NodesStatsResponse> {

    private NodeService nodeService;
    private ClusterService clusterService;
    private TransportService transportService;
    private Class<NodeStats> nodeResponseClass;
    private String transportNodeAction;

    @Inject
    public NodesStatsInfoTransportAction(Settings settings, ThreadPool threadPool, NodeService nodeService,
                                         ClusterService clusterService, TransportService transportService, ActionFilters actionFilters,
                                         IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, NodesStatsInfoAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, NodesStatsInfoRequest::new);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.nodeResponseClass = NodeStats.class;
        this.transportNodeAction = NodesStatsInfoAction.NAME + "[n]";
        this.nodeService = nodeService;
        transportService.registerRequestHandler(transportNodeAction, NodeStatsInfoRequest::new, ThreadPool.Names.MANAGEMENT, new NodeTransportHandler());
    }

    @Override
    protected void doExecute(Task task, NodesStatsInfoRequest request, ActionListener<NodesStatsResponse> listener) {
        try {
            new AsyncAction(task, request, listener).start();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void doExecute(NodesStatsInfoRequest request, ActionListener<NodesStatsResponse> listener) {
    }

    protected void resolveRequest(NodesStatsInfoRequest request, ClusterState clusterState) {
        assert request.concreteNodes() == null : "request concreteNodes shouldn't be set";
        String[] nodesIds = clusterState.nodes().resolveNodes(request.nodesIds());
        DiscoveryNodes discoveryNodes = clusterState.nodes();
        Map<String, Object> attr = request.getAttr();
        /**
         * ?????????????????????????????????????????????????????????
         */
        if (attr == null || attr.size() == 0) {
            request.setConcreteNodes(Arrays.stream(nodesIds).map(discoveryNodes::get).toArray(DiscoveryNode[]::new));
            return;
        }
        List<DiscoveryNode> concreteNodes = new ArrayList<>();
        Map<String, List<String>> attrValues = new HashMap<>(6);
        for (String key : attr.keySet()) {
            Object o = attr.get(key);
            if (o == null) {
                continue;
            }
            List<String> values = (List) o;
            if (values.isEmpty()) {
                continue;
            }
            attrValues.put(key, values);
        }
        for (int i = 0; i < nodesIds.length; i++) {
            String nodeId = nodesIds[i];
            DiscoveryNode discoveryNode = discoveryNodes.get(nodeId);
            Map<String, String> attributes = discoveryNode.getAttributes();
            if (attributes == null || attributes.size() == 0 || attrValues.size() == 0) {
                continue;
            }
            int count = 0;
            for (Map.Entry<String, List<String>> et : attrValues.entrySet()) {
                if (!attributes.containsKey(et.getKey())) {
                    continue;
                }
                String value = attributes.get(et.getKey());
                if (!et.getValue().contains(value)) {
                    break;
                }
                count++;
            }
            if (count == attrValues.keySet().size()) {
                concreteNodes.add(discoveryNode);
            }
        }

        request.setConcreteNodes(concreteNodes.stream().toArray(DiscoveryNode[]::new));
    }

    protected NodesStatsResponse newResponse(NodesStatsInfoRequest request, AtomicReferenceArray nodesResponses) {
        final List<NodeStats> responses = new ArrayList<>();
        final List<FailedNodeException> failures = new ArrayList<>();

        for (int i = 0; i < nodesResponses.length(); ++i) {
            Object response = nodesResponses.get(i);

            if (response instanceof FailedNodeException) {
                failures.add((FailedNodeException) response);
            } else {
                responses.add(nodeResponseClass.cast(response));
            }
        }

        return new NodesStatsResponse(clusterService.getClusterName(), responses, failures);
    }

    protected NodeStatsInfoRequest newNodeRequest(String nodeId, NodesStatsInfoRequest request) {
        return new NodeStatsInfoRequest(nodeId, request);
    }

    class AsyncAction {
        private final NodesStatsInfoRequest request;
        private final ActionListener<NodesStatsResponse> listener;
        private final AtomicReferenceArray<Object> responses;
        private final AtomicInteger counter = new AtomicInteger();
        private final Task task;

        AsyncAction(Task task, NodesStatsInfoRequest request, ActionListener<NodesStatsResponse> listener) {
            this.task = task;
            this.request = request;
            this.listener = listener;
            if (request.concreteNodes() == null) {
                resolveRequest(request, clusterService.state());
                assert request.concreteNodes() != null;
            }
            this.responses = new AtomicReferenceArray<>(request.concreteNodes().length);
        }

        void start() throws NoSuchMethodException {
            final DiscoveryNode[] nodes = request.concreteNodes();
            if (nodes.length == 0) {
                // nothing to notify
                threadPool.generic().execute(() -> listener.onResponse(newResponse(request, responses)));
                return;
            }
            TransportRequestOptions.Builder builder = TransportRequestOptions.builder();
            if (request.timeout() != null) {
                builder.withTimeout(request.timeout());
            }
            Constructor<NodeStats> declaredConstructor = NodeStats.class.getDeclaredConstructor();
            declaredConstructor.setAccessible(true);
            for (int i = 0; i < nodes.length; i++) {
                final int idx = i;
                final DiscoveryNode node = nodes[i];
                final String nodeId = node.getId();
                try {
                    if (node == null) {
                        onFailure(idx, nodeId, new NoSuchNodeException(nodeId));
                    } else {
                        TransportRequest nodeRequest = newNodeRequest(nodeId, request);
                        if (task != null) {
                            nodeRequest.setParentTask(clusterService.localNode().getId(), task.getId());
                        }

                        transportService.sendRequest(node, transportNodeAction, nodeRequest, builder.build(),
                                new TransportResponseHandler<NodeStats>() {
                                    @Override
                                    public NodeStats read(StreamInput in) throws IOException {
                                        NodeStats nodeResponse = null;
                                        try {
                                            nodeResponse = declaredConstructor.newInstance();
                                        } catch (IllegalAccessException e) {
                                            e.printStackTrace();
                                        } catch (InstantiationException e) {
                                            e.printStackTrace();
                                        } catch (InvocationTargetException e) {
                                            e.printStackTrace();
                                        }
                                        nodeResponse.readFrom(in);
                                        return nodeResponse;
                                    }

                                    @Override
                                    public void handleResponse(NodeStats response) {
                                        onOperation(idx, response);
                                    }

                                    @Override
                                    public void handleException(TransportException exp) {
                                        onFailure(idx, node.getId(), exp);
                                    }

                                    @Override
                                    public String executor() {
                                        return ThreadPool.Names.SAME;
                                    }
                                });
                    }
                } catch (Exception e) {
                    onFailure(idx, nodeId, e);
                }
            }
        }

        private void onOperation(int idx, NodeStats nodeResponse) {
            responses.set(idx, nodeResponse);
            if (counter.incrementAndGet() == responses.length()) {
                finishHim();
            }
        }

        private void onFailure(int idx, String nodeId, Throwable t) {
            if (logger.isDebugEnabled() && !(t instanceof NodeShouldNotConnectException)) {
                logger.debug(new ParameterizedMessage("failed to execute on node [{}]", nodeId), t);
            }
            responses.set(idx, new FailedNodeException(nodeId, "Failed node [" + nodeId + "]", t));
            if (counter.incrementAndGet() == responses.length()) {
                finishHim();
            }
        }

        private void finishHim() {
            NodesStatsResponse finalResponse;
            try {
                finalResponse = newResponse(request, responses);
            } catch (Exception e) {
                logger.debug("failed to combine responses from nodes", e);
                listener.onFailure(e);
                return;
            }
            listener.onResponse(finalResponse);
        }
    }

    protected NodeStats nodeOperation(NodeStatsInfoRequest nodeStatsInfoRequest) {
        NodesStatsInfoRequest request = nodeStatsInfoRequest.request;
        return nodeService.stats(request.indices(), request.os(), request.process(), request.jvm(), request.threadPool(),
                request.fs(), request.transport(), request.http(), request.breaker(), request.script(), request.discovery(),
                request.ingest(), request.adaptiveSelection());
    }

    public static class NodeStatsInfoRequest extends BaseNodeRequest {

        NodesStatsInfoRequest request;

        public NodeStatsInfoRequest() {
        }

        NodeStatsInfoRequest(String nodeId, NodesStatsInfoRequest request) {
            super(nodeId);
            this.request = request;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            request = new NodesStatsInfoRequest();
            request.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }

    class NodeTransportHandler implements TransportRequestHandler<NodeStatsInfoRequest> {

        @Override
        public void messageReceived(NodeStatsInfoRequest request, TransportChannel channel, Task task) throws Exception {
            channel.sendResponse(nodeOperation(request));
        }

        @Override
        public void messageReceived(NodeStatsInfoRequest request, TransportChannel channel) throws Exception {
            channel.sendResponse(nodeOperation(request));
        }

    }
}
