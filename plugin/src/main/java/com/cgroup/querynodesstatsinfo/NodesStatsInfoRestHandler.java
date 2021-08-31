package com.cgroup.querynodesstatsinfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.RestActions;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Created by zzq on 2021/8/31.
 */
public class NodesStatsInfoRestHandler extends BaseRestHandler {
    private static final Logger logger = LogManager.getLogger(NodesStatsInfoRestHandler.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);

    public NodesStatsInfoRestHandler(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(GET, "/_nodes/stats_info", this);
        controller.registerHandler(GET, "/_nodes/{nodeId}/stats_info", this);

        controller.registerHandler(GET, "/_nodes/stats_info/{metric}", this);
        controller.registerHandler(GET, "/_nodes/{nodeId}/stats_info/{metric}", this);

        controller.registerHandler(GET, "/_nodes/stats_info/{metric}/{index_metric}", this);

        controller.registerHandler(GET, "/_nodes/{nodeId}/stats_info/{metric}/{index_metric}", this);
    }

    static final Map<String, Consumer<NodesStatsInfoRequest>> METRICS;

    static {
        final Map<String, Consumer<NodesStatsInfoRequest>> metrics = new HashMap<>();
        metrics.put("os", r -> r.os(true));
        metrics.put("jvm", r -> r.jvm(true));
        metrics.put("thread_pool", r -> r.threadPool(true));
        metrics.put("fs", r -> r.fs(true));
        metrics.put("transport", r -> r.transport(true));
        metrics.put("http", r -> r.http(true));
        metrics.put("indices", r -> r.indices(true));
        metrics.put("process", r -> r.process(true));
        metrics.put("breaker", r -> r.breaker(true));
        metrics.put("script", r -> r.script(true));
        metrics.put("discovery", r -> r.discovery(true));
        metrics.put("ingest", r -> r.ingest(true));
        metrics.put("adaptive_selection", r -> r.adaptiveSelection(true));
        METRICS = Collections.unmodifiableMap(metrics);
    }

    static final Map<String, Consumer<CommonStatsFlags>> FLAGS;

    static {
        final Map<String, Consumer<CommonStatsFlags>> flags = new HashMap<>();
        for (final CommonStatsFlags.Flag flag : CommonStatsFlags.Flag.values()) {
            flags.put(flag.getRestName(), f -> f.set(flag, true));
        }
        FLAGS = Collections.unmodifiableMap(flags);
    }

    @Override
    public String getName() {
        return "nodes_stats_info_action";
    }

    /**
     * GET /_nodes/stats_info?attr=group_type|g2|g3
     *
     * rest接口
     *
     * @param request
     * @param client
     * @return
     * @throws IOException
     */
    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        Set<String> metrics = Strings.tokenizeByCommaToSet(request.param("metric", "_all"));

        NodesStatsInfoRequest nodesStatsInfoRequest = new NodesStatsInfoRequest(nodesIds);

        addAttrArea(request, nodesStatsInfoRequest);

        nodesStatsInfoRequest.timeout(request.param("timeout"));

        if (metrics.size() == 1 && metrics.contains("_all")) {
            if (request.hasParam("index_metric")) {
                throw new IllegalArgumentException(
                        String.format(
                                Locale.ROOT,
                                "request [%s] contains index metrics [%s] but all stats requested",
                                request.path(),
                                request.param("index_metric")));
            }
            nodesStatsInfoRequest.all();
            nodesStatsInfoRequest.indices(CommonStatsFlags.ALL);
        } else if (metrics.contains("_all")) {
            throw new IllegalArgumentException(
                    String.format(Locale.ROOT,
                            "request [%s] contains _all and individual metrics [%s]",
                            request.path(),
                            request.param("metric")));
        } else {
            nodesStatsInfoRequest.clear();

            // use a sorted set so the unrecognized parameters appear in a reliable sorted order
            final Set<String> invalidMetrics = new TreeSet<>();
            for (final String metric : metrics) {
                final Consumer<NodesStatsInfoRequest> handler = METRICS.get(metric);
                if (handler != null) {
                    handler.accept(nodesStatsInfoRequest);
                } else {
                    invalidMetrics.add(metric);
                }
            }

            if (!invalidMetrics.isEmpty()) {
                throw new IllegalArgumentException(unrecognized(request, invalidMetrics, METRICS.keySet(), "metric"));
            }

            // check for index specific metrics
            if (metrics.contains("indices")) {
                Set<String> indexMetrics = Strings.tokenizeByCommaToSet(request.param("index_metric", "_all"));
                if (indexMetrics.size() == 1 && indexMetrics.contains("_all")) {
                    nodesStatsInfoRequest.indices(CommonStatsFlags.ALL);
                } else {
                    CommonStatsFlags flags = new CommonStatsFlags();
                    flags.clear();
                    // use a sorted set so the unrecognized parameters appear in a reliable sorted order
                    final Set<String> invalidIndexMetrics = new TreeSet<>();
                    for (final String indexMetric : indexMetrics) {
                        final Consumer<CommonStatsFlags> handler = FLAGS.get(indexMetric);
                        if (handler != null) {
                            if ("suggest".equals(indexMetric)) {
                                deprecationLogger.deprecated(
                                        "the suggest index metric is deprecated on the nodes stats API [" + request.uri() + "]");
                            }
                            handler.accept(flags);
                        } else {
                            invalidIndexMetrics.add(indexMetric);
                        }
                    }

                    if (!invalidIndexMetrics.isEmpty()) {
                        throw new IllegalArgumentException(unrecognized(request, invalidIndexMetrics, FLAGS.keySet(), "index metric"));
                    }

                    nodesStatsInfoRequest.indices(flags);
                }
            } else if (request.hasParam("index_metric")) {
                throw new IllegalArgumentException(
                        String.format(
                                Locale.ROOT,
                                "request [%s] contains index metrics [%s] but indices stats not requested",
                                request.path(),
                                request.param("index_metric")));
            }
        }

        if (nodesStatsInfoRequest.indices().isSet(CommonStatsFlags.Flag.FieldData) && (request.hasParam("fields") || request.hasParam("fielddata_fields"))) {
            nodesStatsInfoRequest.indices().fieldDataFields(
                    request.paramAsStringArray("fielddata_fields", request.paramAsStringArray("fields", null)));
        }
        if (nodesStatsInfoRequest.indices().isSet(CommonStatsFlags.Flag.Completion) && (request.hasParam("fields") || request.hasParam("completion_fields"))) {
            nodesStatsInfoRequest.indices().completionDataFields(
                    request.paramAsStringArray("completion_fields", request.paramAsStringArray("fields", null)));
        }
        if (nodesStatsInfoRequest.indices().isSet(CommonStatsFlags.Flag.Search) && (request.hasParam("groups"))) {
            nodesStatsInfoRequest.indices().groups(request.paramAsStringArray("groups", null));
        }
        if (nodesStatsInfoRequest.indices().isSet(CommonStatsFlags.Flag.Indexing) && (request.hasParam("types"))) {
            nodesStatsInfoRequest.indices().types(request.paramAsStringArray("types", null));
        }
        if (nodesStatsInfoRequest.indices().isSet(CommonStatsFlags.Flag.Segments)) {
            nodesStatsInfoRequest.indices().includeSegmentFileSizes(request.paramAsBoolean("include_segment_file_sizes", false));
        }

        return restChannel -> client.executeLocally(NodesStatsInfoAction.instance, nodesStatsInfoRequest, new RestActions.NodesResponseRestListener<NodesStatsResponse>(restChannel){
            @Override
            public RestResponse buildResponse(NodesStatsResponse response, XContentBuilder builder) throws Exception {
                return super.buildResponse(response, builder);
            }
        });
    }

    /**
     * 根据自定义条件过滤出es性能统计指标，自定义条件限制最多10个
     *
     * @param request
     * @param nodesStatsInfoRequest
     */
    private void addAttrArea(RestRequest request, NodesStatsInfoRequest nodesStatsInfoRequest) {
        String attr = request.param("attr");
        if (attr == null || "".equals(attr)) {
            return;
        }
        Map<String, Object> attrMap = new HashMap<>(10);
        String[] attrAry = attr.split("\\,");
        for (int i = 0; i < attrAry.length; i++) {
            String[] kv = attrAry[i].split("\\|");
            List<String> values = new ArrayList<>();
            for (int j = 1; j < kv.length && kv.length > 1; j++) {
                values.add(kv[j]);
            }
            attrMap.put(kv[0], values);
        }
        nodesStatsInfoRequest.setAttr(attrMap);
    }

    private final Set<String> RESPONSE_PARAMS = Collections.singleton("level");

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}
