package com.cgroup.esbulkrouting;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * Created by zzq on 2021/7/5.
 */
public class FastBulkRestHandler extends BaseRestHandler {
    protected final org.apache.logging.log4j.Logger logger = LogManager.getLogger(this.getClass());

    private final boolean allowExplicitIndex;

    public FastBulkRestHandler(Settings settings, RestController controller) {
        super(settings);

        /**
         * 仅仅支持url指定索引的bulk写入
         */
        controller.registerHandler(POST, "/{index}/_fast_bulk", this);
        controller.registerHandler(PUT, "/{index}/_fast_bulk", this);
        controller.registerHandler(POST, "/{index}/{type}/_fast_bulk", this);
        controller.registerHandler(PUT, "/{index}/{type}/_fast_bulk", this);

        //es请求body中的索引名称是否可以覆盖url参数中的索引名称
        this.allowExplicitIndex = MULTI_ALLOW_EXPLICIT_INDEX.get(settings);
    }

    @Override
    public String getName() {
        return "fast_bulk_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        paramsCheck(request);
        String defaultRouting = request.param("routing");
        String defaultShardNo = request.param("shard_no");

        FastBulkRequest fastBulkRequest = new FastBulkRequest();
        String defaultIndex = request.param("index");
        String defaultType = request.param("type");
        FetchSourceContext defaultFetchSourceContext = FetchSourceContext.parseFromRestRequest(request);
        String fieldsParam = request.param("fields");
        if (fieldsParam != null) {
            logger.info("Deprecated field [fields] used, expected [_source] instead");
        }
        String[] defaultFields = fieldsParam != null ? Strings.commaDelimitedListToStringArray(fieldsParam) : null;
        String defaultPipeline = request.param("pipeline");
        String waitForActiveShards = request.param("wait_for_active_shards");
        if (waitForActiveShards != null) {
            fastBulkRequest.waitForActiveShards(ActiveShardCount.parseString(waitForActiveShards));
        }
        fastBulkRequest.timeout(request.paramAsTime("timeout", BulkShardRequest.DEFAULT_TIMEOUT));
        fastBulkRequest.setRefreshPolicy(request.param("refresh"));
        fastBulkRequest.add(request.requiredContent(), defaultIndex, defaultType, defaultRouting, defaultShardNo, defaultFields,
                defaultFetchSourceContext, defaultPipeline, null, allowExplicitIndex, request.getXContentType());

        return restChannel -> client.executeLocally(FastBulkAction.INSTANCE, fastBulkRequest, new RestBuilderListener<BulkResponse>(restChannel) {
            @Override
            public RestResponse buildResponse(BulkResponse responses, XContentBuilder builder) throws Exception {
                return new BytesRestResponse(RestStatus.OK, responses.toXContent(builder, ToXContent.EMPTY_PARAMS));
            }
        });
    }

    private void paramsCheck(RestRequest request) {
        String shardNo = request.param("shard_no");
        if (shardNo != null) {
            Integer.valueOf(shardNo);
        }
    }

    @Override
    public boolean supportsContentStream() {
        return true;
    }
}
