package com.cgroup.esupdatesegment;

import jdk.nashorn.internal.runtime.logging.Logger;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.RestBuilderListener;

import java.io.IOException;

/**
 * Created by zzq on 2021/6/11.
 */
@Logger
public class LoadSegmentRestHandler extends BaseRestHandler {
    protected final org.apache.logging.log4j.Logger logger = LogManager.getLogger(this.getClass());
    protected LoadSegmentRestHandler(Settings settings, RestController restController) {
        super(settings);
        restController.registerHandler(RestRequest.Method.POST, Constants.web_url, this);
        restController.registerHandler(RestRequest.Method.GET, Constants.web_url, this);
    }

    @Override
    public String getName() {
        return Constants.plugin_rest_name;
    }

    /**
     * rest请求处理器
     *
     * @param request
     * @param client
     * @return
     * @throws IOException
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        LoadSegmentActionRequest loadSegmentActionRequest = new LoadSegmentActionRequest();
        logger.info("prepareRequest执行了");
        loadSegmentActionRequest.indexName = request.param("indexName");
        loadSegmentActionRequest.indexUUID = request.param("indexUUID");
        loadSegmentActionRequest.shardIdNo = Integer.valueOf(request.param("shardIdNo"));
        loadSegmentActionRequest.segmentDirs = request.param("segmentDirs");
        loadSegmentActionRequest.documentPrimeKey = request.param("documentPrimeKey");

        /**
         * 对应具体的TransportAction
         */
        return restChannel -> client.executeLocally(LoadSegmentAction.instance, loadSegmentActionRequest, new RestBuilderListener<LoadSegmentActionResponse>(restChannel) {
            @Override
            public RestResponse buildResponse(LoadSegmentActionResponse loadSegmentActionResponse, XContentBuilder builder) throws Exception {
                return new BytesRestResponse(RestStatus.OK, loadSegmentActionResponse.toXContent(builder, ToXContent.EMPTY_PARAMS));
            }
        });
    }
}
