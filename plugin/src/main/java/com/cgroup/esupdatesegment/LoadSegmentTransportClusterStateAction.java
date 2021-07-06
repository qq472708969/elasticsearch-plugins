package com.cgroup.esupdatesegment;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.info.TransportClusterInfoAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.function.Supplier;

/**
 * Created by zzq on 2021/6/20/020.
 * <p>
 * 负责将制定目录的lucene文件加载到制定的shard中，但索引的indexUUID是从集群状态中获取的，不用外部传入；使用TransportClusterInfoAction可直接获取集群状态
 */
public class LoadSegmentTransportClusterStateAction extends TransportClusterInfoAction<LoadSegmentActionClusterInfoRequest, LoadSegmentActionResponse> {
    protected final org.apache.logging.log4j.Logger logger = LogManager.getLogger(this.getClass());
    private ThreadPool threadPool;
    private IndicesService indicesService;
    private LoadSegmentExecutor executor = new LoadSegmentExecutor();

    @Inject
    public LoadSegmentTransportClusterStateAction(Settings settings, String actionName, TransportService transportService,
                                                  ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters,
                                                  IndexNameExpressionResolver indexNameExpressionResolver, Supplier<LoadSegmentActionClusterInfoRequest> request,
                                                  IndicesService indexServices) {
        super(settings, actionName, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, request);
        this.threadPool = threadPool;
        this.indicesService = indexServices;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected LoadSegmentActionResponse newResponse() {
        return new LoadSegmentActionResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(LoadSegmentActionClusterInfoRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ,
                indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    @Override
    protected void doMasterOperation(LoadSegmentActionClusterInfoRequest request, String[] concreteIndices, ClusterState state, ActionListener<LoadSegmentActionResponse> listener) {
//        logger.info("==================ThreadPool-GENERIC-doExecute===================");
//        String indexUUID = state.metaData().index(request.indexName).getIndexUUID();
//        logger.info("==================indexName:{},indexUUID:{}===================", request.indexName, indexUUID);
//        try {
//            long count = executor.commitSegment(request.indexName, indexUUID, request.shardIdNo, request.documentPrimeKey,
//                    request.getSegmentDirs(), indicesService);
//            LoadSegmentActionResponse response = new LoadSegmentActionResponse();
//            response.removeDocumentCount = count;
//            listener.onResponse(response);
//        } catch (Exception e) {
//            listener.onFailure(e);
//        }
    }
}
