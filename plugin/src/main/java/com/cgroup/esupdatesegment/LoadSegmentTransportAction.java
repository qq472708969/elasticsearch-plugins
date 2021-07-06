package com.cgroup.esupdatesegment;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Created by zzq on 2021/6/11.
 * <p>
 * 负责将制定目录的lucene文件加载到制定的shard中
 */
public class LoadSegmentTransportAction extends TransportAction<LoadSegmentActionRequest, LoadSegmentActionResponse> {
    protected final org.apache.logging.log4j.Logger logger = LogManager.getLogger(this.getClass());
    private ThreadPool threadPool;
    private IndicesService indicesService;
    private ClusterService clusterService;
    private LoadSegmentExecutor executor = new LoadSegmentExecutor();

    @Inject
    public LoadSegmentTransportAction(Settings settings,
                                      ThreadPool threadPool,
                                      ActionFilters actionFilters,
                                      IndexNameExpressionResolver indexNameExpressionResolver,
                                      TransportService transportService,
                                      IndicesService indexServices,
                                      ClusterService clusterService) {

        super(settings, Constants.load_segment_name, threadPool, actionFilters, indexNameExpressionResolver, transportService.getTaskManager());
        this.threadPool = threadPool;
        this.indicesService = indexServices;
        this.clusterService = clusterService;
    }

    /**
     * 使用es的基础线程池执行，segment加载任务
     *
     * @param request
     * @param listener
     */
    @Override
    protected void doExecute(LoadSegmentActionRequest request, ActionListener<LoadSegmentActionResponse> listener) {
        threadPool.executor(ThreadPool.Names.GENERIC).submit(() -> {
            logger.info("==================ThreadPool-GENERIC-doExecute===================");
            String indexUUID = clusterService.state().metaData().index(request.indexName).getIndexUUID();
            logger.info("==================indexName:{},indexUUID:{}===================", request.indexName, indexUUID);
            try {
                long count = executor.commitSegment(request.indexName, indexUUID, request.shardIdNo, request.documentPrimeKey,
                        request.getSegmentDirs(), indicesService);
                LoadSegmentActionResponse response = new LoadSegmentActionResponse();
                response.removeDocumentCount = count;
                listener.onResponse(response);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
