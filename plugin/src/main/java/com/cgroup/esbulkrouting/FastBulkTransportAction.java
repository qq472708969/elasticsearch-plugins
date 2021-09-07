package com.cgroup.esbulkrouting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.SparseFixedBitSet;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.IngestActionForwarder;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.*;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.*;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.index.*;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static com.cgroup.esbulkrouting.FastBulkAction.SETTING_ROUTING_SLOT;
import static java.util.Collections.emptyMap;

/**
 * Created by zzq on 2021/7/5.
 */
public class FastBulkTransportAction extends HandledTransportAction<FastBulkRequest, BulkResponse> {
    protected final org.apache.logging.log4j.Logger logger = LogManager.getLogger(this.getClass());
    private final AutoCreateIndex autoCreateIndex;
    private final ClusterService clusterService;
    private final IndicesService indexServices;
    private final IngestService ingestService;
    private final TransportShardBulkAction shardBulkAction;
    private final TransportCreateIndexAction createIndexAction;
    private final LongSupplier relativeTimeProvider;
    private final IngestActionForwarder ingestForwarder;
    private static final String DROPPED_ITEM_WITH_AUTO_GENERATED_ID = "auto-generated";

    @Inject
    public FastBulkTransportAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                   ClusterService clusterService, IndicesService indexServices, IngestService ingestService,
                                   TransportShardBulkAction shardBulkAction, TransportCreateIndexAction createIndexAction,
                                   ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                   AutoCreateIndex autoCreateIndex) {
        super(settings, FastBulkAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, FastBulkRequest::new);
        LongSupplier relativeTimeProvider = System::nanoTime;
        this.clusterService = clusterService;
        this.indexServices = indexServices;
        this.ingestService = ingestService;
        this.shardBulkAction = shardBulkAction;
        this.createIndexAction = createIndexAction;
        this.autoCreateIndex = autoCreateIndex;
        this.relativeTimeProvider = relativeTimeProvider;
        this.ingestForwarder = new IngestActionForwarder(transportService);
        clusterService.addStateApplier(this.ingestForwarder);
    }

    /**
     * Retrieves the {@link FastIndexRequest} from the provided {@link DocWriteRequest} for index or upsert actions.  Upserts are
     * modeled as {@link FastIndexRequest} inside the {@link UpdateRequest}. Ignores {@link org.elasticsearch.action.delete.DeleteRequest}'s
     *
     * @param docWriteRequest The request to find the {@link FastIndexRequest}
     * @return the found {@link FastIndexRequest} or {@code null} if one can not be found.
     */
    public static FastIndexRequest getIndexWriteRequest(DocWriteRequest docWriteRequest) {
        FastIndexRequest fastIndexRequest = null;
        if (docWriteRequest instanceof FastIndexRequest) {
            fastIndexRequest = (FastIndexRequest) docWriteRequest;
        } else if (docWriteRequest instanceof UpdateRequest) {
            FastUpdateRequest fastUpdateRequest = (FastUpdateRequest) docWriteRequest;
            fastIndexRequest = fastUpdateRequest.docAsUpsert() ? fastUpdateRequest.doc() : fastUpdateRequest.upsertRequest();
        }
        return fastIndexRequest;
    }

    @Override
    protected final void doExecute(final FastBulkRequest fastBulkRequest, final ActionListener<BulkResponse> listener) {
        throw new UnsupportedOperationException("task parameter is required for this operation");
    }

    @Override
    protected void doExecute(Task task, FastBulkRequest fastBulkRequest, ActionListener<BulkResponse> listener) {
        final long startTime = relativeTime();
        final AtomicArray<BulkItemResponse> responses = new AtomicArray<>(fastBulkRequest.requests().size());

        boolean hasIndexRequestsWithPipelines = false;
        final MetaData metaData = clusterService.state().getMetaData();
        ImmutableOpenMap<String, IndexMetaData> indicesMetaData = metaData.indices();
        for (DocWriteRequest<?> actionRequest : fastBulkRequest.requests()) {
            FastIndexRequest fastIndexRequest = getIndexWriteRequest(actionRequest);
            if (fastIndexRequest != null) {
                // get pipeline from request
                String pipeline = fastIndexRequest.getPipeline();
                if (pipeline == null) {
                    // start to look for default pipeline via settings found in the index meta data
                    IndexMetaData indexMetaData = indicesMetaData.get(actionRequest.index());
                    if (indexMetaData == null && fastIndexRequest.index() != null) {
                        // if the write request if through an alias use the write index's meta data
                        AliasOrIndex indexOrAlias = metaData.getAliasAndIndexLookup().get(fastIndexRequest.index());
                        if (indexOrAlias != null && indexOrAlias.isAlias()) {
                            AliasOrIndex.Alias alias = (AliasOrIndex.Alias) indexOrAlias;
                            indexMetaData = alias.getWriteIndex();
                        }
                    }
                    if (indexMetaData != null) {
                        // Find the the default pipeline if one is defined from and existing index.
                        String defaultPipeline = IndexSettings.DEFAULT_PIPELINE.get(indexMetaData.getSettings());
                        fastIndexRequest.setPipeline(defaultPipeline);
                        if (IngestService.NOOP_PIPELINE_NAME.equals(defaultPipeline) == false) {
                            hasIndexRequestsWithPipelines = true;
                        }
                    } else if (fastIndexRequest.index() != null) {
                        // No index exists yet (and is valid request), so matching index templates to look for a default pipeline
                        List<IndexTemplateMetaData> templates = MetaDataIndexTemplateService.findTemplates(metaData, fastIndexRequest.index());
                        assert (templates != null);
                        String defaultPipeline = IngestService.NOOP_PIPELINE_NAME;
                        // order of templates are highest order first, break if we find a default_pipeline
                        for (IndexTemplateMetaData template : templates) {
                            final Settings settings = template.settings();
                            if (IndexSettings.DEFAULT_PIPELINE.exists(settings)) {
                                defaultPipeline = IndexSettings.DEFAULT_PIPELINE.get(settings);
                                break;
                            }
                        }
                        fastIndexRequest.setPipeline(defaultPipeline);
                        if (IngestService.NOOP_PIPELINE_NAME.equals(defaultPipeline) == false) {
                            hasIndexRequestsWithPipelines = true;
                        }
                    }
                } else if (IngestService.NOOP_PIPELINE_NAME.equals(pipeline) == false) {
                    hasIndexRequestsWithPipelines = true;
                }
            }
        }

        if (hasIndexRequestsWithPipelines) {
            // this method (doExecute) will be called again, but with the bulk requests updated from the ingest node processing but
            // also with IngestService.NOOP_PIPELINE_NAME on each request. This ensures that this on the second time through this method,
            // this path is never taken.
            try {
                if (clusterService.localNode().isIngestNode()) {
                    processFastBulkIndexIngestRequest(task, fastBulkRequest, listener);
                } else {
                    ingestForwarder.forwardIngestRequest(BulkAction.INSTANCE, fastBulkRequest, listener);
                }
            } catch (Exception e) {
                listener.onFailure(e);
            }
            return;
        }

        if (needToCheck()) {
            // Attempt to create all the indices that we're going to need during the bulk before we start.
            // Step 1: collect all the indices in the request
            final Set<String> indices = fastBulkRequest.requests().stream()
                    // delete requests should not attempt to create the index (if the index does not
                    // exists), unless an external versioning is used
                    .filter(request -> request.opType() != DocWriteRequest.OpType.DELETE
                            || request.versionType() == VersionType.EXTERNAL
                            || request.versionType() == VersionType.EXTERNAL_GTE)
                    .map(DocWriteRequest::index)
                    .collect(Collectors.toSet());
            /* Step 2: filter that to indices that don't exist and we can create. At the same time build a map of indices we can't create
             * that we'll use when we try to run the requests. */
            final Map<String, IndexNotFoundException> indicesThatCannotBeCreated = new HashMap<>();
            Set<String> autoCreateIndices = new HashSet<>();
            ClusterState state = clusterService.state();
            for (String index : indices) {
                boolean shouldAutoCreate;
                try {
                    shouldAutoCreate = shouldAutoCreate(index, state);
                } catch (IndexNotFoundException e) {
                    shouldAutoCreate = false;
                    indicesThatCannotBeCreated.put(index, e);
                }
                if (shouldAutoCreate) {
                    autoCreateIndices.add(index);
                }
            }
            // Step 3: create all the indices that are missing, if there are any missing. start the bulk after all the creates come back.
            if (autoCreateIndices.isEmpty()) {
                executeBulk(task, fastBulkRequest, startTime, listener, responses, indicesThatCannotBeCreated);
            } else {
                final AtomicInteger counter = new AtomicInteger(autoCreateIndices.size());
                for (String index : autoCreateIndices) {
                    createIndex(index, fastBulkRequest.timeout(), new ActionListener<CreateIndexResponse>() {
                        @Override
                        public void onResponse(CreateIndexResponse result) {
                            if (counter.decrementAndGet() == 0) {
                                executeBulk(task, fastBulkRequest, startTime, listener, responses, indicesThatCannotBeCreated);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            if (!(ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException)) {
                                // fail all requests involving this index, if create didn't work
                                for (int i = 0; i < fastBulkRequest.requests().size(); i++) {
                                    DocWriteRequest request = fastBulkRequest.requests().get(i);
                                    if (request != null && setResponseFailureIfIndexMatches(responses, i, request, index, e)) {
                                        fastBulkRequest.requests().set(i, null);
                                    }
                                }
                            }
                            if (counter.decrementAndGet() == 0) {
                                executeBulk(task, fastBulkRequest, startTime, ActionListener.wrap(listener::onResponse, inner -> {
                                    inner.addSuppressed(e);
                                    listener.onFailure(inner);
                                }), responses, indicesThatCannotBeCreated);
                            }
                        }
                    });
                }
            }
        } else {
            executeBulk(task, fastBulkRequest, startTime, listener, responses, emptyMap());
        }
    }

    boolean needToCheck() {
        return autoCreateIndex.needToCheck();
    }

    boolean shouldAutoCreate(String index, ClusterState state) {
        return autoCreateIndex.shouldAutoCreate(index, state);
    }

    void createIndex(String index, TimeValue timeout, ActionListener<CreateIndexResponse> listener) {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest();
        createIndexRequest.index(index);
        createIndexRequest.cause("auto(fast_bulk api)");
        createIndexRequest.masterNodeTimeout(timeout);
        createIndexAction.execute(createIndexRequest, listener);
    }

    private boolean setResponseFailureIfIndexMatches(AtomicArray<BulkItemResponse> responses, int idx, DocWriteRequest request,
                                                     String index, Exception e) {
        if (index.equals(request.index())) {
            responses.set(idx, new BulkItemResponse(idx, request.opType(), new BulkItemResponse.Failure(request.index(), request.type(),
                    request.id(), e)));
            return true;
        }
        return false;
    }

    private long buildTookInMillis(long startTimeNanos) {
        return TimeUnit.NANOSECONDS.toMillis(relativeTime() - startTimeNanos);
    }

    /**
     * retries on retryable cluster blocks, resolves item requests,
     * constructs shard bulk requests and delegates execution to shard bulk action
     */
    private final class FastBulkOperation extends AbstractRunnable {
        private final Task task;
        private final FastBulkRequest fastBulkRequest;
        private final ActionListener<BulkResponse> listener;
        private final AtomicArray<BulkItemResponse> responses;
        private final long startTimeNanos;
        private final ClusterStateObserver observer;
        private final Map<String, IndexNotFoundException> indicesThatCannotBeCreated;
        private int shardNo;
        //槽位个数
        private int slotCount;
        //每个槽位对应的容量（shard个数）
        private int slotSize;
        private int SLOT_COUNT_DEFAULT = -1;

        FastBulkOperation(Task task, FastBulkRequest fastBulkRequest, ActionListener<BulkResponse> listener, AtomicArray<BulkItemResponse> responses,
                          long startTimeNanos, Map<String, IndexNotFoundException> indicesThatCannotBeCreated) {
            this.task = task;
            this.fastBulkRequest = fastBulkRequest;
            this.listener = listener;
            this.responses = responses;
            this.startTimeNanos = startTimeNanos;
            this.indicesThatCannotBeCreated = indicesThatCannotBeCreated;
            this.observer = new ClusterStateObserver(clusterService, fastBulkRequest.timeout(), logger, threadPool.getThreadContext());
            buildBasicInfo(fastBulkRequest);
        }

        @Override
        protected void doRun() throws Exception {
            long doRunStartMills = System.currentTimeMillis();
            final ClusterState clusterState = observer.setAndGetObservedState();
            if (handleBlockExceptions(clusterState)) {
                return;
            }
            final ConcreteIndices concreteIndices = new ConcreteIndices(clusterState, indexNameExpressionResolver);
            MetaData metaData = clusterState.metaData();
            for (int i = 0; i < fastBulkRequest.requests().size(); i++) {
                DocWriteRequest docWriteRequest = fastBulkRequest.requests().get(i);
                //the request can only be null because we set it to null in the previous step, so it gets ignored
                if (docWriteRequest == null) {
                    continue;
                }
                if (addFailureIfIndexIsUnavailable(docWriteRequest, i, concreteIndices, metaData)) {
                    continue;
                }
                Index concreteIndex = concreteIndices.resolveIfAbsent(docWriteRequest);
                try {
                    switch (docWriteRequest.opType()) {
                        case CREATE:
                        case INDEX:
                            FastIndexRequest fastIndexRequest = (FastIndexRequest) docWriteRequest;
                            final IndexMetaData indexMetaData = metaData.index(concreteIndex);
                            MappingMetaData mappingMd = indexMetaData.mappingOrDefault(
                                    indexMetaData.resolveDocumentType(fastIndexRequest.type()));
                            Version indexCreated = indexMetaData.getCreationVersion();
                            fastIndexRequest.resolveRouting(metaData);
                            fastIndexRequest.process(indexCreated, mappingMd, concreteIndex.getName());
                            break;
                        case UPDATE:
                            FastUpdateRequest fastUpdateRequest = (FastUpdateRequest) docWriteRequest;
                            fastUpdateRequest.routing((metaData.resolveWriteIndexRouting(fastUpdateRequest.parent(), fastUpdateRequest.routing(), fastUpdateRequest.index())));
                            // Fail fast on the node that received the request, rather than failing when translating on the index or delete request.
                            if (fastUpdateRequest.routing() == null && metaData.routingRequired(concreteIndex.getName(), fastUpdateRequest.type())) {
                                throw new RoutingMissingException(concreteIndex.getName(), fastUpdateRequest.type(), fastUpdateRequest.id());
                            }
                            break;
                        case DELETE:
                            docWriteRequest.routing(metaData.resolveWriteIndexRouting(docWriteRequest.parent(), docWriteRequest.routing(),
                                    docWriteRequest.index()));
                            // check if routing is required, if so, throw error if routing wasn't specified
                            if (docWriteRequest.routing() == null && metaData.routingRequired(concreteIndex.getName(),
                                    docWriteRequest.type())) {
                                throw new RoutingMissingException(concreteIndex.getName(), docWriteRequest.type(), docWriteRequest.id());
                            }
                            break;
                        default:
                            throw new AssertionError("request type not supported: [" + docWriteRequest.opType() + "]");
                    }
                } catch (ElasticsearchParseException | IllegalArgumentException | RoutingMissingException e) {
                    BulkItemResponse.Failure failure = new BulkItemResponse.Failure(concreteIndex.getName(), docWriteRequest.type(),
                            docWriteRequest.id(), e);
                    BulkItemResponse bulkItemResponse = new BulkItemResponse(i, docWriteRequest.opType(), failure);
                    responses.set(i, bulkItemResponse);
                    // make sure the request gets never processed again
                    fastBulkRequest.requests().set(i, null);
                }
            }

            // first, go over all the requests and create a ShardId -> Operations mapping
            Map<ShardId, List<BulkItemRequest>> requestsByShard = new HashMap<>();
            final AtomicInteger itemSize = new AtomicInteger(0);
            for (int i = 0; i < fastBulkRequest.requests().size(); i++) {
                FastDocWriteRequest request = (FastDocWriteRequest) fastBulkRequest.requests().get(i);
                if (request == null) {
                    continue;
                }
                String requestShardNo = request.shardNo();
                ShardId shardId;
                String concreteIndex = concreteIndices.getConcreteIndex(request.index()).getName();
                if (requestShardNo == null) {
                    RoutingTable routingTable = clusterState.getRoutingTable();
                    /**
                     * 如果没有配置routing_slot则直接使用外部传入的es实际物理shard_no编号
                     */
                    if (SLOT_COUNT_DEFAULT == slotCount) {
                        shardId = routingTable.shardRoutingTable(concreteIndex, shardNo).shardId();
                    } else {
                        /**
                         * 该方法用来计算依据逻辑slot，映射到的物理shard
                         */
                        shardId = getShardId(routingTable, concreteIndex, slotCount, slotSize, request.routing(), request.id());
                    }
                } else {
                    /**
                     * url中指定了shard_no参数，则始终向指定的shard分配数据
                     */
                    shardId = clusterState.getRoutingTable().shardRoutingTable(concreteIndex, Integer.valueOf(requestShardNo)).shardId();
                }
                List<BulkItemRequest> shardRequests = requestsByShard.computeIfAbsent(shardId, shard -> new ArrayList<>());
                IndexRequest indexRequest = newIndexRequest(request);
                shardRequests.add(new BulkItemRequest(i, indexRequest));
                itemSize.incrementAndGet();
            }

            if (requestsByShard.isEmpty()) {
                listener.onResponse(new BulkResponse(responses.toArray(new BulkItemResponse[responses.length()]),
                        buildTookInMillis(startTimeNanos)));
                return;
            }

            final AtomicInteger counter = new AtomicInteger(requestsByShard.size());
            String nodeId = clusterService.localNode().getId();
            for (Map.Entry<ShardId, List<BulkItemRequest>> entry : requestsByShard.entrySet()) {
                final ShardId shardId = entry.getKey();
                final List<BulkItemRequest> requests = entry.getValue();
                BulkShardRequest bulkShardRequest = new BulkShardRequest(shardId, fastBulkRequest.getRefreshPolicy(),
                        requests.toArray(new BulkItemRequest[requests.size()]));
                bulkShardRequest.waitForActiveShards(fastBulkRequest.waitForActiveShards());
                bulkShardRequest.timeout(fastBulkRequest.timeout());
                if (task != null) {
                    bulkShardRequest.setParentTask(nodeId, task.getId());
                }
                long shardRequestStartMills = System.currentTimeMillis();
                shardBulkAction.execute(bulkShardRequest, new ActionListener<BulkShardResponse>() {
                    Long maxExecuteMills = 0L;
                    Long sumExecuteMills = 0L;

                    @Override
                    public void onResponse(BulkShardResponse bulkShardResponse) {
                        //每个单独的ShardRequest返回时执行
                        for (BulkItemResponse bulkItemResponse : bulkShardResponse.getResponses()) {
                            // we may have no response if item failed
                            if (bulkItemResponse.getResponse() != null) {
                                bulkItemResponse.getResponse().setShardInfo(bulkShardResponse.getShardInfo());
                            }
                            responses.set(bulkItemResponse.getItemId(), bulkItemResponse);
                        }
                        long shardRequestExecuteMills = System.currentTimeMillis() - shardRequestStartMills;
                        sumExecuteMills += shardRequestExecuteMills;
                        if (shardRequestExecuteMills > maxExecuteMills) {
                            maxExecuteMills = shardRequestExecuteMills;
                        }

                        if (counter.decrementAndGet() == 0) {
                            finishHim();
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // create failures for all relevant requests
                        for (BulkItemRequest request : requests) {
                            final String indexName = concreteIndices.getConcreteIndex(request.index()).getName();
                            DocWriteRequest docWriteRequest = request.request();
                            responses.set(request.id(), new BulkItemResponse(request.id(), docWriteRequest.opType(),
                                    new BulkItemResponse.Failure(indexName, docWriteRequest.type(), docWriteRequest.id(), e)));
                        }
                        if (counter.decrementAndGet() == 0) {
                            finishHim();
                        }
                    }

                    /**
                     * 当最后一个ShardRequest返回时回调
                     */
                    private void finishHim() {
                        logger.info("fast_bulk_execute|itemSize={}|bulkShardRequestSize={}|totalMills={}|avgShardRequestMills={}|maxShardRequestMills={}"
                                , itemSize.get(), requestsByShard.size(), (System.currentTimeMillis() - doRunStartMills)
                                , (sumExecuteMills / requestsByShard.size()), maxExecuteMills);
                        listener.onResponse(new BulkResponse(responses.toArray(new BulkItemResponse[responses.length()]),
                                buildTookInMillis(startTimeNanos)));
                    }
                });
            }
        }

        private void buildBasicInfo(FastBulkRequest fastBulkRequest) {
            /**
             * doc中已经禁用了这个参数，如果url中指定了shard_no，则写入数据列表中每条数据的shardNo编号是同一个；
             */
            FastIndexRequest fastIndexRequest = (FastIndexRequest) fastBulkRequest.requests().get(0);
            String requestShardNo = fastIndexRequest.shardNo();
            String indexName = fastIndexRequest.index();
            IndexMetaData esIndexMetaData = clusterService.state().metaData().index(indexName);
            int routingNumShards = esIndexMetaData.getRoutingNumShards();
            if (requestShardNo == null) {
                /**
                 * shard_no不存在
                 */
                this.slotCount = esIndexMetaData.getSettings().getAsInt(SETTING_ROUTING_SLOT, SLOT_COUNT_DEFAULT);
                if (SLOT_COUNT_DEFAULT == slotCount) {
                    /**
                     * shard_no不存在，并且index中没有配置routing_slot，则始终执行向doc数量少的shard写入数据，该批量操作为同一shard
                     */
                    littleHighPriority(indexName, esIndexMetaData, routingNumShards);
                } else {
                    this.slotSize = routingNumShards / slotCount;
                    logger.info("=====索引设置了slot数量:{}", slotCount);
                }
            } else {
                /**
                 * 只要url中指定了shard_no编号参数，则本次批量写入操作均指向该shard
                 */
                logger.info("=====使用外部参数指定的物理shard编号:{}", requestShardNo);
            }
        }

        private void littleHighPriority(String indexName, IndexMetaData esIndexMetaData, int routingNumShards) {
            String indexUUID = esIndexMetaData.getIndexUUID();
            IndexService indexShards = indexServices.indexServiceSafe(new Index(indexName, indexUUID));
            logger.info("=====routingNumShards:{}", routingNumShards);
            long minCount = Long.MAX_VALUE;
            for (int i = 0; i < routingNumShards; i++) {
                IndexShard shard = indexShards.getShardOrNull(i);
                /**
                 * 集群环境下shard分布在不同机器上，很可有能某个索引获取为null
                 */
                if (shard == null) {
                    continue;
                }
                long count = shard.docStats().getCount();
                if (count < minCount) {
                    minCount = count;
                    shardNo = i;
                }
            }
            logger.info("=====fast_bulk自动计算shard编号:{}", shardNo);
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }

        /**
         * 计算逻辑slot对应的物理shard
         *
         * @param routingTable
         * @param indexName
         * @param slotSize     每个槽位对应物理shard个数
         * @param routing      doc原生_routing属性
         * @param id           doc的原生_id属性
         * @return
         */
        private ShardId getShardId(RoutingTable routingTable, String indexName, int slotCount, int slotSize, String routing, String id) throws NoSuchAlgorithmException {
            String effectiveRouting;
            if (routing == null) {
                effectiveRouting = id;
            } else {
                effectiveRouting = routing;
            }
            int hashVal = Murmur3HashFunction.hash(effectiveRouting);

            /**
             * 获取槽儿位置
             */
            int slotNo = Math.floorMod(hashVal, slotCount);
            int shardNo = Math.floorMod(getRandomNo(), slotSize) + slotNo * slotSize;
            logger.info("=====effectiveRouting:{}  slotNo:{}  shardNo:{}", effectiveRouting, slotNo, shardNo);
            return routingTable.shardRoutingTable(indexName, shardNo).shardId();
        }

        private int getRandomNo() throws NoSuchAlgorithmException {
            return SecureRandom.getInstanceStrong().nextInt(100);
        }

        private IndexRequest newIndexRequest(FastDocWriteRequest request) throws java.io.IOException {
            BytesStreamOutput bytesStreamOutput = new BytesStreamOutput();
            request.writeToStream(bytesStreamOutput);
            BytesReference bytes = bytesStreamOutput.bytes();
            StreamInput streamInput = bytes.streamInput();
            IndexRequest indexRequest = new IndexRequest();
            indexRequest.readFrom(streamInput);
            return indexRequest;
        }

        private boolean handleBlockExceptions(ClusterState state) {
            ClusterBlockException blockException = state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
            if (blockException != null) {
                if (blockException.retryable()) {
                    logger.trace("cluster is blocked, scheduling a retry", blockException);
                    retry(blockException);
                } else {
                    onFailure(blockException);
                }
                return true;
            }
            return false;
        }

        void retry(Exception failure) {
            assert failure != null;
            if (observer.isTimedOut()) {
                // we running as a last attempt after a timeout has happened. don't retry
                onFailure(failure);
                return;
            }
            observer.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    run();
                }

                @Override
                public void onClusterServiceClose() {
                    onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    // Try one more time...
                    run();
                }
            });
        }

        private boolean addFailureIfIndexIsUnavailable(DocWriteRequest request, int idx,
                                                       final ConcreteIndices concreteIndices,
                                                       final MetaData metaData) {
            IndexNotFoundException cannotCreate = indicesThatCannotBeCreated.get(request.index());
            if (cannotCreate != null) {
                addFailure(request, idx, cannotCreate);
                return true;
            }
            Index concreteIndex = concreteIndices.getConcreteIndex(request.index());
            if (concreteIndex == null) {
                try {
                    concreteIndex = concreteIndices.resolveIfAbsent(request);
                } catch (IndexClosedException | IndexNotFoundException ex) {
                    addFailure(request, idx, ex);
                    return true;
                }
            }
            IndexMetaData indexMetaData = metaData.getIndexSafe(concreteIndex);
            if (indexMetaData.getState() == IndexMetaData.State.CLOSE) {
                addFailure(request, idx, new IndexClosedException(concreteIndex));
                return true;
            }
            return false;
        }

        private void addFailure(DocWriteRequest request, int idx, Exception unavailableException) {
            BulkItemResponse.Failure failure = new BulkItemResponse.Failure(request.index(), request.type(), request.id(),
                    unavailableException);
            BulkItemResponse bulkItemResponse = new BulkItemResponse(idx, request.opType(), failure);
            responses.set(idx, bulkItemResponse);
            // make sure the request gets never processed again
            fastBulkRequest.requests().set(idx, null);
        }
    }

    void executeBulk(Task task, final FastBulkRequest fastBulkRequest, final long startTimeNanos, final ActionListener<BulkResponse> listener,
                     final AtomicArray<BulkItemResponse> responses, Map<String, IndexNotFoundException> indicesThatCannotBeCreated) {
        new FastBulkOperation(task, fastBulkRequest, listener, responses, startTimeNanos, indicesThatCannotBeCreated).run();
    }

    private static class ConcreteIndices {
        private final ClusterState state;
        private final IndexNameExpressionResolver indexNameExpressionResolver;
        private final Map<String, Index> indices = new HashMap<>();

        ConcreteIndices(ClusterState state, IndexNameExpressionResolver indexNameExpressionResolver) {
            this.state = state;
            this.indexNameExpressionResolver = indexNameExpressionResolver;
        }

        Index getConcreteIndex(String indexOrAlias) {
            return indices.get(indexOrAlias);
        }

        Index resolveIfAbsent(DocWriteRequest request) {
            Index concreteIndex = indices.get(request.index());
            if (concreteIndex == null) {
                concreteIndex = indexNameExpressionResolver.concreteWriteIndex(state, request);
                indices.put(request.index(), concreteIndex);
            }
            return concreteIndex;
        }
    }

    private long relativeTime() {
        return relativeTimeProvider.getAsLong();
    }

    void processFastBulkIndexIngestRequest(Task task, FastBulkRequest original, ActionListener<BulkResponse> listener) {
        long ingestStartTimeInNanos = System.nanoTime();
        FastBulkRequestModifier fastBulkRequestModifier = new FastBulkRequestModifier(original);
        ingestService.executeBulkRequest(() -> fastBulkRequestModifier,
                (indexRequest, exception) -> {
                    logger.debug(() -> new ParameterizedMessage("failed to execute pipeline [{}] for document [{}/{}/{}]",
                            indexRequest.getPipeline(), indexRequest.index(), indexRequest.type(), indexRequest.id()), exception);
                    fastBulkRequestModifier.markCurrentItemAsFailed(exception);
                }, (exception) -> {
                    if (exception != null) {
                        logger.error("failed to execute pipeline for a bulk request", exception);
                        listener.onFailure(exception);
                    } else {
                        long ingestTookInMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - ingestStartTimeInNanos);
                        FastBulkRequest fastBulkRequest = fastBulkRequestModifier.getFastBulkRequest();
                        ActionListener<BulkResponse> actionListener = fastBulkRequestModifier.wrapActionListenerIfNeeded(ingestTookInMillis,
                                listener);
                        if (fastBulkRequest.requests().isEmpty()) {
                            // at this stage, the transport bulk action can't deal with a bulk request with no requests,
                            // so we stop and send an empty response back to the client.
                            // (this will happen if pre-processing all items in the bulk failed)
                            actionListener.onResponse(new BulkResponse(new BulkItemResponse[0], 0));
                        } else {
                            doExecute(task, fastBulkRequest, actionListener);
                        }
                    }
                },
                indexRequest -> fastBulkRequestModifier.markCurrentItemAsDropped());
    }

    static final class FastBulkRequestModifier implements Iterator<DocWriteRequest<?>> {

        final FastBulkRequest fastBulkRequest;
        final SparseFixedBitSet failedSlots;
        final List<BulkItemResponse> itemResponses;

        int currentSlot = -1;
        int[] originalSlots;

        FastBulkRequestModifier(FastBulkRequest fastBulkRequest) {
            this.fastBulkRequest = fastBulkRequest;
            this.failedSlots = new SparseFixedBitSet(fastBulkRequest.requests().size());
            this.itemResponses = new ArrayList<>(fastBulkRequest.requests().size());
        }

        @Override
        public DocWriteRequest next() {
            return fastBulkRequest.requests().get(++currentSlot);
        }

        @Override
        public boolean hasNext() {
            return (currentSlot + 1) < fastBulkRequest.requests().size();
        }

        FastBulkRequest getFastBulkRequest() {
            if (itemResponses.isEmpty()) {
                return fastBulkRequest;
            } else {
                FastBulkRequest fastBulkRequest = new FastBulkRequest();
                fastBulkRequest.setRefreshPolicy(this.fastBulkRequest.getRefreshPolicy());
                fastBulkRequest.waitForActiveShards(this.fastBulkRequest.waitForActiveShards());
                fastBulkRequest.timeout(this.fastBulkRequest.timeout());

                int slot = 0;
                List<DocWriteRequest<?>> requests = this.fastBulkRequest.requests();
                originalSlots = new int[requests.size()]; // oversize, but that's ok
                for (int i = 0; i < requests.size(); i++) {
                    DocWriteRequest request = requests.get(i);
                    if (failedSlots.get(i) == false) {
                        fastBulkRequest.add(request);
                        originalSlots[slot++] = i;
                    }
                }
                return fastBulkRequest;
            }
        }

        ActionListener<BulkResponse> wrapActionListenerIfNeeded(long ingestTookInMillis, ActionListener<BulkResponse> actionListener) {
            if (itemResponses.isEmpty()) {
                return ActionListener.wrap(
                        response -> actionListener.onResponse(new BulkResponse(response.getItems(),
                                response.getTook().getMillis(), ingestTookInMillis)),
                        actionListener::onFailure);
            } else {
                return new IngestFastBulkResponseListener(ingestTookInMillis, originalSlots, itemResponses, actionListener);
            }
        }

        void markCurrentItemAsDropped() {
            FastIndexRequest fastIndexRequest = getIndexWriteRequest(fastBulkRequest.requests().get(currentSlot));
            failedSlots.set(currentSlot);
            final String id = fastIndexRequest.id() == null ? DROPPED_ITEM_WITH_AUTO_GENERATED_ID : fastIndexRequest.id();
            itemResponses.add(
                    new BulkItemResponse(currentSlot, fastIndexRequest.opType(),
                            new UpdateResponse(
                                    new ShardId(fastIndexRequest.index(), IndexMetaData.INDEX_UUID_NA_VALUE, 0),
                                    fastIndexRequest.type(), id, fastIndexRequest.version(), DocWriteResponse.Result.NOOP
                            )
                    )
            );
        }

        void markCurrentItemAsFailed(Exception e) {
            FastIndexRequest fastIndexRequest = getIndexWriteRequest(fastBulkRequest.requests().get(currentSlot));
            // We hit a error during preprocessing a request, so we:
            // 1) Remember the request item slot from the bulk, so that we're done processing all requests we know what failed
            // 2) Add a bulk item failure for this request
            // 3) Continue with the next request in the bulk.
            failedSlots.set(currentSlot);
            BulkItemResponse.Failure failure = new BulkItemResponse.Failure(fastIndexRequest.index(), fastIndexRequest.type(),
                    fastIndexRequest.id(), e);
            itemResponses.add(new BulkItemResponse(currentSlot, fastIndexRequest.opType(), failure));
        }

    }

    static final class IngestFastBulkResponseListener implements ActionListener<BulkResponse> {

        private final long ingestTookInMillis;
        private final int[] originalSlots;
        private final List<BulkItemResponse> itemResponses;
        private final ActionListener<BulkResponse> actionListener;

        IngestFastBulkResponseListener(long ingestTookInMillis, int[] originalSlots, List<BulkItemResponse> itemResponses,
                                       ActionListener<BulkResponse> actionListener) {
            this.ingestTookInMillis = ingestTookInMillis;
            this.itemResponses = itemResponses;
            this.actionListener = actionListener;
            this.originalSlots = originalSlots;
        }

        @Override
        public void onResponse(BulkResponse response) {
            BulkItemResponse[] items = response.getItems();
            for (int i = 0; i < items.length; i++) {
                itemResponses.add(originalSlots[i], response.getItems()[i]);
            }
            actionListener.onResponse(new BulkResponse(
                    itemResponses.toArray(new BulkItemResponse[itemResponses.size()]),
                    response.getTook().getMillis(), ingestTookInMillis));
        }

        @Override
        public void onFailure(Exception e) {
            actionListener.onFailure(e);
        }
    }
}
