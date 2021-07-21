package com.cgroup.essearch;

import com.cgroup.essearch.base.*;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.LongSupplier;

import static com.cgroup.esbulkrouting.FastBulkAction.SETTING_ROUTING_SLOT;
import static org.elasticsearch.action.search.SearchType.QUERY_THEN_FETCH;

/**
 * Created by zzq on 2021/7/20.
 */
public class SlotSearchTransportAction extends HandledTransportAction<SlotSearchRequest, SlotSearchResponse> {
    /**
     * The maximum number of shards for a single search request.
     */
    public static final Setting<Long> SHARD_COUNT_LIMIT_SETTING = Setting.longSetting(
            "action.search.shard_count.limit", Long.MAX_VALUE, 1L, Setting.Property.Dynamic, Setting.Property.NodeScope);

    private final ClusterService clusterService;
    private final SlotSearchTransportService searchTransportService;
    private final RemoteClusterService remoteClusterService;
    private final SlotSearchPhaseController searchPhaseController;
    private final SearchService searchService;
    private final SlotOperationRouting slotOperationRouting;

    @Inject
    public SlotSearchTransportAction(Settings settings, ThreadPool threadPool, TransportService transportService, SearchService searchService,
                                     SlotSearchTransportService searchTransportService, SlotSearchPhaseController searchPhaseController,
                                     ClusterService clusterService, ActionFilters actionFilters,
                                     IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, SlotSearchAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, SlotSearchRequest::new);
        this.searchPhaseController = searchPhaseController;
        this.searchTransportService = searchTransportService;
        this.remoteClusterService = searchTransportService.getRemoteClusterService();
        SlotSearchTransportService.registerRequestHandler(transportService, searchService);
        this.clusterService = clusterService;
        this.searchService = searchService;
        this.slotOperationRouting = new SlotOperationRouting(settings, clusterService.getClusterSettings());
    }

    private Map<String, AliasFilter> buildPerIndexAliasFilter(SlotSearchRequest request, ClusterState clusterState,
                                                              Index[] concreteIndices, Map<String, AliasFilter> remoteAliasMap) {
        final Map<String, AliasFilter> aliasFilterMap = new HashMap<>();
        final Set<String> indicesAndAliases = indexNameExpressionResolver.resolveExpressions(clusterState, request.indices());
        for (Index index : concreteIndices) {
            clusterState.blocks().indexBlockedRaiseException(ClusterBlockLevel.READ, index.getName());
            AliasFilter aliasFilter = searchService.buildAliasFilter(clusterState, index.getName(), indicesAndAliases);
            assert aliasFilter != null;
            aliasFilterMap.put(index.getUUID(), aliasFilter);
        }
        aliasFilterMap.putAll(remoteAliasMap);
        return aliasFilterMap;
    }

    private Map<String, Float> resolveIndexBoosts(SlotSearchRequest searchRequest, ClusterState clusterState) {
        if (searchRequest.source() == null) {
            return Collections.emptyMap();
        }

        SearchSourceBuilder source = searchRequest.source();
        if (source.indexBoosts() == null) {
            return Collections.emptyMap();
        }

        Map<String, Float> concreteIndexBoosts = new HashMap<>();
        for (SearchSourceBuilder.IndexBoost ib : source.indexBoosts()) {
            Index[] concreteIndices =
                    indexNameExpressionResolver.concreteIndices(clusterState, searchRequest.indicesOptions(), ib.getIndex());

            for (Index concreteIndex : concreteIndices) {
                concreteIndexBoosts.putIfAbsent(concreteIndex.getUUID(), ib.getBoost());
            }
        }
        return Collections.unmodifiableMap(concreteIndexBoosts);
    }

    /**
     * Search operations need two clocks. One clock is to fulfill real clock needs (e.g., resolving
     * "now" to an index name). Another clock is needed for measuring how long a search operation
     * took. These two uses are at odds with each other. There are many issues with using a real
     * clock for measuring how long an operation took (they often lack precision, they are subject
     * to moving backwards due to NTP and other such complexities, etc.). There are also issues with
     * using a relative clock for reporting real time. Thus, we simply separate these two uses.
     */
    public static class SearchTimeProvider {

        private final long absoluteStartMillis;
        private final long relativeStartNanos;
        private final LongSupplier relativeCurrentNanosProvider;

        /**
         * Instantiates a new search time provider. The absolute start time is the real clock time
         * used for resolving index expressions that include dates. The relative start time is the
         * start of the search operation according to a relative clock. The total time the search
         * operation took can be measured against the provided relative clock and the relative start
         * time.
         *
         * @param absoluteStartMillis          the absolute start time in milliseconds since the epoch
         * @param relativeStartNanos           the relative start time in nanoseconds
         * @param relativeCurrentNanosProvider provides the current relative time
         */
        public SearchTimeProvider(
                final long absoluteStartMillis,
                final long relativeStartNanos,
                final LongSupplier relativeCurrentNanosProvider) {
            this.absoluteStartMillis = absoluteStartMillis;
            this.relativeStartNanos = relativeStartNanos;
            this.relativeCurrentNanosProvider = relativeCurrentNanosProvider;
        }

        public long getAbsoluteStartMillis() {
            return absoluteStartMillis;
        }

        public long getRelativeStartNanos() {
            return relativeStartNanos;
        }

        public long getRelativeCurrentNanos() {
            return relativeCurrentNanosProvider.getAsLong();
        }
    }

//    @Override
//    protected void doExecute(SlotSearchRequest searchRequest, ActionListener<SlotSearchResponse> listener) {
//        throw new UnsupportedOperationException("the task parameter is required");
//    }

    @Override
    protected void doExecute(Task task, SlotSearchRequest searchRequest, ActionListener<SlotSearchResponse> listener) {
        final long relativeStartNanos = System.nanoTime();
        final SearchTimeProvider timeProvider =
                new SearchTimeProvider(searchRequest.getOrCreateAbsoluteStartMillis(), relativeStartNanos, System::nanoTime);
        ActionListener<SearchSourceBuilder> rewriteListener = ActionListener.wrap(source -> {
            if (source != searchRequest.source()) {
                // only set it if it changed - we don't allow null values to be set but it might be already null be we want to catch
                // situations when it possible due to a bug changes to null
                searchRequest.source(source);
            }
            final ClusterState clusterState = clusterService.state();
            final Map<String, OriginalIndices> remoteClusterIndices = remoteClusterService.groupIndices(searchRequest.indicesOptions(),
                    searchRequest.indices(), idx -> indexNameExpressionResolver.hasIndexOrAlias(idx, clusterState));
            OriginalIndices localIndices = remoteClusterIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            if (remoteClusterIndices.isEmpty()) {
                executeSearch((SearchTask) task, timeProvider, searchRequest, localIndices, Collections.emptyList(),
                        (clusterName, nodeId) -> null, clusterState, Collections.emptyMap(), listener,
                        clusterState.getNodes().getDataNodes().size(), SlotSearchResponse.Clusters.EMPTY);
            } else {
                remoteClusterService.collectSearchShards(searchRequest.indicesOptions(), searchRequest.preference(),
                        searchRequest.routing(), remoteClusterIndices, ActionListener.wrap((searchShardsResponses) -> {
                            List<SearchShardIterator> remoteShardIterators = new ArrayList<>();
                            Map<String, AliasFilter> remoteAliasFilters = new HashMap<>();
                            BiFunction<String, String, DiscoveryNode> clusterNodeLookup = processRemoteShards(searchShardsResponses,
                                    remoteClusterIndices, remoteShardIterators, remoteAliasFilters);
                            int numNodesInvolved = searchShardsResponses.values().stream().mapToInt(r -> r.getNodes().length).sum()
                                    + clusterState.getNodes().getDataNodes().size();
                            SlotSearchResponse.Clusters clusters = buildClusters(localIndices, remoteClusterIndices, searchShardsResponses);
                            executeSearch((SearchTask) task, timeProvider, searchRequest, localIndices,
                                    remoteShardIterators, clusterNodeLookup, clusterState, remoteAliasFilters, listener, numNodesInvolved,
                                    clusters);
                        }, listener::onFailure));
            }
        }, listener::onFailure);
        if (searchRequest.source() == null) {
            rewriteListener.onResponse(searchRequest.source());
        } else {
            Rewriteable.rewriteAndFetch(searchRequest.source(), searchService.getRewriteContext(timeProvider::getAbsoluteStartMillis),
                    rewriteListener);
        }
    }

    static SlotSearchResponse.Clusters buildClusters(OriginalIndices localIndices, Map<String, OriginalIndices> remoteIndices,
                                                     Map<String, ClusterSearchShardsResponse> searchShardsResponses) {
        int localClusters = localIndices == null ? 0 : 1;
        int totalClusters = remoteIndices.size() + localClusters;
        int successfulClusters = localClusters;
        for (ClusterSearchShardsResponse searchShardsResponse : searchShardsResponses.values()) {
            if (searchShardsResponse != ClusterSearchShardsResponse.EMPTY) {
                successfulClusters++;
            }
        }
        int skippedClusters = totalClusters - successfulClusters;
        return new SlotSearchResponse.Clusters(totalClusters, successfulClusters, skippedClusters);
    }

    static BiFunction<String, String, DiscoveryNode> processRemoteShards(Map<String, ClusterSearchShardsResponse> searchShardsResponses,
                                                                         Map<String, OriginalIndices> remoteIndicesByCluster,
                                                                         List<SearchShardIterator> remoteShardIterators,
                                                                         Map<String, AliasFilter> aliasFilterMap) {
        Map<String, Map<String, DiscoveryNode>> clusterToNode = new HashMap<>();
        for (Map.Entry<String, ClusterSearchShardsResponse> entry : searchShardsResponses.entrySet()) {
            String clusterAlias = entry.getKey();
            ClusterSearchShardsResponse searchShardsResponse = entry.getValue();
            HashMap<String, DiscoveryNode> idToDiscoveryNode = new HashMap<>();
            clusterToNode.put(clusterAlias, idToDiscoveryNode);
            for (DiscoveryNode remoteNode : searchShardsResponse.getNodes()) {
                idToDiscoveryNode.put(remoteNode.getId(), remoteNode);
            }
            final Map<String, AliasFilter> indicesAndFilters = searchShardsResponse.getIndicesAndFilters();
            for (ClusterSearchShardsGroup clusterSearchShardsGroup : searchShardsResponse.getGroups()) {
                //add the cluster name to the remote index names for indices disambiguation
                //this ends up in the hits returned with the search response
                ShardId shardId = clusterSearchShardsGroup.getShardId();
                final AliasFilter aliasFilter;
                if (indicesAndFilters == null) {
                    aliasFilter = AliasFilter.EMPTY;
                } else {
                    aliasFilter = indicesAndFilters.get(shardId.getIndexName());
                    assert aliasFilter != null : "alias filter must not be null for index: " + shardId.getIndex();
                }
                String[] aliases = aliasFilter.getAliases();
                String[] finalIndices = aliases.length == 0 ? new String[]{shardId.getIndexName()} : aliases;
                // here we have to map the filters to the UUID since from now on we use the uuid for the lookup
                aliasFilterMap.put(shardId.getIndex().getUUID(), aliasFilter);
                final OriginalIndices originalIndices = remoteIndicesByCluster.get(clusterAlias);
                assert originalIndices != null : "original indices are null for clusterAlias: " + clusterAlias;
                SearchShardIterator shardIterator = new SearchShardIterator(clusterAlias, shardId,
                        Arrays.asList(clusterSearchShardsGroup.getShards()), new OriginalIndices(finalIndices,
                        originalIndices.indicesOptions()));
                remoteShardIterators.add(shardIterator);
            }
        }
        return (clusterAlias, nodeId) -> {
            Map<String, DiscoveryNode> clusterNodes = clusterToNode.get(clusterAlias);
            if (clusterNodes == null) {
                throw new IllegalArgumentException("unknown remote cluster: " + clusterAlias);
            }
            return clusterNodes.get(nodeId);
        };
    }

    private Index[] resolveLocalIndices(OriginalIndices localIndices,
                                        IndicesOptions indicesOptions,
                                        ClusterState clusterState,
                                        SearchTimeProvider timeProvider) {
        if (localIndices == null) {
            return Index.EMPTY_ARRAY; //don't search on any local index (happens when only remote indices were specified)
        }
        return indexNameExpressionResolver.concreteIndices(clusterState, indicesOptions,
                timeProvider.getAbsoluteStartMillis(), localIndices.indices());
    }

    private void executeSearch(SearchTask task, SearchTimeProvider timeProvider, SlotSearchRequest searchRequest,
                               OriginalIndices localIndices, List<SearchShardIterator> remoteShardIterators,
                               BiFunction<String, String, DiscoveryNode> remoteConnections, ClusterState clusterState,
                               Map<String, AliasFilter> remoteAliasMap, ActionListener<SlotSearchResponse> listener, int nodeCount,
                               SlotSearchResponse.Clusters clusters) {

        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);
        // TODO: I think startTime() should become part of ActionRequest and that should be used both for index name
        // date math expressions and $now in scripts. This way all apis will deal with now in the same way instead
        // of just for the _search api
        final Index[] indices = resolveLocalIndices(localIndices, searchRequest.indicesOptions(), clusterState, timeProvider);
        Map<String, AliasFilter> aliasFilter = buildPerIndexAliasFilter(searchRequest, clusterState, indices, remoteAliasMap);
        Map<String, Set<String>> routingMap = indexNameExpressionResolver.resolveSearchRouting(clusterState, searchRequest.routing(), searchRequest.indices());
        routingMap = routingMap == null ? Collections.emptyMap() : Collections.unmodifiableMap(routingMap);
        String[] concreteIndices = new String[indices.length];
        for (int i = 0; i < indices.length; i++) {
            concreteIndices[i] = indices[i].getName();
        }
        Map<String, Long> nodeSearchCounts = searchTransportService.getPendingSearchRequests();

        GroupShardsIterator<ShardIterator> localShardsIterator = slotOperationRouting.searchShards(clusterState,
                concreteIndices, routingMap, searchRequest.preference(), searchService.getResponseCollectorService(), nodeSearchCounts);
        GroupShardsIterator<SearchShardIterator> shardIterators = mergeShardsIterators(localShardsIterator, localIndices,
                searchRequest.getLocalClusterAlias(), remoteShardIterators);

        failIfOverShardCountLimit(clusterService, shardIterators.size());

        Map<String, Float> concreteIndexBoosts = resolveIndexBoosts(searchRequest, clusterState);

        // optimize search type for cases where there is only one shard group to search on
        if (shardIterators.size() == 1) {
            // if we only have one group, then we always want Q_T_F, no need for DFS, and no need to do THEN since we hit one shard
            searchRequest.searchType(QUERY_THEN_FETCH);
        }
        if (searchRequest.allowPartialSearchResults() == null) {
            // No user preference defined in search request - apply cluster service default
            searchRequest.allowPartialSearchResults(searchService.defaultAllowPartialSearchResults());
        }
        if (searchRequest.isSuggestOnly()) {
            // disable request cache if we have only suggest
            searchRequest.requestCache(false);
            switch (searchRequest.searchType()) {
                case DFS_QUERY_THEN_FETCH:
                    // convert to Q_T_F if we have only suggest
                    searchRequest.searchType(QUERY_THEN_FETCH);
                    break;
            }
        }

        final DiscoveryNodes nodes = clusterState.nodes();
        BiFunction<String, String, Transport.Connection> connectionLookup = buildConnectionLookup(searchRequest.getLocalClusterAlias(),
                nodes::get, remoteConnections, searchTransportService::getConnection);
        assert nodeCount > 0 || shardIterators.size() == 0 : "non empty search iterators but node count is 0";
        setMaxConcurrentShardRequests(searchRequest, nodeCount);
        boolean preFilterSearchShards = shouldPreFilterSearchShards(searchRequest, shardIterators);
        searchAsyncAction(task, searchRequest, shardIterators, timeProvider, connectionLookup, clusterState.version(),
                Collections.unmodifiableMap(aliasFilter), concreteIndexBoosts, routingMap, listener, preFilterSearchShards, clusters).start();
    }

    /**
     * 废弃的方法，这里不需要重新构建routing参数
     *
     * @param state
     * @param routing
     * @param expressions
     * @return
     * @throws ClassNotFoundException
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     * @throws InstantiationException
     * @throws NoSuchFieldException
     */
    @Deprecated
    private Map<String, Set<String>> resolveSearchRouting(ClusterState state, String routing, String[] expressions)
            throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchFieldException {
        List<String> resolvedExpressions = expressions != null ? Arrays.asList(expressions) : Collections.emptyList();

        String className = "org.elasticsearch.cluster.metadata.IndexNameExpressionResolver";
        Class<?> indexNameExpressionResolverClass = Class.forName(className);
        Class<?> contextClass = indexNameExpressionResolverClass.getDeclaredClasses()[3];
        Constructor<?> contextClassConstructor = contextClass.getDeclaredConstructor(ClusterState.class, IndicesOptions.class);

        Object contextObj = contextClassConstructor.newInstance(state, IndicesOptions.lenientExpandOpen());

        Field expressionResolversField = indexNameExpressionResolver.getClass().getDeclaredField("expressionResolvers");

        Object expressionResolversObj = expressionResolversField.get(indexNameExpressionResolver);

        for (Object expressionResolverItem : (List) expressionResolversObj) {
            Method resolveMethod = expressionResolverItem.getClass().getMethod("resolve", contextClass, List.class);
            Object resolveObj = resolveMethod.invoke(expressionResolverItem, contextObj, resolvedExpressions);
            resolvedExpressions = (List<String>) resolveObj;
        }

        Map<String, Set<String>> routings = null;
        Set<String> paramRouting = null;
        // List of indices that don't require any routing
        Set<String> norouting = new HashSet<>();
        if (routing != null) {
            paramRouting = Sets.newHashSet(Strings.splitStringByCommaToArray(routing));
        }

        for (String expression : resolvedExpressions) {
            AliasOrIndex aliasOrIndex = state.metaData().getAliasAndIndexLookup().get(expression);
            if (aliasOrIndex != null && aliasOrIndex.isAlias()) {
                AliasOrIndex.Alias alias = (AliasOrIndex.Alias) aliasOrIndex;
                for (Tuple<String, AliasMetaData> item : alias.getConcreteIndexAndAliasMetaDatas()) {
                    String concreteIndex = item.v1();
                    AliasMetaData aliasMetaData = item.v2();
                    if (!norouting.contains(concreteIndex)) {
                        if (!aliasMetaData.searchRoutingValues().isEmpty()) {
                            // Routing alias
                            if (routings == null) {
                                routings = new HashMap<>();
                            }
                            Set<String> r = routings.get(concreteIndex);
                            if (r == null) {
                                r = new HashSet<>();
                                routings.put(concreteIndex, r);
                            }
                            r.addAll(aliasMetaData.searchRoutingValues());
                            if (paramRouting != null) {
                                r.retainAll(paramRouting);
                            }
                            if (r.isEmpty()) {
                                routings.remove(concreteIndex);
                            }
                        } else {
                            // Non-routing alias
                            if (!norouting.contains(concreteIndex)) {
                                norouting.add(concreteIndex);
                                if (paramRouting != null) {
                                    Set<String> r = new HashSet<>(paramRouting);
                                    if (routings == null) {
                                        routings = new HashMap<>();
                                    }
                                    routings.put(concreteIndex, r);
                                } else {
                                    if (routings != null) {
                                        routings.remove(concreteIndex);
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                // Index
                if (!norouting.contains(expression)) {
                    norouting.add(expression);
                    if (paramRouting != null) {
                        Set<String> r = new HashSet<>(paramRouting);
                        if (routings == null) {
                            routings = new HashMap<>();
                        }
                        routings.put(expression, r);
                    } else {
                        if (routings != null) {
                            routings.remove(expression);
                        }
                    }
                }
            }

        }
        if (routings == null || routings.isEmpty()) {
            return null;
        }
        return routings;
    }

    static void setMaxConcurrentShardRequests(SlotSearchRequest searchRequest, int nodeCount) {
        if (searchRequest.isMaxConcurrentShardRequestsSet() == false) {
            // we try to set a default of max concurrent shard requests based on the node count but upper-bound it by 256 by default to
            // keep it sane. A single search request that fans out to lots of shards should hit a cluster too hard while 256 is already
            // a lot. we multiply it by the default number of shards such that a single request in a cluster of 1 would hit all shards of
            // a default index. We take into account that we may be in a cluster with no data nodes searching against no shards.
            searchRequest.setMaxConcurrentShardRequests(Math.min(256, Math.max(nodeCount, 1)
                    * IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getDefault(Settings.EMPTY)));
        }
    }

    static BiFunction<String, String, Transport.Connection> buildConnectionLookup(String requestClusterAlias,
                                                                                  Function<String, DiscoveryNode> localNodes,
                                                                                  BiFunction<String, String, DiscoveryNode> remoteNodes,
                                                                                  BiFunction<String, DiscoveryNode, Transport.Connection> nodeToConnection) {
        return (clusterAlias, nodeId) -> {
            final DiscoveryNode discoveryNode;
            final boolean remoteCluster;
            if (clusterAlias == null || requestClusterAlias != null) {
                assert requestClusterAlias == null || requestClusterAlias.equals(clusterAlias);
                discoveryNode = localNodes.apply(nodeId);
                remoteCluster = false;
            } else {
                discoveryNode = remoteNodes.apply(clusterAlias, nodeId);
                remoteCluster = true;
            }
            if (discoveryNode == null) {
                throw new IllegalStateException("no node found for id: " + nodeId);
            }
            return nodeToConnection.apply(remoteCluster ? clusterAlias : null, discoveryNode);
        };
    }

    private static boolean shouldPreFilterSearchShards(SlotSearchRequest searchRequest,
                                                       GroupShardsIterator<SearchShardIterator> shardIterators) {
        SearchSourceBuilder source = searchRequest.source();
        return searchRequest.searchType() == QUERY_THEN_FETCH && // we can't do this for DFS it needs to fan out to all shards all the time
                SearchService.canRewriteToMatchNone(source) &&
                searchRequest.getPreFilterShardSize() < shardIterators.size();
    }

    static GroupShardsIterator<SearchShardIterator> mergeShardsIterators(GroupShardsIterator<ShardIterator> localShardsIterator,
                                                                         OriginalIndices localIndices,
                                                                         @Nullable String localClusterAlias,
                                                                         List<SearchShardIterator> remoteShardIterators) {
        List<SearchShardIterator> shards = new ArrayList<>(remoteShardIterators);
        for (ShardIterator shardIterator : localShardsIterator) {
            shards.add(new SearchShardIterator(localClusterAlias, shardIterator.shardId(), shardIterator.getShardRoutings(), localIndices));
        }
        return new GroupShardsIterator<>(shards);
    }

    @Override
    protected final void doExecute(SlotSearchRequest searchRequest, ActionListener<SlotSearchResponse> listener) {
        throw new UnsupportedOperationException("the task parameter is required");
    }

    private AbstractSearchAsyncAction searchAsyncAction(SearchTask task, SlotSearchRequest searchRequest,
                                                        GroupShardsIterator<SearchShardIterator> shardIterators,
                                                        SearchTimeProvider timeProvider,
                                                        BiFunction<String, String, Transport.Connection> connectionLookup,
                                                        long clusterStateVersion,
                                                        Map<String, AliasFilter> aliasFilter,
                                                        Map<String, Float> concreteIndexBoosts,
                                                        Map<String, Set<String>> indexRoutings,
                                                        ActionListener<SlotSearchResponse> listener,
                                                        boolean preFilter,
                                                        SlotSearchResponse.Clusters clusters) {
        Executor executor = threadPool.executor(ThreadPool.Names.SEARCH);
        if (preFilter) {
//            return new CanMatchPreFilterSearchPhase(logger, searchTransportService, connectionLookup,
//                    aliasFilter, concreteIndexBoosts, indexRoutings, executor, searchRequest, listener, shardIterators,
//                    timeProvider, clusterStateVersion, task, (iter) -> {
//                AbstractSearchAsyncAction action = searchAsyncAction(task, searchRequest, iter, timeProvider, connectionLookup,
//                        clusterStateVersion, aliasFilter, concreteIndexBoosts, indexRoutings, listener, false, clusters);
//                return new SearchPhase(action.getName()) {
//                    @Override
//                    public void run() {
//                        action.start();
//                    }
//                };
//            }, clusters);
        } else {
            AbstractSearchAsyncAction searchAsyncAction;
            switch (searchRequest.searchType()) {
                case DFS_QUERY_THEN_FETCH:
                    searchAsyncAction = new SearchDfsQueryThenFetchAsyncAction(logger, searchTransportService, connectionLookup,
                            aliasFilter, concreteIndexBoosts, indexRoutings, searchPhaseController, executor, searchRequest, listener,
                            shardIterators, timeProvider, clusterStateVersion, task, clusters);
                    break;
                case QUERY_AND_FETCH:
                case QUERY_THEN_FETCH:
                    searchAsyncAction = new SearchQueryThenFetchAsyncAction(logger, searchTransportService, connectionLookup,
                            aliasFilter, concreteIndexBoosts, indexRoutings, searchPhaseController, executor, searchRequest, listener,
                            shardIterators, timeProvider, clusterStateVersion, task, clusters);
                    break;
                default:
                    throw new IllegalStateException("Unknown search type: [" + searchRequest.searchType() + "]");
            }
            return searchAsyncAction;
        }
        return null;
    }

    private static void failIfOverShardCountLimit(ClusterService clusterService, int shardCount) {
        final long shardCountLimit = clusterService.getClusterSettings().get(SHARD_COUNT_LIMIT_SETTING);
        if (shardCount > shardCountLimit) {
            throw new IllegalArgumentException("Trying to query " + shardCount + " shards, which is over the limit of "
                    + shardCountLimit + ". This limit exists because querying many shards at the same time can make the "
                    + "job of the coordinating node very CPU and/or memory intensive. It is usually a better idea to "
                    + "have a smaller number of larger shards. Update [" + SHARD_COUNT_LIMIT_SETTING.getKey()
                    + "] to a greater value if you really want to query that many shards at the same time.");
        }
    }
}
