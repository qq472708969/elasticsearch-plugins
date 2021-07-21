package com.cgroup.essearch;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.node.ResponseCollectorService;

import java.util.*;
import java.util.stream.Collectors;

import static com.cgroup.esbulkrouting.FastBulkAction.SETTING_ROUTING_SLOT;

/**
 * Created by zzq on 2021/7/21.
 */
public class SlotOperationRouting extends OperationRouting {
    private final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(this.getClass()));
    private List<String> awarenessAttributes;
    private boolean useAdaptiveReplicaSelection;

    public SlotOperationRouting(Settings settings, ClusterSettings clusterSettings) {
        super(settings, clusterSettings);
        this.awarenessAttributes = AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.get(settings);
        this.useAdaptiveReplicaSelection = USE_ADAPTIVE_REPLICA_SELECTION_SETTING.get(settings);
    }

    private static final Map<String, Set<String>> EMPTY_ROUTING = Collections.emptyMap();
    private final int SLOT_COUNT_DEFAULT = -1;

    @Override
    public GroupShardsIterator<ShardIterator> searchShards(ClusterState clusterState, String[] concreteIndices,
                                                           Map<String, Set<String>> routing, String preference,
                                                           ResponseCollectorService collectorService, Map<String, Long> nodeCounts) {
        final Set<IndexShardRoutingTable> shards = computeTargetedShards1(clusterState, concreteIndices, routing);
        final Set<ShardIterator> set = new HashSet<>(shards.size());
        for (IndexShardRoutingTable shard : shards) {
            ShardIterator iterator = preferenceActiveShardIterator(shard,
                    clusterState.nodes().getLocalNodeId(), clusterState.nodes(), preference, collectorService, nodeCounts);
            if (iterator != null) {
                set.add(iterator);
            }
        }
        return new GroupShardsIterator<>(new ArrayList<>(set));
    }

    private Set<IndexShardRoutingTable> computeTargetedShards1(ClusterState clusterState, String[] concreteIndices, Map<String, Set<String>> routing) {
        routing = routing == null ? EMPTY_ROUTING : routing;
        final Set<IndexShardRoutingTable> set = new HashSet<>();
        // we use set here and not list since we might get duplicates
        for (String index : concreteIndices) {
            final IndexRoutingTable indexRouting = indexRoutingTable(clusterState, index);
            final IndexMetaData indexMetaData = indexMetaData(clusterState, index);
            int slotCount = indexMetaData.getSettings().getAsInt(SETTING_ROUTING_SLOT, SLOT_COUNT_DEFAULT);
            int routingNumShards = indexMetaData.getRoutingNumShards();
            int slotSize = routingNumShards / slotCount;
            final Set<String> effectiveRouting = routing.get(index);
            if (effectiveRouting != null) {
                for (String r : effectiveRouting) {
                    int slotNo = Math.floorMod(Murmur3HashFunction.hash(r), slotCount);
                    int shardStartIndex = slotNo * slotSize;
                    int shardEndIndex = shardStartIndex + slotSize;
                    for (int shardNo = shardStartIndex; shardNo < shardEndIndex; shardNo++) {
                        final int routingPartitionSize = indexMetaData.getRoutingPartitionSize();
                        for (int partitionOffset = 0; partitionOffset < routingPartitionSize; partitionOffset++) {
                            set.add(getShardRoutingTable(indexRouting, shardNo));
                        }
                    }
                }
            } else {
                for (IndexShardRoutingTable indexShard : indexRouting) {
                    set.add(indexShard);
                }
            }
        }
        return set;
    }

    public IndexShardRoutingTable getShardRoutingTable(IndexRoutingTable indexRouting, int shardId) {
        IndexShardRoutingTable indexShard = indexRouting.shard(shardId);
        if (indexShard == null) {
            throw new ShardNotFoundException(new ShardId(indexRouting.getIndex(), shardId));
        }
        return indexShard;
    }

    private ShardIterator preferenceActiveShardIterator(IndexShardRoutingTable indexShard, String localNodeId,
                                                        DiscoveryNodes nodes, @Nullable String preference,
                                                        @Nullable ResponseCollectorService collectorService,
                                                        @Nullable Map<String, Long> nodeCounts) {
        if (preference == null || preference.isEmpty()) {
            if (awarenessAttributes.isEmpty()) {
                if (useAdaptiveReplicaSelection) {
                    return indexShard.activeInitializingShardsRankedIt(collectorService, nodeCounts);
                } else {
                    return indexShard.activeInitializingShardsRandomIt();
                }
            } else {
                return indexShard.preferAttributesActiveInitializingShardsIt(awarenessAttributes, nodes);
            }
        }
        if (preference.charAt(0) == '_') {
            Preference preferenceType = Preference.parse(preference);
            if (preferenceType == Preference.SHARDS) {
                // starts with _shards, so execute on specific ones
                int index = preference.indexOf('|');

                String shards;
                if (index == -1) {
                    shards = preference.substring(Preference.SHARDS.type().length() + 1);
                } else {
                    shards = preference.substring(Preference.SHARDS.type().length() + 1, index);
                }
                String[] ids = Strings.splitStringByCommaToArray(shards);
                boolean found = false;
                for (String id : ids) {
                    if (Integer.parseInt(id) == indexShard.shardId().id()) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    return null;
                }
                // no more preference
                if (index == -1 || index == preference.length() - 1) {
                    if (awarenessAttributes.isEmpty()) {
                        if (useAdaptiveReplicaSelection) {
                            return indexShard.activeInitializingShardsRankedIt(collectorService, nodeCounts);
                        } else {
                            return indexShard.activeInitializingShardsRandomIt();
                        }
                    } else {
                        return indexShard.preferAttributesActiveInitializingShardsIt(awarenessAttributes, nodes);
                    }
                } else {
                    // update the preference and continue
                    preference = preference.substring(index + 1);
                }
            }
            preferenceType = Preference.parse(preference);
            switch (preferenceType) {
                case PREFER_NODES:
                    final Set<String> nodesIds =
                            Arrays.stream(
                                    preference.substring(Preference.PREFER_NODES.type().length() + 1).split(",")
                            ).collect(Collectors.toSet());
                    return indexShard.preferNodeActiveInitializingShardsIt(nodesIds);
                case LOCAL:
                    return indexShard.preferNodeActiveInitializingShardsIt(Collections.singleton(localNodeId));
                case PRIMARY:
                    deprecationLogger.deprecated("[_primary] has been deprecated in 6.1+, and will be removed in 7.0; " +
                            "use [_only_nodes] or [_prefer_nodes]");
                    return indexShard.primaryActiveInitializingShardIt();
                case REPLICA:
                    deprecationLogger.deprecated("[_replica] has been deprecated in 6.1+, and will be removed in 7.0; " +
                            "use [_only_nodes] or [_prefer_nodes]");
                    return indexShard.replicaActiveInitializingShardIt();
                case PRIMARY_FIRST:
                    deprecationLogger.deprecated("[_primary_first] has been deprecated in 6.1+, and will be removed in 7.0; " +
                            "use [_only_nodes] or [_prefer_nodes]");
                    return indexShard.primaryFirstActiveInitializingShardsIt();
                case REPLICA_FIRST:
                    deprecationLogger.deprecated("[_replica_first] has been deprecated in 6.1+, and will be removed in 7.0; " +
                            "use [_only_nodes] or [_prefer_nodes]");
                    return indexShard.replicaFirstActiveInitializingShardsIt();
                case ONLY_LOCAL:
                    return indexShard.onlyNodeActiveInitializingShardsIt(localNodeId);
                case ONLY_NODES:
                    String nodeAttributes = preference.substring(Preference.ONLY_NODES.type().length() + 1);
                    return indexShard.onlyNodeSelectorActiveInitializingShardsIt(nodeAttributes.split(","), nodes);
                default:
                    throw new IllegalArgumentException("unknown preference [" + preferenceType + "]");
            }
        }
        // if not, then use it as the index
        int routingHash = Murmur3HashFunction.hash(preference);
        if (nodes.getMinNodeVersion().onOrAfter(Version.V_6_0_0_alpha1)) {
            // The AllocationService lists shards in a fixed order based on nodes
            // so earlier versions of this class would have a tendency to
            // select the same node across different shardIds.
            // Better overall balancing can be achieved if each shardId opts
            // for a different element in the list by also incorporating the
            // shard ID into the hash of the user-supplied preference key.
            routingHash = 31 * routingHash + indexShard.shardId().hashCode();
        }
        if (awarenessAttributes.isEmpty()) {
            return indexShard.activeInitializingShardsIt(routingHash);
        } else {
            return indexShard.preferAttributesActiveInitializingShardsIt(awarenessAttributes, nodes, routingHash);
        }
    }

}
