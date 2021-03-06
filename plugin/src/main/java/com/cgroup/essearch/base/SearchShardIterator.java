/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.cgroup.essearch.base;

import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.cluster.routing.PlainShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchShardTarget;

import java.util.List;

/**
 * Extension of {@link PlainShardIterator} used in the search api, which also holds the {@link OriginalIndices}
 * of the search request (useful especially with cross-cluster search, as each cluster has its own set of original indices) as well as
 * the cluster alias.
 * @see OriginalIndices
 */
public final class SearchShardIterator extends PlainShardIterator {

    private final OriginalIndices originalIndices;
    private final String clusterAlias;
    private boolean skip = false;

    /**
     * Creates a {@link PlainShardIterator} instance that iterates over a subset of the given shards
     * this the a given <code>shardId</code>.
     *
     * @param clusterAlias the alias of the cluster where the shard is located
     * @param shardId shard id of the group
     * @param shards  shards to iterate
     * @param originalIndices the indices that the search request originally related to (before any rewriting happened)
     */
    public SearchShardIterator(@Nullable String clusterAlias, ShardId shardId, List<ShardRouting> shards, OriginalIndices originalIndices) {
        super(shardId, shards);
        this.originalIndices = originalIndices;
        this.clusterAlias = clusterAlias;
    }

    /**
     * Returns the original indices associated with this shard iterator, specifically with the cluster that this shard belongs to.
     */
    public OriginalIndices getOriginalIndices() {
        return originalIndices;
    }

    /**
     * Returns the alias of the cluster where the shard is located.
     */
    @Nullable
    public String getClusterAlias() {
        return clusterAlias;
    }

    /**
     * Creates a new shard target from this iterator, pointing at the node identified by the provided identifier.
     * @see SearchShardTarget
     */
    SearchShardTarget newSearchShardTarget(String nodeId) {
        return new SearchShardTarget(nodeId, shardId(), clusterAlias, originalIndices);
    }

    /**
     * Reset the iterator and mark it as skippable
     * @see #skip()
     */
    void resetAndSkip() {
        reset();
        skip = true;
    }

    /**
     * Returns <code>true</code> if the search execution should skip this shard since it can not match any documents given the query.
     */
    boolean skip() {
        return skip;
    }
}
