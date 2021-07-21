package com.cgroup.essearch.internal;

import com.cgroup.essearch.SlotSearchRequest;
import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchRequest;

import java.io.IOException;
import java.util.Optional;

/**
 * Created by zzq on 2021/7/20.
 */
public class ShardSearchLocalRequest implements ShardSearchRequest {
    private String clusterAlias;
    private ShardId shardId;
    private int numberOfShards;
    private SearchType searchType;
    private Scroll scroll;
    private String[] types = Strings.EMPTY_ARRAY;
    private AliasFilter aliasFilter;
    private float indexBoost;
    private SearchSourceBuilder source;
    private Boolean requestCache;
    private long nowInMillis;
    private boolean allowPartialSearchResults;
    private String[] indexRoutings = Strings.EMPTY_ARRAY;
    private String preference;
    private boolean profile;

    public ShardSearchLocalRequest() {
    }

    public ShardSearchLocalRequest(SlotSearchRequest searchRequest, ShardId shardId, int numberOfShards, AliasFilter aliasFilter, float indexBoost,
                                   long nowInMillis, @Nullable String clusterAlias, String[] indexRoutings) {
        this(shardId, numberOfShards, searchRequest.searchType(),
                searchRequest.source(), searchRequest.types(), searchRequest.requestCache(), aliasFilter, indexBoost,
                searchRequest.allowPartialSearchResults(), indexRoutings, searchRequest.preference());
        // If allowPartialSearchResults is unset (ie null), the cluster-level default should have been substituted
        // at this stage. Any NPEs in the above are therefore an error in request preparation logic.
        assert searchRequest.allowPartialSearchResults() != null;
        this.scroll = searchRequest.scroll();
        this.nowInMillis = nowInMillis;
        this.clusterAlias = clusterAlias;
    }

    public ShardSearchLocalRequest(ShardId shardId, String[] types, long nowInMillis, AliasFilter aliasFilter) {
        this.types = types;
        this.nowInMillis = nowInMillis;
        this.aliasFilter = aliasFilter;
        this.shardId = shardId;
        indexBoost = 1.0f;
    }

    public ShardSearchLocalRequest(ShardId shardId, int numberOfShards, SearchType searchType, SearchSourceBuilder source, String[] types,
                                   Boolean requestCache, AliasFilter aliasFilter, float indexBoost, boolean allowPartialSearchResults,
                                   String[] indexRoutings, String preference) {
        this.shardId = shardId;
        this.numberOfShards = numberOfShards;
        this.searchType = searchType;
        this.source = source;
        this.types = types;
        this.requestCache = requestCache;
        this.aliasFilter = aliasFilter;
        this.indexBoost = indexBoost;
        this.allowPartialSearchResults = allowPartialSearchResults;
        this.indexRoutings = indexRoutings;
        this.preference = preference;
    }

    @Override
    public ShardId shardId() {
        return shardId;
    }

    @Override
    public String[] types() {
        return types;
    }

    @Override
    public SearchSourceBuilder source() {
        return source;
    }

    @Override
    public AliasFilter getAliasFilter() {
        return aliasFilter;
    }

    @Override
    public void setAliasFilter(AliasFilter aliasFilter) {
        this.aliasFilter = aliasFilter;
    }

    @Override
    public void source(SearchSourceBuilder source) {
        this.source = source;
    }

    @Override
    public int numberOfShards() {
        return numberOfShards;
    }

    @Override
    public SearchType searchType() {
        return searchType;
    }

    @Override
    public float indexBoost() {
        return indexBoost;
    }

    @Override
    public long nowInMillis() {
        return nowInMillis;
    }

    @Override
    public Boolean requestCache() {
        return requestCache;
    }

    @Override
    public Boolean allowPartialSearchResults() {
        return allowPartialSearchResults;
    }


    @Override
    public Scroll scroll() {
        return scroll;
    }

    @Override
    public String[] indexRoutings() {
        return indexRoutings;
    }

    @Override
    public String preference() {
        return preference;
    }

    @Override
    public void setProfile(boolean profile) {
        this.profile = profile;
    }

    @Override
    public boolean isProfile() {
        return profile;
    }

    public void setSearchType(SearchType type) {
        this.searchType = type;
    }

    public void innerReadFrom(StreamInput in) throws IOException {
        shardId = ShardId.readShardId(in);
        searchType = SearchType.fromId(in.readByte());
        numberOfShards = in.readVInt();
        scroll = in.readOptionalWriteable(Scroll::new);
        source = in.readOptionalWriteable(SearchSourceBuilder::new);
        types = in.readStringArray();
        aliasFilter = new AliasFilter(in);
        if (in.getVersion().onOrAfter(Version.V_5_2_0)) {
            indexBoost = in.readFloat();
        } else {
            // Nodes < 5.2.0 doesn't send index boost. Read it from source.
            if (source != null) {
                Optional<SearchSourceBuilder.IndexBoost> boost = source.indexBoosts()
                        .stream()
                        .filter(ib -> ib.getIndex().equals(shardId.getIndexName()))
                        .findFirst();
                indexBoost = boost.isPresent() ? boost.get().getBoost() : 1.0f;
            } else {
                indexBoost = 1.0f;
            }
        }
        nowInMillis = in.readVLong();
        requestCache = in.readOptionalBoolean();
        if (in.getVersion().onOrAfter(Version.V_5_6_0)) {
            clusterAlias = in.readOptionalString();
        }
        if (in.getVersion().onOrAfter(Version.V_6_3_0)) {
            allowPartialSearchResults = in.readOptionalBoolean();
        }
        if (in.getVersion().onOrAfter(Version.V_6_4_0)) {
            indexRoutings = in.readStringArray();
            preference = in.readOptionalString();
        } else {
            indexRoutings = Strings.EMPTY_ARRAY;
            preference = null;
        }
    }

    public void innerWriteTo(StreamOutput out, boolean asKey) throws IOException {
        shardId.writeTo(out);
        out.writeByte(searchType.id());
        if (!asKey) {
            out.writeVInt(numberOfShards);
        }
        out.writeOptionalWriteable(scroll);
        out.writeOptionalWriteable(source);
        out.writeStringArray(types);
        aliasFilter.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_5_2_0)) {
            out.writeFloat(indexBoost);
        }
        if (asKey == false) {
            out.writeVLong(nowInMillis);
        }
        out.writeOptionalBoolean(requestCache);
        if (out.getVersion().onOrAfter(Version.V_5_6_0)) {
            out.writeOptionalString(clusterAlias);
        }
        if (out.getVersion().onOrAfter(Version.V_6_3_0)) {
            out.writeOptionalBoolean(allowPartialSearchResults);
        }
        if (asKey == false) {
            if (out.getVersion().onOrAfter(Version.V_6_4_0)) {
                out.writeStringArray(indexRoutings);
                out.writeOptionalString(preference);
            }
        }
    }

    @Override
    public BytesReference cacheKey() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        this.innerWriteTo(out, true);
        // copy it over, most requests are small, we might as well copy to make sure we are not sliced...
        // we could potentially keep it without copying, but then pay the price of extra unused bytes up to a page
        return new BytesArray(out.bytes().toBytesRef(), true);// do a deep copy
    }

    @Override
    public String getClusterAlias() {
        return clusterAlias;
    }

    @Override
    public Rewriteable<Rewriteable> getRewriteable() {
        return new RequestRewritable(this);
    }

    static class RequestRewritable implements Rewriteable<Rewriteable> {

        final ShardSearchRequest request;

        RequestRewritable(ShardSearchRequest request) {
            this.request = request;
        }

        @Override
        public Rewriteable rewrite(QueryRewriteContext ctx) throws IOException {
            SearchSourceBuilder newSource = request.source() == null ? null : Rewriteable.rewrite(request.source(), ctx);
            AliasFilter newAliasFilter = Rewriteable.rewrite(request.getAliasFilter(), ctx);
            if (newSource == request.source() && newAliasFilter == request.getAliasFilter()) {
                return this;
            } else {
                request.source(newSource);
                request.setAliasFilter(newAliasFilter);
                return new RequestRewritable(request);
            }
        }
    }
}
