package com.cgroup.esbulkrouting;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;

/**
 * Created by zzq on 2021/7/14.
 */
public class FastBulkRequest extends ActionRequest implements CompositeIndicesRequest, WriteRequest<FastBulkRequest> {
    private static final DeprecationLogger DEPRECATION_LOGGER =
            new DeprecationLogger(LogManager.getLogger(FastBulkRequest.class));

    private static final int REQUEST_OVERHEAD = 50;

    private static final ParseField INDEX = new ParseField("_index");
    private static final ParseField TYPE = new ParseField("_type");
    private static final ParseField ID = new ParseField("_id");
    private static final ParseField ROUTING = new ParseField("routing", "_routing");
    private static final ParseField SHARDNO = new ParseField("shard_no", "_shard_no");
    private static final ParseField PARENT = new ParseField("parent", "_parent");
    private static final ParseField OP_TYPE = new ParseField("op_type", "opType");
    private static final ParseField VERSION = new ParseField("version", "_version");
    private static final ParseField VERSION_TYPE = new ParseField("version_type", "_version_type", "_versionType", "versionType");
    private static final ParseField RETRY_ON_CONFLICT = new ParseField("retry_on_conflict", "_retry_on_conflict", "_retryOnConflict");
    private static final ParseField PIPELINE = new ParseField("pipeline");
    private static final ParseField FIELDS = new ParseField("fields");
    private static final ParseField SOURCE = new ParseField("_source");
    private static final ParseField IF_SEQ_NO = new ParseField("if_seq_no");
    private static final ParseField IF_PRIMARY_TERM = new ParseField("if_primary_term");

    /**
     * Requests that are part of this request. It is only possible to add things that are both {@link ActionRequest}s and
     * {@link WriteRequest}s to this but java doesn't support syntax to declare that everything in the array has both types so we declare
     * the one with the least casts.
     */
    final List<DocWriteRequest<?>> requests = new ArrayList<>();
    private final Set<String> indices = new HashSet<>();
    List<Object> payloads = null;

    protected TimeValue timeout = BulkShardRequest.DEFAULT_TIMEOUT;
    private ActiveShardCount waitForActiveShards = ActiveShardCount.DEFAULT;
    private WriteRequest.RefreshPolicy refreshPolicy = WriteRequest.RefreshPolicy.NONE;
    private String globalPipeline;
    private String globalRouting;
    private String globalIndex;
    private String globalType;

    private long sizeInBytes = 0;

    public FastBulkRequest() {
    }

    public FastBulkRequest(@Nullable String globalIndex, @Nullable String globalType) {
        this.globalIndex = globalIndex;
        this.globalType = globalType;
    }

    /**
     * Adds a list of requests to be executed. Either index or delete requests.
     */
    public FastBulkRequest add(DocWriteRequest<?>... requests) {
        for (DocWriteRequest<?> request : requests) {
            add(request, null);
        }
        return this;
    }

    public FastBulkRequest add(DocWriteRequest<?> request) {
        return add(request, null);
    }

    /**
     * Add a request to the current FastBulkRequest.
     *
     * @param request Request to add
     * @param payload Optional payload
     * @return the current bulk request
     */
    public FastBulkRequest add(DocWriteRequest<?> request, @Nullable Object payload) {
        if (request instanceof IndexRequest) {
            add((IndexRequest) request, payload);
        } else if (request instanceof DeleteRequest) {
            add((DeleteRequest) request, payload);
        } else if (request instanceof UpdateRequest) {
            add((UpdateRequest) request, payload);
        } else {
            throw new IllegalArgumentException("No support for request [" + request + "]");
        }
        indices.add(request.index());
        return this;
    }

    /**
     * Adds a list of requests to be executed. Either index or delete requests.
     */
    public FastBulkRequest add(Iterable<DocWriteRequest<?>> requests) {
        for (DocWriteRequest<?> request : requests) {
            add(request);
        }
        return this;
    }

    /**
     * Adds an {@link IndexRequest} to the list of actions to execute. Follows the same behavior of {@link IndexRequest}
     * (for example, if no id is provided, one will be generated, or usage of the create flag).
     */
    public FastBulkRequest add(FastIndexRequest request) {
        return internalAdd(request, null);
    }

    public FastBulkRequest add(FastIndexRequest request, @Nullable Object payload) {
        return internalAdd(request, payload);
    }

    FastBulkRequest internalAdd(FastIndexRequest request, @Nullable Object payload) {
        Objects.requireNonNull(request, "'request' must not be null");
        applyGlobalMandatoryParameters(request);

        requests.add(request);
        addPayload(payload);
        // lack of source is validated in validate() method
        sizeInBytes += (request.source() != null ? request.source().length() : 0) + REQUEST_OVERHEAD;
        indices.add(request.index());
        return this;
    }

    /**
     * Adds an {@link UpdateRequest} to the list of actions to execute.
     */
    public FastBulkRequest add(FastUpdateRequest request) {
        return internalAdd(request, null);
    }

    public FastBulkRequest add(FastUpdateRequest request, @Nullable Object payload) {
        return internalAdd(request, payload);
    }

    FastBulkRequest internalAdd(FastUpdateRequest request, @Nullable Object payload) {
        Objects.requireNonNull(request, "'request' must not be null");
        applyGlobalMandatoryParameters(request);

        requests.add(request);
        addPayload(payload);
        if (request.doc() != null) {
            sizeInBytes += request.doc().source().length();
        }
        if (request.upsertRequest() != null) {
            sizeInBytes += request.upsertRequest().source().length();
        }
        if (request.script() != null) {
            sizeInBytes += request.script().getIdOrCode().length() * 2;
        }
        indices.add(request.index());
        return this;
    }

    /**
     * Adds an {@link DeleteRequest} to the list of actions to execute.
     */
    public FastBulkRequest add(DeleteRequest request) {
        return add(request, null);
    }

    public FastBulkRequest add(DeleteRequest request, @Nullable Object payload) {
        Objects.requireNonNull(request, "'request' must not be null");
        applyGlobalMandatoryParameters(request);

        requests.add(request);
        addPayload(payload);
        sizeInBytes += REQUEST_OVERHEAD;
        indices.add(request.index());
        return this;
    }

    private void addPayload(Object payload) {
        if (payloads == null) {
            if (payload == null) {
                return;
            }
            payloads = new ArrayList<>(requests.size() + 10);
            // add requests#size-1 elements to the payloads if it null (we add for an *existing* request)
            for (int i = 1; i < requests.size(); i++) {
                payloads.add(null);
            }
        }
        payloads.add(payload);
    }

    /**
     * The list of requests in this bulk request.
     */
    public List<DocWriteRequest<?>> requests() {
        return this.requests;
    }

    /**
     * The list of optional payloads associated with requests in the same order as the requests. Note, elements within
     * it might be null if no payload has been provided.
     * <p>
     * Note, if no payloads have been provided, this method will return null (as to conserve memory overhead).
     */
    @Nullable
    public List<Object> payloads() {
        return this.payloads;
    }

    /**
     * The number of actions in the bulk request.
     */
    public int numberOfActions() {
        return requests.size();
    }

    /**
     * The estimated size in bytes of the bulk request.
     */
    public long estimatedSizeInBytes() {
        return sizeInBytes;
    }

    /**
     * Adds a framed data in binary format
     */
    public FastBulkRequest add(byte[] data, int from, int length, XContentType xContentType) throws IOException {
        return add(data, from, length, null, null, xContentType);
    }

    /**
     * Adds a framed data in binary format
     */
    public FastBulkRequest add(byte[] data, int from, int length, @Nullable String defaultIndex, @Nullable String defaultType,
                               XContentType xContentType) throws IOException {
        return add(new BytesArray(data, from, length), defaultIndex, defaultType, xContentType);
    }

    /**
     * Adds a framed data in binary format
     */
    public FastBulkRequest add(BytesReference data, @Nullable String defaultIndex, @Nullable String defaultType,
                               XContentType xContentType) throws IOException {
        return add(data, defaultIndex, defaultType, null, null, null, null, null, true, xContentType);
    }

    /**
     * Adds a framed data in binary format
     */
    public FastBulkRequest add(BytesReference data, @Nullable String defaultIndex, @Nullable String defaultType, boolean allowExplicitIndex,
                               XContentType xContentType) throws IOException {
        return add(data, defaultIndex, defaultType, null, null, null, null, null, allowExplicitIndex, xContentType);
    }

    public FastBulkRequest add(BytesReference data, @Nullable String defaultIndex, @Nullable String defaultType, @Nullable String
            defaultRouting, @Nullable String[] defaultFields, @Nullable FetchSourceContext defaultFetchSourceContext, @Nullable String
                                       defaultPipeline, @Nullable Object payload, boolean allowExplicitIndex, XContentType xContentType) throws IOException {
        return add(data, defaultIndex, defaultType, null, null, null, null, null, null, allowExplicitIndex, xContentType);
    }

    public FastBulkRequest add(BytesReference data, @Nullable String defaultIndex, @Nullable String defaultType, @Nullable String
            defaultRouting, @Nullable String defaultShardNo, @Nullable String[] defaultFields, @Nullable FetchSourceContext defaultFetchSourceContext, @Nullable String
                                       defaultPipeline, @Nullable Object payload, boolean allowExplicitIndex, XContentType xContentType) throws IOException {
        XContent xContent = xContentType.xContent();
        int line = 0;
        int from = 0;
        int length = data.length();
        byte marker = xContent.streamSeparator();
        while (true) {
            int nextMarker = findNextMarker(marker, from, data, length);
            if (nextMarker == -1) {
                break;
            }
            line++;

            // now parse the action
            // EMPTY is safe here because we never call namedObject
            try (InputStream stream = data.slice(from, nextMarker - from).streamInput();
                 XContentParser parser = xContent
                         .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, stream)) {
                // move pointers
                from = nextMarker + 1;

                // Move to START_OBJECT
                XContentParser.Token token = parser.nextToken();
                if (token == null) {
                    continue;
                }
                if (token != XContentParser.Token.START_OBJECT) {
                    throw new IllegalArgumentException("Malformed action/metadata line [" + line + "], expected "
                            + XContentParser.Token.START_OBJECT + " but found [" + token + "]");
                }
                // Move to FIELD_NAME, that's the action
                token = parser.nextToken();
                if (token != XContentParser.Token.FIELD_NAME) {
                    throw new IllegalArgumentException("Malformed action/metadata line [" + line + "], expected "
                            + XContentParser.Token.FIELD_NAME + " but found [" + token + "]");
                }
                String action = parser.currentName();

                String index = defaultIndex;
                String type = defaultType;
                String id = null;
                String parent = null;
                String shardNo = defaultShardNo;
                String routing = defaultRouting;//去除es全局配置参数影响       valueOrDefault(defaultRouting, globalRouting);
                FetchSourceContext fetchSourceContext = defaultFetchSourceContext;
                String[] fields = defaultFields;
                String opType = null;
                long version = Versions.MATCH_ANY;
                VersionType versionType = VersionType.INTERNAL;
                long ifSeqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
                long ifPrimaryTerm = UNASSIGNED_PRIMARY_TERM;
                int retryOnConflict = 0;
                String pipeline = valueOrDefault(defaultPipeline, globalPipeline);

                // at this stage, next token can either be END_OBJECT (and use default index and type, with auto generated id)
                // or START_OBJECT which will have another set of parameters
                token = parser.nextToken();

                if (token == XContentParser.Token.START_OBJECT) {
                    String currentFieldName = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token.isValue()) {
                            if (INDEX.match(currentFieldName, parser.getDeprecationHandler())) {
                                if (!allowExplicitIndex) {
                                    throw new IllegalArgumentException("explicit index in bulk is not allowed");
                                }
                                index = parser.text();
                            } else if (TYPE.match(currentFieldName, parser.getDeprecationHandler())) {
                                type = parser.text();
                            } else if (ID.match(currentFieldName, parser.getDeprecationHandler())) {
                                id = parser.text();
                            } else if (ROUTING.match(currentFieldName, parser.getDeprecationHandler())) {
                                routing = parser.text();
                            } else if (SHARDNO.match(currentFieldName, parser.getDeprecationHandler())) {
                                /**
                                 * 按照url中指定的shard_no编号分配数据；
                                 *
                                 * 并且不允许doc中出现_shard_no参数，否则自定义分配数据到指定shard，依然存在长尾效应问题；
                                 */
                                String currShardNo = parser.text();
                                if ((shardNo == null || "".equals(shardNo)) && currShardNo != null && !"".equals(currShardNo)) {
                                    throw new IllegalArgumentException("_shard_no参数不允许在doc中使用，只能在url中指定shard_no，回避原始bulk分布式写入模型");
                                }
                            } else if (PARENT.match(currentFieldName, parser.getDeprecationHandler())) {
                                parent = parser.text();
                            } else if (OP_TYPE.match(currentFieldName, parser.getDeprecationHandler())) {
                                opType = parser.text();
                            } else if (VERSION.match(currentFieldName, parser.getDeprecationHandler())) {
                                version = parser.longValue();
                            } else if (VERSION_TYPE.match(currentFieldName, parser.getDeprecationHandler())) {
                                versionType = VersionType.fromString(parser.text());
                            } else if (IF_SEQ_NO.match(currentFieldName, parser.getDeprecationHandler())) {
                                ifSeqNo = parser.longValue();
                            } else if (IF_PRIMARY_TERM.match(currentFieldName, parser.getDeprecationHandler())) {
                                ifPrimaryTerm = parser.longValue();
                            } else if (RETRY_ON_CONFLICT.match(currentFieldName, parser.getDeprecationHandler())) {
                                retryOnConflict = parser.intValue();
                            } else if (PIPELINE.match(currentFieldName, parser.getDeprecationHandler())) {
                                pipeline = parser.text();
                            } else if (FIELDS.match(currentFieldName, parser.getDeprecationHandler())) {
                                throw new IllegalArgumentException("Action/metadata line [" + line + "] contains a simple value for"
                                        + " parameter [fields] while a list is expected");
                            } else if (SOURCE.match(currentFieldName, parser.getDeprecationHandler())) {
                                fetchSourceContext = FetchSourceContext.fromXContent(parser);
                            } else {
                                throw new IllegalArgumentException("Action/metadata line [" + line + "] contains an unknown parameter ["
                                        + currentFieldName + "]");
                            }
                        } else if (token == XContentParser.Token.START_ARRAY) {
                            if (FIELDS.match(currentFieldName, parser.getDeprecationHandler())) {
                                DEPRECATION_LOGGER.deprecated("Deprecated field [fields] used, expected [_source] instead");
                                List<Object> values = parser.list();
                                fields = values.toArray(new String[values.size()]);
                            } else {
                                throw new IllegalArgumentException("Malformed action/metadata line [" + line + "], expected a simple "
                                        + "value for field [" + currentFieldName + "] but found [" + token + "]");
                            }
                        } else if (token == XContentParser.Token.START_OBJECT && SOURCE.match(currentFieldName,
                                parser.getDeprecationHandler())) {
                            fetchSourceContext = FetchSourceContext.fromXContent(parser);
                        } else if (token != XContentParser.Token.VALUE_NULL) {
                            throw new IllegalArgumentException("Malformed action/metadata line [" + line
                                    + "], expected a simple value for field [" + currentFieldName + "] but found [" + token + "]");
                        }
                    }
                } else if (token != XContentParser.Token.END_OBJECT) {
                    throw new IllegalArgumentException("Malformed action/metadata line [" + line + "], expected "
                            + XContentParser.Token.START_OBJECT + " or " + XContentParser.Token.END_OBJECT + " but found [" + token + "]");
                }

                if ("delete".equals(action)) {
                    add(new DeleteRequest(index, type, id).routing(routing).parent(parent).version(version)
                            .versionType(versionType).setIfSeqNo(ifSeqNo).setIfPrimaryTerm(ifPrimaryTerm), payload);
                } else {
                    nextMarker = findNextMarker(marker, from, data, length);
                    if (nextMarker == -1) {
                        break;
                    }
                    line++;

                    // we use internalAdd so we don't fork here, this allows us not to copy over the big byte array to small chunks
                    // of index request.
                    if ("index".equals(action)) {
                        if (opType == null) {
                            internalAdd(new FastIndexRequest(index, type, id).shardNo(shardNo).routing(routing).parent(parent).version(version)
                                    .versionType(versionType).setPipeline(pipeline).setIfSeqNo(ifSeqNo).setIfPrimaryTerm(ifPrimaryTerm)
                                    .source(sliceTrimmingCarriageReturn(data, from, nextMarker, xContentType), xContentType), payload);
                        } else {
                            internalAdd(new FastIndexRequest(index, type, id).shardNo(shardNo).routing(routing).parent(parent).version(version)
                                    .versionType(versionType).create("create".equals(opType)).setPipeline(pipeline)
                                    .setIfSeqNo(ifSeqNo).setIfPrimaryTerm(ifPrimaryTerm)
                                    .source(sliceTrimmingCarriageReturn(data, from, nextMarker, xContentType), xContentType), payload);
                        }
                    } else if ("create".equals(action)) {
                        internalAdd(new FastIndexRequest(index, type, id).shardNo(shardNo).routing(routing).parent(parent).version(version)
                                .versionType(versionType).create(true).setPipeline(pipeline)
                                .setIfSeqNo(ifSeqNo).setIfPrimaryTerm(ifPrimaryTerm)
                                .source(sliceTrimmingCarriageReturn(data, from, nextMarker, xContentType), xContentType), payload);
                    } else if ("update".equals(action)) {
                        FastUpdateRequest updateRequest = new FastUpdateRequest(index, type, id).routing(routing)
                                .retryOnConflict(retryOnConflict)
                                .version(version).versionType(versionType)
                                .setIfSeqNo(ifSeqNo).setIfPrimaryTerm(ifPrimaryTerm)
                                .routing(routing)
                                .shardNo(shardNo)
                                .parent(parent);
                        // EMPTY is safe here because we never call namedObject
                        try (InputStream dataStream = sliceTrimmingCarriageReturn(data, from, nextMarker, xContentType).streamInput();
                             XContentParser sliceParser = xContent.createParser(NamedXContentRegistry.EMPTY,
                                     LoggingDeprecationHandler.INSTANCE, dataStream)) {
                            updateRequest.fromXContent(sliceParser);
                        }
                        if (fetchSourceContext != null) {
                            updateRequest.fetchSource(fetchSourceContext);
                        }
                        if (fields != null) {
                            updateRequest.fields(fields);
                        }

                        FastIndexRequest upsertRequest = updateRequest.upsertRequest();
                        if (upsertRequest != null) {
                            upsertRequest.version(version);
                            upsertRequest.versionType(versionType);
                            upsertRequest.setPipeline(defaultPipeline);
                        }
                        FastIndexRequest doc = updateRequest.doc();
                        if (doc != null) {
                            doc.version(version);
                            doc.versionType(versionType);
                        }

                        internalAdd(updateRequest, payload);
                    }
                    // move pointers
                    from = nextMarker + 1;
                }
            }
        }
        return this;
    }

    /**
     * Returns the sliced {@link BytesReference}. If the {@link XContentType} is JSON, the byte preceding the marker is checked to see
     * if it is a carriage return and if so, the BytesReference is sliced so that the carriage return is ignored
     */
    private BytesReference sliceTrimmingCarriageReturn(BytesReference bytesReference, int from, int nextMarker, XContentType xContentType) {
        final int length;
        if (XContentType.JSON == xContentType && bytesReference.get(nextMarker - 1) == (byte) '\r') {
            length = nextMarker - from - 1;
        } else {
            length = nextMarker - from;
        }
        return bytesReference.slice(from, length);
    }

    /**
     * Sets the number of shard copies that must be active before proceeding with the write.
     * See {@link ReplicationRequest#waitForActiveShards(ActiveShardCount)} for details.
     */
    public FastBulkRequest waitForActiveShards(ActiveShardCount waitForActiveShards) {
        this.waitForActiveShards = waitForActiveShards;
        return this;
    }

    /**
     * A shortcut for {@link #waitForActiveShards(ActiveShardCount)} where the numerical
     * shard count is passed in, instead of having to first call {@link ActiveShardCount#from(int)}
     * to get the ActiveShardCount.
     */
    public FastBulkRequest waitForActiveShards(final int waitForActiveShards) {
        return waitForActiveShards(ActiveShardCount.from(waitForActiveShards));
    }

    public ActiveShardCount waitForActiveShards() {
        return this.waitForActiveShards;
    }

    @Override
    public FastBulkRequest setRefreshPolicy(WriteRequest.RefreshPolicy refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
        return this;
    }

    @Override
    public WriteRequest.RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to {@code 1m}.
     */
    public final FastBulkRequest timeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    public final FastBulkRequest pipeline(String globalPipeline) {
        this.globalPipeline = globalPipeline;
        return this;
    }

    public final FastBulkRequest routing(String globalRouting) {
        this.globalRouting = globalRouting;
        return this;
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to {@code 1m}.
     */
    public final FastBulkRequest timeout(String timeout) {
        return timeout(TimeValue.parseTimeValue(timeout, null, getClass().getSimpleName() + ".timeout"));
    }

    public TimeValue timeout() {
        return timeout;
    }

    public String pipeline() {
        return globalPipeline;
    }

    public String routing() {
        return globalRouting;
    }

    private int findNextMarker(byte marker, int from, BytesReference data, int length) {
        for (int i = from; i < length; i++) {
            if (data.get(i) == marker) {
                return i;
            }
        }
        if (from != length) {
            throw new IllegalArgumentException("The bulk request must be terminated by a newline [\n]");
        }
        return -1;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (requests.isEmpty()) {
            validationException = addValidationError("no requests added", validationException);
        }
        for (DocWriteRequest<?> request : requests) {
            // We first check if refresh has been set
            if (((WriteRequest<?>) request).getRefreshPolicy() != WriteRequest.RefreshPolicy.NONE) {
                validationException = addValidationError(
                        "RefreshPolicy is not supported on an item request. Set it on the FastBulkRequest instead.", validationException);
            }
            ActionRequestValidationException ex = ((WriteRequest<?>) request).validate();
            if (ex != null) {
                if (validationException == null) {
                    validationException = new ActionRequestValidationException();
                }
                validationException.addValidationErrors(ex.validationErrors());
            }
        }

        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        waitForActiveShards = ActiveShardCount.readFrom(in);
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            requests.add(DocWriteRequest.readDocumentRequest(in));
        }
        refreshPolicy = WriteRequest.RefreshPolicy.readFrom(in);
        timeout = in.readTimeValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        waitForActiveShards.writeTo(out);
        out.writeVInt(requests.size());
        for (DocWriteRequest<?> request : requests) {
            DocWriteRequest.writeDocumentRequest(out, request);
        }
        refreshPolicy.writeTo(out);
        out.writeTimeValue(timeout);
    }

    @Override
    public String getDescription() {
        return "requests[" + requests.size() + "], indices[" + Strings.collectionToDelimitedString(indices, ", ") + "]";
    }

    private void applyGlobalMandatoryParameters(DocWriteRequest<?> request) {
        request.index(valueOrDefault(request.index(), globalIndex));
        request.type(valueOrDefault(request.type(), globalType));
    }

    private static String valueOrDefault(String value, String globalDefault) {
        if (Strings.isNullOrEmpty(value) && !Strings.isNullOrEmpty(globalDefault)) {
            return globalDefault;
        }
        return value;
    }
}
