package com.cgroup.esbulkrouting;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.support.single.instance.InstanceShardOperationRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

/**
 * Created by zzq on 2021/7/14.
 */
public class FastUpdateRequest extends InstanceShardOperationRequest<FastUpdateRequest> implements FastDocWriteRequest<FastUpdateRequest>, WriteRequest<FastUpdateRequest>, ToXContentObject {
    private static final DeprecationLogger DEPRECATION_LOGGER = new DeprecationLogger(LogManager.getLogger(FastUpdateRequest.class));

    @Nullable
    private String shardNo;

    @Override
    public FastUpdateRequest shardNo(String shardNo) {
        if (shardNo != null && shardNo.length() == 0) {
            this.shardNo = null;
        } else {
            this.shardNo = shardNo;
        }
        return this;
    }


    @Override
    public String shardNo() {
        return this.shardNo;
    }

    private String type;
    private String id;
    @Nullable
    private String routing;


    @Nullable
    private String parent;

    @Nullable
    Script script;

    private String[] fields;
    private FetchSourceContext fetchSourceContext;

    private long version = Versions.MATCH_ANY;
    private VersionType versionType = VersionType.INTERNAL;
    private int retryOnConflict = 0;
    private long ifSeqNo = UNASSIGNED_SEQ_NO;
    private long ifPrimaryTerm = UNASSIGNED_PRIMARY_TERM;


    private RefreshPolicy refreshPolicy = RefreshPolicy.NONE;

    private ActiveShardCount waitForActiveShards = ActiveShardCount.DEFAULT;

    private FastIndexRequest upsertRequest;

    private boolean scriptedUpsert = false;
    private boolean docAsUpsert = false;
    private boolean detectNoop = true;

    @Nullable
    private FastIndexRequest doc;

    public FastUpdateRequest() {

    }

    public FastUpdateRequest(String index, String type, String id) {
        super(index);
        this.type = type;
        this.id = id;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (version != Versions.MATCH_ANY && upsertRequest != null) {
            validationException = addValidationError("can't provide both upsert request and a version", validationException);
        }
        if(upsertRequest != null && upsertRequest.version() != Versions.MATCH_ANY) {
            validationException = addValidationError("can't provide version in upsert request", validationException);
        }
        if (Strings.isEmpty(type)) {
            validationException = addValidationError("type is missing", validationException);
        }
        if (Strings.isEmpty(id)) {
            validationException = addValidationError("id is missing", validationException);
        }

        if (versionType != VersionType.INTERNAL) {
            validationException = addValidationError("version type [" + versionType + "] is not supported by the update API",
                    validationException);
        } else {

            if (version != Versions.MATCH_ANY && retryOnConflict > 0) {
                validationException = addValidationError("can't provide both retry_on_conflict and a specific version",
                        validationException);
            }

            if (!versionType.validateVersionForWrites(version)) {
                validationException = addValidationError("illegal version value [" + version +
                        "] for version type [" + versionType.name() + "]", validationException);
            }
        }

        validationException = DocWriteRequest.validateSeqNoBasedCASParams(this, validationException);

        if (ifSeqNo != UNASSIGNED_SEQ_NO && retryOnConflict > 0) {
            validationException = addValidationError("compare and write operations can not be retried", validationException);
        }

        if (ifSeqNo != UNASSIGNED_SEQ_NO && docAsUpsert) {
            validationException = addValidationError("compare and write operations can not be used with upsert", validationException);
        }

        if (script == null && doc == null) {
            validationException = addValidationError("script or doc is missing", validationException);
        }
        if (script != null && doc != null) {
            validationException = addValidationError("can't provide both script and doc", validationException);
        }
        if (doc == null && docAsUpsert) {
            validationException = addValidationError("doc must be specified if doc_as_upsert is enabled", validationException);
        }

        return validationException;
    }

    /**
     * The type of the indexed document.
     */
    @Override
    public String type() {
        return type;
    }

    /**
     * Sets the type of the indexed document.
     */
    @Override
    public FastUpdateRequest type(String type) {
        this.type = type;
        return this;
    }

    /**
     * The id of the indexed document.
     */
    @Override
    public String id() {
        return id;
    }

    /**
     * Sets the id of the indexed document.
     */
    public FastUpdateRequest id(String id) {
        this.id = id;
        return this;
    }

    /**
     * Controls the shard routing of the request. Using this value to hash the shard
     * and not the id.
     */
    @Override
    public FastUpdateRequest routing(String routing) {
        if (routing != null && routing.length() == 0) {
            this.routing = null;
        } else {
            this.routing = routing;
        }
        return this;
    }

    /**
     * Controls the shard routing of the request. Using this value to hash the shard
     * and not the id.
     */
    @Override
    public String routing() {
        return this.routing;
    }

    /**
     * The parent id is used for the upsert request.
     */
    public FastUpdateRequest parent(String parent) {
        this.parent = parent;
        return this;
    }

    @Override
    public String parent() {
        return parent;
    }

    public ShardId getShardId() {
        return this.shardId;
    }

    public Script script() {
        return this.script;
    }

    /**
     * The script to execute. Note, make sure not to send different script each times and instead
     * use script params if possible with the same (automatically compiled) script.
     */
    public FastUpdateRequest script(Script script) {
        this.script = script;
        return this;
    }

    /**
     * @deprecated Use {@link #script()} instead
     */
    @Deprecated
    public String scriptString() {
        return this.script == null ? null : this.script.getIdOrCode();
    }

    /**
     * @deprecated Use {@link #script()} instead
     */
    @Deprecated
    public ScriptType scriptType() {
        return this.script == null ? null : this.script.getType();
    }

    /**
     * @deprecated Use {@link #script()} instead
     */
    @Deprecated
    public Map<String, Object> scriptParams() {
        return this.script == null ? null : this.script.getParams();
    }

    /**
     * The script to execute. Note, make sure not to send different script each
     * times and instead use script params if possible with the same
     * (automatically compiled) script.
     *
     * @deprecated Use {@link #script(Script)} instead
     */
    @Deprecated
    public FastUpdateRequest script(String script, ScriptType scriptType) {
        updateOrCreateScript(script, scriptType, null, null);
        return this;
    }

    /**
     * The script to execute. Note, make sure not to send different script each
     * times and instead use script params if possible with the same
     * (automatically compiled) script.
     *
     * @deprecated Use {@link #script(Script)} instead
     */
    @Deprecated
    public FastUpdateRequest script(String script) {
        updateOrCreateScript(script, ScriptType.INLINE, null, null);
        return this;
    }

    /**
     * The language of the script to execute.
     *
     * @deprecated Use {@link #script(Script)} instead
     */
    @Deprecated
    public FastUpdateRequest scriptLang(String scriptLang) {
        updateOrCreateScript(null, null, scriptLang, null);
        return this;
    }

    /**
     * @deprecated Use {@link #script()} instead
     */
    @Deprecated
    public String scriptLang() {
        return script == null ? null : script.getLang();
    }

    /**
     * Add a script parameter.
     *
     * @deprecated Use {@link #script(Script)} instead
     */
    @Deprecated
    public FastUpdateRequest addScriptParam(String name, Object value) {
        Script script = script();
        if (script == null) {
            HashMap<String, Object> scriptParams = new HashMap<>();
            scriptParams.put(name, value);
            updateOrCreateScript(null, null, null, scriptParams);
        } else {
            Map<String, Object> scriptParams = script.getParams();
            if (scriptParams == null) {
                scriptParams = new HashMap<>();
                scriptParams.put(name, value);
                updateOrCreateScript(null, null, null, scriptParams);
            } else {
                scriptParams.put(name, value);
            }
        }
        return this;
    }

    /**
     * Sets the script parameters to use with the script.
     *
     * @deprecated Use {@link #script(Script)} instead
     */
    @Deprecated
    public FastUpdateRequest scriptParams(Map<String, Object> scriptParams) {
        updateOrCreateScript(null, null, null, scriptParams);
        return this;
    }

    private void updateOrCreateScript(String scriptContent, ScriptType type, String lang, Map<String, Object> params) {
        Script script = script();
        if (script == null) {
            script = new Script(type == null ? ScriptType.INLINE : type, lang, scriptContent == null ? "" : scriptContent, params);
        } else {
            String newScriptContent = scriptContent == null ? script.getIdOrCode() : scriptContent;
            ScriptType newScriptType = type == null ? script.getType() : type;
            String newScriptLang = lang == null ? script.getLang() : lang;
            Map<String, Object> newScriptParams = params == null ? script.getParams() : params;
            script = new Script(newScriptType, newScriptLang, newScriptContent, newScriptParams);
        }
        script(script);
    }

    /**
     * The script to execute. Note, make sure not to send different script each
     * times and instead use script params if possible with the same
     * (automatically compiled) script.
     *
     * @deprecated Use {@link #script(Script)} instead
     */
    @Deprecated
    public FastUpdateRequest script(String script, ScriptType scriptType, @Nullable Map<String, Object> scriptParams) {
        this.script = new Script(scriptType, Script.DEFAULT_SCRIPT_LANG, script, scriptParams);
        return this;
    }

    /**
     * The script to execute. Note, make sure not to send different script each
     * times and instead use script params if possible with the same
     * (automatically compiled) script.
     *
     * @param script
     *            The script to execute
     * @param scriptLang
     *            The script language
     * @param scriptType
     *            The script type
     * @param scriptParams
     *            The script parameters
     *
     * @deprecated Use {@link #script(Script)} instead
     */
    @Deprecated
    public FastUpdateRequest script(String script, @Nullable String scriptLang, ScriptType scriptType,
                                @Nullable Map<String, Object> scriptParams) {
        this.script = new Script(scriptType, scriptLang, script, scriptParams);
        return this;
    }

    /**
     * Explicitly specify the fields that will be returned. By default, nothing is returned.
     * @deprecated Use {@link UpdateRequest#fetchSource(String[], String[])} instead
     */
    @Deprecated
    public FastUpdateRequest fields(String... fields) {
        this.fields = fields;
        return this;
    }

    /**
     * Indicate that _source should be returned with every hit, with an
     * "include" and/or "exclude" set which can include simple wildcard
     * elements.
     *
     * @param include
     *            An optional include (optionally wildcarded) pattern to filter
     *            the returned _source
     * @param exclude
     *            An optional exclude (optionally wildcarded) pattern to filter
     *            the returned _source
     */
    public FastUpdateRequest fetchSource(@Nullable String include, @Nullable String exclude) {
        FetchSourceContext context = this.fetchSourceContext == null ? FetchSourceContext.FETCH_SOURCE : this.fetchSourceContext;
        String[] includes = include == null ? Strings.EMPTY_ARRAY : new String[]{include};
        String[] excludes = exclude == null ? Strings.EMPTY_ARRAY : new String[]{exclude};
        this.fetchSourceContext = new FetchSourceContext(context.fetchSource(), includes, excludes);
        return this;
    }

    /**
     * Indicate that _source should be returned, with an
     * "include" and/or "exclude" set which can include simple wildcard
     * elements.
     *
     * @param includes
     *            An optional list of include (optionally wildcarded) pattern to
     *            filter the returned _source
     * @param excludes
     *            An optional list of exclude (optionally wildcarded) pattern to
     *            filter the returned _source
     */
    public FastUpdateRequest fetchSource(@Nullable String[] includes, @Nullable String[] excludes) {
        FetchSourceContext context = this.fetchSourceContext == null ? FetchSourceContext.FETCH_SOURCE : this.fetchSourceContext;
        this.fetchSourceContext = new FetchSourceContext(context.fetchSource(), includes, excludes);
        return this;
    }

    /**
     * Indicates whether the response should contain the updated _source.
     */
    public FastUpdateRequest fetchSource(boolean fetchSource) {
        FetchSourceContext context = this.fetchSourceContext == null ? FetchSourceContext.FETCH_SOURCE : this.fetchSourceContext;
        this.fetchSourceContext = new FetchSourceContext(fetchSource, context.includes(), context.excludes());
        return this;
    }

    /**
     * Explicitly set the fetch source context for this request
     */
    public FastUpdateRequest fetchSource(FetchSourceContext context) {
        this.fetchSourceContext = context;
        return this;
    }


    /**
     * Get the fields to be returned.
     * @deprecated Use {@link UpdateRequest#fetchSource()} instead
     */
    @Deprecated
    public String[] fields() {
        return fields;
    }

    /**
     * Gets the {@link FetchSourceContext} which defines how the _source should
     * be fetched.
     */
    public FetchSourceContext fetchSource() {
        return fetchSourceContext;
    }

    /**
     * Sets the number of retries of a version conflict occurs because the document was updated between
     * getting it and updating it. Defaults to 0.
     */
    public FastUpdateRequest retryOnConflict(int retryOnConflict) {
        this.retryOnConflict = retryOnConflict;
        return this;
    }

    public int retryOnConflict() {
        return this.retryOnConflict;
    }

    @Override
    public FastUpdateRequest version(long version) {
        this.version = version;
        return this;
    }

    @Override
    public long version() {
        return this.version;
    }

    @Override
    public FastUpdateRequest versionType(VersionType versionType) {
        this.versionType = versionType;
        return this;
    }

    @Override
    public VersionType versionType() {
        return this.versionType;
    }

    /**
     * only perform this update request if the document's modification was assigned the given
     * sequence number. Must be used in combination with {@link #setIfPrimaryTerm(long)}
     *
     * If the document last modification was assigned a different sequence number a
     * {@link org.elasticsearch.index.engine.VersionConflictEngineException} will be thrown.
     */
    @Override
    public FastUpdateRequest setIfSeqNo(long seqNo) {
        if (seqNo < 0 && seqNo != UNASSIGNED_SEQ_NO) {
            throw new IllegalArgumentException("sequence numbers must be non negative. got [" +  seqNo + "].");
        }
        ifSeqNo = seqNo;
        return this;
    }

    /**
     * only performs this update request if the document's last modification was assigned the given
     * primary term. Must be used in combination with {@link #setIfSeqNo(long)}
     *
     * If the document last modification was assigned a different term a
     * {@link org.elasticsearch.index.engine.VersionConflictEngineException} will be thrown.
     */
    @Override
    public FastUpdateRequest setIfPrimaryTerm(long term) {
        if (term < 0) {
            throw new IllegalArgumentException("primary term must be non negative. got [" + term + "]");
        }
        ifPrimaryTerm = term;
        return this;
    }

    /**
     * If set, only perform this update request if the document was last modification was assigned this sequence number.
     * If the document last modification was assigned a different sequence number a
     * {@link org.elasticsearch.index.engine.VersionConflictEngineException} will be thrown.
     */
    @Override
    public long ifSeqNo() {
        return ifSeqNo;
    }

    /**
     * If set, only perform this update request if the document was last modification was assigned this primary term.
     *
     * If the document last modification was assigned a different term a
     * {@link org.elasticsearch.index.engine.VersionConflictEngineException} will be thrown.
     */
    @Override
    public long ifPrimaryTerm() {
        return ifPrimaryTerm;
    }

    @Override
    public OpType opType() {
        return OpType.UPDATE;
    }

    @Override
    public FastUpdateRequest setRefreshPolicy(RefreshPolicy refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
        return this;
    }

    @Override
    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    public ActiveShardCount waitForActiveShards() {
        return this.waitForActiveShards;
    }

    /**
     * Sets the number of shard copies that must be active before proceeding with the write.
     * See {@link ReplicationRequest#waitForActiveShards(ActiveShardCount)} for details.
     */
    public FastUpdateRequest waitForActiveShards(ActiveShardCount waitForActiveShards) {
        this.waitForActiveShards = waitForActiveShards;
        return this;
    }

    /**
     * A shortcut for {@link #waitForActiveShards(ActiveShardCount)} where the numerical
     * shard count is passed in, instead of having to first call {@link ActiveShardCount#from(int)}
     * to get the ActiveShardCount.
     */
    public FastUpdateRequest waitForActiveShards(final int waitForActiveShards) {
        return waitForActiveShards(ActiveShardCount.from(waitForActiveShards));
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public FastUpdateRequest doc(FastIndexRequest doc) {
        this.doc = doc;
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public FastUpdateRequest doc(XContentBuilder source) {
        safeDoc().source(source);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public FastUpdateRequest doc(Map source) {
        safeDoc().source(source);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public FastUpdateRequest doc(Map source, XContentType contentType) {
        safeDoc().source(source, contentType);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public FastUpdateRequest doc(String source, XContentType xContentType) {
        safeDoc().source(source, xContentType);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public FastUpdateRequest doc(byte[] source, XContentType xContentType) {
        safeDoc().source(source, xContentType);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public FastUpdateRequest doc(byte[] source, int offset, int length, XContentType xContentType) {
        safeDoc().source(source, offset, length, xContentType);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified, the doc provided
     * is a field and value pairs.
     */
    public FastUpdateRequest doc(Object... source) {
        safeDoc().source(source);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified, the doc provided
     * is a field and value pairs.
     */
    public FastUpdateRequest doc(XContentType xContentType, Object... source) {
        safeDoc().source(xContentType, source);
        return this;
    }

    public FastIndexRequest doc() {
        return this.doc;
    }

    private FastIndexRequest safeDoc() {
        if (doc == null) {
            doc = new FastIndexRequest();
        }
        return doc;
    }

    /**
     * Sets the index request to be used if the document does not exists. Otherwise, a
     * {@link org.elasticsearch.index.engine.DocumentMissingException} is thrown.
     */
    public FastUpdateRequest upsert(FastIndexRequest upsertRequest) {
        this.upsertRequest = upsertRequest;
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public FastUpdateRequest upsert(XContentBuilder source) {
        safeUpsertRequest().source(source);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public FastUpdateRequest upsert(Map source) {
        safeUpsertRequest().source(source);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public FastUpdateRequest upsert(Map source, XContentType contentType) {
        safeUpsertRequest().source(source, contentType);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public FastUpdateRequest upsert(String source, XContentType xContentType) {
        safeUpsertRequest().source(source, xContentType);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public FastUpdateRequest upsert(byte[] source, XContentType xContentType) {
        safeUpsertRequest().source(source, xContentType);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public FastUpdateRequest upsert(byte[] source, int offset, int length, XContentType xContentType) {
        safeUpsertRequest().source(source, offset, length, xContentType);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists. The doc
     * includes field and value pairs.
     */
    public FastUpdateRequest upsert(Object... source) {
        safeUpsertRequest().source(source);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists. The doc
     * includes field and value pairs.
     */
    public FastUpdateRequest upsert(XContentType xContentType, Object... source) {
        safeUpsertRequest().source(xContentType, source);
        return this;
    }

    public FastIndexRequest upsertRequest() {
        return this.upsertRequest;
    }

    private FastIndexRequest safeUpsertRequest() {
        if (upsertRequest == null) {
            upsertRequest = new FastIndexRequest();
        }
        return upsertRequest;
    }

    /**
     * Should this update attempt to detect if it is a noop? Defaults to true.
     * @return this for chaining
     */
    public FastUpdateRequest detectNoop(boolean detectNoop) {
        this.detectNoop = detectNoop;
        return this;
    }

    /**
     * Should this update attempt to detect if it is a noop? Defaults to true.
     */
    public boolean detectNoop() {
        return detectNoop;
    }

    public FastUpdateRequest fromXContent(XContentParser parser) throws IOException {
        Script script = null;
        XContentParser.Token token = parser.nextToken();
        if (token == null) {
            return this;
        }
        String currentFieldName = null;
        int depth = 0;
        while (token != null) {
            token = parser.nextToken();
            // have to track depth to avoid premature end of parsing due to nested objects as
            // update request could have item ( script / scripted_upsert / upsert / doc etc ) at any nested level
            if (token == XContentParser.Token.START_OBJECT) {
                depth++;
            } else if (token == XContentParser.Token.END_OBJECT) {
                currentFieldName = null;
                if (depth == 0) {
                    break;
                }
                depth--;
                continue;
            }

            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if ("script".equals(currentFieldName)) {
                script = Script.parse(parser);
            } else if ("if_seq_no".equals(currentFieldName)) {
                setIfSeqNo(parser.longValue());
            } else if ("if_primary_term".equals(currentFieldName)) {
                setIfPrimaryTerm(parser.longValue());
            } else if ("scripted_upsert".equals(currentFieldName)) {
                scriptedUpsert = parser.booleanValue();
            } else if ("upsert".equals(currentFieldName)) {
                XContentBuilder builder = XContentFactory.contentBuilder(parser.contentType());
                builder.copyCurrentStructure(parser);
                safeUpsertRequest().source(builder);
            } else if ("doc".equals(currentFieldName)) {
                XContentBuilder docBuilder = XContentFactory.contentBuilder(parser.contentType());
                docBuilder.copyCurrentStructure(parser);
                safeDoc().source(docBuilder);
            } else if ("doc_as_upsert".equals(currentFieldName)) {
                docAsUpsert(parser.booleanValue());
            } else if ("detect_noop".equals(currentFieldName)) {
                detectNoop(parser.booleanValue());
            } else if ("fields".equals(currentFieldName)) {
                DEPRECATION_LOGGER.deprecated("Deprecated field [fields] used, expected [_source] instead");
                List<Object> fields = null;
                if (token == XContentParser.Token.START_ARRAY) {
                    fields = (List) parser.list();
                } else if (token.isValue()) {
                    fields = Collections.singletonList(parser.text());
                }
                if (fields != null) {
                    fields(fields.toArray(new String[fields.size()]));
                }
            } else if ("_source".equals(currentFieldName)) {
                fetchSourceContext = FetchSourceContext.fromXContent(parser);
            } else {
                DEPRECATION_LOGGER.deprecated("Unknown field [{}] used in {} which has no value and will not be accepted in future",
                        currentFieldName, FastUpdateRequest.class.getSimpleName());
            }

            // copyCurrentStructure / SomeObject.fromXContent moves current token to END_OBJECT
            if (parser.currentToken() == XContentParser.Token.END_OBJECT) {
                depth--;
            }
        }
        if (script != null) {
            this.script = script;
        }
        return this;
    }

    public boolean docAsUpsert() {
        return this.docAsUpsert;
    }

    public FastUpdateRequest docAsUpsert(boolean shouldUpsertDoc) {
        this.docAsUpsert = shouldUpsertDoc;
        return this;
    }

    public boolean scriptedUpsert(){
        return this.scriptedUpsert;
    }

    public FastUpdateRequest scriptedUpsert(boolean scriptedUpsert) {
        this.scriptedUpsert = scriptedUpsert;
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        waitForActiveShards = ActiveShardCount.readFrom(in);
        type = in.readString();
        id = in.readString();
        routing = in.readOptionalString();
        shardNo=in.readOptionalString();
        parent = in.readOptionalString();
        if (in.readBoolean()) {
            script = new Script(in);
        }
        retryOnConflict = in.readVInt();
        refreshPolicy = RefreshPolicy.readFrom(in);
        if (in.readBoolean()) {
            doc = new FastIndexRequest();
            doc.readFrom(in);
        }
        fields = in.readOptionalStringArray();
        fetchSourceContext = in.readOptionalWriteable(FetchSourceContext::new);
        if (in.readBoolean()) {
            upsertRequest = new FastIndexRequest();
            upsertRequest.readFrom(in);
        }
        docAsUpsert = in.readBoolean();
        version = in.readLong();
        versionType = VersionType.fromValue(in.readByte());
        if (in.getVersion().onOrAfter(Version.V_6_7_0)) {
            ifSeqNo = in.readZLong();
            ifPrimaryTerm = in.readVLong();
        }
        detectNoop = in.readBoolean();
        scriptedUpsert = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        waitForActiveShards.writeTo(out);
        out.writeString(type);
        out.writeString(id);
        out.writeOptionalString(routing);
        out.writeOptionalString(shardNo);
        out.writeOptionalString(parent);
        boolean hasScript = script != null;
        out.writeBoolean(hasScript);
        if (hasScript) {
            script.writeTo(out);
        }
        out.writeVInt(retryOnConflict);
        refreshPolicy.writeTo(out);
        if (doc == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            // make sure the basics are set
            doc.index(index);
            doc.type(type);
            doc.id(id);
            doc.writeTo(out);
        }
        out.writeOptionalStringArray(fields);
        out.writeOptionalWriteable(fetchSourceContext);
        if (upsertRequest == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            // make sure the basics are set
            upsertRequest.index(index);
            upsertRequest.type(type);
            upsertRequest.id(id);
            upsertRequest.writeTo(out);
        }
        out.writeBoolean(docAsUpsert);
        out.writeLong(version);
        out.writeByte(versionType.getValue());
        if (out.getVersion().onOrAfter(Version.V_6_7_0)) {
            out.writeZLong(ifSeqNo);
            out.writeVLong(ifPrimaryTerm);
        }
        out.writeBoolean(detectNoop);
        out.writeBoolean(scriptedUpsert);
    }

    @Override
    public void writeToStream(StreamOutput out) throws IOException {
        super.writeTo(out);
        waitForActiveShards.writeTo(out);
        out.writeString(type);
        out.writeString(id);
        out.writeOptionalString(routing);
        out.writeOptionalString(parent);
        boolean hasScript = script != null;
        out.writeBoolean(hasScript);
        if (hasScript) {
            script.writeTo(out);
        }
        out.writeVInt(retryOnConflict);
        refreshPolicy.writeTo(out);
        if (doc == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            // make sure the basics are set
            doc.index(index);
            doc.type(type);
            doc.id(id);
            doc.writeTo(out);
        }
        out.writeOptionalStringArray(fields);
        out.writeOptionalWriteable(fetchSourceContext);
        if (upsertRequest == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            // make sure the basics are set
            upsertRequest.index(index);
            upsertRequest.type(type);
            upsertRequest.id(id);
            upsertRequest.writeTo(out);
        }
        out.writeBoolean(docAsUpsert);
        out.writeLong(version);
        out.writeByte(versionType.getValue());
        if (out.getVersion().onOrAfter(Version.V_6_7_0)) {
            out.writeZLong(ifSeqNo);
            out.writeVLong(ifPrimaryTerm);
        }
        out.writeBoolean(detectNoop);
        out.writeBoolean(scriptedUpsert);
    }

    @Override
    public void readFromStream(StreamInput in) throws IOException {
        super.readFrom(in);
        waitForActiveShards = ActiveShardCount.readFrom(in);
        type = in.readString();
        id = in.readString();
        routing = in.readOptionalString();
        parent = in.readOptionalString();
        if (in.readBoolean()) {
            script = new Script(in);
        }
        retryOnConflict = in.readVInt();
        refreshPolicy = RefreshPolicy.readFrom(in);
        if (in.readBoolean()) {
            doc = new FastIndexRequest();
            doc.readFrom(in);
        }
        fields = in.readOptionalStringArray();
        fetchSourceContext = in.readOptionalWriteable(FetchSourceContext::new);
        if (in.readBoolean()) {
            upsertRequest = new FastIndexRequest();
            upsertRequest.readFrom(in);
        }
        docAsUpsert = in.readBoolean();
        version = in.readLong();
        versionType = VersionType.fromValue(in.readByte());
        if (in.getVersion().onOrAfter(Version.V_6_7_0)) {
            ifSeqNo = in.readZLong();
            ifPrimaryTerm = in.readVLong();
        }
        detectNoop = in.readBoolean();
        scriptedUpsert = in.readBoolean();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (docAsUpsert) {
            builder.field("doc_as_upsert", docAsUpsert);
        }
        if (doc != null) {
            XContentType xContentType = doc.getContentType();
            try (XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE, doc.source(), xContentType)) {
                builder.field("doc");
                builder.copyCurrentStructure(parser);
            }
        }

        if (ifSeqNo != UNASSIGNED_SEQ_NO) {
            builder.field("if_seq_no", ifSeqNo);
            builder.field("if_primary_term", ifPrimaryTerm);
        }

        if (script != null) {
            builder.field("script", script);
        }
        if (upsertRequest != null) {
            XContentType xContentType = upsertRequest.getContentType();
            try (XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE, upsertRequest.source(), xContentType)) {
                builder.field("upsert");
                builder.copyCurrentStructure(parser);
            }
        }
        if (scriptedUpsert) {
            builder.field("scripted_upsert", scriptedUpsert);
        }
        if (detectNoop == false) {
            builder.field("detect_noop", detectNoop);
        }
        if (fields != null) {
            builder.array("fields", fields);
        }
        if (fetchSourceContext != null) {
            builder.field("_source", fetchSourceContext);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        StringBuilder res = new StringBuilder()
                .append("update {[").append(index)
                .append("][").append(type)
                .append("][").append(id).append("]");
        res.append(", doc_as_upsert[").append(docAsUpsert).append("]");
        if (doc != null) {
            res.append(", doc[").append(doc).append("]");
        }
        if (script != null) {
            res.append(", script[").append(script).append("]");
        }
        if (upsertRequest != null) {
            res.append(", upsert[").append(upsertRequest).append("]");
        }
        res.append(", scripted_upsert[").append(scriptedUpsert).append("]");
        res.append(", detect_noop[").append(detectNoop).append("]");
        if (fields != null) {
            res.append(", fields[").append(Arrays.toString(fields)).append("]");
        }
        return res.append("}").toString();
    }
}
