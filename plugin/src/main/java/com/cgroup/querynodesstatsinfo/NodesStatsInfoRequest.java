package com.cgroup.querynodesstatsinfo;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Map;

/**
 * Created by zzq on 2021/8/31.
 * <p>
 * 用于过滤自定义属性的节点
 */
public class NodesStatsInfoRequest extends BaseNodesRequest<NodesStatsInfoRequest> {

    private CommonStatsFlags indices = new CommonStatsFlags();
    private boolean os;
    private boolean process;
    private boolean jvm;
    private boolean threadPool;
    private boolean fs;
    private boolean transport;
    private boolean http;
    private boolean breaker;
    private boolean script;
    private boolean discovery;
    private boolean ingest;
    private boolean adaptiveSelection;

    public NodesStatsInfoRequest() {
    }

    /**
     * Get stats from nodes based on the nodes ids specified. If none are passed, stats
     * for all nodes will be returned.
     */
    public NodesStatsInfoRequest(String... nodesIds) {
        super(nodesIds);
    }

    public Map<String, Object> attr;

    public Map<String, Object> getAttr() {
        return attr;
    }

    public void setAttr(Map<String, Object> attr) {
        this.attr = attr;
    }

    /**
     * Sets all the request flags.
     */
    public NodesStatsInfoRequest all() {
        this.indices.all();
        this.os = true;
        this.process = true;
        this.jvm = true;
        this.threadPool = true;
        this.fs = true;
        this.transport = true;
        this.http = true;
        this.breaker = true;
        this.script = true;
        this.discovery = true;
        this.ingest = true;
        this.adaptiveSelection = true;
        return this;
    }

    /**
     * Clears all the request flags.
     */
    public NodesStatsInfoRequest clear() {
        this.indices.clear();
        this.os = false;
        this.process = false;
        this.jvm = false;
        this.threadPool = false;
        this.fs = false;
        this.transport = false;
        this.http = false;
        this.breaker = false;
        this.script = false;
        this.discovery = false;
        this.ingest = false;
        this.adaptiveSelection = false;
        return this;
    }

    public CommonStatsFlags indices() {
        return indices;
    }

    public NodesStatsInfoRequest indices(CommonStatsFlags indices) {
        this.indices = indices;
        return this;
    }

    /**
     * Should indices stats be returned.
     */
    public NodesStatsInfoRequest indices(boolean indices) {
        if (indices) {
            this.indices.all();
        } else {
            this.indices.clear();
        }
        return this;
    }

    /**
     * Should the node OS be returned.
     */
    public boolean os() {
        return this.os;
    }

    /**
     * Should the node OS be returned.
     */
    public NodesStatsInfoRequest os(boolean os) {
        this.os = os;
        return this;
    }

    /**
     * Should the node Process be returned.
     */
    public boolean process() {
        return this.process;
    }

    /**
     * Should the node Process be returned.
     */
    public NodesStatsInfoRequest process(boolean process) {
        this.process = process;
        return this;
    }

    /**
     * Should the node JVM be returned.
     */
    public boolean jvm() {
        return this.jvm;
    }

    /**
     * Should the node JVM be returned.
     */
    public NodesStatsInfoRequest jvm(boolean jvm) {
        this.jvm = jvm;
        return this;
    }

    /**
     * Should the node Thread Pool be returned.
     */
    public boolean threadPool() {
        return this.threadPool;
    }

    /**
     * Should the node Thread Pool be returned.
     */
    public NodesStatsInfoRequest threadPool(boolean threadPool) {
        this.threadPool = threadPool;
        return this;
    }

    /**
     * Should the node file system stats be returned.
     */
    public boolean fs() {
        return this.fs;
    }

    /**
     * Should the node file system stats be returned.
     */
    public NodesStatsInfoRequest fs(boolean fs) {
        this.fs = fs;
        return this;
    }

    /**
     * Should the node Transport be returned.
     */
    public boolean transport() {
        return this.transport;
    }

    /**
     * Should the node Transport be returned.
     */
    public NodesStatsInfoRequest transport(boolean transport) {
        this.transport = transport;
        return this;
    }

    /**
     * Should the node HTTP be returned.
     */
    public boolean http() {
        return this.http;
    }

    /**
     * Should the node HTTP be returned.
     */
    public NodesStatsInfoRequest http(boolean http) {
        this.http = http;
        return this;
    }

    public boolean breaker() {
        return this.breaker;
    }

    /**
     * Should the node's circuit breaker stats be returned.
     */
    public NodesStatsInfoRequest breaker(boolean breaker) {
        this.breaker = breaker;
        return this;
    }

    public boolean script() {
        return script;
    }

    public NodesStatsInfoRequest script(boolean script) {
        this.script = script;
        return this;
    }


    public boolean discovery() {
        return this.discovery;
    }

    /**
     * Should the node's discovery stats be returned.
     */
    public NodesStatsInfoRequest discovery(boolean discovery) {
        this.discovery = discovery;
        return this;
    }

    public boolean ingest() {
        return ingest;
    }

    /**
     * Should ingest statistics be returned.
     */
    public NodesStatsInfoRequest ingest(boolean ingest) {
        this.ingest = ingest;
        return this;
    }

    public boolean adaptiveSelection() {
        return adaptiveSelection;
    }

    /**
     * Should adaptiveSelection statistics be returned.
     */
    public NodesStatsInfoRequest adaptiveSelection(boolean adaptiveSelection) {
        this.adaptiveSelection = adaptiveSelection;
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        attr = in.readMap();
        indices = new CommonStatsFlags(in);
        os = in.readBoolean();
        process = in.readBoolean();
        jvm = in.readBoolean();
        threadPool = in.readBoolean();
        fs = in.readBoolean();
        transport = in.readBoolean();
        http = in.readBoolean();
        breaker = in.readBoolean();
        script = in.readBoolean();
        discovery = in.readBoolean();
        ingest = in.readBoolean();
        if (in.getVersion().onOrAfter(Version.V_6_1_0)) {
            adaptiveSelection = in.readBoolean();
        } else {
            adaptiveSelection = false;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        indices.writeTo(out);
        out.writeMap(attr);
        out.writeBoolean(os);
        out.writeBoolean(process);
        out.writeBoolean(jvm);
        out.writeBoolean(threadPool);
        out.writeBoolean(fs);
        out.writeBoolean(transport);
        out.writeBoolean(http);
        out.writeBoolean(breaker);
        out.writeBoolean(script);
        out.writeBoolean(discovery);
        out.writeBoolean(ingest);
        if (out.getVersion().onOrAfter(Version.V_6_1_0)) {
            out.writeBoolean(adaptiveSelection);
        }
    }
}
