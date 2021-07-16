package com.cgroup.esbulkrouting;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Created by zzq on 2021/7/14.
 */
public interface FastDocWriteRequest<T> extends DocWriteRequest<T> {
    T shardNo(String routing);

    String shardNo();

    void writeToStream(StreamOutput out) throws IOException;

    void readFromStream(StreamInput in) throws IOException;
}
