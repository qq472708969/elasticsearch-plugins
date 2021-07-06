package com.cgroup.esupdatesegment;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Created by zzq on 2021/6/11.
 */
public class LoadSegmentActionResponse extends ActionResponse implements ToXContentFragment {
    public long removeDocumentCount;

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("msg", "Load—Segment—OK");
        builder.field("delete_count", removeDocumentCount);
        builder.endObject();
        return builder;
    }
}
