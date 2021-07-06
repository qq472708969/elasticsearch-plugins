package com.cgroup.esupdatesegment;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.info.ClusterInfoRequest;

/**
 * Created by zzq on 2021/6/20/020.
 */
public class LoadSegmentActionClusterInfoRequest extends ClusterInfoRequest<LoadSegmentActionClusterInfoRequest> {
    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
