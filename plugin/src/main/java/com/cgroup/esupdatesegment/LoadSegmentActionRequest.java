package com.cgroup.esupdatesegment;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zzq on 2021/6/11.
 */
public class LoadSegmentActionRequest extends ActionRequest {

    public String indexName;

    public String indexUUID;

    public int shardIdNo;

    public String segmentDirs;

    public String documentPrimeKey;

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public void check() throws Exception {
        check(indexName, "indexName is empty");
        check(indexUUID, "uuid is empty");

        if (shardIdNo < 0) {
            throw new Exception("shardId <0, shardId:" + shardIdNo);
        }

        check(segmentDirs, "appendSegmentDirs is empty");
        for (String dir : segmentDirs.split(",")) {
            check(dir, "dir is empty");
        }
    }

    public List<String> getSegmentDirs() {
        List<String> dirs = new ArrayList<>();

        for (String dir : segmentDirs.split(",")) {
            dirs.add(dir);
        }

        return dirs;
    }

    private void check(String str, String errMsg) throws Exception {
        if (isBlank(str)) {
            throw new Exception(errMsg);
        }
    }

    private boolean isBlank(String str) {
        if (str == null) {
            return true;
        }

        if (str.trim().length() == 0) {
            return true;
        }

        return false;
    }
}
