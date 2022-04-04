package com.chartgroup.data;

import java.util.List;
import java.util.Map;

/**
 * Created by zzq on 2022/4/4/004.
 */
public class DataSetResult {
    public List<Map<String, Object>> results;

    public List<Map<String, Object>> getResults() {
        return results;
    }

    public void setResults(List<Map<String, Object>> results) {
        this.results = results;
    }
}
