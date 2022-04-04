package com.chartgroup.data;

import java.util.*;

/**
 * Created by zzq on 2022/4/4/004.
 */
public class DataSetQuery {
    private Map<String, List<Object>> paramMap = new HashMap<>(6);
    /**
     * 结果数格式
     */
    private ResultDataFormat resultDataFormat = null;

    public ResultDataFormat getResultDataFormat() {
        return resultDataFormat;
    }

    public void setResultDataFormat(ResultDataFormat resultDataFormat) {
        this.resultDataFormat = resultDataFormat;
    }

    public final void addParams(String paramName, Object... paramValues) {
        List<Object> params = paramMap.computeIfAbsent(paramName, k -> new ArrayList<>(6));
        params.addAll(Arrays.asList(paramValues));
    }

    public void setParams(String paramName, Object... paramValues) {
        this.paramMap.put(paramName, new ArrayList<>(Arrays.asList(paramValues)));
    }

    public Map<String, List<Object>> getParamMap() {
        return paramMap;
    }

    public List<Object> getParamMaps(String paramName) {
        return paramMap.getOrDefault(paramName, new ArrayList<Object>(0));
    }
}
