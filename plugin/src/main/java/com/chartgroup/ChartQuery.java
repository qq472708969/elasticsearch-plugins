package com.chartgroup;

import com.chartgroup.data.DataSetQuery;
import com.chartgroup.data.ResultDataFormat;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by zzq on 2022/4/4/004.
 */
public class ChartQuery {
    private Map<String, DataSetQuery> dataSetQueryMap = new HashMap<>(6);

    public Map<String, DataSetQuery> getDataSetQueryMap() {
        return dataSetQueryMap;
    }

    public void setDataSetQueryMap(Map<String, DataSetQuery> dataSetQueryMap) {
        this.dataSetQueryMap = dataSetQueryMap;
    }

    public DataSetQuery getQuery(String id) {
        return dataSetQueryMap.get(id);
    }

    public void addQuery(String id, DataSetQuery query) {
        dataSetQueryMap.put(id, query);
    }
}
