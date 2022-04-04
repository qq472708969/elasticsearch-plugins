package com.chartgroup.data.example;

import com.chartgroup.AbstractIdentifiable;
import com.chartgroup.data.DataSet;
import com.chartgroup.data.DataSetQuery;
import com.chartgroup.data.DataSetResult;
import com.chartgroup.data.ResultDataFormat;

import java.util.List;
import java.util.Map;

/**
 * Created by zzq on 2022/4/4/004.
 */
public class SqlDataSet extends AbstractIdentifiable implements DataSet {
    @Override
    public String getName() {
        return "sql";
    }

    @Override
    public DataSetResult getResult(DataSetQuery query) {
        Map<String, List<Object>> paramMap = query.getParamMap();
        ResultDataFormat resultDataFormat = query.getResultDataFormat();

        return null;
    }
}
