package com.chartgroup;

import com.chartgroup.data.ChartDataSet;
import com.chartgroup.data.DataSetQuery;
import com.chartgroup.data.DataSetResult;

import java.util.Map;
import java.util.Set;

/**
 * Created by zzq on 2022/4/4/004.
 */
public abstract class Chart<ChartExample> extends ChartDefinition {
    public abstract ChartExample convertToChartExample(ChartResult chartResult);

    public ChartResult getResult(ChartQuery chartQuery) {
        ChartResult chartResult = new ChartResult();
        Set<Map.Entry<String, ChartDataSet>> entrySet = getChartDataSets().entrySet();
        for (Map.Entry<String, ChartDataSet> entry : entrySet) {
            DataSetQuery dataSetQuery = chartQuery.getQuery(entry.getKey());
            DataSetResult dataSetResult = entry.getValue().getDataSetResult(dataSetQuery);
            chartResult.addDataSetResult(dataSetResult);
        }
        return chartResult;
    }
}
