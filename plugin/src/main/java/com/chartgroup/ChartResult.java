package com.chartgroup;

import com.chartgroup.data.DataSetResult;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zzq on 2022/4/4/004.
 */
public class ChartResult extends AbstractIdentifiable implements Serializable {

    private List<DataSetResult> dataSetResults = new ArrayList<>(8);

    public List<DataSetResult> getDataSetResults() {
        return dataSetResults;
    }

    public void setDataSetResults(List<DataSetResult> dataSetResults) {
        this.dataSetResults = dataSetResults;
    }

    public void addDataSetResult(DataSetResult result) {
        dataSetResults.add(result);
    }
}
