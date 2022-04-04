package com.chartgroup.data;

import com.chartgroup.AbstractIdentifiable;

import java.io.Serializable;

/**
 * Created by zzq on 2022/4/4/004.
 */
public class ChartDataSet extends AbstractIdentifiable implements Serializable {
    /**
     * 数据集
     */
    private DataSet dataSet;

    public DataSetResult getDataSetResult(DataSetQuery query) {
        return dataSet.getResult(query);
    }
}
