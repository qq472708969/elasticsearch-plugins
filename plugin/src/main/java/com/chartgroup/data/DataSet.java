package com.chartgroup.data;

import com.chartgroup.Identifiable;

import java.io.Serializable;

/**
 * Created by zzq on 2022/4/4/004.
 */
public interface DataSet extends Identifiable, Serializable {
    /**
     * 数据集名称。
     *
     * @return
     */
    String getName();

    /**
     * 获取结果
     *
     * @param query
     * @return
     */
    DataSetResult getResult(DataSetQuery query);
}
