package com.chartgroup;

import com.chartgroup.data.ChartDataSet;
import com.chartgroup.data.DataSetQuery;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/**
 * Created by zzq on 2022/4/4/004.
 * <p>
 * 图表组件
 */
public class ChartDefinition extends AbstractIdentifiable implements Serializable {
    /**
     * 图表名称
     */
    protected String name;
    /**
     * 图表的属性
     */
    private Map<String, Object> attributes = Collections.emptyMap();

    /**
     * 图表数据集
     */
    private Map<String, ChartDataSet> chartDataSets = Collections.emptyMap();

    public Map<String, ChartDataSet> getChartDataSets() {
        return chartDataSets;
    }

    public void setChartDataSets(Map<String, ChartDataSet> chartDataSets) {
        this.chartDataSets = chartDataSets;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }
}
