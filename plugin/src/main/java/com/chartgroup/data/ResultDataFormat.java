package com.chartgroup.data;

/**
 * Created by zzq on 2022/4/4/004.
 */
public class ResultDataFormat {
    /**
     * 格式化类型：数值，表示格式化为数值
     */
    public static final String TYPE_NUMBER = "NUMBER";

    /**
     * 格式化类型：字符串，表示格式化为字符串
     */
    public static final String TYPE_STRING = "STRING";

    /**
     * 格式化类型：无，表示不格式化，保持原类型
     */
    public static final String TYPE_NONE = "NONE";

    /**
     * 日期格式化类型
     */
    private String dateType = TYPE_STRING;

    /**
     * 时间格式化类型
     */
    private String timeType = TYPE_STRING;

    /**
     * 时间戳格式化类型
     */
    private String timestampType = TYPE_STRING;

    //=====下面是处理时间=====

    /**
     * 默认日期格式：yyyy-MM-dd
     */
    public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";

    /**
     * 默认时间格式：hh:mm:ss
     */
    public static final String DEFAULT_TIME_FORMAT = "HH:mm:ss";

    /**
     * 默认时间戳格式：yyyy-MM-dd hh:mm:ss
     */
    public static final String DEFAULT_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";

    /**
     * 日期格式
     */
    private String dateFormat = DEFAULT_DATE_FORMAT;

    /**
     * 时间格式
     */
    private String timeFormat = DEFAULT_TIME_FORMAT;

    /**
     * 时间戳格式
     */
    private String timestampFormat = DEFAULT_TIMESTAMP_FORMAT;

    public void setDateType(String dateType) {
        this.dateType = dateType;
    }

    public void setTimeType(String timeType) {
        this.timeType = timeType;
    }

    public void setTimestampType(String timestampType) {
        this.timestampType = timestampType;
    }

    public void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
    }

    public void setTimeFormat(String timeFormat) {
        this.timeFormat = timeFormat;
    }

    public void setTimestampFormat(String timestampFormat) {
        this.timestampFormat = timestampFormat;
    }
}
