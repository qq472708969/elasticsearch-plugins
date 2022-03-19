package com.db.mapper;

import lombok.Data;

/**
 * Created by zzq on 2022/3/19.
 */
@Data
public class Condition<T> {

    /**
     * 类型
     */
    private ConditionTypeEnum type;

    /**
     * 字段名称
     */
    private String fieldName;

    /**
     * 字段值
     */
    private T value;
}
