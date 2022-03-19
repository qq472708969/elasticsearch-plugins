package com.db.mapper;


/**
 * Created by zzq on 2022/3/19.
 */
public enum ConditionTypeEnum {
    EQ("等于", "="),
    NE("不等于", "!="),
    GT("大于", ">"),
    LT("小于", "<"),
    GE("大于等于", ">="),
    LE("小于等于", "<="),

    IN("包含", "in"),

    //通配符传入value值中即可，尽量只使用后缀匹配
    LIKE("通配符", "like"),

    SORT_ASC("升序排序", "asc"),
    SORT_DESC("降序排序", "desc");

    //LIMIT("数量限制", "limit");
    ConditionTypeEnum(String desc, String operator) {
        this.desc = desc;
        this.operator = operator;
    }

    //描述
    private String desc;
    //操作符
    private String operator;

    public String getDesc() {
        return desc;
    }

    public String getOperator() {
        return operator;
    }
}
