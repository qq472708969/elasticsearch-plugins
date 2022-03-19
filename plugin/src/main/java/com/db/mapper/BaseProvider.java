package com.db.mapper;

import com.google.common.base.CaseFormat;
import com.google.common.base.Joiner;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.util.List;

/**
 * Created by zzq on 2022/3/19.
 */
@Slf4j
public class BaseProvider<T> {
    protected String buildConditionStatement(T t, String extra, boolean allowEmpty) throws IllegalAccessException {
        Class<?> clazz = t.getClass();
        StringBuilder conditionStr = new StringBuilder(64);
        while (clazz != null) {
            for (Field field : clazz.getDeclaredFields()) {
                field.setAccessible(true);
                if (((field.getModifiers() & java.lang.reflect.Modifier.STATIC) == java.lang.reflect.Modifier.STATIC)
                        || field.isAnnotationPresent(Invisible.class)
                        || field.get(t) == null) {
                    continue;
                }
                conditionStr.append(formatFieldName(field.getName()) + "=#{"
                        + extra + field.getName() + "} AND ");
            }
            clazz = clazz.getSuperclass();
        }
        String conditionSort = buildExtraCondition(t, conditionStr);
        if (conditionStr.lastIndexOf("AND") != -1) {
            conditionStr.delete(conditionStr.lastIndexOf("AND"), conditionStr.lastIndexOf("AND") + 3);
        }
        String sql = conditionStr.toString();
        if (!allowEmpty && StringUtils.isBlank(sql)) {
            throw new RuntimeException("构造查询条件为空");
        } else if (!StringUtils.isBlank(sql)) {
            return " WHERE " + conditionStr.toString() + conditionSort;
        } else {
            return "";
        }
    }

    protected String formatFieldName(String src) {
        return CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, src);
    }

    /**
     * 构造额外的查询条件和排序条件，格式例如
     * " and gmt_created >= '2019-01-14 and gmt_create <= 2019-02-03'"
     *
     * @param t
     * @return
     */
    protected String buildExtraCondition(T t, StringBuilder tmp) {
        try {
            Field field = t.getClass().getField("extra");
            StringBuilder conditionSort = new StringBuilder(64);
            if (field != null) {
                Object obj = field.get(t);
                if (obj != null && obj instanceof List) {
                    List<Condition> list = (List<Condition>) (obj);
                    for (Condition condition : list) {
                        switch (condition.getType()) {
                            case EQ:
                            case NE:
                            case GT:
                            case GE:
                            case LT:
                            case LE:
                            case LIKE:
                                Object value = condition.getValue();
                                if (value == null) {
                                    continue;
                                }
                                if (value instanceof String) {
                                    tmp.append(formatFieldName(condition.getFieldName())).append(" ").append(condition.getType().getOperator());
                                    tmp.append(" ").append("'").append(value).append("' AND ");
                                } else if (value instanceof Number) {
                                    tmp.append(formatFieldName(condition.getFieldName())).append(" ").append(condition.getType().getOperator());
                                    tmp.append(" ").append(value).append(" AND ");
                                }
                                break;
                            case IN:
                                value = condition.getValue();
                                if (value == null || !(value instanceof Iterable)) {
                                    break;
                                }

                                StringBuilder sb = new StringBuilder(128);
                                String values = sb.append("('").append(Joiner.on("','").skipNulls().join((Iterable) value)).append("')").toString();
                                tmp.append(formatFieldName(condition.getFieldName())).append(" ").append(condition.getType().getOperator());
                                tmp.append(" ").append(values).append(" AND ");
                                break;
                            case SORT_ASC:
                            case SORT_DESC:
                                if (conditionSort.length() != 0) {
                                    conditionSort.append(",");
                                }
                                conditionSort.append(" ").append(formatFieldName(condition.getFieldName())).append(" ").append(condition.getType().getOperator());
                                break;
                            default:
                                break;

                        }
                    }
                }
            }
            if (conditionSort.length() != 0) {
                return " ORDER BY" + conditionSort.toString();
            }

        } catch (Exception e) {
            log.debug("处理extra字段时异常", e);
        }
        return "";
    }
}
