package com.db.mapper;

import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;

/**
 * Created by zzq on 2022/3/19.
 */
public class BaseUpdateProvider<T> extends BaseProvider {

    public String updateSelectiveById(final T t, final Long id) throws NullPointerException, IllegalAccessException {
        StringBuilder sql = new StringBuilder();
        buildBaseUpdateStatement(t, sql, false);
        sql.append(" WHERE id = " + id + " AND valid = 1");
        return sql.toString();
    }

    public String updateById(final T t, final Long id) throws NullPointerException, IllegalAccessException {
        StringBuilder sql = new StringBuilder();
        buildBaseUpdateStatement(t, sql, true);
        sql.append(" WHERE id = " + id + " AND valid = 1");
        return sql.toString();
    }

    public String updateSelective(final T newObj, final T oldObj) throws NullPointerException, IllegalAccessException {
        StringBuilder sql = new StringBuilder();
        buildBaseUpdateStatement(newObj, sql, false);
        sql.append(buildConditionStatement(oldObj, "param2.", false));
        return sql.toString();
    }

    private void buildBaseUpdateStatement(T t, StringBuilder sql, boolean updateNull) throws IllegalAccessException {
        StringBuilder fields = new StringBuilder();
        Class<?> clazz = t.getClass();
        String value = clazz.getAnnotation(TableName.class).value();
        sql.append("UPDATE ");
        sql.append(value);
        for (; clazz != null; ) {
            for (Field field : clazz.getDeclaredFields()) {
                field.setAccessible(true);
                //静态字段跳出
                if ((field.getModifiers() & java.lang.reflect.Modifier.STATIC) == java.lang.reflect.Modifier.STATIC) {
                    continue;
                }
                //不需要访问的字段跳出
                if (field.isAnnotationPresent(Invisible.class)) {
                    continue;
                }
                //id字段跳出
                if ("id".equals(field.getName())) {
                    continue;
                }
                //值为null的字段跳出
                if (field.get(t) == null && !updateNull) {
                    continue;
                }
                fields.append(formatFieldName(field.getName()) + "=#{param1." + field.getName() + "} , ");
            }
            clazz = clazz.getSuperclass();
        }
        if (StringUtils.isBlank(fields.toString())) {
            throw new RuntimeException("更新对象为空");
        }
        sql.append(" SET " + fields.toString());
        if (sql.lastIndexOf(",") != -1) {
            sql.deleteCharAt(sql.lastIndexOf(","));
        }
    }

}
