package com.db.mapper;

import com.google.common.base.CaseFormat;
import org.apache.commons.collections.CollectionUtils;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

/**
 * Created by zzq on 2022/3/19.
 */
public class BaseInsertProvider<T> extends BaseProvider {
    public String insertOne(final T t) {
        Class<?> clazz = t.getClass();
        StringBuilder sql = new StringBuilder();
        String value = clazz.getAnnotation(TableName.class).value();
        if (clazz.getDeclaredFields() != null && clazz.getDeclaredFields().length > 0) {
            sql.append("INSERT INTO  ");
            sql.append(value);
            sql.append(" ( ");

            StringBuilder fields = new StringBuilder();
            while (clazz != null) {
                for (Field field : clazz.getDeclaredFields()) {
                    if (((field.getModifiers() & java.lang.reflect.Modifier.STATIC) == java.lang.reflect.Modifier.STATIC)
                            || field.isAnnotationPresent(Invisible.class)) {
                        continue;
                    }
                    sql.append(formatFieldName(field.getName()) + ",");
                    fields.append("#{" + field.getName() + "},");
                }
                clazz = clazz.getSuperclass();
            }
            if (sql.lastIndexOf(",") != -1) {
                sql.deleteCharAt(sql.lastIndexOf(","));
            }
            if (fields.lastIndexOf(",") != -1) {
                fields.deleteCharAt(fields.lastIndexOf(","));
            }
            sql.append(") values (" + fields.toString() + ")");
        }
        return sql.toString();
    }

    public String insertSelective(final T t) throws IllegalAccessException {
        Class<?> clazz = t.getClass();
        StringBuilder sql = new StringBuilder();
        StringBuilder values = new StringBuilder();
        String value = clazz.getAnnotation(TableName.class).value();
        if (clazz.getDeclaredFields() != null && clazz.getDeclaredFields().length > 0) {
            sql.append("INSERT INTO  ");
            sql.append(value);
            sql.append(" ( ");
            while (clazz != null) {
                for (Field field : clazz.getDeclaredFields()) {
                    field.setAccessible(true);
                    if (((field.getModifiers() & java.lang.reflect.Modifier.STATIC) == java.lang.reflect.Modifier.STATIC)
                            || field.isAnnotationPresent(Invisible.class)
                            || field.get(t) == null) {
                        continue;
                    }
                    sql.append(formatFieldName(field.getName()) + ",");
                    values.append("#{" + field.getName() + "},");
                }
                clazz = clazz.getSuperclass();
            }
            if (sql.lastIndexOf(",") != -1) {
                sql.deleteCharAt(sql.lastIndexOf(","));
            }
            if (values.lastIndexOf(",") != -1) {
                values.deleteCharAt(values.lastIndexOf(","));
            }
            sql.append(") values (" + values.toString() + ")");
        }
        return sql.toString();
    }

    /**
     * List方式插入
     *
     * @param map
     * @return
     */
    public String insertListSelective(final Map<String, Object> map) {
        List<T> list = (List<T>) map.get("list");
        if (CollectionUtils.isEmpty(list)) {
            throw new RuntimeException("insertListSelective传入的参数为空");
        }
        Class<?> clazz = list.get(0).getClass();
        String value = clazz.getAnnotation(TableName.class).value();
        StringBuilder sql = new StringBuilder();
        try {
            // 采用循环调用insertSelective方式，多条insert操作
            for (int i = 0; i < list.size(); i++) {
                T t = list.get(i);
                StringBuilder sqlItem = new StringBuilder();
                clazz = t.getClass();
                StringBuilder values = new StringBuilder();
                if (clazz.getDeclaredFields() != null && clazz.getDeclaredFields().length > 0) {
                    sqlItem.append("INSERT INTO  ");
                    sqlItem.append(value);
                    sqlItem.append(" ( ");
                    while (clazz != null) {
                        for (Field field : clazz.getDeclaredFields()) {
                            field.setAccessible(true);
                            if (((field.getModifiers() & java.lang.reflect.Modifier.STATIC)
                                    == java.lang.reflect.Modifier.STATIC) || field.isAnnotationPresent(Invisible.class)
                                    || field.get(t) == null) {
                                continue;
                            }
                            sqlItem.append(CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, field.getName()) + ",");
                            values.append("#{list[" + i + "]." + field.getName() + "},");
                        }
                        clazz = clazz.getSuperclass();
                    }

                    if (sqlItem.lastIndexOf(",") != -1) {
                        sqlItem.deleteCharAt(sqlItem.lastIndexOf(","));
                    }
                    if (values.lastIndexOf(",") != -1) {
                        values.deleteCharAt(values.lastIndexOf(","));
                    }
                    sqlItem.append(") values (" + values.toString() + ")");
                }
                sql.append(sqlItem.toString()).append(";");
            }
        } catch (IllegalAccessException e) {
            // 事务保障 需要使用运行时异常
            throw new RuntimeException("insertListSelective落库时抛出IllegalAccessException异常", e);
        }
        return sql.toString();
    }
}
