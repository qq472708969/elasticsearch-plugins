package com.db.mapper;

import com.google.common.base.CaseFormat;

import java.lang.reflect.Field;

/**
 * Created by zzq on 2022/3/19.
 */
public class BaseDeleteProvider<T> {

    public String delete (final T obj) throws NullPointerException, IllegalAccessException {
        StringBuilder sql = new StringBuilder();
        Class<?> clazz = obj.getClass();
        String value = clazz.getAnnotation(TableName.class).value();
        sql.append("DELETE FROM ");
        sql.append(value);
        StringBuffer tmp = new StringBuffer();
        if (clazz.getDeclaredFields() != null && clazz.getDeclaredFields().length > 0) {
            for (Field field : clazz.getDeclaredFields()) {
                if ((field.getModifiers() & java.lang.reflect.Modifier.STATIC) == java.lang.reflect.Modifier.STATIC) {
                    continue;
                }
                if (!field.isAnnotationPresent(Invisible.class)) {
                    boolean old = field.isAccessible();
                    if (old && field.get(obj) != null) {
                        tmp.append(CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, field
                                .getName()) + "=#{" + field.getName() + "} AND ");
                    } else {
                        field.setAccessible(true);
                        if (field.get(obj) != null) {
                            tmp.append(CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE,
                                    field.getName()) +
                                    "=#{" +
                                    field.getName() +
                                    "} AND ");
                        }
                        field.setAccessible(false);
                    }
                }
            }
            if (tmp.lastIndexOf("AND") != -1) {
                tmp.delete(tmp.lastIndexOf("AND"), tmp.lastIndexOf("AND") + 3);
            }
            if (tmp.length() == 0) {
                throw new RuntimeException("oldObj为空");
            }

            sql.append(" WHERE " + tmp.toString());
        }
        return sql.toString();
    }

    public String deleteById (Class<T> clazz) throws NullPointerException {
        StringBuilder sql = new StringBuilder();
        String value = clazz.getAnnotation(TableName.class).value();
        sql.append("DELETE FROM ");
        sql.append(value);
        sql.append(" WHERE id = #{id}");
        return sql.toString();
    }

    public String deleteByIdLogically (Class<T> clazz) throws NullPointerException {
        StringBuilder sql = new StringBuilder();
        String value = clazz.getAnnotation(TableName.class).value();
        sql.append("UPDATE ");
        sql.append(value);
        sql.append(" SET valid = 0 WHERE id = #{id}");
        return sql.toString();
    }

}
