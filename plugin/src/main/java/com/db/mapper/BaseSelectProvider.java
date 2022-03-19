package com.db.mapper;

/**
 * Created by zzq on 2022/3/19.
 */
public class BaseSelectProvider<T> extends BaseProvider {
    public String selectById(Class<T> clazz) throws NullPointerException {
        StringBuilder sql = new StringBuilder();
        String value = clazz.getAnnotation(TableName.class).value();
        sql.append("SELECT * from ");
        sql.append(value);
        sql.append(" WHERE id = #{id}");
        return sql.toString();
    }

    public String selectOne(final T t) throws NullPointerException, IllegalAccessException {
        Class<?> clazz = t.getClass();
        StringBuilder sql = new StringBuilder();
        String value = clazz.getAnnotation(TableName.class).value();
        sql.append("SELECT * FROM ");
        sql.append(value);
        sql.append(buildConditionStatement(t, "", false));

        return sql.toString();
    }

    public String selectPage(final T t) throws NullPointerException, IllegalAccessException {
        Class<?> clazz = t.getClass();
        StringBuilder sql = new StringBuilder();
        String value = clazz.getAnnotation(TableName.class).value();
        sql.append("SELECT * FROM ");
        sql.append(value);
        sql.append(buildConditionStatement(t, "", true));

        return sql.toString();
    }
}
