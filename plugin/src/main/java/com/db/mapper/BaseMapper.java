package com.db.mapper;

import org.apache.ibatis.annotations.*;

import java.util.List;

/**
 * Created by zzq on 2022/3/19.
 */
public interface BaseMapper<T> {

    @InsertProvider(type = BaseInsertProvider.class, method = "insertOne")
    @Options(useGeneratedKeys = true)
    int insertOne(T t);

    @InsertProvider(type = BaseInsertProvider.class, method = "insertSelective")
    @Options(useGeneratedKeys = true)
    int insertSelective(T t);

    @InsertProvider(type = BaseInsertProvider.class, method = "insertListSelective")
    @Options(useGeneratedKeys = true)
    int insertListSelective(@Param("list") List<T> var1);

    /**
     * 根据条件更新
     *
     * @param newObj
     * @param oldObj
     * @return
     */
    @UpdateProvider(type = BaseUpdateProvider.class, method = "updateSelective")
    int updateSelective(T newObj, T oldObj);

    /**
     * 根据id更新
     *
     * @param t
     * @param id
     * @return
     */
    @UpdateProvider(type = BaseUpdateProvider.class, method = "updateSelectiveById")
    int updateSelectiveById(T t, Long id);

    /**
     * 根据id更新（没传的字段，会修改为空）
     *
     * @param t
     * @param id
     * @return
     */
    @UpdateProvider(type = BaseUpdateProvider.class, method = "updateById")
    int updateById(T t, Long id);


    @SelectProvider(type = BaseSelectProvider.class, method = "selectOne")
    T selectOne(T conditon);

    @SelectProvider(type = BaseSelectProvider.class, method = "selectOne")
    List<T> selectList(T conditon);

    @SelectProvider(type = BaseSelectProvider.class, method = "selectById")
    T selectById(Class<T> clazz, @Param("id") Long id);

    @UpdateProvider(type = BaseDeleteProvider.class, method = "deleteById")
    int deleteById(Class<T> clazz, @Param("id") Long id);

    @UpdateProvider(type = BaseDeleteProvider.class, method = "deleteByIdLogically")
    int deleteByIdLogically(Class<T> clazz, @Param("id") Long id);
}
