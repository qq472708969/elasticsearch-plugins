package com.db.mapper;

import lombok.Data;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Date;
import java.util.List;

/**
 * Created by zzq on 2022/3/19.
 */
@Data
public class BaseDO {

    /**
     * 自增主键
     */
    private Long id;

    /**
     * 逻辑删除标识:1 未删除 ;0 删除
     */
    private Integer valid;

    /**
     * 创建人
     */
    private String createBy;

    /**
     * 记录创建时间
     */
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date createAt;

    /**
     * 创建人
     */
    private String changeBy;

    /**
     * 记录创建时间
     */
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date changeAt;

    /**
     * 附加的条件，主要用于查询和更新时的条件上
     */
    @Invisible
    @JsonIgnore
    public List<Condition> extra;
}
