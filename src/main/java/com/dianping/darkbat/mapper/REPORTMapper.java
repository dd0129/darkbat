package com.dianping.darkbat.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Param;

public interface REPORTMapper {
    List<String> explainSql(@Param("sqlStr")String sqlStr);
}
