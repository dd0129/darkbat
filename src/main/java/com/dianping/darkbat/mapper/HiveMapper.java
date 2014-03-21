package com.dianping.darkbat.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Param;

import com.dianping.darkbat.entity.ColumnEntity;
import com.dianping.darkbat.entity.TableEntity;

public interface HiveMapper {

	List<String> getAllDatabase();
	
    List<TableEntity> searchTable(@Param("table") TableEntity table, @Param("limit") Integer limit, @Param("offset") Integer offset);

    int searchTableCount(@Param("table") TableEntity table);

    List<ColumnEntity> getAllColumn(@Param("table") TableEntity table);

}
