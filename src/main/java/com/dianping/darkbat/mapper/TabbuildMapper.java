package com.dianping.darkbat.mapper;

import java.util.List;
import org.apache.ibatis.annotations.Param;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Repository;

import com.dianping.darkbat.entity.ColumnEntity;
import com.dianping.darkbat.entity.TableInfo;

@Scope("singleton")
@Repository
public interface TabbuildMapper {
	List<ColumnEntity> getColumnList(@Param("databaseName") String databaseName, @Param("tableName") String tableName);

	List<ColumnEntity> getPartitionKey(@Param("databaseName") String databaseName, @Param("tableName") String tableName);

	TableInfo getTableInfo(@Param("databaseName") String databaseName, @Param("tableName") String tableName);

	int ifTableExist(@Param("databaseName") String databaseName, @Param("tableName") String tableName);
}
