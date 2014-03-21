package com.dianping.darkbat.entity;

import java.util.List;

import com.dianping.darkbat.entity.ColumnEntity;
import com.dianping.darkbat.entity.TableInfo;

public class HiveTableEntity {
	List<ColumnEntity> columnEntity;
	List<ColumnEntity> partitionKey;
	TableInfo tableInfo;
	String tableName;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public List<ColumnEntity> getColumnEntity() {
		return columnEntity;
	}

	public void setColumnEntity(List<ColumnEntity> columnEntity) {
		this.columnEntity = columnEntity;
	}

	public List<ColumnEntity> getPartitionKey() {
		return partitionKey;
	}

	public void setPartitionKey(List<ColumnEntity> partitionKey) {
		this.partitionKey = partitionKey;
	}

	public TableInfo getTableInfo() {
		return tableInfo;
	}

	public void setTableInfo(TableInfo tableInfo) {
		this.tableInfo = tableInfo;
	}

}
