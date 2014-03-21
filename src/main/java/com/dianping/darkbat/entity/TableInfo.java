package com.dianping.darkbat.entity;

public class TableInfo {
	String tableComment;
	String tableType;
	String tableSerde;
	String tableInputformat;
	String tableOutputformat;
	String tableLocation;
	int    tableBucketsNum;
	String tableBucketscol;
	String tableSortcol;
	String tableFielddlim;
	String tableColelctiondelim;
	String mapKeydelim;
	String serializationFormat;
	String tableLinedelim;
	
	public String getTableLinedelim() {
		return tableLinedelim;
	}

	public void setTableLinedelim(String tableLinedelim) {
		this.tableLinedelim = tableLinedelim;
	}


	public String getTableComment() {
		return tableComment;
	}

	public void setTableComment(String tableComment) {
		this.tableComment = tableComment;
	}

	public String getTableType() {
		return tableType;
	}

	public void setTableType(String tableType) {
		this.tableType = tableType;
	}

	public String getTableSerde() {
		return tableSerde;
	}

	public void setTableSerde(String tableSerde) {
		this.tableSerde = tableSerde;
	}

	public String getTableInputformat() {
		return tableInputformat;
	}

	public void setTableInputformat(String tableInputformat) {
		this.tableInputformat = tableInputformat;
	}

	public String getTableOutputformat() {
		return tableOutputformat;
	}

	public void setTableOutputformat(String tableOutputformat) {
		this.tableOutputformat = tableOutputformat;
	}

	public String getTableLocation() {
		return tableLocation;
	}

	public void setTableLocation(String tableLocation) {
		this.tableLocation = tableLocation;
	}

	public int getTableBucketsNum() {
		return tableBucketsNum;
	}

	public void setTableBucketsNum(int tableBucketsNum) {
		this.tableBucketsNum = tableBucketsNum;
	}

	public String getTableBucketscol() {
		return tableBucketscol;
	}

	public void setTableBucketscol(String tableBucketscol) {
		this.tableBucketscol = tableBucketscol;
	}

	public String getTableSortcol() {
		return tableSortcol;
	}

	public void setTableSortcol(String tableSortcol) {
		this.tableSortcol = tableSortcol;
	}

	public String getTableFielddlim() {
		return tableFielddlim;
	}

	public void setTableFielddlim(String tableFielddlim) {
		this.tableFielddlim = tableFielddlim;
	}

	public String getTableColelctiondelim() {
		return tableColelctiondelim;
	}

	public void setTableColelctiondelim(String tableColelctiondelim) {
		this.tableColelctiondelim = tableColelctiondelim;
	}

	public String getMapKeydelim() {
		return mapKeydelim;
	}

	public void setMapKeydelim(String mapKeydelim) {
		this.mapKeydelim = mapKeydelim;
	}

	public String getSerializationFormat() {
		return serializationFormat;
	}

	public void setSerializationFormat(String serializationFormat) {
		this.serializationFormat = serializationFormat;
	}

}
