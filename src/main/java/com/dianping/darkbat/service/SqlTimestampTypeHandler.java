package com.dianping.darkbat.service;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.apache.ibatis.type.BaseTypeHandler;
import org.apache.ibatis.type.JdbcType;
import org.joda.time.DateTime;

public class SqlTimestampTypeHandler extends BaseTypeHandler<DateTime> {

	@Override
	public DateTime getNullableResult(ResultSet rs, String columnName)
			throws SQLException {
		Timestamp t = rs.getTimestamp(columnName);
		return t == null ? null : new DateTime(t);
	}

	@Override
	public DateTime getNullableResult(CallableStatement cs, int columnIndex)
			throws SQLException {
		Timestamp t = cs.getTimestamp(columnIndex);
		return t == null ? null : new DateTime(t);
	}

	@Override
	public void setNonNullParameter(PreparedStatement ps, int i,
			DateTime parameter, JdbcType jdbcType) throws SQLException {
		Timestamp t = new Timestamp(parameter.getMillis());
		ps.setTimestamp(i, t);
	}

	@Override
	public DateTime getNullableResult(ResultSet rs, int columnIndex)
			throws SQLException {
		Timestamp t = rs.getTimestamp(columnIndex);
		return t == null ? null : new DateTime(t);
	}

}
