package com.dtdb.master.util;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;

public class ColumnInterpreter implements ResultSetExtractor<String> {

	@Override
	public String extractData(ResultSet resultSet) throws SQLException, DataAccessException {
		ResultSetMetaData metadata = resultSet.getMetaData();
		List<String> columns = new ArrayList<String>();
		String columnNames = "";
		for (int i = 1; i <= metadata.getColumnCount(); i++) {
			String columnName = metadata.getColumnName(i);
			columns.add(columnName);
		}
		return columns.toString().substring(1,columns.toString().length()-1);
	}

}
