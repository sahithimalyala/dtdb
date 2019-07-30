package com.dtdb.master.parser;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Component;

@Component
public class ConsolidationQueryGenerator implements IQueryGenerator {

	private @Autowired JdbcTemplate jdbcTemplate;

	@Override
	public String generateQuery(String distributedQuery, Map<String, String> keys) {
		System.out.println("Distribution Query: "+	distributedQuery);
		
		String consolidationQuery =  jdbcTemplate.query(distributedQuery, new DataTypeExtractor(keys));
		//System.out.println(consolidationQuery);
		
		return consolidationQuery;
	}
}

class DataTypeExtractor implements ResultSetExtractor<String> {
	
	private Map<String, String> keys = null;
	public DataTypeExtractor(Map<String, String> keys){
		this.keys = keys;
	}

	@Override
	public String extractData(ResultSet resultSet) throws SQLException,
			DataAccessException {
		ResultSetMetaData metadata = resultSet.getMetaData();
		String groupBy = "GROUP BY ";
		
		for (int i = 1; i <= metadata.getColumnCount(); i++) {
			String columnName = metadata.getColumnName(i);
			if(columnName.equalsIgnoreCase("ckey")) continue;
			Integer type = metadata.getColumnType(i);
			//System.out.println(columnName+" - "+type);
			if(!keys.get(columnName).contains("(")) groupBy = groupBy+" "+columnName+",";
			switch(type){
			case 7: keys.put(columnName, keys.get(columnName).replaceFirst("("+columnName+")", "(cast(rowdata->>'"+columnName+"' as real))")+" AS "+columnName); break;
			case 2 : keys.put(columnName, keys.get(columnName).replaceFirst("("+columnName+")", "(cast(rowdata->>'"+columnName+"' as numeric))")+" AS "+columnName); break;
			case 4 : keys.put(columnName, keys.get(columnName).replaceFirst("("+columnName+")", "(cast(rowdata->>'"+columnName+"' as integer))")+" AS "+columnName); break;
			case 8:	keys.put(columnName, keys.get(columnName).replaceFirst("("+columnName+")", "(cast(rowdata->>'"+columnName+"' as double precision))")+" AS "+columnName); break;
			case -5: keys.put(columnName, keys.get(columnName).replaceFirst("("+columnName+")", "(cast(rowdata->>'"+columnName+"' as bigint))")+" AS "+columnName); break;
			case 5:	keys.put(columnName, keys.get(columnName).replaceFirst("("+columnName+")", "(cast(rowdata->>'"+columnName+"' as smallint))")+" AS "+columnName); break;
			case 12: keys.put(columnName, keys.get(columnName).replaceFirst("("+columnName+")", "(cast(rowdata->>'"+columnName+"' as character varying))")+" AS "+columnName);break;
			case 93: keys.put(columnName, keys.get(columnName).replaceFirst("("+columnName+")", "(cast(rowdata->>'"+columnName+"' as timestamp))")+" AS "+columnName);break;
			default: System.out.println("Unmapped Type: "+type);
			}
			if(keys.get(columnName).startsWith("COUNT")){
				keys.put(columnName, keys.get(columnName).replaceFirst("COUNT","SUM"));
			}
		}
		String selectPart = keys.values().toString().replace("[", "").replace("]", "");
		selectPart = selectPart+" VITREOS_REPLACER_STRING ";
		if(groupBy.length()>9){
			selectPart = selectPart+groupBy.substring(0, groupBy.length()-1);
		}
		return selectPart;
	}

}
