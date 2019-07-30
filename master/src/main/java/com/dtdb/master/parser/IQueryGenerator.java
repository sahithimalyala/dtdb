package com.dtdb.master.parser;

import java.util.Map;

public interface IQueryGenerator {
	public String generateQuery(String consolidationQuery, Map<String, String> keys);
}
