package com.dtdb.master.parser;

import java.util.Map;

public interface IParser {
	public Map<QueryType, String> parseQuery(String query);
}
