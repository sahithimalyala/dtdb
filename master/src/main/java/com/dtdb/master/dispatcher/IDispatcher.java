package com.dtdb.master.dispatcher;

import java.util.Map;

import com.dtdb.master.parser.QueryType;

public interface IDispatcher {
	String executeQuery(Map<QueryType, String> data);
}
