package com.dtdb.master.rest;

import java.util.Map;

import lombok.Data;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.dtdb.master.dispatcher.IDispatcher;
import com.dtdb.master.dispatcher.QueryCache;
import com.dtdb.master.dispatcher.QueryCacheRepository;
import com.dtdb.master.parser.IParser;
import com.dtdb.master.parser.QueryType;

@RestController
public class QueryExecutor {
	
	private @Autowired IParser parser;
	private @Autowired IDispatcher workDispatcher;
	private @Autowired QueryCacheRepository cache;
	
	@RequestMapping(value = "/testPlan", method = RequestMethod.POST, produces = org.springframework.http.MediaType.APPLICATION_JSON_VALUE)
	public @ResponseBody String testPlan(
			@RequestBody String query)
			throws Exception {
		System.out.println(query);
		Map<QueryType, String> queries = parser.parseQuery(query);
		System.out.println(queries);
		String finalQuery = null;
		return finalQuery;
	}
	
	@RequestMapping(value = "/execute", method = RequestMethod.POST, produces = org.springframework.http.MediaType.APPLICATION_JSON_VALUE)
	public @ResponseBody String executeQuery(
			@RequestBody String query)
			throws Exception {
		Long timestamp = System.currentTimeMillis();
		System.out.println(query);
		if(cache.findByActualQuery(query)!=null){
			return cache.findByActualQuery(query).getResult();
		}
		Map<QueryType, String> queries = parser.parseQuery(query.toUpperCase().replace(";", ""));
		System.out.println("After Parsing");
		String finalQuery = workDispatcher.executeQuery(queries);
		/*System.out.println();
		System.out.println();
		System.out.println();
		System.out.println(finalQuery);
		System.out.println();
		System.out.println();
		System.out.println();*/
		QueryCache cacheObj = new QueryCache();
		cacheObj.setActualQuery(query);
		cacheObj.setResult(finalQuery);
		this.cache.save(cacheObj);
		System.out.println(" Total Execution Time -  "+((System.currentTimeMillis()-timestamp)/1000)+" Seconds");
		return finalQuery;
	}
}

@Data
class QueryBody{
	String query;
}
